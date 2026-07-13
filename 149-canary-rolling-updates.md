# Metric-gated canary rolling updates for Kafka brokers

Add an optional verification phase to the KafkaRoller: after restarting the first N broker nodes it observes a set of metrics for a soak period, and only rolls the remaining nodes when the metrics confirm the restarted nodes behave as before the change.

## Current situation

The KafkaRoller restarts Kafka nodes one at a time in a safe order, as described in [proposal #60](./060-kafka-roller-kraft.md).
Before restarting a broker it applies pre-restart safety checks: it does not restart a broker performing log recovery, a broker whose restart would take any topic below `min.insync.replicas`, or a KRaft controller whose restart would take the number of caught-up controllers below the quorum majority.

After a restart, the KafkaRoller waits for the Pod to become `Ready` and immediately proceeds to the next node.
Readiness is a binary availability signal; there is no verification that the restarted node performs as well as before the change.
A change that degrades performance without breaking availability — a configuration change that increases produce latency, an image with a JVM regression, a resource change causing GC churn — passes readiness on every node and only becomes visible once the whole cluster is rolled.

The Strimzi Canary project provided produce/consume probing during rolling updates, but it only observed and never gated the roller, and it was archived in [proposal #86](./086-archive-canary.md).

## Motivation

Rolling a change through a large production cluster is the riskiest routine operation Strimzi performs.
Users running latency-sensitive clusters want the pattern common in deployment tooling elsewhere: apply the change to one or two nodes, verify health metrics against pre-change behavior, then roll the remainder.

Today this can only be approximated with workarounds: a dedicated one-broker `KafkaNodePool` (does not cover cluster-scoped changes such as upgrades), pausing reconciliation and restarting manually (disables the operator and its safety checks), or an external controller watching metrics between restarts (reinvented by every affected user).

The KafkaRoller already owns the restart sequencing and safety checks, so it is the natural place for a post-restart verification gate, and adding it upstream benefits every user.

## Proposal

A new optional `canary` section is added under `.spec.kafka.rollingUpdate` in the `Kafka` custom resource.
When configured, and when the new `CanaryRollingUpdates` feature gate is enabled, the KafkaRoller inserts a verification phase after restarting each of the first `nodes` broker nodes of a rolling update.

```yaml
spec:
  kafka:
    rollingUpdate:
      canary:
        # Number of restarted nodes to verify before rolling the rest without soaking.
        # Setting this to the cluster size (or higher) verifies every node.
        nodes: 2
        # Observation window after the restarted node becomes Ready.
        soakTimeSeconds: 300
        # Optional; a default check set is used when omitted (see below).
        checks:
          - name: produce-p99
            metric: kafka_network_requestmetrics_totaltimems
            labels:
              request: Produce
              quantile: "0.99"
            maxDeviationFromPeersPercent: 50
          - name: under-min-isr
            metric: kafka_cluster_partition_underminisr
            maxValue: 0
```

A check selects a metric by name plus an optional map of label matchers — plain fields matched literally against the scraped samples, not a query language.

### Verification flow

1. The KafkaRoller restarts a node following the existing order, triggers, and pre-restart checks, which are unchanged.
2. Once the Pod is `Ready`, if the node is one of the first `nodes` restarted brokers of this rollout, the roller triggers preferred leader election for the node's partitions — so it serves its normal traffic share during verification rather than waiting for the periodic leader balancer — and starts the soak window.
3. During the soak window the operator evaluates the configured checks against the restarted node.
4. If all checks pass, the roller proceeds to the next node; after the first `nodes` brokers pass, the remaining nodes are rolled with the existing behavior, without soaking.
5. If verification fails, the roller stops the rolling update, sets a `RollingUpdatePaused` warning condition on the `Kafka` resource, and emits a Kubernetes event and a restart event with the failed check and observed values.

A failed verification is latched: subsequent reconciliations keep the condition and do not re-run the soak, so a transient deviation cannot silently un-pause the rollout.
The user resumes a paused rollout either by reverting the change (the operator rolls the canary nodes back to the previous revision), or by annotating the `Kafka` resource with `strimzi.io/continue-rolling-update="true"` to accept the deviation, after which the operator removes the annotation, records the acceptance for this revision in the status, and rolls the remaining nodes without further soaking.
The acceptance applies only to that revision; a subsequent change starts a fresh canary phase.
The operator never reverts `.spec` itself; the specification is owned by the user or their GitOps tooling.

### Metric source

Checks are evaluated by the operator scraping the Prometheus endpoint of the restarted broker (and, for peer-relative checks, of the peer brokers) directly.
Unlike the operator's existing TLS connections to the KafkaAgent and the Kafka Admin API, the Metrics Reporter endpoint is plain HTTP on its own port, so the operator-managed NetworkPolicies are extended accordingly.
Each scrape is restricted to the metric names used by the configured checks via the name-filtering query parameter of the Prometheus exposition endpoint, so the full metrics payload of a large broker is never transferred.

The checks require the [Strimzi Metrics Reporter](./064-prometheus-metrics-reporter.md), because its metric names are canonical and predictable; with the JMX Prometheus Exporter the names depend on user-supplied mapping rules, so it is left as a possible follow-up.
No Prometheus server or other external monitoring dependency is required.

### Thresholds and baselines

Each check asserts one of:

- `maxValue` / `minValue`: an absolute bound on the metric — the right tool for state metrics such as under-replicated or under-min-ISR partition counts.
- `maxDeviationFromPeersPercent`: a relative bound against the median of the peer set — the primary mechanism for performance metrics. Comparing against live peers rather than pre-rollout recordings is robust against diurnal traffic changes and needs no persisted baseline.

The peer set is the brokers not yet restarted in this rollout.
When fewer than two remain (for example when `nodes` equals the cluster size), the peer set is instead the brokers already restarted and verified.
On a single-broker cluster there are no peers; peer-relative checks are skipped and an event records the reduced coverage.

A peer-relative evaluation is counted only when the restarted broker's request rate is at least a tenth of the peer median; lower-traffic evaluations are discarded rather than passed.
Together with the preferred leader election at soak start, this prevents a vacuous pass on a broker still running as a follower for all its partitions.
If the broker produces no countable evaluations during the last half of the window, the gate fails with a distinct "no comparable traffic" reason — not taking traffic back is itself a regression signal.
On an idle cluster the rate requirement is trivially met and the comparison degrades gracefully to comparing idle brokers.

A restart always causes transient leadership movement and elevated metrics, so the gate requires the checks to pass continuously for the last half of the soak window: "recovered to normal within the window" rather than "never deviated", which would fail on every restart.
The fraction is deliberately not configurable in the first version.

### Default checks

When `canary` is configured without an explicit `checks` list, the operator applies a default set:

- `kafka_server_replicamanager_underreplicatedpartitions`: `maxValue: 0`.
- `kafka_cluster_partition_underminisr`: `maxValue: 0`.
- Produce and Fetch request p99 total time: `maxDeviationFromPeersPercent: 50`.
- Request handler average idle ratio: `maxDeviationFromPeersPercent: 30`.
- JVM GC time per second: `maxDeviationFromPeersPercent: 50`.
- JVM heap usage after GC: `maxDeviationFromPeersPercent: 30`.

CPU and memory pressure surface through the idle ratio, GC, and latency metrics, so no dependency on the Kubernetes metrics API is introduced.
The default thresholds are deliberately loose — meant to catch regressions, not noise.
The user-supplied `checks` list is in scope for the first version precisely because the defaults are loose: it is the escape hatch for unusual workloads.

### Scope of the gate

The verification phase applies only to broker and combined nodes, and only to restarts triggered by a change: a Pod revision change, a manual rolling update annotation, or a certificate renewal.
It does not apply to restarts of unready or stuck Pods, where delaying recovery would make an outage worse, nor to controller-only nodes, whose health is already gated by the quorum check and whose behavior client-facing metrics cannot capture.

### Reconciliation model

A soak window of minutes multiplied by the number of canary nodes does not fit the operational timeout of a single node roll, so the rollout state survives reconciliations:

- The operator records the rollout progress in the `Kafka` resource status: the revision being rolled out, the nodes restarted so far, the soak start time of the node under verification, and the pass/fail state.
- A reconciliation that reaches a node whose soak window has not elapsed finishes with the rollout marked as in progress; the next reconciliation resumes where it left off.
- Which nodes still need a restart is already recomputed from Pod revision annotations on every reconciliation, so a restarted operator resumes correctly without additional state.

The recorded progress is keyed to the revision being rolled out.
When a reconciliation observes a different target revision — a forward fix or a revert — it discards the canary progress, clears any `RollingUpdatePaused` condition, and starts a new rollout with a fresh canary phase, making both recovery paths uniform.

While a rollout is soaking or paused the cluster remains fully available, so the `Ready` condition of the `Kafka` resource stays `True`; the state is surfaced through status fields, the warning condition, and events.
This matches how the operator already treats other long-running operations, such as Cruise Control rebalances, as state machines advanced by successive reconciliations.

## Affected/not affected projects

The Cluster Operator is the only affected project: the `api` module for the new CRD section, the KafkaRoller for the verification phase, and the status handling for rollout progress and the paused condition.
The Strimzi Metrics Reporter is a runtime prerequisite but needs no changes.
The KafkaAgent, the Topic and User Operators, the Bridge, and Kafka Connect are not affected.

## Compatibility

The new API section is optional, and the behavior is additionally protected by the `CanaryRollingUpdates` feature gate following the usual alpha, beta, GA progression.
With the gate disabled or the section absent, the KafkaRoller behavior is exactly as today.

## Rejected alternatives

### Reviving the Strimzi Canary project as the gate

The Canary was an external observer, not a gate, and was archived for lack of maintenance capacity.
Gating the roller on an external component would require a coordination protocol that is strictly more complex than evaluating checks in the roller itself.

### Requiring an external Prometheus for all checks

Delegating all evaluation to PromQL against a user Prometheus makes the feature unusable where the operator has no network path or credentials to the monitoring stack, and makes Strimzi's behavior depend on scrape intervals and recording rules it does not control.
Direct scraping keeps the built-in checks self-contained.

### A `prometheusQuery` check type in the first version

An earlier draft included a second check type running user-supplied PromQL against an external endpoint, covering signals brokers cannot see such as end-to-end consumer lag.
It roughly doubles the API surface (endpoint configuration, credential Secrets, query validation, unreachable-endpoint failure modes) for a fully optional capability.
It is deferred rather than rejected: a `type` discriminator can be added to the check schema later without changing this design.

### A generic hook executed between restarts

An arbitrary user-supplied hook (an HTTP callback or executable, potentially via the [Gatekeeper plugin system](./144-Strimzi-Gatekeeper-plugin-system.md)) is maximally flexible but not declarative, hard to make portable and secure, and pushes deciding what "healthy" means back onto every user.
A hook-based check type can be added later if the plugin system matures.

### Automatic rollback on check failure

Automatic reversion would require the operator to write to `.spec`, fighting the user's GitOps tooling and hiding failures instead of surfacing them.
Pausing with a condition and an event keeps the human in the loop.

### Canary via a dedicated KafkaNodePool

Documenting the "one-broker node pool" pattern only covers pool-scoped changes, leaves the metric watching to the user, and doubles the resources to manage — a workaround rather than a solution.
