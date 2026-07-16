# Per-topic storage budgets with principal-level enforcement

This proposal extends the [kafka-quotas-plugin](https://github.com/strimzi/kafka-quotas-plugin) so that operators can assign storage budgets to individual topics.
When a topic exceeds its budget, only the principals writing to that topic are throttled, while every other tenant continues at full rate.
The existing cluster-wide protection remains in place as a backstop.

## Current situation

Since [proposal 047](https://github.com/strimzi/proposals/blob/main/047-cluster-wide-volume-usage-quota-management.md), the kafka-quotas-plugin provides cluster-wide out-of-disk protection.
`VolumeSource` polls `Admin#describeLogDirs` (KIP-827) every `storage.check.interval` to collect the total and available bytes of each volume.
If any volume breaches `storage.per.volume.limit.min.available.{bytes,ratio}`, the throttle factor drops to 0.0 and `StaticQuotaCallback` cuts the produce rate of all non-excluded principals to roughly 1 byte/s (Kafka cannot represent a zero quota).

This design is intentional: proposal 047 scoped the plugin as a last line of defense against running out of disk, and it fills that role well.
As a consequence, the enforcement decision is binary and global: the plugin does not track which topic is consuming the disk and has no way to act on a subset of clients.
The per-principal control it offers is a static produce/fetch rate that is identical for all principals and independent of storage state.

## Motivation

Consider a large multi-tenant cluster with hundreds of brokers, multi-terabyte volumes, short retention, and high per-broker ingest, protected only by the cluster-wide volume limit.

The cluster-wide protection reliably prevents the disk from filling up, which is exactly its job, but the blast radius of enforcement is the whole cluster.
A single tenant ramping up a new topic can cross a volume threshold quickly, at which point produce freezes for every tenant on every broker.
A single-tenant incident becomes a full-cluster producer outage.

Several existing mechanisms come close, but each was designed for a different problem:

- Per-user quotas (`KafkaUser.spec.quotas` or the built-in callback) cap request rates, but they cannot be combined with this plugin, because Kafka allows only one `client.quota.callback.class` per broker.
  They are also static rather than driven by disk pressure, and a rate cap does not bound storage, since the footprint grows with time and broker count.
- `retention.bytes` bounds the size of each partition rather than the topic, cannot reclaim the active segment, and gives the producer no backpressure (see Rejected alternatives).
- Cruise Control disk goals balance replicas across volumes, which helps with skew, but the total bytes stay the same.
- Tiered storage (KIP-405) lowers steady-state local usage, but it cannot stop ingest that outruns offload and does not fit short-retention clusters.

This proposal builds on the foundation these mechanisms provide, adding the missing piece: detect the topic that is over its storage allowance, throttle the writers of that topic, and keep everyone else at full rate.

## Proposal

1. Operators declare an ordered list of budget entries, each carrying a topic pattern, an absolute budget in bytes that applies to every matching topic, and the principals that write to those topics.
2. `VolumeSource` additionally aggregates per-topic sizes from the `replicaInfos` map that is already present in the `describeLogDirs` response it fetches today, so no new admin calls are needed.
   A topic's usage is a single cluster-wide number: the sum of its replica sizes across all brokers and all log dirs, excluding future replicas; replicas are counted because budgets bound physical disk consumption.
3. When a topic exceeds its budget, the plugin drops the produce quota of the principals named in the matching entry to a configured floor (1 byte/s by default), leaving all other principals untouched.
4. A topic must fall below 90% of its budget before its principals are restored, so enforcement does not flap around the threshold; the hysteresis margin is fixed in the first version to keep the configuration surface small.

Because usage is a cluster-wide aggregate compared against one number, detection is independent of how partitions are distributed across brokers and of size skew between partitions.
A budget is a capacity-accounting bound ("this topic may occupy at most X bytes of the cluster"), not a volume-placement protection: a topic within its budget can still fill a skewed volume, and guarding against that remains the job of the unchanged cluster-wide `minAvailable*` backstop.
Only topics matching a budget entry are tracked, which bounds memory usage and metric cardinality.

### Why enforcement is principal-level

`ClientQuotaCallback` only sees the quota type, principal, and client ID, and produce throttling is applied per connection before the broker routes partitions.
A true per-topic rate quota is therefore not achievable within the quota callback contract and would require a new interception point in Apache Kafka, which means a KIP.
The callback API is also moving away from topic awareness: `updateClusterMetadata(Cluster)`, its only topic-aware hook, was never supported in KRaft mode and is deprecated in Kafka 4.4 for removal in 5.0 ([KIP-1200](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=373887083)).
This proposal therefore detects per topic using the log dir data, but enforces per principal using the existing quota path, and does not depend on `updateClusterMetadata`.

### Principal-to-topic mapping

The plugin needs to know which principals write to a topic.
In the first version there is exactly one source for this mapping: the operator names the principals inline in each budget entry, so the pattern, the budget, and the writers live in one place and cannot drift apart.
This is deterministic and auditable, and nothing is inferred from ACLs, observed traffic, or Strimzi custom resources; where the operator sources the mapping (for example a provisioning system) is outside the plugin's scope.

Internally the mapping source sits behind a small interface, so alternative resolvers can be added later without changing the enforcement logic.
Deriving the mapping from ACLs or from observed traffic is rejected for the first version (see Rejected alternatives).
Principals on `excluded.principal.name.list` are never budget-throttled.

### Configuration

New properties are added under the existing `client.quota.callback.static.` prefix:

| Property | Meaning |
|---|---|
| `storage.per.topic.budget.<n>.pattern` | Topic pattern of entry `n`; entries are evaluated in order and the first match wins |
| `storage.per.topic.budget.<n>.bytes` | Budget in bytes, applied to each topic matching the pattern |
| `storage.per.topic.budget.<n>.principals` | Comma-separated principals writing to these topics; may be empty for metrics-only tracking |
| `storage.per.topic.throttle.floor` | Produce rate for restricted principals (default 1) |

Keeping each entry self-contained — one pattern, one budget, one principal list — means there is a single ordered list to reason about, and the flat properties mirror the `Kafka` CR structure one-to-one.

The `Kafka` CR (`spec.kafka.quotas`, `type: strimzi`) gains an optional `topicBudgets` list that the Cluster Operator renders into these properties:

```yaml
quotas:
  type: strimzi
  minAvailableRatioPerVolume: 0.05          # unchanged cluster-wide backstop
  topicBudgets:
    - topicPattern: "my-events-.*"
      bytes: 21990232555520                  # 20 TiB per matching topic
      principals: [User:my-producer]
```

### Enforcement semantics

- Sensor tags must be stable, because when `quotaResetRequired` fires Kafka re-evaluates `quotaLimit()` for the existing metric tags of active sensors but does not recompute `quotaMetricTags()` for connected clients.
  `quotaMetricTags()` therefore adds a per-user tag for every principal named in any budget entry, unconditionally and regardless of the current budget state, so that each budgeted principal is metered by its own sensor whose tags never change at runtime.
- On each poll, the plugin computes the restricted set: the union of the principals of all over-budget entries, minus excluded principals.
  For a budgeted principal's PRODUCE sensor, `quotaLimit()` returns the floor while the principal is restricted and the normal limit otherwise, and changes to the restricted set trigger the existing `quotaResetRequired` path; only the returned limit changes, never the tags, so the update takes effect within one quota-manager refresh even for long-lived connections.
- Each broker computes the restricted set independently from the same cluster-wide data, just like the global throttle factor today, so no coordination is added and brokers can diverge for at most one poll interval.
- The effective produce limit for a principal is `min(static quota × global throttle factor, floor if restricted)`.
- If the per-topic data goes stale past `throttle.factor.validity.duration`, budget enforcement disengages and the cluster-wide `minAvailable*` limit remains as the fail-safe.
- Internal topics such as `__consumer_offsets` and the transaction state topic are never budgeted.

### Metrics

The plugin exposes new gauges following the existing `io.strimzi.kafka.quotas` conventions: `TopicUsedBytes`, `TopicBudgetBytes`, and `TopicOverBudget` tagged by topic, plus `RestrictedPrincipals`, `TrackedTopics`, and `TopicUsageStale`.
Comparing used bytes against the budget enables tenant burn-down dashboards and alerting before the throttle engages, for example at 80% of budget.

### Known limitations

- Enforcement granularity is the principal, so a restricted principal's writes to healthy topics are throttled too.
  This is inherent to the callback API and can be mitigated by using one principal per tenant or pipeline.
- Budgets bound topics, not volumes: a topic within its budget can still fill a skewed volume, and detection lags by up to one poll interval.
  The cluster-wide limit remains the hard guarantee against running out of disk; budgets reduce how often it fires and how many tenants it hits.
- Release depends on retention reclaiming data: once writers are floored, usage only falls when segments are deleted, so a topic with time-based retention and slow churn, or a compacted topic, can stay over budget indefinitely.
  The remediation is an operator action — raise the budget, tighten `retention.ms`/`retention.bytes`, or delete records — and the metrics above make the condition visible before and during enforcement.
- Cross-broker partition reassignment temporarily counts both the old and the new replica, since only future (intra-broker) replicas are excluded, so a topic's measured usage can approach twice its steady state during a Cruise Control rebalance.
  Budgets therefore need headroom above steady-state usage; excluding partitions with an active reassignment (via `listPartitionReassignments`) is a possible future refinement that this version omits to avoid an extra admin call.
- Budget entries with no principals only produce metrics, which doubles as a dry-run mode.

## Affected/not affected projects

- Affected: `strimzi/kafka-quotas-plugin` and `strimzi/strimzi-kafka-operator` (extending the `QuotasPluginStrimzi` CRD type, config rendering, and documentation).
- Not affected: bridge, canary, drain-cleaner, and the behavior of the Topic Operator, User Operator, and Cruise Control, whose principals stay excluded.

## Compatibility

The change is additive: clusters that set no `storage.per.topic.*` property keep today's behavior, and no existing property or metric changes.
There is one behavioral consequence for clusters that opt in: principals named in a budget entry are metered by their own quota sensor instead of the shared one, so for those principals the static produce quota applies individually rather than being shared with all other clients.
This only affects principals the operator explicitly lists, and is required so that sensor tags stay stable while enforcement toggles (see Enforcement semantics).
There are no new Kafka API requirements, since `replicaInfos` predates KIP-827.

## Rejected alternatives

- True per-topic produce rate quotas: the quota callback contract does not support this, since it only sees the principal and client ID, enforces per connection, and has no topic-aware hook after KIP-1200.
  This would require an Apache Kafka KIP, and such a KIP could later supersede the enforcement half of this proposal while keeping the budget model.
- Using `retention.bytes` as enforcement: the decisive difference is semantics rather than granularity.
  `retention.bytes` meets its bound by deleting committed data while the producer keeps writing at full rate, so an overage is resolved with silent data loss for lagging consumers and the producing tenant gets no signal that anything is wrong; a budget instead applies backpressure to the writer and leaves the data intact.
  It is also mechanically loose as a topic-level bound: the active segment cannot be reclaimed, so with large `segment.bytes` and many partitions the real footprint overshoots the nominal limit, and because it applies per partition it must be set to `budget / partitionCount` assuming even distribution, which partition skew and partition-count changes both break.
  It works well for tenants that prefer data expiry over throttling, so it is complementary rather than an alternative.
- Deriving the principal mapping from ACLs (`describeAcls`): this only works when the authorizer answers `describeAcls`, which excludes deployments using third-party or external authorizers whose data lives outside the broker, and even where it works it over-approximates the writer set, since holding `WRITE` permission does not mean a principal actively produces.
  An ACL-based resolver may be added later behind an opt-in flag, via the mapping-source interface, for clusters using the built-in `StandardAuthorizer`.
- Budgets as a ratio of total cluster storage: the denominator moves when a broker is down, a volume is offline, or the cluster is resized, so every ratio budget would shrink during an unrelated outage and throttle innocent tenants — the exact blast-radius problem this proposal exists to fix.
  Absolute bytes keep enforcement predictable; ratio budgets can be revisited once there is a stable notion of configured capacity to divide by.
- Tiered storage: a good fit for reducing steady-state local usage, but it cannot stop ingest that outruns offload, and short-retention clusters gain little from it.
- An external watchdog using metrics and `AlterClientQuotas`: with the plugin installed, `StaticQuotaCallback` does not apply dynamic quota updates, so the watchdog cannot act through the native path.
  Its scrape-and-reconcile reaction time of minutes is also slow against the fill rates of high-throughput clusters, and in-broker enforcement on data that is already being polled avoids operating an additional system.
- Mapping principals from observed traffic: there is no supported produce-path hook that exposes the principal-topic pair, and scraping per-client JMX metrics across a large cluster would be hard to keep reliable.
  The mapping abstraction leaves room for this mode if Kafka ever exposes write attribution.
