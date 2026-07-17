# Support for custom quota plugins in the `Kafka` custom resource

This proposal adds a new `custom` type to the `spec.kafka.quotas` API of the `Kafka` custom resource.
It lets users run their own `ClientQuotaCallback` implementation with first-class operator support, following the precedent of custom authorization (`spec.kafka.authorization` with `type: custom`) and custom tiered storage (`spec.kafka.tieredStorage` with `type: custom`).

## Current situation

[Proposal 068](https://github.com/strimzi/proposals/blob/main/068-quotas-management.md) introduced the `spec.kafka.quotas` section with two plugin types.
`type: kafka` uses Kafka's built-in quota mechanism and lets the operator manage the default user quotas through the Kafka Admin API.
`type: strimzi` configures the [Strimzi quotas plugin](https://github.com/strimzi/kafka-quotas-plugin): the operator sets `client.quota.callback.class`, renders the plugin options from the structured fields, and automatically generates the TLS Admin client configuration the plugin needs to poll the cluster.

Kafka itself supports any quota plugin that implements the `org.apache.kafka.server.quota.ClientQuotaCallback` interface.
Proposal 068 deliberately kept `client.quota.callback.class` out of the forbidden options, so a custom plugin can still be wired through `spec.kafka.config`.
In practice, this path has significant gaps:

- The core options of the Strimzi plugin — `client.quota.callback.static.produce`, `.fetch`, the `.storage.per.volume.limit.min.available.` limits, `.excluded.principal.name.list`, and the whole `.kafka.admin.` Admin client section — are forbidden in `spec.kafka.config`, because the operator owns them when `type: strimzi` is used.
  The prohibition applies even when `type: strimzi` is not used.
  As a result, a fork or extension of the Strimzi plugin, or a newer plugin release with options that the structured `type: strimzi` API does not expose yet, cannot be fully configured through `spec.kafka.config`.
- The `type: strimzi` rendering generates a per-broker TLS Admin client configuration (`client.quota.callback.static.kafka.admin.*`) that references each pod's own certificate and key through the Kubernetes secret configuration provider.
  These values differ per pod, while `spec.kafka.config` is shared by all nodes, so a user cannot reproduce this configuration by hand.
  A custom plugin that needs an authenticated Admin client — for example, one that polls `describeLogDirs` for storage information — has no secure way to bootstrap it.
- The plugin wiring is mixed into the free-form broker configuration, with no dedicated place in the API, no validation, and no visibility for tooling.

## Motivation

The quotas plugin area receives limited maintainer time, so plugin features arrive slowly and structured API support for them arrives even later.
A `custom` quotas type decouples these: users can run the plugin they need — their own implementation, a fork, or a newer release of the Strimzi plugin — without waiting for the operator API to model its options, and without the community having to adopt or maintain plugin behavior that only some deployments need.

Strimzi already applies this pattern to the two other pluggable broker mechanisms.
A custom authorizer is configured with `type: custom` and `authorizerClass`, and a custom remote storage manager with `type: custom` and a class name plus an opaque configuration map.
Quotas are the remaining pluggable mechanism without this escape hatch.

## Proposal

A new `QuotasPluginCustom` type is added to the `spec.kafka.quotas` API:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    quotas:
      type: custom
      quotaCallbackClass: com.example.kafka.quotas.MyQuotaCallback
      kafkaAdminClientConfigPrefix: com.example.quotas.kafka.admin
      config:
        com.example.quotas.storage.check.interval: "30000"
        com.example.quotas.limit.ratio: "0.05"
    # ...
```

The type has the following fields:

- `quotaCallbackClass` (required): fully qualified name of a class implementing `org.apache.kafka.server.quota.ClientQuotaCallback`, which must be available on the broker classpath.
  The operator sets it as the value of `client.quota.callback.class`.
  The naming follows `authorizerClass` in the custom authorization API.
- `config` (optional): a map of plugin options written verbatim into the broker configuration.
  Because the options live in the quotas API rather than in `spec.kafka.config`, the forbidden-options list does not apply to them, so plugins using the `client.quota.callback.static.` option namespace can be configured.
  The operator rejects keys that collide with options it manages itself (for example `listeners` or `ssl.` options) and the `client.quota.callback.class` key, which is owned by `quotaCallbackClass`.
- `kafkaAdminClientConfigPrefix` (optional): when set, the operator renders the same per-broker TLS Admin client configuration that `type: strimzi` receives today — bootstrap address of the replication listener, security protocol, per-pod PEM keystore, and cluster CA truststore — under `<prefix>.` option keys.
  This gives a custom plugin the one piece of configuration a user cannot express manually, because the keystore values are per-pod.
  A plugin only needs to read its Admin client settings from a configurable or matching prefix to use it.

### Operator behavior

- The operator writes `client.quota.callback.class` and the `config` entries into the broker configuration, in a dedicated quotas section, for brokers and controllers alike (matching how quotas configuration is rendered today).
- The existing conflict handling from proposal 068 applies unchanged: if `spec.kafka.config` also sets `client.quota.callback.class`, the operator raises a warning condition and uses the quotas API.
- The existing default-user-quota handling applies unchanged: as for `type: strimzi`, the operator clears the default user quotas managed for `type: kafka`.
- No change to the forbidden options in `spec.kafka.config` is needed, because plugin options are supplied through `quotas.config` instead.

### Providing the plugin

As with custom authorizers and custom remote storage managers, the plugin JAR must be added to the Kafka container image, typically by building a custom image from the Strimzi base image.
The documentation for the new type will link the existing custom-image guidance.
Runtime provisioning of plugin artifacts is out of scope of this proposal.

### Interaction with `KafkaUser` quotas

Quotas set in `KafkaUser.spec.quotas` are stored through the Kafka Admin API and interpreted by whatever quota callback is active.
Whether a custom plugin honors them depends on the plugin, exactly as it does for `type: strimzi` today.
The documentation will state this.

## Affected/not affected projects

- Affected: `strimzi/strimzi-kafka-operator` — the `api` module (new `QuotasPluginCustom` class), the broker configuration rendering and validation in the cluster operator, documentation, and a system test.
- Not affected: `strimzi/kafka-quotas-plugin` (no change is required for it, though it can be run through the new type), the Topic Operator, the User Operator, and all other Strimzi projects.

## Compatibility

The change is purely additive.
Clusters that do not use `type: custom` are not affected in any way.
Configuring a custom plugin through `spec.kafka.config` keeps working exactly as described in proposal 068.

## Rejected alternatives

- Keep wiring custom plugins through `spec.kafka.config` only: this remains possible, but it cannot configure plugins that use the forbidden `client.quota.callback.static.` option namespace, and it cannot express the per-pod TLS Admin client configuration at all.
- Relax the forbidden-options list instead of adding an API type: this would allow free-form options to conflict with the operator-rendered configuration when `type: strimzi` is used, would still not solve the Admin client problem, and would keep plugin configuration invisible to the API.
- Add structured API support for specific third-party plugins: modeling every plugin's options in the CRD does not scale and puts a maintenance burden on Strimzi for code it does not own; the opaque `config` map covers these cases.
- Runtime download of plugin JARs (init containers, image volumes): a supply-chain and security question of its own, and inconsistent with how custom authorizers and remote storage managers are provided today; it can be proposed separately if there is demand.
