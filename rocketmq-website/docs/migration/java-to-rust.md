---
title: "Migrate from Java RocketMQ"
---

# Migrate from Java RocketMQ

Choose the migration boundary before changing a deployment. Replacing an application client, introducing a Rust Broker cluster, and moving stored data are different operations. This guide provides a procedure for an existing RocketMQ workload; it does not claim a universal in-place conversion of Java processes or data directories.

The current capability manifest uses Java RocketMQ 5.5.0 as a comparison baseline and lists explicit exclusions. Read [protocol compatibility](../reference/protocol-compatibility.md) alongside your actual versions and workload. A baseline declaration is not evidence that every Java SDK version, Controller configuration or advanced message mode is interchangeable.

## Select the migration path

| Path | Initially retain | Change first | Evidence needed before increasing traffic |
| --- | --- | --- | --- |
| Java application to Rust application | Existing service endpoints, topics, groups and business contracts | SDK integration, runtime ownership and result handling | Exact client/server operation, serialization, ACL/TLS, retry and shutdown behavior |
| Java services to a separate Rust cluster | Application payload schema and business identity | New NameServer/Broker deployment and operational tooling | Topology, configuration, feature semantics, durability, recovery and capacity for the target profile |
| Historical data or progress migration | A recoverable source environment and recorded business positions | Explicit replay/bridge or a separately established compatible offline procedure | Message coverage, duplicates, ordering, offsets and recovery for the selected method |

Separate the NameServer addresses and storage roots of source and target clusters during evaluation. Registering a new implementation under an existing Broker identity can change routes used by live clients. A second process must not share a live store directory with the old process.

## 1. Inventory the application contract

For each application, record the following concrete inputs. They form a migration worksheet, not a document approval workflow.

| Area | Record |
| --- | --- |
| Endpoint | Java SDK version, remoting versus Proxy gRPC, NameServer list, advertised Broker addresses, TLS/ACL and namespace behavior |
| Messages | Topic and message type, payload encoding/schema, keys/tags/properties, size distribution and compression |
| Producers | Sync/async/oneway use, timeout budget, retry policy, queue selection, transaction checks and delay/recall use |
| Consumers | Group, clustering/broadcasting, Push/Pull/POP/Lite mode, selector, queue assignment, retry/DLQ and initial-position rules |
| Business completion | Idempotency key, durable business transaction boundary, offset/ACK ordering, allowed duplicates and ordering domain |
| Operations | Peak volume, retention, recovery objective, monitoring signals, administrative scripts and maintenance ownership |

Include failure behavior in the contract. A timeout after sending may leave an unknown outcome; replay must not depend on the assumption that no message was stored. Message IDs and queue offsets are useful transport identities but are not a replacement for an application-defined idempotency key.

## 2. Map concepts to the actual Rust surface

| Java-oriented concept | Rust migration decision |
| --- | --- |
| JVM process options, heap and GC tuning | Select the Rust build profile and native dependencies; configure runtime/thread/admission/storage settings from their owners. JVM flags have no direct Rust meaning. |
| Flat Broker properties | Use canonical TOML or the explicit Broker properties converter and inspect its report. Preserve intended values, not the flat layout. |
| Producer lifecycle | Supply an application-owned `Arc<ClientRuntime>` to the builder, start the facade, inspect send results, then shut down the facade and shared runtime deliberately. |
| Push listener | Choose concurrent or orderly processing and preserve success/reconsume semantics, assignment and the business transaction boundary. |
| Classic Pull | Keep explicit queue/offset ownership using the runtime-backed compatibility builder, or deliberately adopt LitePull assignment/polling. |
| Consumer offset | Distinguish next-read position, local offset-store state, remote submission and Broker persistence. |
| SQL selector | Enable the Broker's property-filter support in addition to configuring the client selector. |
| DLedger / Java Controller | Do not reuse Java consensus membership, snapshots or internal protocols. Rust Controller uses its own OpenRaft and HA authority contracts. |
| mqadmin script | Translate each invocation to the Rust CLI domain and leaf; verify flags, credentials, output and partial-failure behavior. |

Use the [Rust API migration guide](./rust-api.md), [client configuration](../configuration/client-config.md), and the relevant [producer](../producer/overview.md)/[consumer](../consumer/overview.md) article for executable examples. A class with a familiar name does not imply identical constructor or callback signatures.

## 3. Build an isolated target

Start with [local source setup](../getting-started/local-source.md) and the [first-message procedure](../getting-started/quick-start.md), then select [multi-node deployment](../deployment/multi-node.md) or [HA deployment](../deployment/high-availability.md) for the intended topology. Use distinct Broker identities and data paths.

Translate configuration section by section: Broker identity and listener, NameServer discovery, Broker metadata root, message-store root, retention/flush policy, topic/group settings, security and observability. In canonical TOML, listener settings are not arbitrary nested Java properties; [Broker configuration](../configuration/broker-config.md) defines accepted sections and derived fields.

If testing the existing Java properties file, the explicit converter path is:

```bash
cargo run -p rocketmq-broker -- -c /path/to/broker.properties --config-format properties -p
```

Replace the path with a separate working copy. The conversion writes a conversion report and configuration printing exits before binding service ports. Inspect rejected/converted settings and the printed effective values; successful parsing does not demonstrate a healthy deployment. This illustrative command was not executed against a Java configuration during documentation work.

Do not bring over DLedger settings expecting fallback to a different HA mode: the Broker rejects them. Do not use the Java Controller state directory as a Rust Controller storage root. For gRPC applications, deploy/configure [Proxy](../deployment/proxy.md); the Broker remoting port is not a gRPC endpoint.

## 4. Exercise representative message paths

| Scenario | Observe |
| --- | --- |
| Ordinary send and consume | Payload and properties, send status, assigned queue, consumed content and business completion |
| Timeout and retry | Overall request budget, unknown outcomes, duplicate suppression and retry load |
| Rebalance/restart | Revoked queues stop processing, new assignment resumes from the intended progress, bounded shutdown |
| Filtering and ordering | Actual selectors and queue/order domain under reassignment, not only a single-producer happy path |
| Transaction/delay/POP if used | Transaction-check recovery; timer and recall races; invisible timeout, ACK and redelivery behavior |
| Service failure if required | The selected flush/replication policy, failover authority, recovery time and replay range |

Run only scenarios relevant to the application, but do not generalize a basic send/consume result to untested advanced modes. A zero-lag group can still have failed external business effects. An HTTP/transport success can still contain an operation-specific failure status.

The website's first-message example was exercised against a local Rust NameServer/Broker with five sends and five received messages. This does not constitute a Java/Rust interoperability trial or a data-migration trial; perform those against your exact source/target pair.

## 5. Move traffic and progress deliberately

1. Choose a bounded application/tenant/partition cohort and its rollback route. Prepare topics, groups, permissions and observability on the target.
2. For a consumer implementation change on the same cluster, record completed business progress, stop the old owner, then start the new owner with the intended group and mode. Existing group offsets can take precedence over an initial-position setting.
3. For a separate target cluster, establish how historical messages arrive: business-source replay, an application bridge, or another explicitly supported method. Keep the source readable for the required retention window.
4. Treat offsets as cluster/queue-specific positions. Do not copy a numeric offset into another queue and assume it identifies the same message. Map progress through the chosen replay method and business identity.
5. Increase traffic after observing target results, backlog and error behavior. If two producers or a bridge write concurrently, specify how duplicate and ordering effects are handled; dual writing is not an atomic cross-cluster commit.
6. Retire the source only after the application's replay/recovery window and operational obligations are satisfied.

Using a new consumer group for comparison creates a separate progress history and can replay old data; route business effects to an isolated destination or implement idempotency. Running old and new implementations in one group may divide queues between them rather than provide an identical shadow stream.

## 6. Keep rollback operationally possible

A client rollback can restore the previous executable and endpoint configuration, but it does not undo already completed business effects or offset advances. A cluster rollback needs a decision about writes accepted only by the new cluster. Reconcile/replay them before discarding the target or changing the route back.

Preserve source configuration, recoverable data and the previous application artifact. If a procedure changes persisted layouts, follow [upgrade and rollback](../operations/upgrade-rollback.md) and [backup/recovery](../operations/backup-recovery.md). Replacing a binary or changing `storeType` alone is not a storage conversion.

After switching, keep the normal operation records: exact versions, selected features/modes, configuration differences, tested scenarios, observed limitations and the owner of unresolved migration work. This makes a later incident diagnosable without claiming compatibility beyond the evidence.

Sources: [capability scope](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json), [Broker entry point](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/bin/broker_bootstrap_server.rs), [client public API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs), [HA contracts](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/src/ha_contract.rs).
