# Module and Interface Deepening Acceptance Record

## Scope

Phase 2 deepens the existing Security, Store, Client, Transport, Controller, and
Proxy modules. It adds no workspace crate, does not change RocketMQ wire fields
or persisted checkpoint layouts, and keeps compatibility facades for one
release cycle.

The dependency direction after this phase is:

```text
Broker/Admin ingress -> protocol DTO
Broker/Admin ingress -> store-api checkpoint contract -> security-api maintenance contract
rocketmq-auth policy loader ---------------------------> security-api maintenance contract

Broker send/pull use case -> narrow store port -> store capability
legacy MessageStore ------> crate-private compatibility adapter -> store capability

proxy ingress -> proxy contracts
proxy contracts -X-> proxy ingress
```

The strict architecture target ledger is empty. The remaining target ledger
entries are approved compatibility and test-only edges, not production target
debt.

## Security maintenance boundary

`rocketmq-security-api::maintenance` owns policy validation, fail-closed
authorization, capabilities, budgets, roles, and opaque authorization grants.
A grant's fields are private, and the module exposes no unchecked constructor.
Only a validated policy decision can produce a grant, so Store callers cannot
forge a deadline, fencing token, capability, or resource budget.

`rocketmq-auth` now owns only the external policy-loading adapter: path
confinement, file I/O, JSON decoding, version matching, and SHA-256 pinning.
The former `rocketmq_auth::*` contract exports are deprecated in favor of
`rocketmq_security_api::*` and are scheduled for removal in 2.0.0. Loader types
such as `MaintenancePolicyReference`, `LoadedMaintenancePolicy`, and
`MaintenancePolicyError` remain owned by `rocketmq-auth`.

Controller security is injected through `ControllerSecurity` and
`MaintenancePrincipalAuthenticator`. `ControllerManager::new_with_security`
is the composition boundary. A Controller configured for authentication,
authorization, or maintenance without an injected adapter fails closed; the
ordinary security-disabled configuration remains supported by
`ControllerManager::new`.

## Checkpoint wire/domain separation

`rocketmq-store-api::checkpoint` owns the storage domain:

- `CheckpointRequest`
- `CheckpointManifest`
- `CheckpointStorageIdentity`
- `CheckpointOffsets`
- `CheckpointRestoreVerification`
- `CheckpointArtifact`
- `CheckpointBackend`

`ReleaseCheckpointStore` accepts that domain request plus a security-api grant.
`rocketmq-store-api`, `rocketmq-store-local`, and `rocketmq-store-rocksdb` no
longer depend directly on `rocketmq-auth` or `rocketmq-protocol` for checkpoint
behavior.

The Broker maintenance processor is the wire adapter. It maps the existing
protocol DTO to the domain request and maps the manifest back to the unchanged
camelCase wire shape. Round-trip and serialized-field tests cover both Local
and RocksDB backend values. Protocol response codes, headers, Serde names, and
checkpoint storage identity semantics are unchanged.

## Store capability migration

The existing store-api capabilities are the canonical target interfaces.
Store capability implementations now bind directly to the canonical Local,
RocksDB, and Tiered owners. The unused `LegacyMessageStoreAdapter<S>` and its
compatibility module were removed at the approved major-version boundary:

| Use case | Canonical capability | Current primary seam | Remaining compatibility |
|---|---|---|---|
| Single and batch send | `MessageAppender<M>` plus `StoreHealth` | Broker `SendMessageStorePort` | Other Broker lifecycle/context fields still hold the concrete store |
| Pull and lookup | `MessageReader` plus `OffsetIndex` | Broker `PullMessageStorePort` | Non-request-path Broker features still use legacy methods |
| Release maintenance | `ReleaseCheckpointStore` | Broker maintenance request adapter | None in store-api domain |
| Startup and shutdown | `StoreLifecycle` | Local/RocksDB conformance surface | Broker process lifecycle still owns the concrete implementation |
| Replication and HA | `ReplicationControl` or a narrower use-case port | Existing native HA modules | Legacy `MessageStore` remains until HA consumers migrate |

`MessageStoreInner` was removed rather than re-exported or renamed. The
`trait_variant` transformation now generates the public, `Send` future
contract in place as `MessageStore`; capability consumers do not depend on an
inner compatibility trait.

`capability_conformance_tests` exercises the shared lifecycle and capability
surface for Local by default and RocksDB with `rocksdb_store`. Tiered Store
continues to expose the capabilities it actually implements.

Tiered provider selection deliberately remains `ProviderKind` for the built-in
Posix and Memory implementations, with the existing generic injection seam for
custom providers. A registry will be reconsidered only when a third approved
production provider requires runtime selection; hypothetical S3/OSS support is
not enough to trigger it.

## Producer capabilities and legacy mapping

New code can depend on six independent interfaces:

- `ProducerLifecycle`
- `MessageProducer`
- `TransactionalProducer`
- `RequestReplyProducer`
- `ProducerQuery`
- `TopicAdmin`

The request structs collapse legacy overloads into explicit destination, mode,
timeout, selector, callback, query, and topic-create contracts. The following
table classifies every legacy `MQProducer` trait method:

| Capability | Legacy methods |
|---|---|
| `ProducerLifecycle` | `start`, `shutdown` |
| `ProducerQuery` | `fetch_publish_message_queues`, `search_offset`, `max_offset`, `min_offset`, `earliest_msg_store_time`, `query_message`, `view_message` |
| `TopicAdmin` | `create_topic`, `create_topic_with_flag` |
| `MessageProducer` | `send`, `send_with_timeout`, `send_with_callback`, `send_with_callback_timeout`, `send_oneway`, `send_to_queue`, `send_to_queue_with_timeout`, `send_to_queue_with_callback`, `send_to_queue_with_callback_timeout`, `send_oneway_to_queue`, `send_with_selector`, `send_with_selector_timeout`, `send_with_selector_callback`, `send_with_selector_callback_timeout`, `send_oneway_with_selector`, `send_batch`, `send_batch_with_timeout`, `send_batch_to_queue`, `send_batch_to_queue_with_timeout`, `send_batch_with_callback`, `send_batch_with_callback_timeout`, `send_batch_to_queue_with_callback`, `send_batch_to_queue_with_callback_timeout`, `recall_message` |
| `TransactionalProducer` | `send_message_in_transaction` |
| `RequestReplyProducer` | `request`, `request_with_callback`, `request_with_selector`, `request_with_selector_callback`, `request_to_queue`, `request_to_queue_with_callback` |
| Legacy compatibility plumbing | `as_any`, `as_any_mut` |

The source inventory contains 44 trait methods. The previously reported count
of 45 declarations also counted the private
`unsupported_mq_admin_operation` helper immediately above the trait; it is not
a public producer operation. That helper remains part of the legacy facade's
unchanged unsupported-operation behavior. There are no unclassified trait
methods.

`DefaultMQProducer` implements lifecycle, normal send/recall, request/reply,
query, and topic admin. It does not implement `TransactionalProducer`.
`TransactionMQProducer` implements those base capabilities and
`TransactionalProducer`. Therefore a normal producer is no longer forced to
pretend to support transactions.

The old and new send forms are:

```rust,ignore
// Compatibility facade
let result = MQProducer::send_with_timeout(&mut producer, message, 3_000).await?;

// Focused capability
let result = MessageProducer::send_message(
    &mut producer,
    SendRequest {
        message,
        destination: SendDestination::Automatic,
        mode: SendMode::AwaitResult,
        timeout_millis: Some(3_000),
    },
)
.await?;
```

The old `MQProducer` facade is preserved through the compatibility cycle; its
error and unsupported-operation behavior is not silently changed. The
capability implementation delegates to the same operations.

`MQAdminExtInner`, its empty implementation alias, modules, and crate-root
re-exports were removed at the 2.0.0 boundary. Admin registration stores only
group presence, and behavior tests cover duplicate registration, lookup,
unregistration, and absence.

## Immutable configuration snapshots

Producer configuration has one clone-shared `ArcSwap` current value. Internal
callers use `client_config_snapshot()` and `producer_config_snapshot()`.
Borrowed config getters, `compatibility_borrow`, the retained-generation list,
its unsafe reference reconstruction, and occupancy diagnostics were removed.
Facade accessors that remain useful now return owned collections, strings, or
`Arc` values derived from the current immutable snapshot.
Tests cover 10,000 snapshot updates, preservation of an explicitly held old
snapshot, and current-generation visibility without retained history.

## Pin projection, shared state, and trait policy

`CoreProcessorFuture` uses `pin-project-lite` for all Send, Pull, and Admin
variants. No handwritten unsafe projection remains. Tests cover all variants
and verify cancellation drops the selected future exactly once.

Store HA connection lifecycle resources now have one owner and two legal
states:

| Current state | Event | Next state | Owned resources |
|---|---|---|---|
| `Idle` | successful `start` | `Running` | shutdown sender and task group move together |
| `Idle` | failed `start` | `Idle` | partial local resources are shut down before return |
| `Running` | `shutdown` | `Idle` | sender and task group are atomically taken, cancelled, and awaited |
| `Idle` | repeated `shutdown` | `Idle` | no-op |

The touched public domain traits use native `async fn` with narrow,
reason-bearing `#[allow(async_fn_in_trait)]`. Object-safe Controller and Broker
ports use explicit boxed futures. Touched Proxy contracts and their
Cluster/Local implementations also use explicit boxed futures, removing 30
`async_trait` inventory sites. The reviewed trait inventory decreases from 694
to 661 entries. This phase adds no `#[async_trait]`,
`trait_variant`, type-erasure method, empty marker trait, or positional boolean
API.

## Proxy contracts and ingress decision

Stable Route, Metadata, Assignment, Message, Consumer, and Transaction
interfaces live in `rocketmq_proxy_core::contracts`. Tonic/gRPC services,
middleware, servers, and remoting dispatch live under
`rocketmq_proxy_core::ingress`. The contracts module does not import ingress.
The old `service`, `grpc`, and `remoting` paths are compatibility re-exports.

The decision is to keep these modules inside `rocketmq-proxy-core`:

| Evidence | Before | After |
|---|---:|---:|
| Workspace crates added | 0 | 0 |
| Direct `rocketmq-proxy-core` manifest dependencies | 17 | 17 |
| Dependency/feature fan-out | Existing crate boundary | Unchanged |
| Measured Windows `cargo check -p rocketmq-proxy-core` after source reorganization | Not separately captured | 35.3 seconds with affected dependencies rebuilding |

An independent contract crate would still need model/protocol and async/runtime
types, both production adapters already consume the existing core crate, and
there is no independent release owner. No measured fan-out or compile-time
reduction exists. The extraction conditions are therefore not met; the module
boundary provides locality without adding a shallow crate.

## Hotspot disposition

| Hotspot | Change reasons and invariants | Decision and target module | Public surface | Owner |
|---|---|---|---|---|
| Client Lite Pull implementation | Subscription assignment, pull scheduling, and seek state change together; queue ownership and offset ordering must stay coherent | Keep the existing consumer/use-case modules; deepen only when a capability hides a full change reason | Existing consumer facade | Client Consumer |
| NameServer bootstrap | Configuration, runtime ownership, remoting registration, and shutdown are composition-root work | Keep as the NameServer composition root | Binary/service startup surface | NameServer Runtime |
| Proxy gRPC ingress | Transport admission, middleware, protocol mapping, and service dispatch change for ingress reasons | Deepen under `ingress::grpc` and move the historical `service/mod.rs` root to `service.rs` | Old `grpc` path re-exported | Proxy Ingress |
| Store CommitLog | Append/recovery/index invariants form one algorithmic core; splitting by line count would spread ordering rules | Keep the algorithmic core and its existing owned helper modules | Store implementation detail behind capabilities | Store CommitLog |
| MQClientInstance | Process lifecycle and client registries remain after Phase 1 moved route refresh into `RouteUpdateCoordinator`; admin marker storage was a separate removable concern | Keep the coordinator/composition role; remove marker-based admin dispatch | Existing client runtime facade | Client Runtime |
| Client Admin wire operations | Request construction, wire mapping, response decoding, and admin behavior change together | Keep and deepen the existing admin implementation submodules | Existing admin facade | Client Admin |
| Proxy Cluster adapter | Broker discovery and remote invocation implement stable proxy contracts; they should not leak into ingress | Keep as the Cluster adapter consuming `contracts` | Adapter implementation surface | Proxy Cluster |

Every decision is based on change reason and invariant ownership, not file
length. No new `mod.rs` is introduced; the touched historical Proxy
`grpc/service/mod.rs` is migrated to `ingress/grpc/service.rs`.

## Composition and lifecycle ownership

| Composition root | Interface | Adapter | Lifecycle owner |
|---|---|---|---|
| Controller manager construction | `MaintenancePrincipalAuthenticator`, `MaintenanceAuthorizer` | Deployment-supplied auth adapter and validated policy | `ControllerManager` |
| Broker runtime | Send/pull store ports and store-api capabilities | `EscapeBridge`, Local/RocksDB/Tiered store implementations | Broker service context and concrete store |
| Checkpoint ingress | `ReleaseCheckpointStore` | Broker wire mapper plus Local/RocksDB checkpoint implementation | Broker maintenance request and store lifecycle |
| Client producer builders | Six producer capabilities | `DefaultMQProducer`, `TransactionMQProducer` | Producer instance/client runtime |
| Proxy startup | `contracts` interfaces | Local/Cluster adapters and gRPC/remoting ingress | Proxy runtime/service context |

## Compatibility inventory

| Surface | Why retained | Removal condition | Owner | Target |
|---|---|---|---|---|
| `rocketmq_auth::*` maintenance contract exports | Downstream source compatibility | Consumers import security-api directly | Security | 2.0.0 |
| Legacy `MessageStore` | Real downstream lifecycle and HA value; inner trait and adapter are gone | Next approved major-release review after remaining facade consumers migrate | Store | Next major review |
| Legacy `MQProducer` | Existing overload and error compatibility | Public migration window completed | Client Producer | 2.0.0 decision gate |
| Proxy `service`, `grpc`, and `remoting` paths | Downstream module-path compatibility | Consumers import `contracts`/`ingress` paths | Proxy Core | 2.0.0 |
| `ControllerManager::new` | Security-disabled source compatibility | Keep as convenience constructor while it remains fail closed | Controller | Retained |

Removed at the approved boundary: `MessageStoreInner`, the Store compatibility
adapter, `MQAdminExtInner`, borrowed producer configuration getters/history,
public `RuntimeHandle`, and detached task spawning.

## Acceptance evidence

- The architecture target ledger reports zero active target-debt entries.
- The CI `Architecture Strict Target` job is enforced and no longer
  `continue-on-error`.
- The Rust hygiene inventory decreases from 952 to 947 entries: four manual
  Pin projections and one historical `mod.rs` entry were removed, while the
  pre-existing Proxy admission panic facade was reviewed as a path-only move.
- Security contract, checkpoint mapping, capability conformance, producer
  matrix, immutable snapshot, Pin cancellation, HA transition, and admin
  registration behavior have focused tests.
- Phase 3 owns long-running soak, failure-matrix, and production evidence work;
  those are intentionally not Phase 2 merge prerequisites.

## Proxy Cluster execution ownership

The [Proxy Cluster keyed execution ADR](proxy-cluster-keyed-execution-adr.md)
is accepted. The fixed sixteen-lane hash worker has been removed. Exact
structural ordering keys now select dynamically owned lanes, so the same key is
FIFO without serializing unrelated keys through hash collisions.

| Command family | Class | Ordering owner |
|---|---|---|
| Readiness | Control | Singleton readiness key with reserved queue and I/O capacity |
| Route and topic metadata | Data | Topic or topic/group key |
| Send, recall, end transaction | Data | Effective producer-group key and exclusive mutable producer |
| Pull, receive, ack, invisible time, consumer offset | Data | Consumer group/topic key |
| Queue offset query | Data | Exact queue target |
| Lock and unlock | Data | Consumer group/client key |

Count, retained bytes, maximum queue age, request deadline, global inflight
work, and control reserve are validated configuration. Caller drop, timeout,
cancellation, and shutdown all cancel the owned command future and release its
permits. Runtime diagnostics expose retained age and other aggregates only; no
topic, group, request ID, payload, or credential becomes a metric label.

The `cluster_executor` Criterion target measures same-key FIFO admission and
high-cardinality exact-key admission/retirement as an algorithm regression
signal. It is explicitly separate from the target-hardware mixed workload
profile and does not claim production throughput.
