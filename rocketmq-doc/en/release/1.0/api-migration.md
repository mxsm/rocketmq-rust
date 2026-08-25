# RocketMQ Rust 1.0 API migration

RocketMQ Rust 1.0 freezes the core-release Rust API at the first complete
`1.0.0-rc.1` candidate. The freeze is structural: every public item is recorded
by package, supported feature profile, canonical item path, kind, visibility,
signature, and feature condition. It does not use a commit, file, or artifact
digest as a compatibility decision.

This guide covers the core release only. Dashboard, MCP, SRE, OpenMessaging,
BrokerContainer, and DLedger CommitLog are not part of the 1.0 compatibility
surface.

## Compatibility policy

The 0.9-to-1.0 transition has four explicit classifications:

| Classification | Meaning |
|---|---|
| `compatible-addition` | A typed capability was added without removing a supported wire, storage, or source contract. |
| `approved-break` | A misleading or unsafe pre-1.0 entry point was removed with repository-owner approval and a documented replacement. |
| `renamed-wrapper` | The operation remains available under a name that accurately describes its behavior. |
| `removed-placeholder` | A public placeholder or production test probe was replaced by a working capability or moved behind `test-support`. |

After the freeze, additions remain compatible. Removing an item, changing its
signature or trait bounds, changing a supported feature profile, or changing a
documented default is rejected unless the baseline contains a matching,
reviewed post-freeze approval.

## Direct source migrations

### Request-header derive retirement readiness

`rocketmq_macros::RequestHeaderCodec` (V1) and
`rocketmq_macros::RequestHeaderCodecV2` remain deprecated public 1.x derives
and compatibility adapters. New request headers should use the recommended
`rocketmq_macros::RequestHeaderCodecV3` derive with its explicit
`#[header(...)]` wire metadata.

The checked-in registry and migration guard provide completion evidence:
`152 registered, 152 V3, 0 V2, 0 pending, 0 production legacy derive uses`.
This does not change compatibility behavior. Existing wire behavior,
compatibility adapters, helper attributes, fixtures, and the 13 intentional
deprecated-use allows remain in place.

Any future removal requires the complete release cycle, an explicit 2.0
breaking window, and an individual exact reviewed post-freeze approval for
each frozen item: `rocketmq_macros::RequestHeaderCodec` and
`rocketmq_macros::RequestHeaderCodecV2`. This change creates no approval and
does not announce or approve 2.0.

### Auth security-contract ownership and policy models

Runtime-neutral security contracts are owned by `rocketmq-security-api`. Import
`Principal`, `Resource`, and maintenance-policy contracts directly from that
crate. `rocketmq-auth` owns the ACL policy model; new source should use its
canonical `PolicyDecision`, `PolicyResource`, and `AuthorizationRequest` names.

The frozen 1.x `rocketmq-auth::{Decision, Resource, RequestContext}` names
remain source-compatible aliases with identical types. `SecurityPrincipal` and
`SecurityResource`, together with the twelve maintenance re-exports, remain
available as deprecated 1.x compatibility paths while applications migrate to
the owning crate. This documentation does not delete any alias, create a
deletion approval, or announce or approve a 2.0 release. Any future removal is
only intended for a 2.0 source-compatibility boundary and remains subject to
compatibility, migration, and release gates.

This source-ownership preparation does not change `AuthorizationHandlerChain`
first-success behavior, whitelist or profile behavior, wire or Serde
representations, defaults, error behavior, or fail-closed behavior.

### Fallible client runtime construction

`ClientRuntime::new` was removed because runtime startup can fail. Propagate the
typed error from `try_new` instead of relying on a panic-compatible constructor.

```rust,ignore
// Before 1.0
let runtime = ClientRuntime::new(config);

// 1.0: preserve the existing service context, runtime config, and telemetry.
let runtime = ClientRuntime::try_new(service_context, config, telemetry_handle)?;
```

### Tokio client cached-session reconciliation

`TransportClient::is_address_reachable` is retained as a deprecated
compatibility facade, but it has never performed a DNS, socket, or network
reachability probe. It only inspected the direct cached session and removed an
unhealthy entry.

Use `reconcile_cached_connection` when application logic needs the typed cache
result:

```rust,ignore
use rocketmq_transport::api::v1::CachedConnectionState;

match client.reconcile_cached_connection(&address) {
    CachedConnectionState::Healthy => {}
    CachedConnectionState::UnhealthyRetired => reconnect_after_cleanup(),
    CachedConnectionState::Absent => connect_if_needed(),
}
```

Request processors are fixed when a client is built. Replace the deprecated
`register_processor` compatibility call with
`TransportClient::builder(...).build()?` or
`RemotingClient::builder(...).build()?` so configuration failures remain typed.

Applications own the runtime and pass an `Arc<ClientRuntime>` to producers and
consumers. Client APIs do not create hidden Tokio runtimes.

### `RocketMQRuntime` legacy runtime boundary

`RocketMQRuntime` remains a public, deprecated compatibility API throughout
the 1.x line. This guide does not remove it, change any of its public methods,
or announce a 2.0 release. Its removal is only intended for a future 2.0
source-compatibility boundary, after downstream applications have migrated to
the explicit ownership and shutdown APIs below. Any future removal remains
subject to the full release cycle, a 2.0 breaking window, and an exact,
reviewed post-freeze repository-owner approval record for every affected frozen
public item (package, profile, item path, and change kind). This change creates
no approval record and does not authorize a removal.

| Legacy 1.x API | Migration target | Required ownership or policy decision |
|---|---|---|
| `RocketMQRuntime::new_multi(threads, name)` | `RuntimeOwner::new(RuntimeConfig { .. })?` | Put the worker count and thread name in `RuntimeConfig`; propagate its typed `RuntimeResult` instead of relying on an infallible constructor. |
| `get_handle()` | `RuntimeOwner::root_context().component(...)` or a child derived from `RuntimeContext` | Pass `ChildServiceContext` (or a narrower capability) to a library. Do not replace this call with a raw Tokio handle. |
| `get_runtime()` | `RuntimeOwner::block_on(...)` at an owned synchronous entrypoint, or context-owned task operations | Keep the Tokio runtime private to its owner. Libraries receive a context and use its task group, blocking lanes, and diagnostics rather than a raw runtime reference. |
| `schedule_at_fixed_rate(task, initial_delay, period)` (`Fn`) | `ScheduledTaskGroup::schedule_fixed_rate_no_overlap` | Set `config.initial_delay = initial_delay.unwrap_or(Duration::ZERO)`. The legacy callback was serial, so preserve that non-overlap policy explicitly; a tick is skipped while an async run is active. Choose `schedule_fixed_rate` only when overlapping runs are intentional and reentrant. |
| `schedule_at_fixed_rate_mut(task, initial_delay, period)` (`FnMut`) | `ScheduledTaskGroup::schedule_fixed_delay` | Set `config.initial_delay = initial_delay.unwrap_or(Duration::ZERO)`. This accepts mutable callback state while keeping runs serial. If fixed-rate cadence is required, make the callback state safe for the chosen explicit no-overlap or allow-overlap policy. |
| `shutdown()` | `RuntimeOwner::shutdown_background(self)` | Cancels tracked RocketMQ work, returns an immediate `ShutdownReport`, and asks Tokio to finish runtime shutdown in the background. |
| `shutdown_timeout(timeout)` at a synchronous runtime-owning boundary | `RuntimeOwner::shutdown_runtime_blocking_with_timeout(timeout)?` | Shuts down tracked work and then the owned Tokio runtime within the timeout. It returns typed errors, including `RuntimeError::InsideTokioRuntime` when called from Tokio. |
| async shutdown that must retain the host Tokio runtime | `RuntimeOwner::shutdown_tasks().await` or `RuntimeContext::shutdown_tasks(timeout).await` | Shuts down only tracked RocketMQ work and returns `ShutdownReport`; it does not close the owned or host Tokio runtime. |

For a process entrypoint that owns Tokio, construct one owner from typed
configuration and derive child service contexts from it:

```rust,ignore
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};

let owner = RuntimeOwner::new(RuntimeConfig {
    worker_threads: threads,
    thread_name: service_name.to_owned(),
    ..RuntimeConfig::default()
})?;
let service = owner.root_context().component("consumer");
```

Code that is already executing inside an application-owned Tokio runtime may
use `RuntimeContext::try_from_current("migration")?` as a migration or test
harness and derive a `ChildServiceContext` from it. Production composition
roots use `RuntimeOwner`; neither form grants libraries raw Tokio runtime
access.

Make the periodic-work policy visible at the call site. The no-overlap choice
below is the closest replacement for the legacy `Fn` callback. It keeps a
fixed cadence, skips a tick if the prior run is active, and lets shutdown track
both the driver and any run it owns.

```rust,ignore
use std::time::Duration;

use rocketmq_runtime::ScheduledTaskConfig;

let scheduled = service.scheduled_tasks("consumer-maintenance");
let mut config = ScheduledTaskConfig::fixed_rate_no_overlap(
    "consumer-maintenance.refresh",
    period,
);
config.initial_delay = initial_delay.unwrap_or(Duration::ZERO);
scheduled.schedule_fixed_rate_no_overlap(config, || async {
    refresh_routes().await;
})?;
```

For stateful legacy `FnMut` callbacks, use fixed delay unless a different
overlap policy has been deliberately designed and documented:

```rust,ignore
let mut generation = 0_u64;
let mut config = ScheduledTaskConfig::fixed_delay(
    "consumer-maintenance.persist",
    period,
);
config.initial_delay = initial_delay.unwrap_or(Duration::ZERO);
scheduled.schedule_fixed_delay(config, move || {
    generation += 1;
    async move {
        persist_generation(generation).await;
    }
})?;
```

At shutdown, choose the matching ownership boundary and make the report part
of the application's lifecycle result. `shutdown_background` is the immediate
compatibility path, `shutdown_tasks` closes only tracked RocketMQ work, and
`shutdown_runtime_blocking_with_timeout` is for synchronous process teardown
only; the latter returns a typed error if invoked from inside Tokio.

```rust,ignore
let report = owner.shutdown_runtime_blocking_with_timeout(timeout)?;
report.assert_no_task_leak().map_err(|message| anyhow::anyhow!(message))?;
```

### Typed filter compilation compatibility facade

New filter callers use `Filter::try_compile`, which returns `FilterCompileError`
with a stable kind, stage, optional original UTF-8 byte offset, and source
classification. The error is redaction-safe and converts to `RocketMQError`
with `ErrorKind::Filter`.

```rust,ignore
use rocketmq_filter::filter::{Filter, FilterFactory};

let filter = FilterFactory::instance().get("SQL92").expect("SQL92 is registered");
let expression = filter.try_compile("color = 'blue' AND retries >= 3")?;
```

`Filter::compile` and its local string `FilterError` remain deprecated but
available 1.x compatibility facades for existing callers and filter
implementations. This guide neither removes them nor creates a deletion
approval. Any future deletion requires the complete release cycle, an explicit
2.0 breaking window, and an individual reviewed post-freeze approval for every
affected frozen public item.

Custom `Filter` implementers keep the required legacy `compile` method
throughout 1.x. Only in a future, approved 2.0 breaking window would that
required implementation migrate from `compile` returning the local string
`FilterError` to `try_compile` returning `FilterCompileError`. This is a future
2.0 sketch only, remains subject to the approvals above, and is not current
1.x-compilable code:

```rust,ignore
// Future 2.0 sketch only; not valid for the current 1.x Filter trait.
impl Filter for CustomFilter {
    fn try_compile(&self, expression: &str) -> Result<Box<dyn Expression>, FilterCompileError> {
        // Compile the custom expression and return typed, redaction-safe failures.
    }
}
```

### Mapped buffer reads

The former `MappedBuffer::read_zero_copy` name was inaccurate because the
returned bytes were owned copies. Use `read_copy`; the ownership and allocation
behavior are unchanged.

```rust,ignore
// Before 1.0
let bytes = mapped.read_zero_copy(position..position + length)?;

// 1.0
let bytes = mapped.read_copy(position..position + length)?;
```

### Narrow client capabilities

The placeholder `MQClientAPIExt` and implementation aliases are not part of the
1.0 surface. Depend on the narrow capability needed by the caller:

| Old dependency | 1.0 capability |
|---|---|
| generic producer operations | `ProducerClient` |
| pull, POP, ACK, and consumer operations | `ConsumerClient` |
| route lookup and refresh | `RouteClient` |
| administrative RPC operations | `AdminClient` |
| transaction end/check operations | `TransactionClient` |

Repository integration probes that were once exported from production roots
are available only through the explicit `test-support` feature. Application
code must not depend on those probes.

## Frozen 1.0 entry points

### MappedFileBuilder

`MappedFileBuilder` is a working, fallible builder rather than a pending public
placeholder. A path and size are required. Supported defaults create a mapped
file; invalid sizes, unsupported flush/transient/warmup combinations, and I/O
failures return `MappedFileError`.

```rust,ignore
use rocketmq_store::MappedFileBuilder;

let mapped_file = MappedFileBuilder::new("data/commitlog/00000000000000000000")
    .size_mb(1024)
    .build()?;
```

Do not unwrap builder failures in a Broker startup path. Surface the typed
configuration or I/O error before the service starts accepting traffic.

### Classic Pull compatibility facade

`DefaultMQPullConsumer` remains functional for applications that need explicit
queue and offset control. New Rust-native applications should normally use
`DefaultLitePullConsumer`; migrations from Java Classic Pull may use the stable
facade.

```rust,ignore
let consumer = DefaultMQPullConsumer::builder(client_runtime.clone())
    .consumer_group("manual-pull-group")
    .name_server_addr("127.0.0.1:9876")
    .build()?;

consumer.start().await?;
let result = consumer.pull(&queue, "TagA || TagB", offset, 32).await?;
consumer
    .update_consume_offset(&queue, result.next_begin_offset() as i64)
    .await?;
consumer.shutdown().await?;
```

The facade supports synchronous and asynchronous pull, TAG/SQL92 selectors,
block-if-not-found deadlines, queue discovery, offset persistence, queue
listeners, and scheduled pull callbacks. It uses an injected runtime and never
advertises the LitePull stream request type. Detached compatibility
constructors remain source-compatible but fail closed for operations that need
a runtime.

For overload-style calls, use `PullOptions` so timeout, count, size, offset, and
long-poll invariants are validated before a request is sent.

### Proxy gRPC Settings

Proxy settings are resolved through one immutable `ServerSettingsPolicy`
generation per telemetry request. Server-owned values override client claims;
client identity and subscription expressions remain client-owned.

The 1.0 fallback values are:

| Setting | Default |
|---|---:|
| maximum message body | 4 MiB |
| validate message type | `true` |
| consumer retry attempts | 17 |
| receive batch size | 32 |
| long-poll timeout | 20 seconds |
| FIFO | `false` |
| Lite subscription quota | 2000 |
| maximum Lite topic-name length | 64 |

Producer retry policy is built from `SettingsConfig`; consumer group policy is
resolved from the authoritative backend. If the policy cannot be resolved, the
request fails closed rather than silently reverting to unrelated constants.

Custom embedders may provide a `SettingsPolicyProvider`, but the provider must
return a complete immutable policy for the request lifetime.

## Feature-profile compatibility

The default API of every core library is recorded separately. The 24 public
feature profiles in `m09_compatibility_matrix.py` are also recorded separately,
including no-default, selected-feature, combined-feature, and all-feature
profiles for Protocol, Transport, Store, Admin, and Proxy.

Notable 1.0 defaults include:

- `rocketmq-client-rust`: `admin-full`;
- `rocketmq-transport`: `tls,socks`;
- `rocketmq-store`: `local_file_store,fast-load`;
- `rocketmq-controller`: `storage-rocksdb`;
- `rocketmq-proxy`: `cluster-mode,local-mode`.

Use `--no-default-features` only when the application also selects one of the
frozen supported profiles. An arbitrary Cargo feature power set is not an
implicit compatibility promise.

## Checking an application migration

Before adopting a release candidate:

1. replace the direct source migrations above;
2. select a frozen feature profile;
3. run the application's normal tests against `1.0.0-rc.N`;
4. exercise startup and shutdown so runtime ownership is verified;
5. exercise send, pull/POP, query, and Admin paths used by the application;
6. treat any new compiler error or default-behavior difference as a release
   candidate compatibility finding.

The repository verifies its own frozen surface with:

```powershell
python scripts/public_api_snapshot.py --scope core-release --check scripts/public-api-snapshot-baseline.json --identity structural
python scripts/stable_surface_guard.py --scope core-release --mode target
```

The snapshot command reports additions separately from breaking changes. A
post-freeze break is accepted only when its exact package, profile, item path,
and change kind match a non-empty approval record.
