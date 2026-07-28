# Architecture major-version migration

This release boundary completes the compatibility retirements prepared during
the ownership and interface refactors. It changes Rust source APIs only; wire
codes and headers, Serde field names, persisted record layouts, and typed error
semantics are unchanged.

## Producer configuration

`DefaultMQProducer::client_config()` and
`DefaultMQProducer::producer_config()` were removed. Use the owned immutable
snapshots:

```rust,ignore
let client = producer.client_config_snapshot();
let config = producer.producer_config_snapshot();
```

Producer accessors for groups, topics, retry codes, hooks, dispatchers,
accumulators, and latency arrays now return owned values. Keep a snapshot when
several fields must come from the same generation:

```rust,ignore
let config = producer.producer_config_snapshot();
let group = config.producer_group();
let retry_codes = config.retry_response_codes();
```

The append-only compatibility history and its unsafe borrowed-reference
reconstruction no longer exist. A held `Arc` snapshot remains valid while
subsequent updates publish a new generation.

## Admin marker

`MQAdminExtInner` and `MQAdminExtInnerImpl` were empty compatibility symbols
and have no replacement. Use `MQAdminExt`, `DefaultMQAdminExt`, or
`DefaultMQAdminExtImpl` according to the behavior required by the caller.

## Store interface

`MessageStoreInner` and `LegacyMessageStoreAdapter` were removed. Generic code
that still needs the complete compatibility facade uses `MessageStore`.
New or focused integrations should use the capabilities exported by
`rocketmq-store-api`, such as `MessageAppender`, `MessageReader`,
`OffsetIndex`, `StoreHealth`, and `StoreLifecycle`.

## Runtime ownership

`RuntimeHandle`, `TaskGroup::root`, `TaskGroup::runtime`,
`ChildServiceContext::runtime`, and detached spawn methods are no longer
public. Applications create a `RuntimeOwner`; libraries receive
`ChildServiceContext`, `TaskSpawner`, `ScheduledTaskGroup`,
`BlockingExecutor`, or a child `TaskGroup`.

Tiered Store constructors now require a parent `TaskGroup`. This makes
dispatcher and cleanup work a child of the caller's shutdown owner:

```rust,ignore
let runtime = RuntimeOwner::new(RuntimeConfig::server_default("app"))?;
let service = runtime.root_context().child("tiered-store");
let store = TieredStore::new(config, service.task_group().clone())?;
```

`RuntimeContext::from_current` remains document-hidden for tests and migration
harnesses. Production composition roots should not discover an ambient Tokio
runtime.

## Evidence and rollback

The generated
`rocketmq-doc/en/architecture-release-evidence-index.md` maps the current
commit to validation, fuzz/Miri/Loom, performance, fault, and soak workflows.
Artifacts are accepted only when their identity contains the candidate commit.
If downstream migration was not completed before this major boundary, revert
the retirement commit as a unit; do not restore an unbounded config history,
empty marker trait, or public raw runtime escape independently.
