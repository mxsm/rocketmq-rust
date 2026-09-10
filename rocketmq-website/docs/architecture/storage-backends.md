---
title: "Storage backend composition and recovery"
---

RocketMQ Rust offers a local primary-log path, optional RocksDB-derived storage, and optional tiered secondary storage. These are different roles in a composed store, not three equivalent engines that can be switched over the same directory.

The [storage contract](storage.md) defines append receipts, durable boundaries, derived progress, and lifecycle ownership. This chapter explains how implementations satisfy those contracts.

## Responsibilities by layer

| Layer | Owns | Activation |
| --- | --- | --- |
| `rocketmq-store-api` | Runtime-neutral read/write/admin/replication contracts and outcome types | Shared dependency; no running store |
| `rocketmq-store` | `StoreFactory`, exclusive `StorePorts`, Broker integration, dispatch and service composition | Validated backend configuration and compiled features |
| `rocketmq-store-local` | CommitLog, mapped files, recovery, local ConsumeQueue/index, flush and HA primitives | Used by the composed store |
| `rocketmq-store-rocksdb` | Column families, codecs, ConsumeQueue/index/timer/transaction derived state, snapshots | `rocksdb_store` integration plus runtime selection |
| `rocketmq-tieredstore` | Secondary dispatch/fetch, segments, metadata, provider and retention services | `tieredstore` integration plus storage policy/provider |

## Primary log and derived structures

The local CommitLog stores primary records. ConsumeQueue maps a queue's logical offset to the primary record; key indexes support lookup. Dispatch advances these derived structures from primary-log records. A derived update does not itself prove that the primary record is durable.

The RocksDB store composition retains the local file CommitLog. Its derived-state integration consumes a `WalPort`, and RocksDB manages the selected queue/index/timer/transaction structures. Opening a RocksDB database successfully does not establish that it matches the current primary log or that replay has caught up.

Derived cursors identify the engine, source epoch, and exclusive physical progress boundary. Advancing progress must respect durable primary-log coverage and contiguous processing. A cursor is not a consumer offset and cannot be reset arbitrarily to make a recovery warning disappear.

## Opening and loading

`StoreFactory` creates the selected composition; initialization, load/recovery, start, and shutdown remain separate stages. Recovery can fail after configuration validation because filesystem state, native database opening, or persisted records are unsuitable.

`RocksDbOpenPlan::from_config` validates configuration without opening storage and returns no plan when disabled or invalid. A valid plan is not a filesystem probe. Opening uses a supplied child service context, and maintenance/rebuild tasks remain under that owner.

The store's default feature set includes `fast-load`. In local primitives, `safe-load` alone selects sequential loading; `fast-load` takes precedence when both are enabled, and the documented safe-load environment override can force the sequential path. These switches change loading strategy, not the required record/recovery contract.

## Tiered storage is a secondary path

Tiered dispatch writes message data and queue/index information to secondary segments and metadata. Fetch supports logical queue offset, store timestamp, and key lookup under the selected policy. Bundled providers are POSIX files and in-memory storage; the latter is useful for tests and does not survive process loss.

Recovery reconciles metadata and actual segment sizes, including partially committed segments. Retention removes expired segments and associated index metadata. A tiered layer therefore has its own durability, cleanup, and recovery work. Adding it does not automatically strengthen the primary send acknowledgement or create an independent backup.

The provided POSIX backend is not evidence of a bundled S3-compatible provider. A custom provider needs its own persistence and failure contract.

## Failure and maintenance implications

| Situation | Correct interpretation |
| --- | --- |
| Append accepted, flush/replica wait times out | Record presence may already be established; inspect the typed outcome before retrying |
| Derived storage lags | Primary bytes and queryability can be at different positions |
| RocksDB open fails | A validated configuration cannot override native or filesystem failures |
| Tiered dispatch fails | Secondary availability/progress is affected; do not infer primary rollback |
| Cleanup finds live file leases | Referenced files cannot be retired until lifetime contracts permit it |
| Shutdown deadline expires | Flush, tasks, leases, or retirement may remain incomplete |

Backups and migration must account for primary records, derived progress, metadata, backend layout, and any tiered provider state together. Switching `storeType` is not a migration procedure. Restore compatibility and actual recovery need to be demonstrated on an isolated copy.

## Build and platform constraints

`rocketmq-store-rocksdb` always depends on native RocksDB even though its own default feature list is empty. Building its integration requires the native C++/binding toolchain. Optional `io_uring` support is Linux-specific and still requires host capability checks; compiling the feature does not prove activation.

Choose features for the service binary that owns the store. Building a primitive crate by itself neither selects a Broker backend nor starts its services.

## Source map

- [Store composition](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/README.md).
- [Local primitives](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-local/README.md), [RocksDB foundation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-rocksdb/README.md).
- [Tiered implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tieredstore/README.md).
- Continue with [HA durability](ha-controller.md) and [deployment boundaries](../deployment/overview.md).
