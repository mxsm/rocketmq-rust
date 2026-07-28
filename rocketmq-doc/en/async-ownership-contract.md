# Async Ownership Contract

## Purpose

This contract records the ownership, mutation, versioning, and shutdown rules introduced by the async predictability refactor. The affected modules remain inside their existing crates and expose the narrowest capability needed by callers.

## Ownership table

| Module | Lifecycle owner | Unique mutation point | Shared value | Snapshot or version rule | Shutdown owner |
|---|---|---|---|---|---|
| `rocketmq_runtime::blocking` | `RuntimeOwner`, through the root `ServiceContext` | `GlobalBlockingBudget` admission state and each lane executor queue | Cloned budget handles share one capacity derived from `RuntimeConfig::max_blocking_threads` | A `BlockingExecutorSnapshot` reports global and lane capacity, reservations, borrowed capacity, running work, rejection counts, and queue age | `RuntimeOwner` stops admission and drains the root context |
| Controller `BrokerRoleNotifier` | `ControllerManager` task group | The notifier mailbox is the only writer of pending, in-flight, retry, generation, and notified state | `BrokerRoleNotifier` clones share one bounded mailbox and one bounded key channel | Per-key epoch comparison is latest-wins; leadership reset advances the generation; `NotifySnapshot` exposes retained keys, outcomes, retries, and RPC latency | `ControllerManager` closes admission, cancels the task group, and awaits owned work |
| Client `RouteUpdateCoordinator` | `MQClientInstance` | A short synchronous versioned commit updates route, broker, endpoint, and refresh-version tables | Snapshot computation owns cloned producer and consumer handles; no registry guard crosses an await point | Refresh follows `snapshot -> compute -> versioned commit -> notify`; a changed version returns `Stale` without partial writes | `MQClientInstance` owns the coordinator; notifications complete within the caller's refresh operation |
| `TaskSpawner` | Its parent `TaskGroup` | `TaskGroup` remains the only task registry and lifecycle mutation point | Clones share task-group identity and cancellation authority, but cannot expose a raw Tokio handle or detach work | Task diagnostics and cancellation state are inherited from the parent group | The parent service context cancels and awaits the task group |

## Blocking invariants

- A root service context creates exactly one managed blocking budget. Child contexts and lane executors only clone its handle.
- Global running work never exceeds `max_blocking_threads`. Each storage, metadata, and CPU/crypto lane has a ceiling and a minimum reservation.
- Idle reservations may be borrowed. Once a lane waits, its unmet reservation is protected from new borrowing.
- Queueing, admission, and execution share one absolute deadline.
- The admission permit is held by the actual blocking closure. Caller timeout, cancellation, panic, or join failure cannot release capacity while that closure is still running.
- A configuration with fewer than three managed blocking threads is rejected because it cannot reserve one slot for every lane.

## Controller notifier invariants

- The mailbox retains at most 1,024 unique broker keys across pending, in-flight, and retry-waiting states.
- A repeated state is `Coalesced`, a newer state `Replaced`, an older state `Stale`, and a new key beyond capacity `Full`.
- A retry remains part of the same retained-key bound and must re-enter the same mailbox. It cannot create a detached or unaccounted queue.
- A leadership change disables admission, advances the generation, and discards obsolete pending and retry state.
- Notification uses the existing concrete remoting client. There is only one production implementation, so this refactor deliberately adds no public RPC trait.

## Client route-update invariants

- Snapshotting clones owned route data and `Arc` handles, then releases all DashMap and synchronization guards.
- Computation and asynchronous consumer checks use only the owned snapshot.
- Commit acquires the existing transition lock, checks the route version, performs all table writes synchronously, increments the version, and releases the lock before notification.
- Producer and consumer notification runs outside the commit lock with a concurrency limit of 16 and one three-second absolute refresh deadline shared by asynchronous checks and notifications.
- Public behavior retains the `Applied`, `Unchanged`, and `Stale` outcomes. Version conflicts and partial notifications are recorded in route-refresh diagnostics.

## Task escape policy

Business modules should receive `TaskSpawner`, `TaskGroup`, scheduled-task capabilities, or `BlockingExecutor`; they should not receive a raw runtime handle or detached-spawn capability.

Temporary compatibility boundaries are listed in [`scripts/runtime-task-escape-policy.json`](../../scripts/runtime-task-escape-policy.json). Every entry records its caller, owner, termination condition, join or deadline policy, diagnostics, target capability, reason, and removal phase. Raw runtime and detached task APIs are deprecated and scheduled for the phase-3 compatibility boundary.
