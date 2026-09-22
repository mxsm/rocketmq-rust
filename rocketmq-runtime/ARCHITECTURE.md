# Runtime ownership and implementation boundaries

Application entrypoints own `RuntimeOwner`; components receive
`ChildServiceContext` or narrower capabilities. A task scope owns lifecycle,
while a resource budget owns explicit accounting. Neither substitutes for the
other. Managed retained bytes describe reservations, not allocator usage, RSS,
or an operating-system memory limit.

## State responsibility

| Responsibility | Implementation | Invariant |
| --- | --- | --- |
| Task admission and submission | [submission](src/task_group/submission.rs) | Registration and handle installation use the owner admission gate. |
| Final task settlement | [completion](src/task_group/completion.rs) | Future destruction precedes failure accounting and completion publication. |
| Active task and child registration | [registry](src/task_group/registry.rs) | Child links are weak; live descendants retain their ancestors. |
| Deadline and retained shutdown result | [coordinator](src/task_group/shutdown.rs), [group façade](src/task_group.rs) | Later calls never extend an accepted deadline or replace a published report. |
| Aggregate and bounded task scans | [task diagnostics](src/task_group/diagnostics.rs) | Local counts exclude children; bounded details disclose truncation. |
| Diagnostic schemas and collection | [diagnostics](src/diagnostics.rs) | V1/V2 public types and wire names remain stable. |
| Sanitized conversion | [conversion](src/diagnostics/conversion.rs) | Labels derive from typed identity, never vector position; conversion performs no business I/O. |
| Process lifecycle transitions | [service lifecycle](src/service_lifecycle.rs) | Readiness, terminal states and the first shutdown request are state decisions. |
| Health-probe transport and routing | [probe](src/service_lifecycle/probe.rs) | Routes call lifecycle APIs instead of modifying state atomics. |
| Metadata admission and publication | [metadata actor](src/metadata_io.rs) | Actor state owns waiters, retained bytes and generation ordering under one coordination protocol. |
| Atomic replacement and durability | [filesystem](src/metadata_io/filesystem.rs) | Platform operations do not modify actor state or diagnostic schemas. |

These submodules are private. A split does not create another state owner or
require a public helper API. Public paths retain their original façade modules.

## Completion and observation

`TaskGroup::wait_task` succeeds when a task is already absent;
`abort_task_and_wait` returns false for an absent task. Both refer to local
registration and destruction, not business success. A normal `Future<Output =
()>` return does not establish that its business operation succeeded.

`cancel` only signals. `shutdown_until` seals admission and awaits owned work.
`shutdown_now` cannot confirm destruction and retains its immediate report.
Dropping a group handle is neither cancellation nor graceful shutdown. A
service with final I/O must observe cancellation and perform its cleanup;
dropping the service future cannot perform that cleanup.

A blocking timeout stops the observer's wait. The real closure can still run,
so capacity and result obligations remain owned until it exits. Metadata
acceptance is not durability, and observer timeout is not rollback. Unknown
outcomes retain reconciliation obligations. Target registration is
process-local, not a cross-process lock or crash-recovery proof.

`BudgetedQueue::try_pop` releases accounting before returning the payload.
`try_pop_budgeted` transfers the charge to the consumer. Closed dynamic
generations reject new admission, including rebinds, but outstanding permits
can release or migrate out.

## Observation boundaries

`ServiceLifecycleObserver` attaches once while the process is Starting. State
changes and observer installation share a short synchronization boundary;
callbacks run after all lifecycle locks have been released. Only committed
changes emit events. Concurrent transitions can deliver callbacks out of order,
so each event identifies its actual previous and next state. Callbacks must be
nonblocking and panic-free. Exporter shutdown belongs to the composition root.

`OperationOutcome` is separate from `ShutdownReport.completed`. It describes
each accepted operation task: normal return, operation cancellation, deadline,
owner cancellation, panic, or abort before another outcome was selected. A
finalizer publishes the result only after the user future is destroyed, including
an unpolled abort or a destructor panic. Rejected submissions do not increment
outcomes. The operation retains six counters rather than per-task history.
Draining operations continue to ignore owner cancellation until their accepted
work completes, is operation-cancelled, expires, or is explicitly aborted.

`MetadataIoObserver` and selected-name `ScheduledTaskObserver` use weak
references, so diagnostic consumers do not become write or execution owners.
The metadata registry capacity bounds its resource scan; schedule lookup cost
is proportional to the fixed selection supplied by the component.

## Maintenance example

Partial or reordered blocking-lane inputs exposed a label bug: conversion used
array position. The fix belongs in
`diagnostics/conversion.rs::sanitize_blocking_lane`; the behavioral regression
belongs beside V1/V2 consumer tests in `diagnostics.rs`. Task submission,
lifecycle state and metadata persistence need no change. Tests cover a single
lane, reordered lanes, missing lanes and distinct counts.

A probe route belongs in `probe.rs`, a platform replacement operation belongs
in `filesystem.rs`, and a new task classification updates the exhaustive
`TaskKind` mapping and wire conversion. Task-kind arrays and traversal share
the same private enumeration.

## Scheduling boundaries

The [entrypoint matrix](README.md#scheduled-tasks) distinguishes serial
maintenance, bounded overlap, mutable callbacks, operation ownership,
calendar/trigger jobs and dedicated threads. Ordinary and operation-bound
fixed-delay adapters share execution and final settlement; their outer
cancellation owners remain distinct. Legacy rate and Cron protocols retain
their timing semantics. Unread compatibility fields remain documented as such.

The Broker transaction adapter uses an injected parent group for checks and
operation batching. Cancellation stops new operation admission even if the
batch task is aborted before polling. A cooperative exit closes existing
queues, waits for each active batch, and writes remaining partial batches while
the Store is still available. A failed append retains its body and reservations;
the Broker report remains unhealthy and retains the service for inspection or
retry. The shared Broker deadline bounds this drain; aborting at that deadline
cannot establish durability. Queue retirement is permanent for that service
instance. Compatibility constructors remain available, while production
composition supplies the owner.

Transaction-metrics persistence uses the bounded serial scheduler with explicit
`MissedTickPolicy::Skip`: a delayed flush never replays missed ticks or overlaps
another flush. Shutdown still performs a final dirty-metrics persist. This
consumer decision does not change legacy scheduler defaults.

## Sampling cost and consistency

Service-context V1/V2 sampling reads blocking aggregates directly from the
registry. It does not allocate individual blocking task names or detail
objects. The explicit `BlockingExecutor::snapshot` API retains its full-detail
contract. Task subtree and local counts now come from the same traversal.

Age maxima, long-running thresholds and group counts still require an exact
scan of each observed entry. No new counter is maintained on submission or
completion, so this optimization adds no accounting responsibility to those
paths. A scan is consistent per visited registry entry, not a globally atomic
instant across groups or blocking lanes. Concurrent changes may therefore be
observed at different times. Local counts are included in the subtree total
from that same traversal.

The diagnostics benchmark creates one stable task population outside each
timed sampling loop, and shuts it down after measurement. Task population and
group count vary independently. Detail scan/output budgets still describe the
detail section alone; they do not limit the aggregate age scan. Sampling
measurements exclude runtime construction, population and shutdown costs.

## Metadata target retirement

An owner plan accepts a nonzero metadata target capacity (default 4,096).
Idle history and fenced history consume slots. Each actor independently bounds
its retained resource cache by the same capacity: another actor retiring shared
history must not grow an older actor's tombstones without limit.

Normal actor replacement inherits target identity, resource ownership and
confirmed generation. Explicit retirement has a different contract: after
admission closes and the coordinator finishes, the registry atomically checks
identity, durable generation, absence of live write authority and absence of a
reconciliation fence. Only then may a new identity reuse the path and slot.
Old open actors cannot rebind that retired identity; replace them to release
their tombstones. Settled receipts retain their original result without owning
a writer. Compare receipt and actor target identities before using a newer
generation as evidence about an old write.

Registry statistics expose capacity, retained histories, live owners, idle
histories, fenced targets and remaining capacity without paths. Fenced and
live counts can overlap. Unknown commits stay fenced: runtime has no generic
format-independent recovery or unconditional fence-clearing operation.
