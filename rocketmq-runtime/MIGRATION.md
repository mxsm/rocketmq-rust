# Runtime contract corrections (unreleased)

These changes are part of the current workspace's compatibility migration.
Consumers must migrate with this batch; the budget accessor change is a source
compatibility break and must not be published as a compatible patch to an API
that returns a mandatory capacity dimension.

## Closed dynamic budgets

`BudgetRejection::dimension()` now returns `Option<BudgetDimension>`.
`Some(Count | Bytes | Rate)` still means capacity exhaustion; `None` means
admission is closed. Prefer the exhaustive `reason()` API:

```rust
use rocketmq_runtime::{BudgetDimension, BudgetRejection, BudgetRejectionReason};

fn rejection_label(rejection: &BudgetRejection) -> &'static str {
    match rejection.reason() {
        BudgetRejectionReason::Closed => "closed",
        BudgetRejectionReason::Capacity(BudgetDimension::Count) => "count",
        BudgetRejectionReason::Capacity(BudgetDimension::Bytes) => "bytes",
        BudgetRejectionReason::Capacity(BudgetDimension::Rate) => "rate",
    }
}
```

Do not map `None` to a capacity dimension, panic, or retry until capacity changes.
`BudgetRejection::is_closed()` is available for early closure handling.
`DynamicKeyRegistrationFailure` also gains `Closed`; exhaustive matches must
handle it. A static `child()` of a closed generation may still be constructed,
but inherits the permanently closed admission gates.

Closing or retiring a dynamic key now closes every escaped budget clone and
descendant. Acquisition and closure are serialized: work admitted first remains
charged until its actual permit release, and work arriving after closure is
rejected. Retirement releases the name only after all reservations drain. Old
handles cannot admit work beside a replacement generation.

Rebinding checks the entire target chain, including shared ancestors and the
same-node fast path. A rejected rebind preserves the source permit. Moving an
existing permit out to an open budget remains supported. Queues return their
existing `QueuePushRejection::Closed`, wake waiting producers and receivers, and
allow accepted entries to drain. Closed admission does not invoke a destructive
full or age policy. `try_push_budgeted` still releases a permit on a normal
rejection; only `ForeignPermit` returns the item and unchanged charge together.

The workspace consumers migrate in the same change. Transport pending requests
report `SessionClosed`; `DeferredAdmissionAcquireOutcome` gains `Closed` rather
than reporting parent capacity exhaustion. Telemetry reports a closed buffer.
Broker capacity rejection messages retain their existing dimension text and
can accurately describe closure.

## Shutdown and failure settlement

Active descendants retain their ancestor groups after intermediate context
handles are dropped. Shutdown seals and retains the accepted subtree before
cancelling it, so fast leaf completion cannot disappear from the report. Empty,
unowned subtrees still unregister; there is no permanent strong child registry.

Critical monitors stop cooperatively with their owner. Cancellation selected
before failure handling leaves the pending record available to another handler.
Handlers process the record they actually take, even if a previous notification
referred to an older failure. Callbacks remain synchronous and should be short.

Critical task settlement includes destructor panics before publishing task
completion. Ordinary cancellation is not a critical failure. Panic propagation
and poisoned task-group behavior remain unchanged. These guarantees apply to
unwinding panics, not `panic=abort` or process termination.

Scheduled run reservations now settle exactly once on every exit path.
`ScheduledTaskSnapshot::runs` continues to count normal returns, including a
controlled stop. `failures` includes timeouts and rejected submissions, and now
also records panics and cancelled/aborted reservations that previously leaked
their active count. A reservation dropped before its first poll is a failed
start. The internal outcomes distinguish completion, timeout, panic,
cancellation, and rejection before start; the published snapshot shape stays
unchanged.

Lifecycle shutdown requests preserve `Failed` and `Stopped` through conditional
atomic transitions. Repeated requests keep the first reason and absolute
deadline.

## Runtime convergence

This batch removes the parallel executor, scheduler and service-thread APIs and
leaves one owner model: `RuntimeOwner`, `ChildServiceContext`, `TaskGroup`,
`ScheduledTaskGroup` and `ServiceManager`. It is a source compatibility break.
The workspace consumers migrate in the same change.

### Removed APIs

| Removed | Replacement |
| --- | --- |
| `TokioExecutorService`, `ScheduledExecutorService`, `FuturesExecutorService`, their plans and `FuturesExecutorServiceBuilder` | Spawn on a `ChildServiceContext` or its `TaskGroup`; use `ScheduledTaskGroup` for periodic work and the managed blocking lanes for blocking work. |
| `schedule` module: `TaskScheduler`, `TaskExecutor`, `ExecutorPool`, `ExecutorConfig`, `SchedulerConfig`, `Task`, `TaskContext`, `TaskStatus`, `LegacyTaskResult`, the `Trigger` types and the `Schedule*Outcome` types | `ScheduledTaskGroup::schedule(config, policy, task)`. Cron triggers have no direct replacement: run a service task that sleeps until the next due time. |
| `ScheduledTaskManager` | `ScheduledTaskGroup` from `context.scheduled_tasks(name)`. |
| `ActorRuntime` | A service task on the owning `TaskGroup`. |
| `compat` module | The owner and context APIs above. |
| `tokio_lock` | `tokio::sync` directly. |
| `Shutdown<T>` | `tokio_util::sync::CancellationToken`, or the owning group's cancellation token. |
| `common::util_all` | Time helpers moved to `common::time_utils` with the same names; Store path helpers moved into `rocketmq-store`. Helpers without workspace callers, such as `get_ip`, `get_ip_str`, `YYYY_MM_DD_HH_MM_SS` and the byte and hex converters, were removed. |
| `time_utils::get_current_millis` and `get_current_nano` (deprecated since 0.8.0) | `current_millis` and `current_nano`. |
| `common::thread` and `common::future` | `ServiceManager` for service loops; `tokio::sync::oneshot` or task handles for completion. |
| `DetachedTaskPolicy`, `TaskSnapshot::detached`, `TaskSnapshot::detached_policy`, `ShutdownReport::detached_still_running` | Nothing: every task belongs to a group and is counted by the ordinary report fields. |
| `ScheduledTaskGroup::schedule_fixed_delay`, `schedule_fixed_rate`, `schedule_fixed_rate_no_overlap`, `schedule_fixed_rate_allow_overlap`, `schedule_bounded` and their `_operation` and `_controlled` forms | `schedule`, `schedule_operation` and `schedule_controlled`; the configuration selects the timing mode (see the table below). |
| `ScheduledTaskConfig::shutdown_timeout` | The shutdown call's timeout or deadline; the field was never read. |
| `ServiceManager::new`, `new_arc`, the `_legacy_compatibility` constructors and the `service_manager!` macro | `ServiceManager::new_with_task_group(service, group)` or `new_arc_with_task_group`. |
| `ServiceManager::is_daemon`, `set_daemon`, `is_started`, `is_stopped`, `wait_for_running`, `task_count` | `get_lifecycle_state()`; the service loop uses `ServiceTaskContext`. |
| `ServiceLifecycle` (the `ServiceManager` state enum) | `ServiceTaskState`. |
| `ServiceManagerLifecycleProbe` and `run_service_manager_lifecycle_probe` | `last_task_group_shutdown_report()`. |
| `RuntimeOperation` and `RuntimeContractPolicy` variants that only the removed APIs used | Nothing. |

`ScheduledTaskGroup` timing modes:

| Previous method | Configuration | Policy |
| --- | --- | --- |
| `schedule_fixed_delay` | `ScheduledTaskConfig::fixed_delay(name, period)` | `ScheduledExecutionPolicy::serial(..)` |
| `schedule_fixed_rate_no_overlap` | `ScheduledTaskConfig::fixed_rate_no_overlap(name, period)` | `ScheduledExecutionPolicy::serial(..)` |
| `schedule_fixed_rate`, `schedule_fixed_rate_allow_overlap`, `schedule_bounded` | `ScheduledTaskConfig::fixed_rate(name, period)` | `ScheduledExecutionPolicy::bounded(n, ..)` |

A policy that contradicts the configuration is rejected as unsupported.
`ScheduledExecutionPolicy::default()` runs one execution at a time and skips
ticks that arrive during it.

```rust,ignore
use rocketmq_runtime::{ScheduledExecutionPolicy, ScheduledTaskConfig};

let scheduled = context.scheduled_tasks("maintenance");
scheduled.schedule(
    ScheduledTaskConfig::fixed_delay("maintenance.persist", period).with_initial_delay(delay),
    ScheduledExecutionPolicy::default(),
    move || async move { persist().await },
)?;
```

### Changed contracts

- Task names are `TaskName`. A `&'static str` name does not allocate; `String`,
  `&String`, `Arc<str>`, `Box<str>` and `Cow<'static, str>` are accepted
  unchanged. A borrowed `&str` that is not `'static` no longer converts; pass
  `name.to_owned()` instead.
- `TaskGroupId` values are unique within the process, across owners.
- A `TaskId` records the group that issued it; its JSON form is still the
  sequence number. A group returns `false` for another group's id from
  `wait_task`, `abort_task`, `abort_task_and_wait` and `contains_task` instead
  of reporting it finished or reaching its own task with the same number. Use
  `TaskId::group_id` or `TaskGroup::owns_task` to tell the cases apart.
- `RuntimeError::kind()` returns a `RuntimeErrorKind` to match on instead of
  inferring the reason from `operation()`. A submission to a closing or closed
  group, a closed operation, lane or metadata worker reports `Closed`; a
  poisoned group reports `Poisoned`. Both keep the
  `runtime.context.unavailable` code. `ServiceManager::start` on a closed
  owner now reports the cause's kind and code instead of an internal failure.
- `ShutdownReport::timed_out` counts only tasks that this shutdown's deadline
  aborted or left behind; an abort requested earlier through `abort_task` is
  not a timeout. `remaining_tasks` lists at most
  `ShutdownReport::REMAINING_TASKS_LIMIT` (64) tasks and counts the rest in
  `remaining_tasks_omitted`; `leaked` still includes them.
- A shutdown moves a group through `Closing`, `Closed` and `ShutdownCompleted`.
  A task woken by the shutdown already observes `Closed`.
- Poisoning a group and answering a component request with a closed child each
  log a warning and increment `TaskGroup::event_counts()`; diagnostics
  snapshots carry the same counts in `events`.
- `ServiceManager` runs its loop as a service task of the group passed to
  `new_with_task_group`. `shutdown_until(deadline)` returns no later than the
  earlier of the requested deadline and the parent group's installed deadline;
  without either it uses 30 seconds instead of 90.
- `OperationContext::wait` waits until no operation task is active and the
  owner has settled them, including tasks accepted during the wait. Close
  admission first when new tasks may still arrive.
- Blocking lanes admit waiters in arrival order, and a release wakes at most one
  waiter. The Tokio blocking pool has `RuntimeConfig::tokio_blocking_threads()`
  threads: the managed capacity plus `max(2, capacity / 8)` for direct
  `spawn_blocking`, DNS resolution and `tokio::fs`.
- Budget reservations are atomic along the ancestor chain. Near capacity, a
  request that races with another request's rollback can be rejected although
  the capacity is returned immediately afterwards; callers that retry on
  rejection are unaffected.
- `/drainz` accepts only `POST` by default and answers `GET` with `405` and
  `Allow: POST`. Kubernetes `preStop.httpGet` hooks set
  `ROCKETMQ_HEALTH_DRAIN_METHODS=GET,POST`; the repository charts and manifests
  already do. `ServiceLifecycleConfig` gains `drain_request_methods`.
- Health probes run each connection as a bounded lifecycle task (at most 64),
  read until the end of the request headers, and back off on `accept` failures
  caused by resource exhaustion.
- Broker and NameServer components take a mandatory `ChildServiceContext`
  instead of an optional one, so a missing owner is a compile error rather
  than a shutdown failure, and their shutdown always produces a report.
