# Runtime architecture and invariants

[中文文档](ARCHITECTURE-zh_cn.md)

This guide describes internal state ownership and ordering constraints. See the [README](README.md) for integration, API selection, and validation commands.

Application entrypoints own `RuntimeOwner`; components receive `ChildServiceContext` or narrower capabilities. A task scope owns lifecycle, while a resource budget owns explicit accounting. Neither substitutes for the other. Managed retained bytes describe reservations, not allocator usage, RSS, or an operating-system memory limit.

## State responsibility

| Responsibility | Implementation | Invariant |
| --- | --- | --- |
| Runtime composition | [owner](src/owner.rs), [service context](src/service_context.rs) | Child contexts share the owner's runtime and process resources. |
| Task admission and submission | [submission](src/task_group/submission.rs) | Registration and handle installation use the owner admission gate. |
| Final task settlement | [completion](src/task_group/completion.rs) | Future destruction precedes failure accounting and completion publication. |
| Active task and child registration | [registry](src/task_group/registry.rs) | Child links are weak; live descendants retain their ancestors. |
| Deadline and retained shutdown result | [coordinator](src/task_group/shutdown.rs), [group façade](src/task_group.rs) | Later calls never extend an accepted deadline or replace a published report. |
| Blocking admission and execution | [admission](src/blocking/admission.rs), [executor](src/blocking/executor.rs) | Managed lanes share one global capacity limit; a running closure retains its permit until it exits. |
| Resource reservations and dynamic generations | [budget](src/resource_budget/budget.rs), [dynamic keys](src/resource_budget/dynamic.rs), [queue](src/resource_budget/queue.rs) | Reservations account along the ancestor chain; closed generations reject new admission and incoming permit rebinds. |
| Scheduled drivers and runs | [scheduled tasks](src/scheduled.rs) | Both belong to the scheduler's task group; the entrypoint determines cancellation ownership. |
| Aggregate and bounded task scans | [task diagnostics](src/task_group/diagnostics.rs) | Local counts exclude children; bounded details disclose truncation. |
| Diagnostic schemas and collection | [diagnostics](src/diagnostics.rs) | V1/V2 public types and wire names remain stable. |
| Sanitized conversion | [conversion](src/diagnostics/conversion.rs) | Labels derive from typed identity, never vector position; conversion performs no business I/O. |
| Process lifecycle transitions | [service lifecycle](src/service_lifecycle.rs) | Readiness, terminal states and the first shutdown request are state decisions. |
| Health-probe transport and routing | [probe](src/service_lifecycle/probe.rs) | Routes call lifecycle APIs instead of modifying state atomics. |
| Metadata admission and publication | [metadata actor](src/metadata_io.rs) | Actor state owns waiters, retained bytes and generation ordering under one coordination protocol. |
| Metadata target identity and retirement | [target registry](src/metadata_target.rs) | Actors under one owner share target histories; a slot is reusable only after retirement checks pass. |
| Atomic replacement and durability | [filesystem](src/metadata_io/filesystem.rs) | Platform operations do not modify actor state or diagnostic schemas. |

Keep state with its existing owner and expose capabilities through the established public façade modules. Splitting implementation files must not introduce a second state owner. A new task classification must update the exhaustive `TaskKind` mapping and wire conversion; task-kind arrays and traversal share the same private enumeration.

## Completion and shutdown ordering

Task completion records the destruction of a registered future, not business success. Destruction precedes failure accounting and completion publication, including an abort before the first poll or a panic during destruction. `TaskGroup::wait_task` treats an absent local task as finished; `abort_task_and_wait` returns false for an absent task.

`OperationOutcome` records each accepted operation task separately from `ShutdownReport.completed`: normal return, operation cancellation, deadline, owner cancellation, panic, or abort before another outcome was selected. Its finalizer publishes only after the user future is destroyed. Rejected submissions do not increment the six outcome counters, and no per-task outcome history is retained. Draining operations ignore owner cancellation until the accepted work completes, is operation-cancelled, expires, or is explicitly aborted.

The shutdown coordinator seals admission and retains the published result. An immediate shutdown report cannot confirm future destruction or final I/O; services with final I/O must perform it through cooperative cleanup. See the [shutdown API contracts](README.md#service-lifecycle-and-shutdown) for the guarantees of each entrypoint.

A blocking observer's timeout does not release the real closure's capacity or settle its result. Metadata acceptance does not establish durability, and an observer timeout does not establish rollback. Unknown write outcomes retain reconciliation obligations.

Closed dynamic generations reject admission and incoming permit rebinds, while outstanding permits can still release or move out. Queue accounting follows the [dequeue API's transfer policy](README.md#resource-budgets-and-queues).

## Observation boundaries

`ServiceLifecycleObserver` attaches once while the process is Starting. State changes and observer installation share a short synchronization boundary; callbacks run after all lifecycle locks have been released. Only committed changes emit events. Concurrent transitions can deliver callbacks out of order, so each event identifies its actual previous and next state. Callbacks must be short, nonblocking and panic-free; they must not perform I/O or flush exporters. Exporter shutdown belongs to the composition root.

`MetadataIoObserver` and selected-name `ScheduledTaskObserver` use weak references, so diagnostic consumers do not become write or execution owners. The metadata registry capacity bounds its resource scan; schedule lookup cost is proportional to the fixed selection supplied by the component.

## Scheduling boundaries

Ordinary and operation-bound fixed-delay adapters share execution and final settlement; their outer cancellation owners remain distinct. Legacy rate and Cron protocols retain their timing semantics. See the [entrypoint matrix](README.md#scheduled-tasks) for scheduling policies and compatibility fields.

Consumer-specific drain ordering and persistence policies are documented with the consumer; see [Broker transaction maintenance](../rocketmq-broker/README.md#transaction-maintenance).

## Sampling cost and consistency

Service-context V1/V2 sampling reads blocking aggregates directly from the registry. It does not allocate individual blocking task names or detail objects. The explicit `BlockingExecutor::snapshot` API retains its full-detail contract. Task subtree and local counts come from the same traversal.

Age maxima, long-running thresholds and group counts require scanning the observed entries; aggregate sampling does not maintain extra counters on task submission or completion. A scan is consistent per visited registry entry, not globally atomic across groups or blocking lanes. Concurrent changes may therefore be observed at different times. Local counts are included in the subtree total from that same traversal.

Detail scan/output budgets bound only the detail section; they do not limit the aggregate age scan. See [benchmark measurement boundaries](README.md#benchmark-measurement-boundaries) for sampling setup, timing scope, and allocation measurements.

## Metadata target retirement

Target registration is process-local, not a cross-process lock or a guarantee of crash recovery. An [owner plan](src/owner.rs) accepts a nonzero metadata target capacity (default 4,096). Idle history and fenced history consume slots. Each actor independently bounds its retained resource cache by the same capacity: another actor retiring shared history must not grow an older actor's tombstones without limit.

Normal actor replacement inherits target identity, resource ownership and confirmed generation. Explicit retirement has a different contract: after admission closes and the coordinator finishes, the registry atomically checks identity, durable generation, absence of live write authority and absence of a reconciliation fence. Only then may a new identity reuse the path and slot. Old open actors cannot rebind that retired identity; replace them to release their tombstones. Settled receipts retain their original result without owning a writer. Compare receipt and actor target identities before using a newer generation as evidence about an old write.

Registry statistics expose capacity, retained histories, live owners, idle histories, fenced targets and remaining capacity without paths. Fenced and live counts can overlap. Unknown commits stay fenced: runtime has no generic format-independent recovery or unconditional fence-clearing operation.
