# Runtime completion, admission, and shutdown contracts

- Status: Accepted design; implementation is delivered and verified separately.
- Date: 2026-09-12
- Owners: Runtime, Broker, NameServer, Auth, Transport, Store, and Observability maintainers.
- Scope: the runtime optimization development plan, including completion, metadata ordering, scoped blocking, bounded work, configuration, diagnostics, and consumer integration.

## Context

The runtime has a useful ownership tree and an independent resource budget tree.
The September design review identified gaps where caller observation was treated
as execution completion, scheduler ticks could accumulate waiting futures, and
shutdown reports did not describe every owned activity. This decision preserves
Tokio and the existing root/child capabilities while defining the contracts that
the implementation must establish. Acceptance of this document does not assert
that the current implementation already satisfies them.

This extends the [single writer and runtime ownership decision](single-writer-runtime-ownership-adr.md).
It does not introduce a second runtime backend, a process-wide replacement for
business transaction ownership, or a new on-disk format.

## Completion and cancellation

Caller observation and execution have independent states. A caller can stop
waiting while an operation remains queued or running. A wait timeout must not
release a running operation's execution capacity, retained payload charge, or
exclusive write ownership.

Each async task has one terminal settlement, including cancellation before its
first poll, external `JoinHandle::abort`, panic, normal return, and shutdown
races. Completion is published after the user future and its resources are
destroyed. Requesting abort is not proof of destruction. Active records remain
until settlement; terminal history is bounded. Finalizers are constructed before
submission and do not execute user callbacks or panic in `Drop`.

Blocking work has an execution-owned completion record independent of its
caller's wait. The record survives a dropped observer, and also settles if a
submitted closure is destroyed before it starts. Already running blocking work
cannot be forcibly cancelled. Closed runtime reports retain evidence of real
remaining work rather than implying that a deadline stopped a system call.

## Metadata write ownership and receipts

Within one runtime owner, an explicitly registered metadata target has at most
one writer that can still modify its file. Ownership transfers only after the
actual filesystem operation ends. A newer snapshot may replace a queued complete
snapshot, but cannot bypass a running write. Partial commands and deltas are not
eligible for latest-snapshot coalescing.

Target registration binds the logical resource to a normalized target identity.
Multiple actors or resource names cannot independently acquire the same target.
Registration outlives an unfinished write, including actor shutdown or rebuild.
Retirement closes admission, waits for real work and observers, and invalidates
old handles before an identity can be reused. Dynamic registrations and retained
generation/query history are bounded without silently discarding live deduplication
state. Path aliases and platform normalization have explicit supported rules;
this contract does not claim cross-process or arbitrary hard-link exclusion.

`Accepted` means an immutable snapshot entered bounded ownership. `Durable(g)`
means that generation or a newer coalesced generation completed the applicable
persistence protocol. A wait timeout means the caller has not confirmed the
result. Late success updates authoritative state; it does not retroactively send
success to a departed observer. Failure after target replacement can leave
visibility or durability unconfirmed and must not be described as definitely
uncommitted. Never retry an old snapshot over an already completed newer one.

Broker, NameServer, and Auth mutation owners retain a change identity through
late completion or an explicit reconciliation state. They must not roll back
memory on a mere observation timeout and then publish a stale replacement.
The temporary-write, file-sync, replace, and directory-sync protocol remains
unchanged unless a separate compatibility decision and recovery tests justify it.

Initially the actor may serialize different resources as well. Any subsequent
cross-target concurrency is bounded by actor slots, lane policy, and the global
budget. Completion storage is reserved with execution admission; it cannot be
lost behind a full request queue or require an unowned background observer.

## Scoped blocking and drain

Blocking execution uses the injected owner's runtime handle. Polling a caller
future on another host runtime does not change execution ownership. Shared lanes
have a process gate and capabilities derived for a child have a scope gate.
Cloning shares rights and limits; it does not create new capacity or a new owner.

Both gates transition monotonically from `Open` to `Draining` to `Closed`:

| State | Ordinary submissions | Previously accepted drain work | Running closures |
| --- | --- | --- | --- |
| Open | Subject to bounded admission | Uses the accepted operation's rights | Retains real execution charges |
| Draining | Rejected | May perform necessary I/O within its original scope, quota, and deadline | Remains tracked |
| Closed or expired | Rejected | Cannot start further I/O | Reported until actual exit |

Drain authorization is tied to an accepted operation, not a public bypass flag.
Clones share a finite allowance and cannot extend its deadline. A bounded
finalization slot registered by a lifecycle owner permits its final offset/config
flush without reopening ordinary admission. Scope shutdown leaves siblings able
to use their own capabilities; root closure closes every ordinary capability.
Gate checking and acceptance have a defined linearization point across async
capacity waits.

Application shutdown stops readiness, incoming requests, and periodic producers;
drains accepted work and finalization; then closes their metadata/blocking
providers. Independent subtrees may stop concurrently. Dependencies must not be
closed before the producers that need them. All phases receive the same or an
earlier absolute deadline, including final runtime destruction.

## Bounded work

Admission covers retained items and bytes before constructing large payloads or
waiting tasks. Queueing and in-flight processing retain the appropriate permit;
transfer within a budget tree preserves common-ancestor charges without double
charging or an uncharged window. Explicitly document caller-owned memory that
has not entered the runtime budget.

Metadata bounds separately cover snapshots, operations, observers, registered
targets, and retained completion/query state. Dropped and expired receipts can
unregister exactly once. A shared notification primitive is not itself an upper
bound on the number of observers.

`ScheduledTaskGroup` is the recommended scheduling API. Cadence, concurrency,
missed ticks, and failure policy are explicit. A run slot is acquired before
creating/spawning a run. Pending catch-up is bounded, and coalescing retains one
pending intent. Initial Broker policies are:

- Offset flush: fixed rate, serial, one latest pending intent; retain final flush.
- Broker registration: fixed rate, serial, skip missed runs; verify registration
  freshness and preserve role/isolation guards and RPC deadlines.
- Member-group synchronization: fixed delay, serial; verify topology freshness.

New builders/options carry these policies without silently breaking public
configuration literals. Legacy behavior is migrated by consumer and deprecated
under an explicit source/behavior compatibility policy.

## Reports, configuration, and health

Driver completion is not run success. Reports distinguish cooperative exit,
abort request, confirmed abort, panic, deadline expiry, real remaining blocking
work, and unconfirmed durability. Channel closure without a terminal record is
not normal completion. Each task and shared lane contributes once to its owning
report. A returned report is a frozen observation; live diagnostics may later
show convergence.

V1 diagnostic fields keep their shape and scope. V2 distinguishes direct group,
subtree, and process-shared values. Sampling has a work budget as well as an
output limit; truncation and sample age are visible. Public labels remain low
cardinality and never contain payloads, credentials, resource paths, or request
identifiers.

Root lane policy is the effective source for shared blocking capacity and
defaults. Actor limits cover actor admission; request deadlines only tighten
limits. Legacy inactive metadata timeout fields have an explicit migration path.
An effective profile exposes the resulting values and their sources.

Detected visible memory constraints and managed-byte allowance are distinct.
New policy can choose explicit managed bytes or a fraction and headroom without
silently changing legacy defaults. Linux discovery resolves process cgroup
membership through mount roots and visible ancestors, handling v1/v2, namespaces,
unlimited values, missing files, and permission failures. The budget is not a
complete OOM guarantee and cannot revoke existing permits when lowered.

Critical task failure withdraws dependency readiness even while scheduling
heartbeats continue. The failure fact is sticky; bounded event delivery is only
a notification. A surviving owner makes the health/shutdown decision. Expected
shutdown exits are not failures, and idle services are not declared dead solely
for having no traffic. Storage tasks are not automatically restarted without a
separate idempotency, fencing, backoff, and retry-budget design.

## Implementation and verification map

These identifiers map to development work, not completed implementation claims.
Each batch has its own linked issue, pull request, local results, and merge.

| Work | Owner role | Implementation boundary | Required evidence |
| --- | --- | --- | --- |
| D00 | Runtime | This decision and regression map | Source and consumer paths checked; future outcomes explicit |
| D01 | Runtime | Task finalizer and active registry | T01-T03: before-poll/external abort, races, user-resource destruction |
| D02 | Runtime | Blocking execution completion | T04-T05: observer timeout/drop, never-started closure, panic |
| D03 | Runtime and metadata consumers | Real-completion ordering and target identity | T06-T09: two generations, late failure, actor rebuild, business publication |
| D04 | Runtime and service owners | Owner handle, scope gates, drain/finalization | T10-T12: foreign host, siblings/root, final I/O |
| D05 | Runtime and admission consumers | Snapshot/observer/key/payload bounds | T13-T14: sustained duplicate submits, retirement, permit conservation |
| D06 | Runtime and Broker | Bounded schedules and three Broker migrations | T15-T16: stalled ticks, bounded recovery, freshness and final flush |
| D07 | Runtime and Broker | Unified deadline and shutdown report | T17-T18: failed runs, abort confirmation, shared-lane deduplication |
| D08 | Runtime | Effective configuration and migration options | T19: behavior for each field, request tightening, old literal compatibility |
| D09 | Runtime | Managed memory and Linux discovery | T20: injected layouts, real Linux verification, platform/default compatibility |
| D10 | Runtime and Observability | V2 diagnostics and bounded sampling | T18/T21: scope, truncation, V1 decoding, redaction and sampling cost |
| D11 | Runtime and business owners | Critical failures and readiness | T22: healthy heartbeat with failed business work, full notifications, normal exit |
| D12 | Runtime and direct API consumers | Recommended facade, compat exports, examples | T23: compile examples and root/child capability constraints |
| D13 | Runtime and integration owners | Load, churn, benchmarks, consumer integration | T24: bounded saturation, recovery, real resources and shutdown |

Existing regression homes are `rocketmq-runtime/tests/metadata_io_actor.rs`,
`runtime_resource_ownership.rs`, `runtime_model.rs`, `resource_budget_tree.rs`,
`task_submission_stack.rs`, the two lifecycle/resource Loom targets, and
`service_context_scope_compile_fail.rs`. Defect probes must be changed to assert
the required outcome, rather than treating reproduction of old behavior as a
successful fix. Real Tokio/Drop tests complement simplified Loom models.

Direct consumer inspection starts at:

- `rocketmq-broker/src/broker_runtime/control_plane.rs` and `lifecycle.rs`:
  periodic work, finalization, and component shutdown summaries.
- `rocketmq-namesrv/src/bootstrap/config_apply.rs` and
  `rocketmq-namesrv/src/kvconfig/persistence.rs`: publication after persistence.
- `rocketmq-auth/src/authorization/metadata_provider/local.rs` and
  `rocketmq-auth/src/authentication/provider/local_authentication_metadata_provider.rs`:
  durable metadata and typed error mapping.
- `rocketmq-transport/src/session_executor.rs` and `rocketmq-store/src/runtime.rs`:
  accepted operation drain and process-root budgets.
- `rocketmq-observability/src/runtime_diagnostics.rs`: exported diagnostic scope.

Use synchronization gates and virtual time for deterministic concurrency tests;
release and join every real blocking test closure. Filesystem adapters establish
interleaving behavior, not power-loss guarantees. Select actual affected features
and consumers, including feature absence; standalone projects follow their local
validation guide.

Measure same-group producer contention, budget depth/shared ancestors, irrelevant
wakeups, hot/cold metadata targets, CPU/I/O contention, stalled schedules,
diagnostic sampling, and lifecycle churn. Record workload, environment, latency
distributions, throughput, CPU, allocations, retained objects, and shutdown tails.
Do not infer production SLOs from short queue microbenchmarks. Performance
thresholds and service freshness/recovery windows are recorded with the chosen
workload before comparing candidate results. Unrun platform or load scenarios
remain explicitly unverified.
