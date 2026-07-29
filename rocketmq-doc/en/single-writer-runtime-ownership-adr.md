# Single Writer and Runtime Ownership ADR

- Status: Accepted
- Date: 2026-07-29
- Owner: Runtime, Transport, Broker, and Dashboard maintainers
- Decision scope: async lock ownership, connection writes, session generations, long-lived queues, and blocking budgets

## Context

Several internal execution paths duplicated ownership or made capacity implicit.
The transaction service wrapped the complete bridge in an async mutex even
though most bridge operations only needed immutable access. Broker wakeup
responses added sixty-four hash-sharded mutexes above Transport's per-session
writer. The Web Dashboard held one mutable admin-session slot while remote
requests ran. Application entrypoints repeated fixed blocking-thread counts,
and the admin TUI used an unbounded action channel.

These structures could serialize unrelated work, hide memory and thread
budgets, and make shutdown ownership difficult to prove. The refactor must not
change RocketMQ wire fields, response codes, persisted records, recovery
semantics, or implemented protocol behavior.

## Decision

### Transaction state

`DefaultTransactionalMessageService` owns `TransactionalMessageBridge`
directly. Bridge operations use immutable access, and the operation-queue map
keeps its existing narrow internal lock. Topic lookup and store calls do not
borrow an outer bridge guard across `.await`. Store failures continue through
the established typed result paths.

### Connection writes

Transport's bounded per-session writer is the only owner allowed to write a
session socket. It preserves complete-frame serialization, queue admission,
deadlines, overload reporting, and drain-or-reject shutdown behavior. Broker
wakeup paths submit responses directly to that capability. The obsolete
sixty-four-shard Broker mutex layer and its hash routing are deleted.

Each session has an independent writer actor. A slow peer can consume only its
own writer progress and admission budget; it cannot collide with a different
connection through a process-wide shard. An atomic send lease closes admission
and lets retirement await in-flight sends without retaining a lock guard across
network I/O.

The session capability exposes a low-cardinality writer snapshot with queue
capacity, retained items and bytes, oldest and maximum queue age, write
latency, accepted/completed/failed/rejected counts, and deadline expirations.
It never exports message content, topics, groups, or request identifiers.

### Dashboard generations

Dashboard admin operations take immutable `&self` receivers. The lifecycle
slot contains an `Arc` to a managed generation and is held only while a handle
is selected or replaced. A short per-generation lock increments an explicit
lease count and clones an immutable capability; the lock is released before
remote RPC execution. The lease is released before validating that the
generation and configuration snapshot are still current.

Reconnection installs a new monotonic generation immediately. Retirement of
the old generation is owned by a child service task, closes new lease
admission, and shuts it down after its in-flight leases complete.
A result from a replaced generation returns a typed retryable configuration
error and cannot be committed as current.

### Queue and blocking budgets

The admin TUI action channel is bounded. Refresh progress is best-effort and
coalesced when full; command results use bounded asynchronous admission.
Diagnostics report capacity, queued items, accepted, rejected, and coalesced
counts together with retained-byte estimates and oldest queued age. Production
Tokio unbounded channels remain prohibited.

`RuntimeConfig` derives worker and blocking limits from the cgroup-aware
available parallelism value. The blocking limit is clamped to a supported
process bound and may be overridden through the typed configuration API only
within that bound. Every blocking lane is capped by the same effective global
limit, while its own queue admission remains independently bounded.
Application entrypoints no longer carry divergent fixed constants.

Ambient runtime adapters are not expanded. New background work remains under
injected `ServiceContext` or child task-group ownership, and the enforcing
runtime audit governs the remaining reviewed adapter identities.

## Rejected alternatives

- Keeping a smaller or larger Broker shard array still duplicates ownership
  and permits unrelated connections to collide.
- A mutex around the complete transaction bridge or Dashboard session still
  holds coordination state across remote or store I/O.
- Detached Dashboard retirement can outlive the application lifecycle.
- Raising Tokio's blocking pool without lane queues hides overload rather than
  bounding it.
- An unbounded UI action channel turns burst traffic into unaccounted memory.

## Compatibility

The removed locks, shard helpers, mutable receiver requirements, and fixed
entrypoint constants are internal source contracts and are not retained.
RocketMQ wire/protocol fields, response mapping, persisted layout, recovery
behavior, and user-visible command semantics remain unchanged.

## Evidence

- `rocketmq-broker/src/transaction/queue/transactional_message_bridge.rs`
- `rocketmq-broker/src/transaction/queue/default_transactional_message_service.rs`
- `rocketmq-broker/src/processor/pull_message_processor.rs`
- `rocketmq-transport/tests/session_concurrency.rs`
- `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client.rs`
- `rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/rocketmq_tui_app.rs`
- `rocketmq-runtime/src/config.rs`
- `rocketmq-runtime/tests/runtime_model.rs`
- `scripts/runtime-audit.ps1`

### Performance comparison

The Transport session-writer benchmark runs 256 request/response round trips
through the bounded canonical writer. Both measurements used commit-clean
checkouts, the same benchmark source, the same command, and the same host:

- Host: Windows, Intel Core i7-11700K, 8 cores/16 logical processors, 31.9 GiB RAM.
- Baseline source: `8b001cd57fd632725fef6e2098de5a764d4af674`; the clean local measurement
  commit `e9edd2d6652ea03b31a8a6e3343a307ad9e5fa92` adds only the benchmark harness
  that is committed in the candidate.
- Candidate: `4795a93227f28b9ec6f0bbfcbd770d0c8101bdcc`.
- Command:
  `cargo bench -p rocketmq-transport --bench frame_write -- transport_session_writer/round_trip_256 --noplot --sample-size 20 --warm-up-time 2 --measurement-time 5`.
- Baseline interval/center: 4.8223/4.9387/5.0976 ms.
- Candidate interval/center: 3.3473/3.3993/3.4579 ms.
- Center-value change: -31.17%; the candidate is faster and remains within
  the no-more-than-5% regression threshold.

The correctness gate ran before this comparison. It includes Transport
same-session ordering, cross-session progress, bounded writer diagnostics,
deadline/retirement behavior, runtime Loom models, Dashboard generation
fencing, the TUI burst contract, Broker shutdown lock release, and the
three-controller/two-broker failover-and-rejoin regression.
