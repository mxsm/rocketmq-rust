# Proxy Cluster Keyed Execution ADR

- Status: Accepted
- Date: 2026-07-29
- Owner: Proxy Cluster
- Decision scope: command admission, ordering, remote I/O ownership, cancellation, and shutdown

## Context

The original Cluster adapter executed every remote operation on one worker.
An intermediate implementation replaced that worker with sixteen fixed hash
lanes. It reduced common head-of-line blocking, but unrelated keys could still
collide, data traffic could consume the complete admission budget, and the
fixed lane count was also the implicit and unconfigurable I/O limit.

The adapter must preserve RocketMQ request and response mapping, timeout and
retry semantics, consumer ordering, and exclusive mutable producer access.
Production tasks must remain descendants of the injected service context.

## Decision

The Cluster adapter uses exact structural ordering keys rather than hash
shards. The registry creates one managed lane for each active key and retires
it after a configured idle interval. Registry insertion, enqueue, and
retirement are atomic under a short synchronous metadata lock; no lock guard is
held across remote I/O.

Each keyed lane owns its cache and producer state. Commands with the same key
execute FIFO on that owner. Distinct keys use separate tasks and can enter
remote I/O concurrently up to the configured global limit. Producer commands
use the effective producer group as their key, so one lane exclusively owns
each mutable producer instance.

Admission is fail-fast and retains count and byte permits from enqueue through
execution. Request deadline and maximum queue age are checked before I/O.
The root budget and I/O semaphore both reserve capacity for readiness traffic,
so data saturation cannot starve the control path.

Every lane is spawned through the injected `ChildServiceContext`. Caller drop,
deadline expiry, and service shutdown cancel the owned command future. Permit
types release count, bytes, and inflight capacity on normal return,
cancellation, timeout, or panic. Shutdown closes admission first, cancels
active work, drains queued replies with typed errors, shuts producers before
clients, waits for lane tasks under the shared deadline, and finally shuts down
the Client runtime.

## Fencing model

The registry assigns a monotonic generation to every key-lane incarnation.
Retirement removes a lane only when both the key and generation still match
and its queue is empty. A late retirement cannot remove a replacement lane.

Remote result commit is fused to the single keyed owner: there is never more
than one active command future for the same key. Cancellation drops that future
before any later state mutation can run. Consequently no detached result can
commit after a replacement generation starts.

## Diagnostics

`RocketmqClusterClient::execution_diagnostics` exposes only low-cardinality
aggregates: active keys and lane tasks, retained items and bytes, oldest queued
age, current and maximum inflight work, admission/rejection/timeout/
cancellation/shutdown counters, and closed state. Ordering keys, topics,
groups, request IDs, message bodies, and credentials are never exported as
labels.

## Rejected alternatives

- A larger fixed hash-shard array still serializes unrelated colliding keys.
- One shared async mutex around `ClusterWorkerState` recreates global
  head-of-line blocking and would hold a guard across remote I/O.
- Detached per-command tasks lose producer ownership and bounded shutdown.
- Increasing channel capacity hides overload rather than controlling it.

## Compatibility

The internal fixed-lane implementation is deleted. Source compatibility for
that private implementation is not retained. RocketMQ wire fields, response
codes, persisted formats, retry behavior, and implemented request semantics
remain unchanged.

## Evidence

- `rocketmq-proxy-cluster/src/cluster_admission.rs`
- `rocketmq-proxy-cluster/src/cluster_execution.rs`
- `rocketmq-proxy-cluster/src/cluster_behavior_tests.rs`
- `scripts/runtime-audit.ps1`
- `cargo test -p rocketmq-proxy-cluster`
- `cargo clippy -p rocketmq-proxy-cluster --all-targets --all-features -- -D warnings`
- `cargo bench -p rocketmq-proxy-cluster --features bench-support --bench cluster_executor -- --noplot`
- `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline`
