# Message-path delivery checklist

This checklist closes the message send, Broker/store/high-availability, gRPC Proxy, and consumer optimization program. It measures Java alignment by business semantics and externally visible results, not by copying Java implementation structure. Rust-native ownership, scheduling, batching, resource budgets, and zero-copy techniques are expected.

The program has one optimal implementation path and one set of effective configuration values. It does not include legacy or Java compatibility profiles. DLedger is intentionally unsupported and excluded from implementation, migration, performance, scoring, and release qualification.

## Evidence states

Keep these states separate in reviews and release decisions:

1. **Implemented:** code and focused regression tests demonstrate the intended behavior.
2. **Locally smoke-validated:** a managed Rust NameServer and Rust Broker completed bounded end-to-end workloads.
3. **Release-qualified:** the target environment passed same-contract performance comparison, dynamic fault injection, and a six-hour resource-growth run, producing `status: "pass"` and `release_qualified: true`.

The requested implementation program is complete and locally smoke-validated. It is not yet release-qualified because target-environment fault, soak, and repeated same-hardware comparison evidence remains an operational release activity.

## Delivery register

| Area | Issue / PR | Delivered evidence |
|---|---|---|
| Baseline and evidence schema | #9211 / #9214 | Reproducible collector and metric contract |
| Producer retry, permission, transaction | #9216 / #9220 | Sync/async parity, fail-fast validation, owned blocking work |
| Mapped-file exact range | #9221 / #9222 | Bounded selection and copy metrics |
| POP non-blocking scheduling | #9225 / #9226 | Concurrent in-flight POP and owned shutdown |
| Single optimal store defaults | #9227 / #9228 | Validated production defaults without profiles |
| Cluster long-poll/control isolation | #9229 / #9230 | Independent lanes and ordering domains |
| Local Proxy concurrency/deadlines | #9231 / #9232 | Bounded concurrent execution and absolute deadlines |
| Deadline-driven receipt renewal | #9233 / #9234 | Earliest-deadline scheduler and control routing |
| HA notification and ACK frontier | #9235 / #9236 | No idle busy-poll and no high-offset head-of-line block |
| Broker pending budget | #9237 / #9238 | Count/bytes/age bound before large-body cloning |
| Proxy admission dimensions | #9239 / #9240 | Independent inflight, rate, request, and response budgets |
| Owner-backed mapped range | #9241 / #9242 | Lease, generation, retirement, and fallback contracts |
| Range-first HA/pull transfer | #9243 / #9244 | Zero payload heap-copy range path and wire parity |
| Reput ordered lanes | #9245 / #9246 | Required-lane frontier, bounded queues, recovery parity |
| Producer hot path | #9247 / #9248 | Direct batch encoding and Broker tracker isolation |
| POP BatchAck | #9249 / #9250 | Checkpoint grouping, partial mapping, bounded fallback |
| gRPC body ownership and egress | #9251 / #9252 | `Bytes` sharing, lazy response frames, output budget |
| LitePull and consumer workers | #9253 / #9260 | Effective pull concurrency, fixed workers, owned DelayQueue |
| RocksDB and Timer | #9261 / #9262 | Shared cache/WBM and segmented timer decode |
| gRPC business capabilities | #9263 / #9267 | Priority, LiteTopic, GZIP, compatible Broker batch, Pull ADR |
| Controller qualification evidence | #9268 / #9269 | T0-T5 and strict acknowledged-message audit schemas |
| End-to-end qualification harness | #9270 / #9273 | Policy, safe target confirmation, local four-workload smoke |
| Release and rollback handoff | #9274 | This checklist and the rollback runbook |

## Business-function checklist

### Send and Broker ingress

- [x] Sync and async retries use the same response-code policy and one absolute timeout budget.
- [x] Final callback and after-hook complete once per logical send.
- [x] Read-only Brokers reject normal, ordered, V1/V2, and batch sends consistently.
- [x] Compaction topics reject missing or blank keys.
- [x] Initial transaction listeners run in owned blocking work.
- [x] Compatible gRPC messages use Broker batch; FIFO, delay, transaction, or incompatible queues preserve their required semantics.
- [x] Broker pending work is bounded by count, retained bytes, and age before expensive cloning/task creation.

### Store and read path

- [x] Exact selection never copies the unread mapped-file tail.
- [x] Owner-backed ranges prevent retirement or unmapping while data is in use.
- [x] HA and pull range paths avoid payload heap copies when the platform supports them; fallbacks copy only the exact range.
- [x] CommitLog, ConsumeQueue, Index, Timer, transaction, and offset layouts remain compatible.
- [x] Reput publishes only through the minimum durable frontier of required ordered lanes.
- [x] RocksDB column families share a process-level cache/write-buffer budget without changing keys, values, WAL policy, or rebuild semantics.
- [x] Timer reads borrow mapped ranges and allocate only for a message crossing a range boundary.

### High availability

- [x] Caught-up HA connections sleep on append notification or heartbeat deadline instead of busy-polling.
- [x] Notification registration does not lose a concurrent append wake-up.
- [x] ACK completion uses authority- and membership-aware frontiers; high offsets do not block satisfied lower offsets.
- [x] Clean election remains the only accepted production failover mode.
- [x] Acknowledgements cannot complete before the configured local durability and replica requirements.
- [ ] Target environment proves segmented clean-failover RTO and strict acknowledged-message RPO.

### Consumer

- [x] POP dispatch does not wait synchronously for the network long poll and does not silently discard rejected schedule tokens.
- [x] Same-queue ordering is fenced while different queues can progress concurrently.
- [x] BatchAck preserves per-entry results, bounds malformed input, and falls back only for affected entries.
- [x] LitePull `pull_thread_nums` limits actual network concurrency.
- [x] Listener and retry work use fixed, bounded, lifecycle-owned workers.
- [x] Consecutive successful LitePull batches each create a pollable consume request.

### gRPC and Proxy

- [x] Long polls, short data operations, and control operations have independent bounded execution lanes.
- [x] ACK, renewal, and offset operations are not ordered behind a group-wide long poll.
- [x] Local Proxy execution is concurrent and deadline-aware rather than one global serial actor.
- [x] Receipt renewal wakes at the earliest deadline and uses a control lane.
- [x] Protobuf-to-Proxy-to-remoting body ownership uses reference-counted bytes.
- [x] Receive/Pull responses are emitted incrementally with response-byte backpressure and drop cleanup.
- [x] Priority, LiteTopic, and GZIP have validation and end-to-end business contracts.

### Scope and compatibility

- [x] Java alignment is expressed as message semantics, ordering, statuses, retry/DLQ, offsets, transaction, delay, FIFO, and gRPC behavior.
- [x] Rust internals remain free to use more efficient scheduling and memory ownership.
- [x] No compatibility-profile or legacy-default branch exists.
- [x] DLedger remains a typed unsupported configuration and is excluded from all delivery gates.
- [x] Deprecated pull APIs remain diagnosable with a LitePull migration path rather than false support.

## Local end-to-end smoke evidence

The managed run used a real Rust NameServer and Rust Broker. It is candidate-only smoke evidence, not a baseline/candidate performance comparison and not a production SLO.

| Workload | Completed | Throughput | p99 |
|---|---:|---:|---:|
| Sync send, 128 B | 100 / 100 | 65.871 msg/s | 16,430 us |
| Async send, 1 KiB | 200 / 200 | 262.411 msg/s | 723,747 us |
| Batch 16, 1 KiB | 256 / 256 | 964.860 msg/s | 37,542 us |
| LitePull, 1 KiB | 100 / 100 | 4,008.835 msg/s | 689 us |

All four workloads recorded zero send and response failures. The report deliberately recorded `release_qualified: false` because it was a single local smoke run.

## Performance evidence boundary

- Direct producer batch encoding reduced the existing same-machine microbenchmark latency by approximately 16.4% to 22.0%; this is a microbenchmark, not Broker TPS.
- A compatible 32-message POP acknowledgement is represented by one BatchAck RPC instead of 32 single ACK RPCs; target-environment latency still requires measurement.
- Exact-range selection bounds copied bytes to the requested length, and the owner-backed HA range path records zero payload heap-copy by invariant tests; end-to-end CPU and throughput remain environment-dependent.
- The LitePull regression that exposed only the first pollable batch completed 4 of 100 messages in an earlier run; the fixed managed smoke completed 100 of 100. This is functional regression evidence, not an isolated throughput A/B result.
- Estimates from the original audit remain hypotheses until the same policy, hardware, durability contract, and workload produce a versioned comparison report.

## Target-environment release gates

- [ ] Candidate worktree is clean and artifact SHA matches the qualification report.
- [ ] Baseline and candidate use the same hardware fingerprint, policy, business contract, durability contract, and workload parameters.
- [ ] Every workload has one warm-up and at least five measured repetitions.
- [ ] Every workload has zero missing messages and zero failed responses.
- [ ] Median throughput regression is at most 10% and median p99 regression is at most 15%.
- [ ] Dynamic Controller/Broker/process/network/storage fault evidence passes and artifact hashes validate.
- [ ] Six-hour RSS, task, descriptor, queue-age, cache, and pending-request series has no prohibited monotonic growth.
- [ ] Strict durability runs audit every `PutOk` message ID after clean failover with zero missing IDs.
- [ ] The final report contains `status: "pass"` and `release_qualified: true`.
- [ ] Operators have rehearsed the [message-path rollback runbook](message-path-rollback-runbook.md) in an isolated environment.

## Go / no-go rule

Release is **GO** only when every target-environment gate above is checked and the immutable evidence bundle is signed by the release owner. Any missing, mismatched, dirty, stale, or unverifiable evidence is **NO-GO**. Local smoke, Criterion, code-derived ratios, and estimates cannot override that decision.

Use [message-path release qualification](message-path-release-qualification.md) to generate evidence and [message-path release and rollback runbook](message-path-rollback-runbook.md) to deploy or recover it.
