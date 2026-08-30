# Remoting processor performance baseline

## Scope and interpretation

This report records engineering evidence for the canonical processor and
response ownership model. The benchmarks are review artifacts, not CI gates.
They do not install an API fingerprint, source hash, or machine-specific pass
threshold.

The benchmark source revision is `8748fe79a6ee45322b55d2d1cc3f622cad568cac`
plus the processor-ownership working-tree changes. Measurements in this report were collected
on Windows 11 Pro 10.0.26200, an Intel Core i7-11700K (8 cores, 16 logical
processors), 31.9 GiB RAM, `rustc 1.95.0 (59807616e 2026-04-14)`, and Cargo
1.95.0. Results from other machines must be compared only with a baseline
collected on the same host, toolchain, power plan, feature set, and revision.

## Reproduction

Compile all three groups without running them:

```powershell
cargo bench -p rocketmq-transport --features test-support --bench processor_dispatch --no-run
cargo bench -p rocketmq-transport --features test-support --bench response_plan --no-run
cargo bench -p rocketmq-broker --bench pull_pop_response_benchmark --no-run
```

Run the quick engineering profile:

```powershell
$env:ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS = "1"
$env:ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS = "1"
$env:ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE = "10"
cargo bench -p rocketmq-transport --features test-support --bench processor_dispatch
cargo bench -p rocketmq-transport --features test-support --bench response_plan
cargo bench -p rocketmq-broker --bench pull_pop_response_benchmark
```

The recorded transport run used the same variables and added Criterion's
`--noplot` argument:

```powershell
cargo bench -p rocketmq-transport --features test-support --bench response_plan -- --noplot
cargo bench -p rocketmq-transport --features test-support --bench processor_dispatch -- --noplot
```

The Broker target was compiled with the `--no-run` command above and its
Criterion executable was measured directly as follows. The executable hash is
build-specific, so normal reproduction should use the Cargo command above:

```powershell
& target\release\deps\pull_pop_response_benchmark-a937fb6b8bd7fe27.exe --bench --noplot
```

Run the related writer, deferred, and shutdown groups when reviewing a release:

```powershell
cargo bench -p rocketmq-transport --features test-support --bench write_pipeline
cargo bench -p rocketmq-transport --features test-support --bench admission_pending_hooks
cargo bench -p rocketmq-transport --features test-support --bench frame_write
cargo bench -p rocketmq-broker --bench broker_runtime_lifecycle_bench
```

For a formal comparison, increase warm-up to at least 5 seconds, measurement
to at least 10 seconds, and samples to at least 50. Pin both revisions to the
same machine and compare Criterion's saved baselines. Do not compare HTML
reports copied from different hosts.

## Benchmark coverage

| Group | Cases | Primary evidence |
|---|---|---|
| `transport_processor_dispatch` | materialized command-shape reference; canonical embedded inline dispatch with 0 B, 128 B, and 4 KiB requests | absolute channel-free dispatch latency and throughput; materialization reference is not a complete dispatcher |
| `transport_response_plan` | contiguous legacy materialize/encode; canonical `Bytes`; canonical four-segment body at 128 B, 4 KiB, and 256 KiB | encode/preparation cost, body-copy versus shared `Bytes`, segmented zero-concat preparation |
| `broker_pull_pop_response` | Pull materialized/canonical bytes and Pop materialized/canonical segments at 4 KiB and 256 KiB | real Pull/Pop headers, encode/preparation cost, heap and segmented response ownership |

`ResponsePlanPreparationHarness` is available only through the explicit
`test-support` feature. It exercises the private bind-and-prepare path without
adding response binding, complete-frame encoding, or body access to the
public API.

## Quick-run results

The quick profile is intended to catch order-of-magnitude regressions and
prove that every case runs. Criterion reports an estimate interval for typical
iteration time; it does not report a request-latency P99. Record the exact
console intervals here when refreshing the baseline, together with the Git
revision and machine description above.

The intervals below are Criterion's `[lower, point estimate, upper]` output
from the 2026-08-30 quick run. Throughput is Criterion's byte or element
throughput derived from the same interval; it is not an end-to-end network
rate.

| Case | Input | Criterion estimate interval | Derived throughput |
|---|---:|---:|---:|
| `transport_processor_dispatch/legacy_materialized_contract_reference` | 0 B | `[30.686, 31.278, 31.865] ns` | `[31.382, 31.972, 32.588] Melem/s` |
| `transport_processor_dispatch/canonical_embedded_inline` | 0 B | `[2.7989, 2.8500, 2.8958] us` | `[345.33, 350.87, 357.29] Kelem/s` |
| `transport_processor_dispatch/legacy_materialized_contract_reference` | 128 B | `[106.78, 109.57, 111.99] ns` | `[8.9296, 9.1266, 9.3653] Melem/s` |
| `transport_processor_dispatch/canonical_embedded_inline` | 128 B | `[2.9934, 3.0584, 3.0990] us` | `[322.68, 326.97, 334.07] Kelem/s` |
| `transport_processor_dispatch/legacy_materialized_contract_reference` | 4 KiB | `[472.55, 506.57, 536.95] ns` | `[1.8624, 1.9741, 2.1162] Melem/s` |
| `transport_processor_dispatch/canonical_embedded_inline` | 4 KiB | `[2.9568, 3.0075, 3.0661] us` | `[326.15, 332.50, 338.20] Kelem/s` |
| `transport_response_plan/legacy_materialize_and_contiguous_encode` | 128 B | `[560.39, 564.88, 570.65] ns` | `[213.91, 216.10, 217.83] MiB/s` |
| `transport_response_plan/canonical_bytes_prepare_zero_copy_body` | 128 B | `[560.51, 571.44, 579.98] ns` | `[210.47, 213.62, 217.78] MiB/s` |
| `transport_response_plan/canonical_segmented_prepare_zero_copy_body` | 128 B | `[666.17, 674.82, 687.89] ns` | `[177.46, 180.89, 183.24] MiB/s` |
| `transport_response_plan/legacy_materialize_and_contiguous_encode` | 4 KiB | `[638.14, 649.52, 660.21] ns` | `[5.7780, 5.8731, 5.9778] GiB/s` |
| `transport_response_plan/canonical_bytes_prepare_zero_copy_body` | 4 KiB | `[564.69, 570.22, 576.44] ns` | `[6.6177, 6.6899, 6.7554] GiB/s` |
| `transport_response_plan/canonical_segmented_prepare_zero_copy_body` | 4 KiB | `[652.44, 661.55, 673.03] ns` | `[5.6680, 5.7663, 5.8468] GiB/s` |
| `transport_response_plan/legacy_materialize_and_contiguous_encode` | 256 KiB | `[28.614, 29.236, 29.784] us` | `[8.1970, 8.3506, 8.5321] GiB/s` |
| `transport_response_plan/canonical_bytes_prepare_zero_copy_body` | 256 KiB | `[574.84, 584.87, 597.69] ns` | `[408.47, 417.43, 424.71] GiB/s` |
| `transport_response_plan/canonical_segmented_prepare_zero_copy_body` | 256 KiB | `[760.15, 777.78, 795.53] ns` | `[306.89, 313.90, 321.17] GiB/s` |
| `broker_pull_pop_response/pull_legacy_materialize_and_encode` | 4 KiB | `[537.84, 555.05, 575.30] ns` | `[6.6308, 6.8727, 7.0926] GiB/s` |
| `broker_pull_pop_response/pull_canonical_bytes_prepare` | 4 KiB | `[452.18, 475.61, 496.15] ns` | `[7.6886, 8.0206, 8.4362] GiB/s` |
| `broker_pull_pop_response/pop_legacy_materialize_and_encode` | 4 KiB | `[1.3499, 1.3876, 1.4232] us` | `[2.6803, 2.7491, 2.8259] GiB/s` |
| `broker_pull_pop_response/pop_canonical_segmented_prepare` | 4 KiB | `[1.3353, 1.3869, 1.4645] us` | `[2.6048, 2.7506, 2.8569] GiB/s` |
| `broker_pull_pop_response/pull_legacy_materialize_and_encode` | 256 KiB | `[29.942, 30.487, 30.897] us` | `[7.9018, 8.0080, 8.1539] GiB/s` |
| `broker_pull_pop_response/pull_canonical_bytes_prepare` | 256 KiB | `[452.37, 471.14, 491.08] ns` | `[497.15, 518.20, 539.69] GiB/s` |
| `broker_pull_pop_response/pop_legacy_materialize_and_encode` | 256 KiB | `[25.681, 26.233, 26.898] us` | `[9.0766, 9.3067, 9.5068] GiB/s` |
| `broker_pull_pop_response/pop_canonical_segmented_prepare` | 256 KiB | `[1.2694, 1.3600, 1.4218] us` | `[171.71, 179.51, 192.33] GiB/s` |

Several ten-sample cases reported one or two outliers. These short-run numbers
show that every benchmark path executes and make the large-response copy cost
visible; they are not stable enough for a two-percent release decision. The
very high canonical 256 KiB derived rates represent shared-body ownership and
head preparation, not memory transfer or socket bandwidth.

## Budget evidence

The allocation and ownership budgets are contract assertions. A wall-clock
benchmark alone cannot prove them, so each row cites deterministic structural
or counter evidence in addition to the Criterion group.

| Budget | Evidence | Assessment |
|---|---|---|
| Inline request adds no shared response-state allocation | `inline_slot_tracks_the_four_states_without_allocating_deferred_state` snapshots the deferred-state allocation counter before and after inline state transitions. `take_failures_are_exact_and_only_a_success_allocates_deferred_state` proves that only a successful deferred take increments it. `retained_size_charges_exact_layout_and_each_declared_part` verifies the exact deferred `Arc<ResponseState>` allocation layout and every declared retained-byte part. | Meets the response-state allocation count/bytes budget. This is not a whole-process allocator count; Tokio task/session allocation is outside this narrowly defined budget. |
| Inline shared response state is zero | The same exact counter tests distinguish the inline affine slot from the deferred `Arc<ResponseState>` allocation. | Meets. |
| Request-local `ChannelInner`, pending owner, and `Arc<Mutex<Connection>>` are zero | `public_reply_is_channel_free_zero_copy_bound_and_observed_once_after_one_admitted_clone` exercises the public embedded boundary and verifies one admitted processor clone and body pointer preservation. Public processor types expose no channel or connection snapshot. | Meets for embedded dispatch. Network dispatch reuses its canonical session writer and does not create a processor-local channel snapshot. |
| Response head encode count is one | `encoder_runs_exactly_once_and_limit_failures_never_retry` uses an atomic encoder counter and asserts one invocation on success and failure. | Meets. |
| Segment body concat/memcpy count is zero | `preparation_moves_bytes_segment_buffers_and_the_segment_vector_allocation`, `reply_moves_segment_and_file_owners_without_copy_or_file_access`, and Broker `segments_are_body_only_ordered_and_move_their_backing_allocations` compare backing pointers before and after ownership transfer. | Meets. Head bytes are encoded separately; body segments remain borrowed/shared `Bytes` owners until write completion. |
| Writer count is one per session | `SessionHandle` retains one `writer_task_id`; `session_writer_reports_bounded_queue_and_write_diagnostics`, the no-interleave session concurrency tests, and typed close tests exercise that single queue/task through retirement. | Meets structurally and behaviorally. |
| Writer queue wait is observable | `rocketmq_transport_response_queue_wait_seconds` is recorded at the canonical write boundary; the `frame_write` and `write_pipeline` groups exercise queue/write paths. | Meets observability requirement; no release percentile is claimed by the quick run. |
| Deferred wait/execution permits and retained bytes return to zero | Typed server close tests assert writer queued items/bytes and `DeferredAdmissionSnapshot::retained_bytes()` are zero. Deferred registry acceptance tests assert `invariant_failures() == 0`. | Meets. |
| Lease lifetime and zero-copy eligibility do not regress | Response-plan and Broker pointer/lease tests prove move semantics and exactly-once drops. `linux_file_send` covers native plaintext sendfile eligibility on Linux; TLS and unsupported platforms retain portable fallback. | Meets semantically. Native sendfile was not executed on this Windows host. |
| Shutdown deferred leak is zero | `typed_close_waits_for_deferred_cleanup_executor_drain_and_writer_completion`, `shutdown_drains_a_writer_claimed_deferred_resume_to_one_receipt_and_frame`, and broker runtime lifecycle probes assert healthy reports and zero post-shutdown task/queue/retained counts. | Meets. |

## Comparison scope

There is no second processor implementation in this revision. The
same-revision `legacy_materialized_contract_reference` measures only a
materialized command shape; it is not a complete dispatcher latency baseline.
The response-plan and Pull/Pop groups do provide valid materialized-versus-owned
response preparation comparisons.

Criterion does not produce request P99 values, so this quick run cannot approve
the “no more than 2% throughput/P99 regression” budget for the full dispatch
path. A release owner must collect a same-host cross-revision Criterion baseline
and a histogram-capable end-to-end load run, or explicitly accept the missing
historical comparison while relying on the exact ownership evidence above.

This is an evidence limitation, not a claim that the budget passed. No runtime
regression or failure should be inferred from the absence of a comparable
historical sample.

Criterion measures wall-clock iteration distributions. The quick profile does
not directly record process CPU time, whole-process allocation count/bytes, or
lease duration. Those values require the repository's platform profiler or a
counting allocator run, with its overhead reported separately. The exact
allocation-layout, encoder-count, backing-pointer, lease-drop, permit, and
shutdown tests above remain the authoritative ownership evidence.

## Engineering decision

The three benchmark groups are suitable for ongoing review and reproduce the
important ownership paths without expanding production capability. Structural
budgets for response-state allocation, channel snapshots, head encoding,
segment copies, writer ownership, leases, and shutdown leaks have deterministic
evidence. Throughput and P99 remain release-review decisions until a controlled
pre-FIN baseline and an end-to-end latency histogram are attached.
