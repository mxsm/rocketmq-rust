# Runtime validation experiments

Routine fixes use the focused checks in the repository guide. These experiments
cover platform and workload evidence; they are not mandatory gates for every edit.

## Behavioral and platform coverage

Run `cargo test -p rocketmq-runtime --no-default-features` on Windows and Linux.
The metadata actor suite exercises real filesystem replacement and cleanup in
addition to injected failure/gate cases. This does not prove power-loss recovery
or coordination between independent processes.

`cargo test -p rocketmq-runtime --test runtime_scale -- --nocapture` checks 5,000
nested-scope/dynamic-key generations and 1,024 explicitly retired metadata
histories. The latter uses an injected successful filesystem; real persistence
belongs to the metadata actor suite and metadata benchmark. Retained old
receipts and escaped budgets must not revive a retired identity.

The Linux cgroup test is intentionally ignored by default. Compile it outside
the constrained unit, then run the resulting executable under a real limit:

```bash
cargo test -p rocketmq-runtime --no-default-features --test runtime_scale \
  --no-run --message-format=json > /tmp/runtime-scale-build.json
test_bin=$(python3 -c 'import json; rows=[json.loads(x) for x in open("/tmp/runtime-scale-build.json")]; print(next(x["executable"] for x in rows if x.get("executable") and x.get("target", {}).get("name") == "runtime_scale"))')
sudo systemd-run --wait --pipe --collect \
  --property=MemoryMax=536870912 --property=MemorySwapMax=0 \
  --setenv=ROCKETMQ_TEST_CGROUP_BYTES=536870912 \
  "$test_bin" --ignored --exact \
  detects_actual_cgroup_limit_and_uses_it_in_owner_planning --nocapture
```

This requires Linux cgroup v2 and systemd. The test verifies the actual detected
source/byte limit and the owner's resulting memory budget. File-view tests
remain separate parser evidence. Do not count an ignored test as a pass.

## Measurement boundaries

Set `CARGO_TARGET_DIR` to the intended build disk before running benchmarks.

| Benchmark | Population and measured region |
| --- | --- |
| runtime_diagnostics_bench | Stable 1k/10k/100k populations, fixed 8 child groups; independent group-count and fixed-32-group depth 1/4/16 cases. Setup/shutdown excluded. Criterion includes return-value destruction. |
| budgeted_queue_bench | Reused queues; fill/reject/drain, wait/release, or replacement-at-capacity operations. Queue and Tokio runtime construction excluded. Wait cases include producer spawning and joining. |
| blocking_executor_bench | Submission through completion of 8/32 jobs with four lane slots and 1 ms simulated blocking work. Runtime creation/shutdown excluded; timeout evidence retains the real blocked closure until release. |
| metadata_io_bench | First real filesystem write held at a gate; queued submissions through release and settled receipts timed. Coalesced writes and hot/cold ordering asserted; first gate arrival, setup and shutdown excluded. |

Run these with `cargo bench -p rocketmq-runtime --bench <name>`. Criterion reports
batch-derived estimates and confidence intervals, not an individual request P99.
Reject scenarios intentionally reject one extra item per full batch; wait/churn
scenarios assert successful admission and resource release. Never compare older
Criterion results after changing the timed region as if they measured a code
speedup.

For independent sampling allocations and per-submission observations, also set
`ROCKETMQ_MEASURE_SAMPLING=1` when running the diagnostics benchmark. The
`sampling-costs.json` artifact stores 101 samples per population/detail case and
three pairs of 5,000 submit/completion observations at a 10 ms sampler delay.
Allocation counters cover successful allocation/reallocation requests on the
sampling thread only, not live or peak process memory. Setup/teardown and
return-value destruction are outside these independent sampling timers.

Artifacts are written under `CARGO_TARGET_DIR/runtime-measurements`; Criterion
retains its own raw samples. Record the OS, CPU, toolchain, background load,
actual sampler count, warmup/repetitions and quantile algorithm alongside
results. No timing number here is a release SLO, and no source fingerprints or
file hashes are needed.

