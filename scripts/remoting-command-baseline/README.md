# Remoting command baseline

This directory owns the reproducible measurement contract used before evaluating remoting command hot-path changes. The request-header corpus remains the canonical set of 48 cross-runtime JSON and ROCKETMQ encode/decode cases. The additional Rust benchmarks cover construction, frame assembly, envelope and typed decoding, round trips, cloning, raw forwarding, formatting, limits rejection, fragmentation, concurrency, body sizes, and hook snapshots.

`profile-v1.json` is fail-closed: formal evidence requires ten independent Rust benchmark processes, the full Criterion warmup and measurement profile, and Java JMH with five forks plus GC profiling. Diagnostic Quick output is not accepted. The collector also exports the formal settings to benchmark groups that retain fast local defaults, so group-level configuration cannot silently override the 5 s warmup, 10 s measurement, and 100-sample profile.

Run the collector only from clean Rust and Java worktrees at committed revisions:

```powershell
$output = Join-Path $PWD 'target/remoting-command-refactor/baseline-<run-id>'
./scripts/remoting-command-baseline/collect.ps1 `
  -JavaRepo <clean-java-oracle> `
  -Output $output
```

The Java oracle project version must match the `rocketmq.version` used by `scripts/request-header-codec/java-harness/pom.xml`; the collector fails instead of silently resolving a different artifact. `-Resume` may continue an interrupted run only when source revisions, the profile, and the output directory have not changed.

Raw Criterion samples, JMH output, console logs, and machine-local metadata stay below `target/`. `summarize.py` rejects missing process samples or Java cases and emits compact CSV/JSON tables for review. Checked-in reports must use repository-relative commands and must not include raw benchmark output or local paths.
