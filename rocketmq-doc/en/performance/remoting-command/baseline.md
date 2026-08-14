# Remoting command performance baseline

This document freezes the first formal remoting command baseline after the correctness work for defaults, response construction, endpoint frame limits, and bounded `GO_AWAY` retry. It is a measurement reference, not a claim that Rust or one wire format wins every workload.

## Measurement contract

The run used Rust revision `f12c363dc040018d612e320f37d2543bd8c872ff` and Apache RocketMQ revision `e3458616d207ee636b1762f0f8dcf788a590d59d`. The checked-in compact tables are:

- [`rust-summary.csv`](baseline-data/rust-summary.csv): 251 Rust cases, each summarized from 10 independent benchmark processes.
- [`java-summary.csv`](baseline-data/java-summary.csv): 48 Java JMH request-header cases.
- [`allocation-summary.csv`](baseline-data/allocation-summary.csv): 33 allocation cases.
- [`baseline-summary.json`](baseline-data/baseline-summary.json): revision, count, and object-footprint summary.

Rust Criterion used a 5 second warmup, 10 second measurement window, and 100 samples. Java JMH used 5 forks, 10 one-second warmup iterations, 15 one-second measurement iterations, and the GC profiler. Diagnostic Quick results were rejected. The canonical request-header corpus contains 48 unique cases: JSON and ROCKETMQ, with 24 encode and 24 decode operations.

The run covered construction, header encoding, frame assembly, envelope and typed decoding, round trips, cloning, raw forwarding, display, limit rejection, fragmentation, write batching, admission lookup, pending completion, and hook snapshots. The complete profile is in `scripts/remoting-command-baseline/profile-v1.json`.

## Environment

| Property | Value |
|---|---|
| OS | Windows 11, build 26200 |
| CPU | Intel Core i7-11700K, 8 physical / 16 logical cores |
| Memory | 34,219,347,968 bytes |
| Process affinity | `0xffff` |
| Power scheme | `381b4222-f694-41f0-9685-ff5bb260df2e` |
| Rust | 1.95.0, MSVC, LLVM 22.1.2 |
| Cargo | 1.95.0 |
| Java | Oracle JDK 25.0.1 LTS |
| Maven | 3.9.15 |
| Allocator | System allocator |

## Representative Rust results

Values are medians across the ten process medians. The min/max columns show the range of those process medians. Mixed operations are deliberately not collapsed into a single overall score.

| Case | Median | Process range | Interpretation |
|---|---:|---:|---|
| Header encode, JSON, 0 ext fields | 393.81 ns | 393.07–402.54 ns | Small JSON header cost |
| Header encode, ROCKETMQ, 0 ext fields | 95.19 ns | 93.93–97.00 ns | 4.1× lower than JSON for this case |
| Header encode, JSON, 128 ext fields | 14.76 µs | 14.62–15.17 µs | Field traversal dominates |
| Header encode, ROCKETMQ, 128 ext fields | 11.89 µs | 11.81–12.19 µs | About 19% below JSON |
| Frame assemble, ROCKETMQ, 4 MiB body | 1.39 µs | 1.34–1.60 µs | `Bytes` ownership transfer; not 4 MiB copying |
| Envelope decode, ROCKETMQ, 4 MiB body | 150.48 µs | 147.86–155.23 µs | 27.88 GB/s effective byte rate |
| Round trip, ROCKETMQ, 1 MiB body | 518.87 µs | 507.41–575.02 µs | 2.02 GB/s effective byte rate |
| Bounded batch write, 32 × 4 MiB | 118.49 ms | 117.81–119.12 ms | 1.13 GB/s effective byte rate |
| Admission registry lookup | 1.13 µs | 1.10–1.21 µs | Lookup path reference |
| Admission prepared handle | 540.37 ns | 534.08–571.76 ns | Candidate signal, about 52% below lookup |
| Four-hook `Vec` clone snapshot | 64.63 ns | 62.72–71.80 ns | Existing snapshot comparison |
| Four-hook ArcSwap snapshot | 19.09 ns | 18.77–19.92 ns | Candidate signal, about 70% below clone |

The frame-assembly byte rate is intentionally not presented as memory bandwidth: the benchmark primarily measures reference-counted buffer assembly. Envelope decode, full round trip, and write-pipeline cases are more representative of body handling.

## Java request-header reference

The following values are arithmetic means of 12 per-case medians in each group. They are a protocol trend reference on the same machine, not a cross-language score.

| Operation | Format | Mean of case medians | Case range | Mean allocated bytes/op |
|---|---|---:|---:|---:|
| Encode | JSON | 718.22 ns | 291.60–1,239.86 ns | 9,133.8 B |
| Encode | ROCKETMQ | 511.38 ns | 185.71–1,022.58 ns | 7,944.1 B |
| Decode | JSON | 854.06 ns | 378.87–1,531.79 ns | 9,048.6 B |
| Decode | ROCKETMQ | 680.33 ns | 260.64–1,240.76 ns | 8,784.4 B |

The complete per-case values remain in `java-summary.csv`, so Tier-1 cases are not hidden by these aggregate rows.

## Allocation and footprint

`size_of::<RemotingCommand>()` is 160 bytes. Across ten process samples, creating 100,000 commands increased RSS by a median 16,019,456 bytes, with a range of 16,019,456–16,023,552 bytes. Representative request-header encode allocation measurements are one allocation / 1,024 bytes for JSON and one allocation / 264 bytes for the canonical ROCKETMQ case; the full 33-case table records the exceptions and larger object operations.

## Reproducibility note

An audit of the initial collection found that `write_pipeline` and `admission_pending_hooks` retained local 1/2/10 Criterion group defaults. Their 20 completion markers were invalidated, the shared formal profile was wired into both groups, and all 20 targets were rerun with 5/10/100. Their current logs contain no 1-second warmup or 10-sample measurements. The other 40 target runs and Java results were retained because their benchmark sources and the measured production code were unchanged by that correction.

One later independent spot check of `remoting_command/header_encode/json/ext-16` completed with exit code 0 and a 2,304.21 ns median versus the formal 1,944.30 ns median (+18.5%). This was outside the formal-run process range and demonstrates cross-session frequency/load drift. Candidate reports must therefore use interleaved baseline/candidate measurements from the same session; exact absolute nanoseconds from this document are not an A/B acceptance gate. Additional long spot-check replays were time-boxed out after the formal 10-process run had completed.

## Reproduction

From clean Rust and Java oracle worktrees:

```powershell
$output = Join-Path $PWD 'target/remoting-command-refactor/baseline-<run-id>'
./scripts/remoting-command-baseline/collect.ps1 `
  -JavaRepo <clean-java-oracle> `
  -Output $output
```

The collector records the concrete Cargo, Maven, and JMH commands in `commands.txt`, rejects incomplete 48-case or 10-process results, and emits the compact tables with:

```powershell
python scripts/remoting-command-baseline/summarize.py `
  --run $output `
  --output "$output/summary"
```

Raw Criterion/JMH samples, console logs, profiler output, and machine-local paths remain under `target/` and are not committed.

## Risk and rollback

This change adds benchmark and reporting infrastructure only; it does not modify the production remoting hot path or wire bytes. If the harness becomes unreliable, revert the benchmark target, collector, and this report together. Downstream optimization reports should continue to preserve the 48-case corpus and may replace this baseline only with another complete non-Quick run.
