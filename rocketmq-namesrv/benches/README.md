# NameServer route end-to-end benchmark

This harness measures a real RocketMQ remoting request from client write through
response read. It can drive either the Rust NameServer built from the current
commit or an external Java 5.5.0 distribution with the same deterministic
workload manifest and seed.

The `smoke` workload is a wiring and regression check. It is not a production
capacity result. The 20k/100k Topic profiles are the capacity workloads; run
them on an otherwise idle, fixed-frequency host before changing production
defaults.

## Reproduce

```powershell
.\scripts\run_namesrv_route_e2e_bench.ps1 -Server rust -Profile p0 -Workload smoke
.\scripts\run_namesrv_route_e2e_bench.ps1 -Server rust -Profile p1 -Workload smoke
.\scripts\run_namesrv_route_e2e_bench.ps1 -Server java -Profile java-5.5.0 -Workload smoke `
  -JavaRocketmqHome D:\path\to\rocketmq-5.5.0 -JavaHome D:\path\to\jdk
```

The Rust `p0` profile uses safe defaults. The Rust `p1` profile writes an
isolated run-local configuration enabling typed zone filtering, the bounded
versioned response cache, and enforced semantic admission. It does not modify
repository or user configuration.

Each run writes immutable inputs and raw outputs under
`target/namesrv-bench/<server>-<profile>-<workload>-<timestamp>/`:

- `route-benchmark.json` and `route-benchmark.csv`: QPS, p50/p95/p99/p99.9,
  response bytes, and errors;
- `process-metrics.json`: process-tree CPU and sampled RSS;
- `run-metadata.json`: commit, fixture SHA-256, server/profile, timestamp, and
  explicit `null` for unsupported allocation/op data;
- `logs/`: benchmark and server logs.

Compare absolute values first, then calculate `P1/P0` and `P1/Java` from the
raw JSON. Never compare runs with different fixture hashes, workload names,
host power modes, or concurrent background load.

## 2026-08-11 Windows smoke result

All three runs used seed `9191`, fixture SHA-256
`dcd10b127dc37510039707f71118a4d0d1d773a7a43c52ece9cfe8735132bb8c`,
16 logical CPUs, 100 Topics, 10 Brokers, route width 4, 16 connections, and
2,000 operations. P0 is commit `7d67d2063`; P1 and the Java client harness are
commit `838d7a1e9`. Java is the local RocketMQ 5.5.0 distribution.

| Measurement | Rust P0 | Rust P1 | Java 5.5.0 | P1 vs P0 | P1 vs Java |
|---|---:|---:|---:|---:|---:|
| QPS | 34,608.78 | 39,920.40 | 12,810.04 | +15.3% | +211.6% |
| p50 | 240 us | 229 us | 878 us | -4.6% | -73.9% |
| p95 | 882 us | 746 us | 2,772 us | -15.4% | -73.1% |
| p99 | 4,513 us | 3,714 us | 6,491 us | -17.7% | -42.8% |
| p99.9 | 5,589 us | 4,455 us | 8,762 us | -20.3% | -49.2% |
| errors | 0 | 0 | 0 | unchanged | unchanged |
| CPU delta | 2.219 s | 2.375 s | 8.469 s | +7.0% | -72.0% |
| sampled peak RSS | 50.72 MB | 59.05 MB | 353.70 MB | +16.4% | -83.3% |
| allocation/op | N/A | N/A | N/A | N/A | N/A |

This smoke run shows a real P1 improvement over P0 and a substantial advantage
over Java for this small workload, but it does **not** meet the P1 engineering
targets of +30% QPS, -30% p99, and RSS within +10% of P0. Therefore the
response cache and typed-zone fast path remain disabled in the production
baseline, and admission remains observe-only. The raw artifacts are intentionally
kept under ignored `target/namesrv-bench` storage and must be rerun for the
20k/100k Topic profiles before production enablement.

## Metric boundaries

- `rocketmq_namesrv_route_request_latency` is handler latency only.
- `rocketmq_namesrv_route_stage_latency` separates typed filter, legacy zone
  hook, and live encode work.
- `rocketmq_namesrv_route_response_write_latency` ends when the transport
  channel accepts or rejects the response; it is not peer acknowledgement.
- `rocketmq_namesrv_route_end_to_end_latency` starts at transport dispatch and
  includes hooks, handler, and response-channel completion.
- Cache and admission labels are fixed enums. Topic, Broker address, and remote
  IP are deliberately excluded.

## Rollout and rollback

1. Shared route views and disabled-metrics freshness gating are always active.
2. Set `namesrvTypedZoneRouteShadow=true`; compare route digests before setting
   `namesrvTypedZoneRouteEnable=true`.
3. Keep `namesrvRouteResponseCacheEnable=false` until registration churn and
   RSS remain within budget; then roll out 1%, 10%, 50%, and 100%.
4. Keep `namesrvWorkloadAdmissionObserveOnly=true` until classification and
   saturation metrics are clean; then enable rejection.

Disable each switch independently if response digest differs, error rate rises
by at least 0.1 percentage point, p99 remains above 2x baseline for five
minutes, or RSS exceeds the agreed budget by more than 10%.

## Write scalability and recovery benchmark

The P2 harness isolates the two liveness algorithms with 10,000 Brokers and a
10% simultaneous-expiry event, and separately measures the heartbeat cost of
maintaining the deadline index:

```powershell
cargo bench -p rocketmq-namesrv --bench namesrv_write_recovery_bench
```

Compare `full-scan` with `deadline-index`, and `atomic-only` with
`atomic-plus-deadline-index`. The first pair measures recovery lookup benefit;
the second makes the steady-state heartbeat tax explicit. Do not mix these
microbenchmarks with end-to-end registration or route-query QPS. Production
rollout remains `off -> shadow -> active`, with the five-minute full-scan safety
reconciliation retained in active mode.

### 2026-08-11 Windows P2 microbenchmark

Criterion used 20 samples, one-second warm-up, and two-second measurement on
the same development host. These are isolated in-process algorithm results,
not TCP end-to-end NameServer throughput.

| Operation | Baseline mean | P2 mean | Difference |
|---|---:|---:|---:|
| Find 1,000 expired of 10,000 Brokers | full scan 175.60 us | deadline index 14.741 us | 91.6% lower / 11.9x faster |
| Update one heartbeat | atomic only 68.420 ns | atomic + deadline index 221.49 ns | +153.07 ns / 3.24x cost |

The recovery lookup gain is large, while the active index deliberately adds a
small absolute heartbeat tax. Keep `expiryIndexMode="shadow"` until scan
digests match and Broker heartbeat CPU remains inside budget, then switch to
`active`; use `off` as the immediate rollback.

## Acceptance targets

The following are engineering targets, not measured claims: route p99 at least
30% lower, QPS at least 30% higher, and allocation/query at least 50% lower
than the P0 baseline; RSS must remain at or below baseline plus 10%. Missing
metrics remain `N/A`, never inferred.
