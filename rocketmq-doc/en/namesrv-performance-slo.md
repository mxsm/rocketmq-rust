# NameServer performance and correctness SLO

## Scope

This SLO applies to the Rust NameServer route-read, broker-registration,
failure-recovery, KV/configuration, readiness, and management-query paths. A
release must first preserve the Java 5.5.0 protocol and state-transition corpus;
performance never compensates for a route-digest mismatch or stale-event delete.

## Measurement contract

- Compare Rust and Java on the same host, power policy, manifest hash, seed,
  client, network path, warm-up, and measurement window.
- Record QPS, p50/p95/p99/p99.9, errors, CPU, sampled peak RSS, response bytes,
  admission queues/rejections, mutation wait/hold time, unregister age, expiry
  mismatch, and KV desired/durable/applied generations.
- `rocketmq_namesrv_route_request_latency` is handler time. The end-to-end
  server histogram includes hooks and response-channel completion, but not peer
  acknowledgement. Do not compare the two as if they had the same boundary.
- Missing allocation or native-memory measurements are `N/A`; they are never
  estimated from RSS.

## Release SLOs

| Area | Release objective | Automatic rollback condition |
|---|---|---|
| Correctness | Zero Java-corpus or shadow route-digest mismatch | Any mismatch, partial registration visibility, stale unregister delete, or false-ready event |
| Availability | Listener readiness only after bind/TLS success; accepted shutdown work drains | Bind-ready invariant failure or a non-converging unregister/KV drain |
| Route errors | No more than baseline +0.1 percentage point | Increase of at least 0.1 percentage point for 5 minutes |
| Route latency | Rust p99 lower than Java on the same capacity profile; engineering target is at least 30% below P0 | p99 above 2x Java or the previous Rust release for 5 minutes |
| Route throughput | Same-host Rust QPS at least 1.5x Java; engineering target is at least 30% above P0 | Below the accepted capacity floor for two consecutive windows |
| Memory | Bounded under overload; engineering target is no more than P0 RSS +10% | RSS above the approved budget by more than 10% or sustained growth without convergence |
| Recovery | No unregister drop; expiry index and safety scan agree | Any Full/drop, non-zero shadow mismatch, or oldest pending age above the incident budget |
| KV/config | RPC success only after durable and applied generations converge | Durable/applied gap exceeds one completed batch or remains non-zero beyond 30 seconds |
| TLS | No failed reload; target for 1,000 supported hot reloads is RSS growth below 5 MiB | Reload failure or sustained native-memory growth |

The percentage goals are engineering acceptance targets until a dedicated,
same-host capacity run records them. They must not be reported as measured P3
improvements merely because the implementation exists.

## Recorded evidence boundary

The repository currently records a 2026-08-11 Windows smoke run (100 Topics,
10 Brokers, width 4, 16 connections, 2,000 operations): Rust P1 reached
39,920.40 QPS and 3,714 us p99; Rust P0 reached 34,608.78 QPS and 4,513 us
p99; Java 5.5.0 reached 12,810.04 QPS and 6,491 us p99. This is real E2E smoke
evidence, not a production capacity result and not a P3 post-optimization
claim. Raw reproduction instructions are in
`rocketmq-namesrv/benches/README.md`.

The recorded P2 microbenchmark found 1,000 expired Brokers among 10,000 in
14.741 us with the deadline index versus 175.60 us with a full scan. Heartbeat
maintenance increased from 68.420 ns to 221.49 ns. These figures describe an
in-process algorithm only; they do not represent TCP registration or recovery
latency.

## Error-budget policy

Correctness, stale deletion, readiness, security-policy bypass, KV durability,
and unregister loss have a zero error budget. Latency and resource SLOs may use
the agreed service error budget, but a breached correctness invariant always
stops the rollout even when latency improves.

## Evidence retention

Store the immutable workload manifest, Git commit, Java version, host inventory,
configuration, raw JSON/CSV, process metrics, Prometheus snapshots, and route
digests together. Keep smoke evidence separate from 20k/100k Topic capacity and
24-hour soak evidence.
