# NameServer performance rollout runbook

## Purpose and safety boundary

This runbook promotes the optimized Rust NameServer without treating projected
performance as measured fact. Keep Java NameServer nodes available until one
complete peak business cycle has passed. Never expose port 9876 publicly, and
require TLS plus authentication/authorization for operational requests.

## Prerequisites

- The exact release commit passes the Java golden corpus, mixed Java/Rust
  matrix, route E2E smoke, write-recovery tests, and runtime ownership audit.
- Host clocks, allocator, power policy, kernel/network settings, Java version,
  manifest hash, and monitoring retention are recorded.
- Alerts from `distribution/config/prometheus-namesrv-alerts.yaml` and the
  Grafana dashboard are loaded with low-cardinality labels only.
- The operator can remove one NameServer address without stopping Broker
  registrations to the remaining nodes.
- Backups of `kvConfig.json`, the desired runtime properties, ACL, and TLS
  material are restorable.

## Pre-release soak matrix

| Scenario | Required load | Required evidence |
|---|---|---|
| Large registration | At least 70k Topics using legal continuous chunks; include duplicate, retry, missing/stale chunk cases | 100% complete registration, no half-visible generation, wire/decompressed bytes, decode p99 |
| Registration storm | At least 300 Brokers re-register concurrently, including unchanged and delta payloads | Route query p99/p99.9, mutation wait/hold, dirty/no-op counts, route digest |
| Failure recovery | 10%, 50%, and 100% simultaneous expiry | No unregister loss; recovery latency, oldest pending age, CPU/RSS, safety-scan digest |
| Connections | 64, 256, and 1,024 connections | Active/admitted/rejected, reconnect churn, slow writes, bounded RSS |
| Zone route | 10% zone-filtered route reads, widths 1/4/16 | Java-compatible digest; one typed filter and one encode |
| KV/config | Concurrent writes plus injected create/write/fsync/replace failures | Failed mutation leaves memory unchanged; desired/durable/applied converge |
| TLS | 100 and 1,000 context reloads when hot reload is supported | Zero reload failure; RSS/native memory converges; 1,000-reload target below +5 MiB |
| Steady read | 24 hours at the accepted route profile | Stable p99/p99.9, zero digest error, no queue/generation leak, bounded RSS |

Use the checked-in orchestrator first:

```powershell
.\scripts\run_namesrv_soak.ps1 -Mode Plan
.\scripts\run_namesrv_soak.ps1 -Mode Smoke -JavaRocketmqHome D:\Github\Java\rocketmq
.\scripts\run_namesrv_soak.ps1 -Mode Full -JavaRocketmqHome D:\Github\Java\rocketmq -SteadyReadHours 24
```

`Plan` performs no load. `Smoke` validates wiring. `Full` runs the checked-in
20k/100k route profiles, the 10/50/100% expiry microbenchmarks, KV fault tests,
mixed parity when Java is supplied, and repeats the steady-read profile for the
requested duration. Execute `Full` on an otherwise idle capacity host, not a
developer workstation or shared CI runner.

TLS reload requires a deployment-specific certificate rotation driver. Pass a
reviewed script with `-TlsReloadScript`; omission is recorded as skipped, not
passed. Chunk protocol fault injection similarly remains a required external
Broker scenario when the selected Java/Rust Broker supplies the chunks.

## Rollout sequence

1. **Laboratory:** pass corpus, fault injection, E2E capacity, and soak gates.
2. **Shadow:** Brokers register with Rust, but clients do not list it. Compare
   route digest, Topic count, Broker generation, expiry mismatch, and KV state.
3. **Single node:** add Rust to 1%, 10%, then 50% of client address selection.
   Hold every step for at least two route refresh periods and one registration
   interval; extend the hold when traffic is sparse.
4. **Single AZ:** keep Java nodes in the other AZs. Complete a Broker restart,
   50% expiry drill, ACL denial test, config persist failure, and node restart.
5. **Whole cluster:** retain rollback capacity through one full peak business
   cycle before removing Java nodes.

Feature order is independent and reversible:

1. keep `namesrvWorkloadAdmissionObserveOnly=true`;
2. move `expiryIndexMode` from `off` to `shadow`, then `active` only after zero mismatch;
3. enable typed zone shadow, compare digests, then enable typed filtering;
4. enable registration delta only after unchanged/delta counters and mixed parity agree;
5. enable the route response cache last and only while RSS stays within budget.

## Automatic rollback triggers

Rollback immediately on any route-digest mismatch, partial generation,
stale-event deletion, false-ready, security bypass, unregister Full/drop, expiry
shadow mismatch, or KV durability failure. Also rollback when errors increase by
at least 0.1 percentage point, p99 remains above 2x baseline for five minutes,
or RSS exceeds the approved budget by more than 10%.

## Rollback procedure

1. Stop increasing client traffic; preserve metrics, profiles, logs, and route digests.
2. Remove the canary Rust address from client discovery while Brokers continue
   registering with the remaining NameServers.
3. Set live feature switches to their safe values; for restart-scoped settings,
   restore the last desired properties and restart only the isolated node.
4. Keep the node alive until accepted unregister and KV queues drain and
   durable/applied generations converge. Then stop it cleanly.
5. Verify clients converge to Java/previous Rust routes, Broker registrations are
   complete, no stale route remains, and protected configuration is unchanged.
6. Open an incident with the immutable evidence bundle before retrying rollout.

`kvConfig.json` remains Java-compatible JSON. P3 intentionally adds no WAL, so
rollback has no log-format conversion step.

## WAL decision record

Do not introduce a KV WAL without measurements. Open a dedicated ADR only when
one of these is sustained in a representative environment: snapshot size above
16 MiB, more than 10 mutations/second, mutation p99 above 100 ms, or KV
serialization above 5% CPU. The ADR must compare the current atomic snapshot
batching against WAL recovery time, corruption handling, compaction, and Java
rollback compatibility.

## Delivery checklist

- [ ] Exact Git commit and Java baseline recorded.
- [ ] Golden corpus and both mixed-version directions pass.
- [ ] 70k+ Topic chunks and 300+ Broker storm pass.
- [ ] 10/50/100% expiry and 64/256/1024 connections pass.
- [ ] KV fault injection preserves durable-before-publish.
- [ ] TLS reload is passed or explicitly marked unsupported/skipped.
- [ ] 24-hour soak artifacts contain CPU, RSS, p99/p99.9, queues, generations, and digest.
- [ ] Alerts and dashboards are active before traffic.
- [ ] Shadow, single-node, and single-AZ rollback drills are signed off.
- [ ] Performance targets are labeled measured or unverified; no projection is reported as fact.
