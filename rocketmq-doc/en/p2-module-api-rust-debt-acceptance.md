# P2 module, public API, and Rust debt acceptance

Status: Accepted

## Scope and comparison point

This record closes the module/API/Rust-debt work against
`ba21d59912c3954f351235497a162800fa3d3b94`, the merged Store and Controller
architecture checkpoint. It covers source organization and internal Rust API
contracts only. RocketMQ wire fields, request codes, persisted layouts, recovery
semantics, and implemented behavior remain unchanged.

## Hotspot extraction

The original files remain module facades and are governed by a non-growth
budget while later batches continue their reduction. The first batch extracted
ten single-purpose child modules without adding a legacy `mod.rs`:

| Facade | Before lines | After lines | Extracted modules | Largest new module |
|---|---:|---:|---:|---:|
| `default_lite_pull_consumer_impl.rs` | 4,796 | 4,606 | 2 | 178 |
| `bootstrap.rs` | 4,446 | 4,225 | 2 | 199 |
| `grpc/service.rs` | 4,071 | 3,996 | 2 | 92 |
| `commit_log.rs` | 3,861 | 3,490 | 2 | 367 |
| `mq_client_instance.rs` | 3,612 | 3,419 | 2 | 139 |

Every new module is below the 500-line target. The maintainability guard ranks
files using LOC, churn, contributors, defect/revert history, public surface,
lock/state ownership, fan-out, and test cost. It rejects a new module above 800
production lines, growth of an existing hotspot, and unreviewed public-surface
growth.

## Deliberate public surface

The default-feature rustdoc JSON snapshot covers all 28 workspace library
targets. The reviewed public-path changes are:

| Package | Before | After | Decision |
|---|---:|---:|---|
| `rocketmq-client-rust` | 279 | 280 | add the classified minimal `prelude` module |
| `rocketmq-runtime` | 350 | 351 | add the classified minimal `prelude` module |
| `rocketmq-transport` | 193 | 194 | add the classified minimal `prelude` module |
| `rocketmq-proxy-core` | 522 | 523 | add the object-safe dynamic plugin adapter |
| `rocketmq-error` | 380 | 380 | preserve the exact `ErrorSpec` public-path fingerprint |

Client, Runtime, and Transport root exports are classified as stable,
experimental, or compatibility entries in `scripts/public-api-intent.json`.
The manifest is fail closed for new declarations and category growth.

## Debt burn-down

| Ledger | Before | After | Enforcement |
|---|---:|---:|---|
| production panic surfaces | 938 | 737 | classified owner, reachability, justification, and expiry |
| trait migration identities | 660 | 645 | line-insensitive non-growth identity |
| runtime action-required fingerprints | 28 | 0 | zero-tolerance runtime boundary baseline |
| targeted Rust lint exceptions | ungoverned | 151 | owner, reason, scope, and removal issue required |
| Clippy argument threshold | 20 | 12 | next approved threshold is 8 after registered debt removal |

Production external-input and recoverable-I/O panic identities are zero. The
remaining panic identities are reviewed internal invariants. Proxy static data
paths use native async/RPIT futures; the only boxed future boundary is the
explicit object-safe plugin adapter. Unparented runtime constructors use an
explicit `legacy_compatibility` name, and their old names are deprecated for
removal in 2.0.0.

## Controlled performance comparison

Baseline and candidate were built with Rust 1.95.0 on the same Windows host in
separate empty `CARGO_TARGET_DIR` directories. The command was:

```text
cargo build --release -p rocketmq-proxy --bin rocketmq-proxy-rust
```

| Metric | Baseline | Candidate | Change |
|---|---:|---:|---:|
| clean release build | 391.841 s | 374.791 s | -4.35% |
| Proxy executable | 81,036,800 B | 80,921,088 B | -0.14% |

The Proxy keyed-admission benchmark compared the two compiled binaries directly
with 50 samples and a five-second measurement window per workload:

```text
cargo bench -p rocketmq-proxy-cluster --features bench-support \
  --bench cluster_executor -- --noplot --sample-size 50 --measurement-time 5
```

| Workload | Baseline p99 | Candidate p99 | Change |
|---|---:|---:|---:|
| same key, 256 commands | 237.948 us | 248.438 us | +4.41% |
| distinct keys, 256 commands | 702.062 us | 565.764 us | -19.41% |
| same key, 1,024 commands | 921.577 us | 858.437 us | -6.85% |
| distinct keys, 1,024 commands | 3,377.164 us | 2,989.900 us | -11.47% |

Criterion reported no statistically significant regression: three workloads
were unchanged within the noise threshold and the 1,024-command same-key
workload improved. The only positive p99 delta remains below the five-percent
regression budget.

The clean build and executable size above were refreshed after the final
lane-panic cancellation fix with no concurrent Cargo process. That fix changes
failure publication, cancellation, and closed-semaphore handling only; the
benchmarked `run_cluster_admission_probe` enqueue/retire path and benchmark
source are unchanged, so the controlled keyed-admission comparison remains the
applicable hot-path evidence.

## Final validation

All commands below completed with exit code zero on 2026-07-30:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
- `python scripts/run_architecture_tests.py --tier pr_static`
- `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline`
- `.\scripts\check-error-hygiene.ps1`
- `.\scripts\check-agents-routing.ps1`
- `cargo test -p rocketmq-broker --lib
  broker_runtime::tests::three_controller_two_broker_controller_mode_failover_and_rejoin
  -- --exact`
- affected Client, Controller, NameServer, Store, Transport, Proxy, and Runtime
  package tests
- MCP check, test, Streamable HTTP Clippy, and rustdoc gates
- Example, Tauri backend, and Web backend format/Clippy/build gates
- `cargo +nightly-2026-07-05 check --locked --all-targets --all-features`
  from `fuzz/`
- default-feature rustdoc JSON snapshot generation for all 28 workspace library
  targets
- `git diff --check`

## Acceptance

The architecture guards, focused crate tests, runtime and error audits, root
format/Clippy gates, and affected standalone consumers are the executable
acceptance evidence. Local build and benchmark artifacts are not repository
deliverables and are never published as images or packages.
