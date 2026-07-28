# Async Predictability Acceptance Record

## Scope

This phase reorganizes managed blocking work, controller broker-role notification, client route updates, and runtime task capabilities without adding a workspace crate or changing public protocol and persisted-data compatibility.

## Delivered behavior

- Managed blocking work shares one root-owned hard capacity with lane ceilings, minimum reservations, absolute deadlines, permit-lifetime accounting, and combined diagnostics.
- Broker-role notification uses a bounded latest-wins mailbox. Pending, in-flight, and retry-waiting work is counted by unique key and owned by the controller task group.
- Route refresh follows `snapshot -> compute -> versioned commit -> notify`; no registry or commit guard crosses an await point, and notification fan-out is bounded.
- `TaskSpawner` provides an owned background-work capability. Raw runtime and detached-spawn compatibility APIs are deprecated and tracked in an owner/removal inventory.
- No public trait or test-only production seam was added for the single controller remoting implementation.

## Runtime audit exit counts

The enforcing runtime audit classifies lexical matches separately from findings that require action. The phase exit targets and observed action-required counts are:

| Category | Required | Observed |
|---|---:|---:|
| Runtime creation | 0 | 0 |
| Shutdown | 0 | 0 |
| Blocking | At most 25 | 0 |

The compatibility inventory is maintained in [`scripts/runtime-task-escape-policy.json`](../../scripts/runtime-task-escape-policy.json).

## Validation record

| Command | Result |
|---|---|
| `cargo test -p rocketmq-runtime blocking` | Passed: 12 tests |
| `cargo test -p rocketmq-runtime task_capability` | Passed: 2 tests |
| `cargo test -p rocketmq-controller broker_role_notifier` | Passed: 4 tests |
| `cargo test -p rocketmq-client-rust route_update` | Passed: 2 tests |
| `python -m unittest scripts.tests.test_runtime_task_capability` | Passed: 3 tests |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | Passed; all action-required counts are zero |
| `cargo test -p rocketmq-runtime --test service_context_scope_compile_fail` | Passed: 3 compile-fail fixtures |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | Passed |
| `python scripts/rust_hygiene_guard.py` | Passed; no new panic, manual-pin, or legacy-module debt |
| `git diff --check` | Passed |

Shared-crate consumers were also validated from their standalone Cargo roots:

- `rocketmq-example`: format check and all-target Clippy passed.
- Tauri Rust backend: format check and all-target/all-feature Clippy passed.
- Web dashboard backend: format check, all-target/all-feature Clippy, and all-target/all-feature build passed.

## Known residual risks

- Borrowing an idle blocking-lane reservation improves utilization but may cause brief queueing before a newly active lane's reservation becomes protected. The snapshot exposes queue age and borrowed capacity for follow-up measurement.
- Controller notification intentionally uses one worker to preserve per-key ordering. The bounded mailbox protects memory; throughput tuning requires evidence before increasing concurrency.
- A route commit is not rolled back when a consumer notification times out. The committed version remains authoritative, the partial-notification metric records the event, and a later refresh can notify again.
- Deprecated raw runtime and detached-spawn compatibility APIs remain callable until the phase-3 removal gate. The source guard and owner/removal inventory prevent untracked expansion in the interim.
