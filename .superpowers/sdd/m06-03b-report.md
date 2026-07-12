# M06-03b recovery/load neutral planning report

## Final outcome

M06-03b adds `rocketmq_store_local::commit_log::{load,recovery}` and makes it the single canonical owner of
`LoadStatistics`, `RecoveryMmapAdvice`, `RecoveryFilePrefetch`, `RecoveryStatistics`,
`AbnormalRecoveryFileRange`, `AbnormalRecoveryWindow`, and
`plan_abnormal_recovery_window_from_ranges` with its private pure helpers. `rocketmq-store` keeps its existing
loader/recovery modules and exact re-exports these definitions.

The facade still owns `CommitLogLoader`, `HintResult`, mmap/fadvise/prefetch execution, `BatchMessageIterator`,
`RecoveryContext`, the mapped-file checkpoint adapter, StoreCheckpoint, MessageStoreConfig, message parsing, and
CommitLog orchestration. `HintResult` recording now uses two private free helpers that update the canonical public
statistics fields; no new public hint/result seam was introduced.

## RED -> GREEN evidence

All RED commands ran before production edits.

1. Loader type identity fixture
   - RED: `cargo test -p rocketmq-store --lib m06_load_type_identity` exited 101 with E0433 because
     `rocketmq_store_local::commit_log` did not exist.
   - GREEN: the same target exits 0 with 1/1 test, proving `LoadStatistics`, `RecoveryMmapAdvice`, and
     `RecoveryFilePrefetch` identity plus unchanged defaults and vocabulary through the facade-private legacy
     module.
2. Recovery identity and fixed golden fixture
   - RED: `cargo test -p rocketmq-store --test m06_store_local_commitlog_compatibility` exited 101 because the
     canonical Local recovery module did not exist.
   - GREEN: the target exits 0 with 2/2 tests. It proves identity for all three public recovery value objects and
     the planner re-export, and fixes full outputs for empty, checkpoint-only, bounded checkpoint-missing,
     invalid-range, dispatch-out-of-range, confirm-out-of-range, and normal bounded cases.
3. Store Local ownership contract
   - RED: `python -m unittest scripts.tests.test_m06_store_local_contract` exited 1 because canonical
     `commit_log/load.rs` and `commit_log/recovery.rs` did not exist.
   - GREEN: the final target exits 0 with 11/11 tests. It proves exact struct/enum/function item kinds, one active
     definition, exact active facade re-exports, unchanged loader/recovery module visibility, and all M06-03a
     forbidden owner edges.
4. Clippy review fix
   - RED: Store all-target/all-feature Clippy rejected the fixture's inline seven-argument function pointer with
     `clippy::type-complexity`; production code had no finding.
   - GREEN: a private `RecoveryPlanner` type alias preserves the identity assertion without a lint suppression;
     Store package and root workspace Clippy both exit 0.
5. Tracing target review fix
   - RED: `python -m unittest scripts.tests.test_m06_store_local_contract` exited 1 because the moved load summary
     had no explicit target (`[None]`) instead of its legacy `rocketmq_store::log_file::commit_log_loader` target.
   - GREEN: the final 11/11 contract fixes both load and recovery summaries to their legacy Store module targets.
     The target scanner operates on active Rust positions and its mutation fixture rejects line/block comments,
     ordinary strings, and raw strings as evidence.

## Compatibility and dependency closure

- Fields, derives, defaults, `as_str` vocabulary, fallback reason strings, statistics logging text and targets,
  planner algorithms, output values, and public recovery paths are unchanged. Explicit tracing targets preserve
  the pre-move EnvFilter and log-routing behavior.
- `commit_log_loader` remains `pub(crate)` and `commit_log_recovery` remains public. Because the loader module was
  never externally public, its canonical/legacy identity is checked inside the facade crate; no visibility was
  widened.
- Local adds only the workspace `tracing` dependency needed by the moved unchanged summary logging. Its normal tree
  is `bytes`, `memmap2`, `parking_lot`, `thiserror`, and `tracing` plus foundation dependencies. It contains no
  Store facade, Common, Runtime, Remoting, Broker, RocksDB, or Tiered Store owner edge.
- The ArcMut baseline replaces exactly two unchanged loader import IDs/fingerprints one-to-one and deletes the
  obsolete recovery test-glob identity and its sole occurrence. The promoted occurrence count decreases from 3378
  to 3377; no exception or debt count was added.
- No disk, wire, persisted-record, feature-default, or runtime behavior migration exists.

## Validation

All commands ran from the repository root.

- `cargo test -p rocketmq-store-local` - exit 0; 47/47 unit tests passed, including Local load tests and the
  seven-case full recovery golden; nine existing rustdoc examples remained ignored.
- `cargo test -p rocketmq-store --lib m06_load_type_identity` - exit 0; 1/1.
- `cargo test -p rocketmq-store --test m06_store_local_commitlog_compatibility` - exit 0; 2/2.
- `python -m unittest scripts.tests.test_m06_store_local_contract` - exit 0; 11/11, including active-Rust tracing
  target preservation and comment/string/raw-string false-positive fixtures.
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` - exit 0; 9/9.
- `cargo test -p rocketmq-store --lib commit_log_loader::tests` - exit 0; 11/11.
- `cargo test -p rocketmq-store --lib commit_log_recovery::tests` - exit 0; 1/1.
- `cargo check -p rocketmq-store-local --no-default-features` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features fast-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features fast-load,safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features io_uring` - exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; closure described above.
- `python scripts/tests/test_architecture_dependency_guard.py` - exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` - exit 0; one clean and six violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` - exit 0.
- `python scripts/arc_mut_guard.py --fixtures` - exit 0; 24 fixtures.
- `python scripts/arc_mut_guard.py` - exit 0, `ARC_MUT_GUARD_OK` after the monotonic baseline update.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0 after the fixture-only
  type-complexity fix.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0. Windows emitted only
  the existing `linker_messages` notice, which ignores `-D warnings`, and the existing `proc-macro-error2`
  future-incompatibility notice.
- `cargo fmt --all -- --check` - exit 0 in the final post-report check.
- `git diff --check` - exit 0 in the final post-report check.
- `.\scripts\check-agents-routing.ps1` - exit 0,
  `AGENTS_ROUTING_CHECK_OK standalone_cargo=4 node_projects=3 routes=8`.

Error hygiene was not triggered because no public error/source coverage changed. Runtime audit was not triggered
because no task/thread/runtime/blocking owner changed. Routing validation was run successfully as recorded above.

## Non-goals and remaining work

This slice does not move `CommitLogLoader`, `FileMetadata`, `HintResult`, mmap/fadvise/prefetch execution,
`DefaultMappedFile`, `MappedFile`, iterators, RecoveryContext, checkpoint/config owners, CommitLog append/load
orchestration, flush/group commit, CQ/Index, HA, or storage formats. M06-03 and the M06 Exit Checklist remain open.

Remaining M06-03 work includes later reviewed extraction of the approved CommitLog/mapped-file owners and the
dirty-tail, CRC, segment-roll, crash-before-flush, and other frozen golden corpus. Flush/group commit remains
M06-04 scope.

## Rollback

Restore the facade definitions and private planner helpers, remove the canonical Local commit_log module and
`tracing` dependency, and keep the deterministic golden tests. Restore the removed ArcMut test-glob baseline entry
and old loader fingerprints. No disk or data rollback is required.
