# M06-03c mapped-file progress and lifecycle kernel report

## Final outcome

M06-03c makes `rocketmq-store-local::mapped_file::kernel` the canonical owner of the mapped-file progress state
and reference lifecycle that have no Store message/config dependency. `MappedFileProgress` owns file size,
wrote/committed/flushed positions, store and last-flush timestamps, and start/stop timestamps.
`ReferenceResource`, `ReferenceResourceBase`, and `ReferenceResourceCounter` move unchanged in semantics to the
same narrow Local module.

`rocketmq-store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile` remains the public behavior
adapter. It composes `MappedFileProgress` and the Local reference counter, while the two legacy Store lifecycle
modules contain exact crate-visible re-exports. The `MappedFile` trait and all legacy signatures/paths remain
unchanged.

## RED -> GREEN evidence

All RED commands ran before production edits.

1. Local progress/lifecycle behavior
   - RED: `cargo test -p rocketmq-store-local --test mapped_file_kernel` exited 101 with four E0432 errors because
     `rocketmq_store_local::mapped_file::kernel` did not exist.
   - GREEN: the same target exits 0 with 6/6 tests. It covers legacy initial values, exact segment-full boundary,
     transient versus direct readable position, append/commit/store timestamp behavior, crash-before-flush versus
     successful durable advancement, last/start/stop timestamps, graceful and forced shutdown, and concurrent
     cleanup exactly once.
2. Mutation-resistant ownership contract
   - RED: `python -m unittest scripts.tests.test_m06_store_local_contract` exited 1 with 13 tests and two expected
     failures because `kernel.rs` and `MappedFileProgress` had no canonical owner.
   - GREEN: the same target exits 0 with 13/13 tests. It proves one active definition for all four kernel items,
     exact legacy facade re-exports, one composed progress field, no copied progress atomics/direct accesses, and
     the existing forbidden-dependency closure. Scanner fixtures prove comments, ordinary strings, and raw strings
     cannot satisfy the contract.
3. Store compatibility adapter
   - GREEN: `cargo test -p rocketmq-store default_mapped_file_impl` exits 0 with 26/26 focused tests. The existing
     injected I/O failure keeps the durable position unchanged; the real transient-store path preserves
     wrote/commit/read/flush sequencing.

## Semantic preservation

- `MappedFileProgress` reproduces the original atomic orderings: progress reads use Acquire (store and last-flush
  timestamp reads remain Relaxed), public position setters use SeqCst, append uses AcqRel then store timestamp
  Release, commit/flush use Release, and start/stop timestamp setters use Release.
- Initial values remain wrote/committed/flushed/store/last-flush `0` and start/stop `-1`.
- `is_full` retains exact equality instead of changing to a stricter boundary. Direct mmap reads use wrote
  position; transient-store reads use committed position.
- Durable position and last-flush time advance only after a successful flush. Warmup continues to update only the
  last-flush time after each successful range flush.
- The lifecycle clock uses `SystemTime`/`UNIX_EPOCH`, returns zero before the epoch, and keeps the original
  millisecond and `saturating_sub` forced-shutdown behavior without adding a Common dependency.
- Reference hold/release locks, reference count thresholds, availability/cleanup flags, cleanup-once lock, and all
  atomic orderings are preserved.

## Dependency and ArcMut governance

- Local adds no dependency. Its normal closure remains free of Store, Common, Rust, Remoting, Broker, RocksDB,
  and Tiered Store; default/features remain unchanged.
- Adding the Local progress import changed only adjacent tokens around the existing DefaultMappedFile `ArcMut`
  import. The approved one-for-one relocation keeps identity `ebb212125033591cebb811ba`, moves occurrence
  `b5e35ab8bca98a5f31be4b78` to `26d2e0b2c08b64d6e00a411f`, and changes fingerprint
  `b3a8e0dec60e3c0663ae3a34` to `b7d0781b6fe78b0ade831725`. Item, line, owner, reason, and removal milestone are
  unchanged; promoted ArcMut occurrences remain 3377.
- No dependency-policy, architecture, ArcMut, runtime, error, or lint exception is added.

## Validation

All commands ran from the repository root.

- `cargo test -p rocketmq-store-local` - exit 0; 47/47 unit tests and 6/6 M06-03c integration tests passed
  (53 total); nine existing rustdoc examples remained ignored.
- `cargo test -p rocketmq-store default_mapped_file_impl` - exit 0; 26/26 focused tests passed, including the
  real transient commit/flush round trip and injected flush failure.
- `cargo test -p rocketmq-store --test m06_store_local_compatibility` - exit 0; 3/3 existing leaf identity tests.
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` - exit 0; 9/9.
- `cargo test -p rocketmq-store --test m06_store_local_commitlog_compatibility` - exit 0; 2/2.
- `python -m unittest scripts.tests.test_m06_store_local_contract` - exit 0; 13/13.
- `cargo check -p rocketmq-store-local --no-default-features` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features fast-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features fast-load,safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --no-default-features --features io_uring` - exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; closure contains no forbidden owner edge.
- `python scripts/tests/test_architecture_dependency_guard.py` - exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` - exit 0; one clean and six violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` - exit 0.
- `python scripts/arc_mut_guard.py --fixtures` - exit 0; 24 fixtures.
- `python scripts/arc_mut_guard.py` - exit 0, `ARC_MUT_GUARD_OK`; promoted count remains 3377.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0. Windows emitted only
  the existing `linker_messages` notice (which explicitly ignores `-D warnings`) and the existing
  `proc-macro-error2` future-incompatibility notice.
- `.\scripts\check-agents-routing.ps1` - exit 0,
  `AGENTS_ROUTING_CHECK_OK standalone_cargo=4 node_projects=3 routes=8`.
- `cargo fmt --all -- --check` and `git diff --check` - exit 0 in the final post-report check.

Error hygiene was not triggered because no error definition/mapping changed. Runtime audit was not triggered
because no task, thread, runtime, blocking, or shutdown owner changed. No manifest changed, but the routing guard
was still run as required by the brief.

## Non-goals and remaining work

This slice does not move the `File` handle, preallocation, rename/delete, mmap/`ArcMut<MmapMut>`,
`DefaultMappedFile`, `MappedFile`, append callbacks/messages, `TransientStorePool`, buffer leases, memory locking,
warmup owner, `FlushDiskType`, platform/ffi, CommitLog orchestration, load execution, recovery parsing,
flush/group commit, CQ/Index, HA, or config. M06-03 and every M06 Exit Checklist item remain open.

## Rollback

Restore the Store-owned progress atomics and lifecycle definitions, remove the Local kernel and focused tests,
and restore the prior ArcMut import fingerprint. No disk, data, wire, or persisted-format rollback is required.
