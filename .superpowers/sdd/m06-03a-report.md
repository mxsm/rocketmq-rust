# M06-03a store-local leaf foundation report

## Final outcome

M06-03a creates the physical `rocketmq-store-local` package and makes it the single canonical
owner of six dependency-compatible MappedFile leaves: `direct_io`, `flush_strategy`,
`mapped_buffer`, `mapped_file_error`, `metrics`, and `io_uring_impl`. Their focused unit tests moved
with the definitions. `rocketmq-store` now depends on the canonical crate and preserves the old
root and `io_uring_impl` deep paths by exact re-export; no definition was copied.

The slice does not complete M06-03. `MappedFile`, `MappedFileRefactor`, `DefaultMappedFile`, the
builder/factory/reference-resource owners, CommitLog, load/recovery, flush/group commit, CQ/Index,
HA, and Local store composition remain in `rocketmq-store`. No persisted data, I/O policy, flush
semantics, runtime ownership, or error text changed.

## RED -> GREEN evidence

Both RED commands ran before any production edit.

1. Source and manifest contract
   - RED: `python -m unittest scripts.tests.test_m06_store_local_contract` exited 1 with three
     assertion failures because the canonical `rocketmq-store-local` directory did not exist.
   - GREEN: the same target exits 0 with 3/3 tests. It proves workspace membership, exact feature
     ownership/forwarding, forbidden manifest/source edges, six canonical files, single
     definitions, removed facade copies, and exact facade re-exports.
2. Canonical/legacy compile and behavior fixture
   - RED: `cargo test -p rocketmq-store --test m06_store_local_compatibility` exited 101 with eight
     E0433 errors because `rocketmq_store_local` was not linked.
   - GREEN: the target exits 0 with 3/3 tests covering Direct I/O allocation/alignment/error text,
     `FlushStrategy`, `MappedBuffer` and mapped-file error identity, metrics, and io_uring
     status/runtime-capability identity.
3. Error-hygiene owner relocation
   - RED: after moving the canonical error file, `check-error-hygiene.ps1` reported the old
     `rocketmq-store/.../mapped_file_error.rs` source-preservation path as missing.
   - GREEN for the changed surface: the guard now checks the canonical Local path and includes
     `rocketmq-store-local/src` in its domain scan. The missing-file finding is gone without an
     allowlist or baseline expansion; the command retains only the unchanged repository baseline
     listed below.

## Feature and dependency ownership

- `rocketmq-store-local/default = []`.
- `fast-load = []` and `safe-load = []` are owned by Local.
- `io_uring = ["dep:tokio-uring"]`; the optional dependency is Linux-only.
- `rocketmq-store/default` remains `["local_file_store", "fast-load"]` and forwards the three
  matching features to Local. The facade no longer directly depends on `tokio-uring`.
- `cargo tree -p rocketmq-store-local -e normal` contains only direct `bytes`, `memmap2`,
  `parking_lot`, and `thiserror` edges plus their foundation dependencies. It contains no
  `rocketmq-common`, `rocketmq-rust`, `rocketmq-remoting`, `rocketmq-store`, Broker, RocksDB, or
  Tiered Store edge.

## Compatibility

- The six files are mechanical moves; all enum variants, defaults, error/display text, visibility,
  focused tests, and unsafe behavior are retained. Required `// SAFETY:` explanations were added
  at the moved unsafe sites without changing operations.
- The facade re-exports the canonical `DirectIoBuffer`, `DirectIoRequest`,
  `DirectIoValidationError`, `FlushStrategy`, `MappedBuffer`, `MappedFileError`,
  `MappedFileResult`, `MappedFileMetrics`, `IoUringBackendStatus`, and
  `io_uring_backend_status`. It re-exports the canonical `io_uring_impl` module for all existing
  deep paths.
- The error architecture guard's source-preservation owner moved with `MappedFileError`; no error
  mapping, redaction rule, allowlist count, or exception changed.
- No persisted data migration exists in this slice.

## Validation

All commands ran from the repository root.

- `cargo check -p rocketmq-store-local --no-default-features` - exit 0.
- `cargo test -p rocketmq-store-local` - exit 0; 36/36 unit tests passed and nine existing ignored
  rustdoc examples remained ignored.
- `cargo check -p rocketmq-store-local --features fast-load` - exit 0.
- `cargo check -p rocketmq-store-local --features safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --features fast-load,safe-load` - exit 0.
- `cargo check -p rocketmq-store-local --features io_uring` - exit 0.
- `cargo test -p rocketmq-store --test m06_store_local_compatibility` - exit 0; 3/3.
- `python -m unittest scripts.tests.test_m06_store_local_contract` - exit 0; 3/3.
- `cargo check -p rocketmq-store` - exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; closure described above.
- `python scripts/tests/test_architecture_dependency_guard.py` - exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` - exit 0; one clean and six
  violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` - exit 0.
- `python scripts/arc_mut_guard.py --fixtures` - exit 0; 24 fixtures.
- `python scripts/arc_mut_guard.py` - exit 0, `ARC_MUT_GUARD_OK`.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0.
  Windows emitted only the existing `linker_messages` notice, which ignores `-D warnings`, plus
  the existing `proc-macro-error2` future-incompatibility notice.
- `.\scripts\check-agents-routing.ps1` - exit 0,
  `AGENTS_ROUTING_CHECK_OK standalone_cargo=4 node_projects=3 routes=8`.
- `.\scripts\check-error-hygiene.ps1` - exit 1 only for the unchanged repository baseline: one
  auth source-stringification site, eight MCP `anyhow` sites, and two missing governance
  documents. The canonical moved error surface and every other guard section are clean.
- `cargo fmt --all -- --check` - exit 0 in the final post-report check.
- `git diff --check` - exit 0 in the final post-report check.

`cargo check -p rocketmq-store --no-default-features` exits 101 with 124 pre-existing
non-exhaustive matches because all `GenericMessageStore` backend variants are feature-gated out.
M06-03a does not touch `rocketmq-store/src/message_store.rs`; the file has no diff from base
`e11b2b323`. Fixing no-default facade composition belongs to later M06 facade work and is outside
this six-leaf slice. Default facade check, the exact Local feature matrix, package Clippy, and
workspace all-feature Clippy all pass.

Runtime ownership, RocksDB behavior, observability, and MCP specialized gates were not triggered.
The diff introduces no task/thread/runtime, RocksDB store, telemetry feature, or MCP API change.

## Rollback

Rollback order is:

1. Remove the exact `rocketmq-store` re-exports.
2. Remove the facade dependency and feature forwarding, restoring its direct Linux-only
   `tokio-uring` feature dependency.
3. Move the six canonical leaf files back and remove `rocketmq-store-local` from the workspace.

No persisted data rollback or migration is required.

## Remaining M06-03 work

- Freeze the recovery/dirty-tail golden entry condition.
- Move CommitLog append/load/recovery, MappedFile owners, and the minimum approved config only in a
  later reviewed slice.
- Add the required dirty-tail, CRC, segment-roll, load/recovery, and crash-before-flush golden
  corpus before M06-03 can complete.
- Keep flush/group commit, CQ/Index, HA, Timer/POP/services, and Local composition in their later
  independently reviewed slices.

The PR-M06-03 top-level checklist and the M06 Exit Checklist remain entirely open.
