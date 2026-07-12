# M06-03d mapped-file storage handle kernel report

## Final outcome

M06-03d makes `rocketmq-store-local::mapped_file::file` the canonical owner of the OS file handle, canonical
path, numeric segment offset, preallocation vocabulary, and file lifecycle operations used by
`DefaultMappedFile`. `MappedFileStorage` owns exactly `File`, `PathBuf`, and `file_from_offset`; the existing
Local `MappedFileProgress` remains the only owner of configured file size.

`rocketmq-store` remains the public behavior adapter. It creates directories and validates parent paths before
calling Local, retains the `CheetahString` projection and all existing logging/observability, and continues to own
mmap/`ArcMut`, messages, config, progress, warmup, memory locking, and flush behavior. The legacy
`rocketmq_store::platform` preallocation paths are exact re-exports with unchanged type identity.

## RED -> GREEN evidence

All RED commands ran before production edits.

1. Local storage behavior
   - RED: `cargo test -p rocketmq-store-local --test mapped_file_storage` exited 101 with four E0432 errors because
     `rocketmq_store_local::mapped_file::file` did not exist.
   - GREEN: the focused target exits 0 with 10/10 tests. It covers numeric and invalid segment names, canonical
     path/offset, open/resize, prefix preservation, shrink and zero-length behavior, preallocation classification
     and grow-only decisions, rename success/failure ordering, reopen failure after rename, delete success/failure,
     and file-handle access for Store-owned mmap/I/O.
   - A private injected-preallocator unit test deterministically proves one call for growth and no call for shrink.
2. Store compatibility
   - RED: `cargo test -p rocketmq-store --test store_lifecycle_capability` exited 101 with four E0432 errors because
     the canonical Local module did not exist.
   - GREEN: the target exits 0 with 3/3 tests and proves constant/function/type identity plus unchanged capability
     behavior. `DefaultMappedFile` focused tests cover eager/lazy mmap, transient commit/read/flush, injected flush
     failure, rename success/failure projection and handle behavior, delete, and preallocation observability.
3. Mutation-resistant ownership contract
   - RED: `python scripts/tests/test_m06_store_local_contract.py` exited 1 with 31 tests and two expected failures
     because `file.rs` and the canonical `MappedFileStorage` owner were absent.
   - GREEN: the final target exits 0 with 39/39 tests. It proves one canonical owner, the exact three storage
     fields and types, exact platform re-exports and parse wrappers, exactly one Store storage composition field,
     no direct Store file/offset duplicates, Unix-only `libc`, and the unchanged forbidden dependency closure.
     Synthetic negative fixtures reject aliases, brace/glob re-exports, type aliases, duplicate fields/owners,
     comments, strings, aliased field types, renamed direct `File`/`PathBuf`/plain-`u64` fields, import aliases,
     recursive type aliases, and misplaced `libc` dependencies without misclassifying `AtomicU64`.

## Independent review fixes

- RED: after the first implementation commit, the contract ran 39 tests with five expected failures. The old
  scanner accepted renamed fields resolving to fully-qualified `std::fs::File`, `std::path::PathBuf`, plain
  `u64`, an imported `File as SegmentHandle`, and a recursively aliased `File`. The companion `AtomicU64`
  non-regression fixture passed.
- GREEN: the final 39/39 contract uses a comment/string-aware active source view, exact import records, and a
  memoized alias resolver. It resolves direct standard paths, `use ... as` imports, and recursive non-generic type
  aliases, rejects only exact resolved `File`/`PathBuf`/plain-`u64` fields, and remains linear in the number of
  imports, aliases, and fields.
- Both mapped-file creation sites now carry immediate, operation-specific `// SAFETY:` comments covering the
  valid sized storage handle, mapping lifetime/no-truncation invariant, cleanup ownership, legacy caller
  obligation, and lazy initialization lock. Every public API in the Local file module has production Rustdoc,
  including applicable `# Errors` and `# Panics` contracts and rename/reopen failure ordering.
- The governed ArcMut import approval is one direct BASE-to-HEAD relocation. The intermediate
  `26d2e0b2c08b64d6e00a411f -> e33954dcd98342cf79a55bc4 -> 763657513cccba1f2db06b65`
  chain was collapsed to `26d2e0b2c08b64d6e00a411f -> 763657513cccba1f2db06b65`; the five governed
  fingerprints are represented by exactly five M06-03d approvals.

## Semantic preservation

- Open order remains metadata-length snapshot, read/write/create/non-truncate open, `set_len`, then preallocation
  only when the old length is smaller. Existing bytes are preserved on growth; shrink truncates without
  preallocation; zero length does not invoke preallocation.
- Numeric filename parsing and the Store inherent wrappers preserve the exact invalid-input and compatibility
  panic text. The Local storage offset remains fixed after rename, matching the legacy parsed-at-construction
  behavior.
- Rename failure changes neither canonical path, Store projection, nor handle. Success updates the Local path,
  then Store projection, then performs the same read-only `File::open`; reopen failure leaves the renamed path and
  old open handle intact. Delete returns neutral I/O success/failure so Store retains its exact success/failure
  logging.
- The moved Linux `fallocate` call is unchanged. Its immediate `// SAFETY:` comment documents the borrowed valid
  file descriptor and checked `off_t` length. No new unsafe operation was added.
- File bytes/layout, set-length ordering, preallocation classification, public paths, feature defaults, tracing
  targets/messages, metrics, and observability behavior remain unchanged.

## Dependency and ArcMut governance

- Local adds one direct `libc = 0.2.186` normal dependency under `cfg(unix)` only, which is required by the
  mechanically moved platform implementation. Local remains independent of Store, Common, Rust, Remoting,
  Broker, RocksDB, and Tiered Store.
- The governed ArcMut baseline remains 1,232 identities and 3,377 occurrences. Five changed fingerprints are
  represented by five explicit one-for-one approvals with no new occurrence:
  - `acf169f0918de3dabff93aa8`: `6e40d6b2bf075d97b6755b7d` -> `3b2123c0e754a03714b9b05f`
    (existing eager-map constructor in `try_new_inner`).
  - `ebb212125033591cebb811ba`: `26d2e0b2c08b64d6e00a411f` -> `763657513cccba1f2db06b65`
    (existing `ArcMut` import adjacent to the new Local storage import after repository rustfmt ordering).
  - `f80c7a60a612919c6a52504e`: `b4aa6b87d22d9ca9e6a02260` -> `8622d524765ef0a77c59a703`
    (existing ArcMut field type in `DefaultMappedFile`).
  - `f77276860cc979566f9dbc1e`: `0c6dcf2166eef2fcf442f868` -> `aade4a10d53ff89ec2766419`
    (same governed shared-UnsafeCell wrapper fingerprint).
  - `22e398389b823d706a043784`: `76247274f271039bf8cd8f1b` -> `4d553633a7d26ce689bc0cd1`
    (same `DefaultMappedFile` struct type reference).
- No dependency-policy, architecture, runtime, error, or lint exception was added.

## Validation

All commands ran from the repository root.

- `cargo test -p rocketmq-store-local` - exit 0; 48/48 unit tests, 6/6 kernel integration tests, and 10/10
  storage integration tests passed (64 total); nine existing rustdoc examples remained ignored.
- `cargo test -p rocketmq-store default_mapped_file_impl::tests` - exit 0; focused adapter tests passed, including
  eager/lazy mmap, transient commit/read/flush, injected flush failure, and file lifecycle.
- `cargo test -p rocketmq-store --test store_lifecycle_capability` - exit 0; 3/3.
- `cargo test -p rocketmq-store --test m06_store_local_compatibility` - exit 0; 3/3.
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` - exit 0; 9/9.
- `cargo test -p rocketmq-store --test m06_store_local_commitlog_compatibility` - exit 0; 2/2.
- `python scripts/tests/test_m06_store_local_contract.py` - exit 0; 39/39.
- Local exact feature checks for no-default, fast-load, safe-load, fast+safe, and io_uring - all exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; the active Windows closure has no forbidden owner edge;
  the manifest contract separately verifies the only Unix-only `libc` edge.
- `python scripts/tests/test_architecture_dependency_guard.py` - exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` - exit 0; one clean and six violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` - exit 0.
- `python scripts/arc_mut_guard.py --fixtures` - exit 0; 24/24 fixtures.
- promoted ArcMut candidate and comparison - exit 0; 1,232 identities, 3,377 occurrences, `ARC_MUT_GUARD_OK`.
- `python scripts/arc_mut_guard.py` - exit 0, `ARC_MUT_GUARD_OK`.
- BASE-versus-HEAD approval comparison - exit 0, `M06_03D_ARC_APPROVALS_OK count=5`; the review fix leaves
  `scripts/arc-mut-baseline.json` byte-for-byte unchanged from the implementation commit.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0.
- `$env:RUSTDOCFLAGS='-D warnings'; cargo doc -p rocketmq-store-local --no-deps` - exit 0; all new public API
  documentation and intra-doc links rendered without warnings.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0. Windows emitted only
  the existing `linker_messages` notice (which explicitly ignores `-D warnings`) and the existing
  `proc-macro-error2` future-incompatibility notice.
- `.\scripts\check-agents-routing.ps1` - exit 0,
  `AGENTS_ROUTING_CHECK_OK standalone_cargo=4 node_projects=3 routes=8`.
- `cargo fmt --all -- --check` and `git diff --check` - recorded after the final report update.

Error hygiene was not triggered because no error definition or mapping changed. Runtime audit was not triggered
because no task, thread, runtime, blocking, scheduler, or shutdown owner changed.

## Non-goals and remaining work

This slice does not move mmap, `ArcMut`, `DefaultMappedFile`, the `MappedFile` trait, messages/callbacks, config,
warmup/memory locking, `TransientStorePool`, flush/group commit, CQ/Index, HA, CommitLog orchestration, recovery
execution, runtime ownership, or persisted formats. PR-M06-03 and every M06 Exit Checklist item remain open.

## Rollback

Restore the Store-owned `File`/path/offset and preallocation implementation, remove the Local file kernel and
focused tests, restore the five prior ArcMut fingerprints, and keep the existing progress/lifecycle Local kernel.
No disk, data, wire, persisted-format, or public-path migration is required.
