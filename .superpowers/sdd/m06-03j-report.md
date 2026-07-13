# M06-03j report: CommitLog file validation boundary

## Outcome

Completed the pure CommitLog file-length validation boundary without changing the loader's public API, filesystem
or mmap ownership, persisted bytes, or recovery behavior. `rocketmq-store-local` now owns the canonical
`CommitLogFileMetadata`, load/remove decision, typed validation error, and pure validator. Store remains the owner
of directory enumeration, real filesystem metadata, empty-tail deletion and warnings, rayon orchestration,
`DefaultMappedFile`, mmap/lazy behavior, memory hints, positions, statistics, and timing.

The zero-sized expected-file edge is explicit: an empty last file is removed, an empty non-last file is loaded,
and a non-empty file is rejected. The error exposes path/actual/expected and preserves the exact legacy text.
Because `PathBuf` does not implement `Display`, the `thiserror` attribute formats `path.display()` while producing
the required byte-for-byte message.

## Compatibility and ownership evidence

- Store's private `FileMetadata` and unused metadata `file_name` field were removed. The canonical metadata value
  now flows from `fs::metadata` through both collectors to mapped-file creation.
- Parallel validation remains inside the indexed rayon map and preserves `Result<Vec<Option<_>>>` ordered collect
  plus flatten. Sequential validation remains in the original enumerate loop and returns its first error.
- `RemoveEmptyLast` retains the original non-fatal delete and both warning paths, then filters the file. Validation
  errors remain `io::ErrorKind::InvalidData` with exact text. `files_removed` intentionally remains zero.
- Directory existence/read/filter/sort/greater-than-four routing, mmap creation, lazy last-file selection, memory
  hints, wrote/flushed/committed positions, and statistics/timing remain Store-owned and unchanged.
- The three new Local types are private Store imports, not facade re-exports. The three existing load-value public
  re-exports, `CommitLogLoader` signatures, and log-file module visibility remain unchanged.

## Red/green and contract evidence

- RED: `cargo test -p rocketmq-store-local --test commit_log_load_tests` exited 101 because the canonical metadata,
  decision, and validator imports did not exist.
- GREEN: the same suite passed 7/7, including exact load, empty-last removal, non-last empty, short/long mismatch,
  expected-zero matrix, typed fields, `Error`, and exact display text.
- Store loader tests passed 14/14. New goldens prove sequential first-error leaves a later empty tail untouched,
  parallel combined corruption returns `InvalidData` for either bad path before mmap creation, and parallel empty
  filtering preserves order, totals, and legacy zero `files_removed` accounting. The empty-tail fixture explicitly
  enables lazy mmap and proves that the first three retained historical files stay lazy/unmapped while the final
  retained file stays eager/mapped.
- The 75-case source contract includes five Local owner mutations and seventeen Store review mutations for wrong
  last-file predicates, validator bypass, copied size logic, decision swaps, fatal delete, error kind/text changes,
  validation moved outside its closure, rayon order/flatten, alias/brace/glob imports, signature drift, and
  `files_removed` increments.

## Review follow-up

The first review found that the original contract searched from the RemoveEmptyLast marker through the remainder
of each collector. Two valid Rust mutations could therefore warn and then return a fatal error from the parallel
or sequential delete-failure branch without being rejected. Both mutations were added first and produced the
expected RED failures. The contract now extracts each collector's exact RemoveEmptyLast arm and nested
`remove_file` Err body. That body must contain only the warning and cannot contain `return Err`, `Err(`, panic,
unwrap, expect, or `?`; the parallel arm must terminate in `Ok(None)`, while the sequential arm must fall through
to the next file after filtering. The two new mutations and the complete contract are GREEN. No production
algorithm changed in this follow-up.

## Validation

Passed:

- `cargo test -p rocketmq-store-local --test commit_log_load_tests`: 7 passed
- `cargo test -p rocketmq-store-local`: 108 passed; 9 doctests ignored
- `cargo test -p rocketmq-store commit_log_loader --lib`: 14 passed; 562 filtered
- `cargo test -p rocketmq-store --test commitlog_load_tests`: 7 passed; 1 stress test ignored
- `cargo test -p rocketmq-store --test commitlog_recovery_tests`: 19 passed
- `cargo test -p rocketmq-store --lib`: 576 passed
- `python -m unittest scripts.tests.test_m06_store_local_contract`: 75 passed
- all five Local no-default/default feature checks
- Local, Store, and workspace all-target/all-feature Clippy
- strict Local Rustdoc and normal Store Rustdoc; Store retained four pre-existing unrelated invalid-HTML warnings
- architecture dependency guard: 35 unit tests, fixtures, and baseline mode
- ArcMut guard: 63 unit tests, 24 fixtures, promoted comparison at 1,232 identities/3,377 occurrences, and final
  repository guard. One existing `DefaultMappedFile` return-type fingerprint changed adjacent to the canonical
  metadata parameter; the direct `8c35b4efc56800f35af07b357b3147c422ea1ed1` baseline-to-candidate relocation
  is approved under ADR-013 with no occurrence count change.
- `cargo fmt --all -- --check` and `git diff --check`

The initial unapproved ArcMut scan correctly reported one NEW and one STALE fingerprint. After the direct
relocation approval and baseline promotion, the final repository guard passed.

## Scope and handoff

M06-03j does not move filesystem orchestration, mmap ownership, append/parser/recovery, flush/group commit,
CQ/Index, HA, Timer/POP, runtime ownership, or persisted formats. PR-M06-03 and the M06 Exit Checklist remain open.
