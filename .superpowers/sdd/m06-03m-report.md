# M06-03m report: CommitLog filesystem metadata collection

## Outcome

Moved CommitLog filesystem metadata collection, size validation, empty-final-file deletion, and filtering from
Store to `rocketmq-store-local::commit_log::load`. Store now performs one private Local adapter call before its
unchanged statistics and mapping-plan flow.

No public Store API, persisted format, recovery algorithm, mapped-file factory, position update, or feature
definition changed. The Local manifest adds the existing `rayon 1.12` dependency as an ordinary dependency;
Store retains rayon for mapped-file creation.

## Local ownership contract

- `CommitLogMetadataCollectionOptions { expected_file_size, parallel_enabled }` is a named, copyable options
  value. `collect_commit_log_metadata(&[PathBuf], options)` is the sole public collection entrypoint.
- Execution is parallel only when enabled and the raw input contains more than four paths. The last index is also
  derived from the raw input, before an empty final file can be filtered.
- The private parallel collector uses indexed `par_iter`, preserves ordered collection and flattening, and retains
  `Failed to get metadata for {:?}: {}` with the source error kind. No minimum failing-index guarantee was added.
- The private sequential collector uses raw `fs::metadata(path)?`, so it returns the first traversal error and
  never reaches a later empty final file after an earlier validation error.
- Both paths pass complete metadata through the canonical Local validator. Validation failures remain
  `io::ErrorKind::InvalidData` with the existing typed error and exact display text.
- Empty-final deletion is best effort. Both success and failure use the explicit legacy
  `rocketmq_store::log_file::commit_log_loader` target and unchanged warning text; delete failure is non-fatal.

## Store compatibility

- Store retains directory existence/read/filter/sort, timing and total statistics, `files_removed = 0`, mapping
  planning, rayon mapped-file construction, memory hints, position updates, and all loader constructors/methods.
- Store privately imports only `collect_commit_log_metadata` and `CommitLogMetadataCollectionOptions` for this
  boundary. Its old parallel/sequential metadata collectors and Local validator/decision/metadata imports are
  gone, and the new Local API is not publicly re-exported.
- Filesystem collection uses the raw-path threshold; the already-extracted mapping plan independently uses the
  filtered-metadata threshold. The raw-five/one-empty-final fixture therefore collects in parallel but maps the
  four survivors sequentially, preserving compatibility.

## TDD and mutation evidence

The Local focused RED failed to compile because the options type and collection API were absent. The owner
contract RED likewise failed because the canonical owner set was incomplete. After implementation, Windows and
WSL/Linux Local focused suites passed 36/36 and Store loader suites passed 15/15.

The final contract remains 82 cases. Fifteen focused Local mutations and nine Store adapter mutations reject
threshold/last-index drift, collector swaps, non-indexed iteration, error-context or flatten changes, InvalidData
changes, warning target/text drift, public helper leakage, public/aliased imports, wrong options, adapter bypass,
Store filesystem-owner reintroduction, public signature drift, and `files_removed` changes.

## Feature truth

Passed:

- Local default, no-default, fast, safe, fast+safe, and all-feature checks.
- Store default and all-feature checks.
- Store no-default checks with its real owner explicitly enabled:
  `local_file_store,fast-load`, `local_file_store,safe-load`, and
  `local_file_store,fast-load,safe-load`.

The following raw Store combinations remain a pre-existing non-passing baseline and are not reported as passed:

- `cargo check -p rocketmq-store --no-default-features`
- the same command with only `fast-load`, only `safe-load`, or `fast-load,safe-load`

Each exits 101 because disabling every backend leaves `GenericMessageStore` empty, producing 124 E0004
non-exhaustive-match errors plus the existing unused `ArcMut` import. `git diff --exit-code` from BASE
`c90af5db7543b2d54503eca3d751a40d025b1c3d` proves zero changes to
`rocketmq-store/src/message_store.rs`, `rocketmq-store/Cargo.toml`, and the root `Cargo.toml`. The contract also
freezes the feature definitions. This slice does not alter configuration to conceal the baseline.

## ArcMut audit

The canonical initial and final guards pass. Direct BASE-to-candidate promotion reports 1,232 identities and
3,375 occurrences, exactly matching the canonical baseline, with zero NEW and zero STALE occurrence ids. Nine
existing occurrences have line-only metadata updates after production code moved; every id, fingerprint, and item
is unchanged. No relocation approval exists or is needed, and `scripts/arc-mut-baseline.json` remains unchanged.
The generated candidate stays under `target/` for transient audit only and is not committed.

## Validation

Passed on Windows:

- `cargo fmt --all -- --check`
- Local focused 36/36 and Local full 139 tests; 9 doctests remain intentionally ignored
- Store loader 15/15, load integration 7 passed/1 ignored, recovery integration 19/19, Store lib 577/577
- `python scripts/tests/test_m06_store_local_contract.py`: 82/82
- all Local feature checks, Store default/all checks, and all three explicit `local_file_store` owner combinations
- Local, Store, and workspace all-target/all-feature Clippy with `-D warnings`
- strict Local Rustdoc; normal Store Rustdoc with exactly four unrelated pre-existing invalid-HTML warnings
- AGENTS routing, architecture guard 35 unit tests/fixtures/baseline, and ArcMut 63 unit tests/24 fixtures/final

Passed natively in WSL/Linux using `CARGO_TARGET_DIR=target/wsl-m06-03m`:

- Local focused 36/36, Store loader 15/15, and Local/Store default checks
- `cargo clean --target-dir target/wsl-m06-03m`: removed 8,386 files (4.2 GiB)

Non-authoritative probes are recorded explicitly:

- The first full contract run produced eight expected failures in old Store-owned checkers; after converting them
  to precise Local/Store function scopes, all 82 pass.
- Explicit Arc milestone override `M06` exits 1 with 642 existing M05 deadline findings; the canonical command
  correctly uses the baseline's current milestone and passes.
- An initial promotion output outside `target/` exits 2 by guard policy and writes nothing. The target candidate
  promotion and baseline comparison both pass; a byte-equality wrapper exits 2 only because nine line fields are
  updated, while the governed ids/fingerprints/items and counts are identical.

## Scope and handoff

M06-03m does not move directory enumeration, mapped-file factory or positions, append/parser/recovery,
flush/group commit, CQ/Index, HA, Timer/POP, runtime ownership, or persisted formats. PR-M06-03, its Exit
Checklist, and M06-04 through M06-12 remain open.
