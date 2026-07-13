# M06-03n report: CommitLog file discovery

## Outcome

Moved CommitLog directory discovery, regular-file filtering, and stable filename ordering from Store to
`rocketmq-store-local::commit_log::load`. Store now calls one private Local discovery API and exhaustively maps
the result into its unchanged warnings, timing, metadata collection, mapping, and statistics flow.

No public Store re-export, public loader signature, persisted format, mapped-file factory, recovery algorithm,
runtime ownership, dependency, manifest, or feature definition changed.

## Local ownership contract

- `CommitLogFileDiscovery` has exactly `DirectoryMissing`, `NoFiles`, and `Files(Vec<PathBuf>)` variants and
  derives only `Debug`, `PartialEq`, and `Eq`.
- `discover_commit_log_files(&Path) -> io::Result<CommitLogFileDiscovery>` is the sole discovery owner.
- A missing path returns `DirectoryMissing`. An existing empty directory, a directory containing only
  subdirectories, or a directory whose readable entries contain no regular files returns `NoFiles`.
- A root `fs::read_dir` error propagates. Individual directory-entry errors are skipped exactly as before, and
  directories or other non-regular paths are excluded with `Path::is_file`.
- Files use stable `sort_by` over `file_name().and_then(OsStr::to_str)`. This preserves lexicographic string
  ordering (`"10" < "2"`) and orders a non-UTF filename (`None`) before a UTF filename (`Some`). It does not
  switch to numeric, full-path, unstable, or lossy ordering.

## Store compatibility

- Store privately imports only `discover_commit_log_files` and `CommitLogFileDiscovery` for this new boundary;
  neither is re-exported through the Store facade.
- `DirectoryMissing` preserves `CommitLog directory does not exist: {}` and returns empty files with zero file
  statistics and `total_load_time_ms == 0`.
- `NoFiles` preserves `No commit log files found in {}`, records elapsed total load time, and returns before the
  metadata timer. `Files` continues into the previously extracted metadata collection and mapping flow.
- Store no longer imports `std::fs` or `PathBuf` and no longer owns `read_dir`, entry flattening/path extraction,
  regular-file filtering, or sorting.

## TDD and mutation evidence

The Local focused RED failed with E0432 because `discover_commit_log_files` and `CommitLogFileDiscovery` did not
exist. The contract RED independently failed because the canonical owner set was absent. After implementation,
Windows Local focused passed 41/41 and Store loader passed 16/16; WSL/Linux Local focused passed 42/42, including
the Unix non-UTF filename fixture, and Store loader passed 16/16.

The final M06 contract has 84 passing cases. Ten Local discovery mutations reject variant/derive drift,
`try_exists`, `flatten`, directory filtering, unstable/reversed/numeric/full-path ordering, and empty-result drift.
Nine Store adapter mutations reject public or aliased imports, missing/empty timing drift, bypassed discovery,
reintroduced Store filesystem ownership, and warning/info text changes.

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

Each exits 101 with 124 E0004 non-exhaustive-match errors because no backend owner is enabled. A BASE
`a9b26f62bada1112d0875d4474672f6284b843a5` zero-diff check covers the root `Cargo.toml`, Store `Cargo.toml`,
and `rocketmq-store/src/message_store.rs`, proving this slice did not change the failing feature baseline.

## Platform evidence

Passed natively in WSL/Linux with `CARGO_TARGET_DIR=target/wsl-m06-03n`:

- Local focused 42/42, including `discovery_sorts_non_utf_name_before_utf_name`.
- Store loader 16/16.
- `cargo clean --target-dir target/wsl-m06-03n` removed 5,920 files (3.4 GiB).

Windows coverage independently freezes missing/empty/only-subdirectory discovery, regular-file filtering,
lexicographic ordering, root `read_dir` error propagation, and missing-versus-empty timing behavior.

## ArcMut audit

The canonical initial and final guards pass. The canonical baseline blob is byte-identical to BASE
`a9b26f62bada1112d0875d4474672f6284b843a5`. Direct BASE-to-candidate promotion reports 1,232 identities and
3,375 occurrences on both sides, zero NEW, zero STALE, and zero fingerprint/item changes. Nine existing
occurrences have line-only metadata changes after Store loader lines shifted. No relocation approval is needed,
and `scripts/arc-mut-baseline.json` remains unchanged. The candidate is transient under `target/` only.

## Validation

Passed on Windows:

- `cargo fmt --all -- --check`
- Local focused 41/41 and Local all-feature full 144 tests; 9 doctests remain intentionally ignored
- Store loader 16/16, load integration 7 passed/1 ignored, recovery integration 19/19, Store lib 578/578
- `python scripts/tests/test_m06_store_local_contract.py`: 84/84
- all Local feature checks, Store default/all checks, and all three explicit `local_file_store` owner combinations
- Local, Store, and workspace all-target/all-feature Clippy with `-D warnings`
- strict Local Rustdoc; normal Store Rustdoc with exactly four unrelated pre-existing invalid-HTML warnings
- AGENTS routing, architecture guard 35 unit tests/fixtures/baseline, and ArcMut 63 unit tests/24 fixtures/initial/final
- `git diff --check`, checklist consistency review, BASE feature-owner zero diff, and final clean-worktree check

Non-authoritative probes are recorded explicitly:

- The first Local Rustdoc invocation passed `-D warnings` as a Cargo argument and exited 1 before compilation;
  the corrected `RUSTDOCFLAGS=-D warnings` command passed.
- The four raw Store no-default probes are the documented 124-E0004 pre-existing baseline, not passing feature
  combinations.

## Scope and handoff

M06-03n moves only CommitLog directory discovery, filtering, and filename sorting. It does not change metadata
validation/empty-final filtering, mapped-file construction or positions, append/parser/recovery, flush/group
commit, CQ/Index, HA, Timer/POP, runtime ownership, or persisted formats. PR-M06-03, its Exit Checklist, and
M06-04 through M06-12 remain open.
