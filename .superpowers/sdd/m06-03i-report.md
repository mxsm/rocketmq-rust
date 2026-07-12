# M06-03i report: abnormal recovery boundary extraction

## Outcome

Completed the abnormal CommitLog recovery decision boundary without changing the public recovery signatures or
normal recovery. `rocketmq-store-local` now owns the pure standard/optimized abnormal reducer: three watermarks,
dispatch eligibility, segment/end actions, and checked offset arithmetic. Store remains the orchestration owner
for recovery windows, mapped files, parsing and CRC, fresh confirm limits, dispatch and statistics, ConsumeQueue
cleanup, controller clamp, and physical truncation.

The reducer separates the encoded physical-offset plus raw input-size confirm candidate from the validated record
size used for physical progress. Negative encoded offsets, addition overflow, and values above `i64::MAX` now stop
abnormal recovery through typed adapter/reducer errors without partially committing state. This is a deliberate
fail-closed safety tightening for corrupt input; valid persisted bytes and route semantics remain unchanged.

## Compatibility evidence

- Standard recovery keeps frame-start `last_valid_offset`, frame-end truncation, blank segment roll, and global
  stop on invalid/source-end input.
- Optimized recovery keeps absolute frame-end progress and continues to the next segment for blank,
  invalid-record, and source-end input.
- Ungated, duplication-bounded, and controller-bounded paths obtain the confirm limit for every accepted record.
  An ineligible record advances physical recovery but does not dispatch.
- Controller completion clamps from `confirm_valid_offset`; non-controller completion preserves the route-specific
  `last_valid_offset`. ConsumeQueue compatibility includes the legacy negative-max-offset behavior.
- The normal recovery APIs and implementation, recovery-window selection, checkpoint layout, CommitLog bytes,
  Cargo manifests, dependencies, and runtime ownership are unchanged.

## Red/green and review evidence

- Seven Local reducer tests were RED with unresolved imports before implementation, then passed with exact state,
  action, gate, overflow, and transaction assertions.
- Store tests cover real two-segment bytes, dirty tails, blank hooks, invalid/source ends, later empty segments,
  negative ConsumeQueue offsets, duplication gating, and controller clamping. The duplication golden proves the
  first frame dispatches, the second frame is skipped, and both frames remain in the physical truncate watermark.
- The controller golden proves the first encoded input end is eligible, the second is not, and both Standard and
  Optimized routes clamp the final confirm checkpoint to the first input end.
- The 70-case source contract includes 22 reviewer mutations for gate comparisons, stale limits, raw-versus-
  validated sizes, skip dispatch, blank dispatch, stats, seed selection, summaries, controller branches,
  ConsumeQueue truncation, unchecked arithmetic, aliases, and Store-side policy duplication.
- The real gate fixtures reuse the existing Store constructor without adding any governed `ArcMut` or
  `LocalFileMessageStore` occurrence; the final ArcMut guard reports `ARC_MUT_GUARD_OK`.

## Validation

Passed:

- `cargo test -p rocketmq-store-local` — 101 tests passed; 9 doctests ignored
- the five Local no-default/default/feature checks
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` — 19 passed
- `cargo test -p rocketmq-store --lib` — 573 passed
- focused load, fail-closed record, compatibility, CQ formula, and typed candidate tests
- `python -m unittest scripts.tests.test_m06_store_local_contract` — 70 passed
- Local and Store all-target/all-feature Clippy
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc -p rocketmq-store-local --no-deps`
- architecture dependency guard — 35 unit tests, fixtures, and baseline mode
- ArcMut guard — 63 unit tests, 24 fixtures, and final repository guard
- `cargo fmt --all -- --check` and `git diff --check`

Windows emitted informational MSVC linker-library messages during successful builds. They do not fail the
repository Clippy profile.
