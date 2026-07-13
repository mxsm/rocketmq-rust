# M06-03l report: CommitLog recovery hint platform execution

## Outcome

Moved recovery mmap-advice and file-prefetch platform execution from Store into
`rocketmq-store-local::commit_log::load`. Store now performs only the required lazy-unmapped guard, obtains the
initialized mmap and filename, and calls two private-imported Local adapters. Platform failures remain non-fatal.

No public Store API, persisted format, recovery algorithm, or Store ffi compatibility function changed. The Local
manifest adds only the Windows APIs required by prefetch; `rocketmq-error` was not added.

## Ownership and platform contract

- `apply_recovery_mmap_advice(RecoveryMmapAdvice, &MmapMut, &str) -> HintOutcome` and
  `apply_recovery_file_prefetch(RecoveryFilePrefetch, &MmapMut, &str) -> HintOutcome` are public safe Local
  functions but are not publicly re-exported by Store.
- Disabled and unsupported cases return `HintOutcome::not_attempted()` before starting a timer. The timer covers
  only the real Unix `madvise` or Windows `PrefetchVirtualMemory` call.
- Unix failures retain `Failed to apply sequential memory hint for ...`; Windows failures retain
  `Failed to prefetch recovery mapped file ...`. Both use the explicit
  `rocketmq_store::log_file::commit_log_loader` tracing target.
- The Windows helper keeps the compatibility error text
  `Storage read failed for 'PrefetchVirtualMemory': {windows error}` and has a precise `SAFETY` rationale directly
  above its only unsafe call. The safe public functions expose no raw pointer or unsafe contract.
- Windows dependency `0.62.2` enables exactly `Win32_System_Memory` and `Win32_System_Threading`. Cargo.lock adds
  the existing `windows` package only to `rocketmq-store-local`'s dependency list.

## Store compatibility

- `apply_memory_hints` checks `is_lazy_mmap_enabled() && !is_mapped()` before `get_mapped_file`, then reads the mmap
  and filename once and invokes the two Local adapters.
- Store no longer imports memmap2 advice, Windows cfg/helper code, or implements either platform adapter.
- `rocketmq-store/src/utils/ffi.rs` is byte-for-byte unchanged. Its public `prefetch_virtual_memory` signature and
  full behavior are also frozen by a mutation-resistant contract.
- Directory enumeration, filesystem validation/deletion, rayon ordering, mapped-file construction, position
  updates, append/recovery/flush behavior, statistics reduction, and public loader/re-export compatibility remain
  Store-owned.

## TDD evidence

- Local focused RED exited 1 with E0432 because `apply_recovery_mmap_advice` and
  `apply_recovery_file_prefetch` did not exist.
- Owner-contract RED exited 1 because the Local canonical owner was missing `apply_recovery_mmap_advice` (and the
  same owner set also required file prefetch).
- GREEN focused suites passed on both platforms: Windows Local 25/25 and Store loader 15/15; WSL/Linux Local 25/25
  and Store loader 15/15.
- A private pure mapper deterministically covers Windows-style `Ok(true)`, `Ok(false)`, and error outcomes.
- The Store lazy test enables both hints and proves historical segments remain lazy/unmapped while only the eager
  final segment contributes the one attempt for the platform-supported hint family.

The final contract has 82 cases. Thirty-five M06-03l reviewer mutations pass: 19 Local owner/reducer/platform, 12
Store adapter/skip/import/aggregation, and 4 Store ffi compatibility mutations. They cover false counted as
success, error changed to skip or `?`, platform cfg swaps, target/text drift, skip deletion/movement after mmap
access, adapter swaps, direct memmap use, duplicate owner, public/alias/brace/glob imports, reducer bypass, ordered
aggregation drift, and ffi signature/body drift.

## Final review follow-up

Final review found that `use tracing::warn;` was unconditional even though both warning sites are platform-gated.
That import is used on Unix or Windows but becomes unused on a target satisfying neither cfg, which would fail a
`-D warnings` build. The focused owner contract first failed with the expected unconditional-import and
non-qualified-warning findings. The fix removes the import and invokes both macros as `tracing::warn!`.

The contract now checks the two adapter function bodies precisely. A new mutation restores the unconditional
import and both bare macros and is rejected, while a future legitimate non-platform warning import elsewhere in
the module is not rejected merely by name. Windows Local focused/full, Store loader focused, the complete 82-case
contract, Local/Store Clippy, strict Local Rustdoc, routing, architecture baseline, Arc final, formatting and diff
checks all pass after the review fix. Re-promoting directly from BASE 8596 with the existing scoped approval again
produced exactly 1,232 identities/3,375 occurrences and was byte-for-byte identical to the canonical baseline;
the review fix therefore adds no Arc relocation or baseline change.

## ArcMut audit

The initial BASE `8596c7d7bcf361dd8f3efaf1762ca4e2032f1b11` comparison reported one NEW and three STALE
occurrences:

- `9a6ddc98fa7af24d0f609721`, `fn apply_mmap_advice`: genuinely deleted with the Store platform function.
- `a2a8693a56e430fe111a4e97`, `fn apply_file_prefetch`: genuinely deleted with the Store platform function.
- Import identity `b4ef5272b75fe61fd822816e` moved directly from `ded036bc867beefc6db3270a` to
  `710eb3f2dd4a57100c5e0fe1` because the two Local adapter imports changed adjacent module tokens.

Only the import has a one-for-one ADR-013 relocation approval. The two removed type references have no relocation.
Promotion and the final guard passed at 1,232 identities and 3,375 occurrences, down from 3,377.

## Validation

Passed on Windows:

- `cargo test -p rocketmq-store-local --test commit_log_load_tests`: 25 passed
- `cargo test -p rocketmq-store-local`: 127 passed; 9 doctests ignored
- `cargo test -p rocketmq-store --lib log_file::commit_log_loader::tests`: 15 passed
- `cargo test -p rocketmq-store --test commitlog_load_tests`: 7 passed; 1 stress test ignored
- `cargo test -p rocketmq-store --test commitlog_recovery_tests`: 19 passed
- `cargo test -p rocketmq-store --lib`: 577 passed
- `python scripts/tests/test_m06_store_local_contract.py`: 82 passed
- all five Local feature checks and Store all-feature check
- Local, Store, and workspace all-target/all-feature Clippy with `-D warnings`
- strict Local Rustdoc and normal Store Rustdoc; Store retained exactly four unrelated pre-existing invalid-HTML
  warnings
- AGENTS routing check, architecture guard 35 tests/fixtures/baseline, and ArcMut guard 63 tests/24 fixtures/
  promotion/final

Passed natively in WSL/Linux with `CARGO_TARGET_DIR=target/wsl-m06-03l`:

- Local focused: 25 passed; Store loader focused: 15 passed
- Local and Store all-feature checks
- Local and Store all-target/all-feature Clippy with `-D warnings`
- `cargo clean --target-dir target/wsl-m06-03l`: removed the isolated target successfully

One initial wrapper around WSL Store check reached its 240-second tool timeout and returned exit 124 while the
underlying Cargo process continued. It was not treated as a validation result: the process was allowed to finish,
no duplicate Cargo was started, and the same check was rerun incrementally with a 20-minute limit and exited 0.

Final `cargo fmt --all -- --check`, `git diff --check`, clean-cache, and worktree checks are recorded at handoff.

## Scope and handoff

M06-03l does not move filesystem or rayon orchestration, mapped-file creation, positions, append/parser/recovery,
flush/group commit, CQ/Index, HA, Timer/POP, runtime ownership, or persisted formats. PR-M06-03, its Exit Checklist,
and M06-04 through M06-12 remain open.
