# M06-03k report: CommitLog mapping plan and hint statistics boundary

## Outcome

Completed the CommitLog mapping-plan and memory-hint statistics boundary in two commits. Commit A moved the pure
mapping decision to `rocketmq-store-local`; commit B moved mmap-advice and file-prefetch outcomes and reducers.
Store still owns filesystem orchestration, rayon execution, real mmap and platform calls, logging, and the
canonical public `LoadStatistics` value.

No manifest, dependency, persisted-format, or public Store API changed. PR-M06-03 and its Exit Checklist remain
open.

## Mapping-plan ownership

- Local owns `CommitLogMappingOptions`, `CommitLogMappingExecution`, `CommitLogMappingMode`, and the immutable
  `CommitLogMappingPlan`. The plan preserves validated/filtered metadata order.
- Parallel execution is selected only when explicitly enabled and the filtered plan contains more than four
  entries. Lazy read-only mapping is selected only when enabled and only for non-final entries.
- Store constructs the plan once after validation/filtering and statistics collection. Both execution branches
  consume its entries and modes; mapped-file creation no longer receives indexes/counts or recomputes lazy mode.
- The public `CommitLogLoader` constructors, `with`, `load`, module visibility, and existing Local load-value
  re-exports remain unchanged.

## Hint-outcome ownership

- Local owns non-`Clone`, non-`Copy`, non-`Default` `HintOutcome` with only `not_attempted`, `success(Duration)`,
  and `failure(Duration)` public constructors. Reducers consume outcomes by value.
- Not-attempted outcomes leave every field unchanged. Success and failure update only their own hint family;
  counters and elapsed milliseconds saturate, sub-millisecond elapsed time is zero, and `Duration::MAX` clamps to
  `u64::MAX`.
- Store's Unix/Windows adapters return Local outcomes. Disabled/unsupported paths, Windows `Ok(false)`, and the
  lazy-unmapped skip are not attempted. Parallel outcomes are collected in mapping order and then reduced
  sequentially; the sequential path reduces directly into the same canonical statistics object.
- The legacy public `LoadStatistics` and two hint-function re-exports remain unchanged. No Local public facade
  re-export was added for the new implementation types.

## RED/GREEN and compatibility evidence

- Mapping RED: the focused Local suite exited 101 with unresolved imports for the four mapping-plan types. The
  initial owner contract also failed because `CommitLogMappingOptions` was absent.
- Hint RED: the focused Local suite exited 101 with unresolved imports for `HintOutcome` and both reducers. The
  owner contract likewise failed because `HintOutcome` was absent.
- Mapping GREEN added 15 focused cases. The final focused suite has 23 cases, including eight hint cases for
  not-attempted, success/failure family isolation, sub-millisecond conversion, `Duration::MAX`, and saturation.
- Existing Store raw-five/raw-six compatibility goldens passed before and after the extraction. Final loader,
  load, recovery, and Store-lib suites preserve filtering, order, lazy/eager mapping, error, and statistics
  behavior.
- The final 81-case source contract proves single Local owners, exact APIs and fields, one-time Store planning,
  private imports, ordered hint reduction, and unchanged public compatibility surfaces. Its mutations reject
  copied owners, threshold/mode changes, reducer-family leakage, non-saturating arithmetic, reordered reduction,
  public re-exports, and alias/brace/glob bypasses.

## ArcMut evidence

The initial default-baseline comparison reported exactly eight relevant NEW and eight STALE governed
occurrences. All are direct one-for-one relocations of identity `909cf176383a06022042b628`; no occurrence was
added or removed:

| BASE occurrence | HEAD occurrence | Adjacent boundary change |
|---|---|---|
| `2ac9ba9fffd71630303af555` | `bb43af94bef98a26d1dac64a` | parallel loader result/signature |
| `80a665b77b920adaf46457ce` | `e820ef0e7b9824b99ccb6182` | sequential loader result/signature |
| `1149181ee9e04595c8db7578` | `1a54bcd9461bfdc5db574061` | mapped-file entry parameter |
| `6213960d38a7afa095b2f862` | `06482c333cf283e2b50d7f9d` | lazy constructor mode match |
| `f35b4c661a7435b0506534ba` | `fb223d4d74b6fe1c75435d2f` | eager constructor mode match |
| `da42e42bb50a5fb519f28750` | `837094604bbc640a7c339b3a` | memory-hint outcome boundary |
| `e82261a89f8e3e040679481b` | `9a6ddc98fa7af24d0f609721` | mmap-advice outcome return |
| `f0a39403be9e63ed5834b36f` | `a2a8693a56e430fe111a4e97` | file-prefetch outcome return |

The scoped approvals are recorded in `m06-03k-arc-mut-approvals.json` and copied to the canonical approval file.
Promotion retained exactly 1,232 identities and 3,377 occurrences. The final repository guard passed.

## Validation

Passed:

- `cargo test -p rocketmq-store-local --test commit_log_load_tests`: 23 passed
- `cargo test -p rocketmq-store-local`: 124 passed; 9 doctests ignored
- `cargo test -p rocketmq-store --lib log_file::commit_log_loader::tests`: 15 passed; 562 filtered
- `cargo test -p rocketmq-store --test commitlog_load_tests`: 7 passed; 1 stress test ignored
- `cargo test -p rocketmq-store --test commitlog_recovery_tests`: 19 passed
- `cargo test -p rocketmq-store --lib`: 577 passed
- `python scripts/tests/test_m06_store_local_contract.py`: 81 passed
- all five Local no-default/default feature checks and Store all-feature check
- Local, Store, and workspace all-target/all-feature Clippy with `-D warnings`
- strict Local Rustdoc and normal Store Rustdoc; Store retained four pre-existing unrelated invalid-HTML warnings
- architecture dependency guard: 35 unit tests, fixtures, and baseline mode
- ArcMut guard: 63 unit tests, 24 fixtures, scoped promotion, and final repository guard
- `cargo fmt --all -- --check` and `git diff --check`

## Scope and handoff

M06-03k does not move directory/filesystem metadata validation or deletion, real mmap or platform calls,
append/parser/recovery, flush/group commit, CQ/Index, HA, Timer/POP, runtime ownership, or persisted formats.
PR-M06-03 and every M06 Exit Checklist item remain open for later slices.
