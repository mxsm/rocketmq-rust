# M06-03h normal recovery dual-watermark state machine report

## Final outcome

M06-03h moves only the normal recovery offset state machine into
`rocketmq-store-local::commit_log::recovery`. Standard and optimized normal recovery keep their existing I/O
readers and Store policy mapping, but route segment start, valid message, blank, invalid record, and source end
through the same Local reducer. Abnormal recovery is deliberately unchanged.

The Local state owns exactly three values: last-valid watermark, truncation watermark, and compatibility policy.
It does not own segment iteration, checkpoint selection, controller/duplication decisions, dispatch, CQ cleanup,
flush/commit writes, runtime work, or a Store/MappedFile/ArcMut dependency.

## RED to GREEN evidence

- Local RED: `cargo test -p rocketmq-store-local --test normal_recovery_state` exited 101 with six expected E0432
  errors because policy/event/action/error/state/summary did not exist. GREEN is 8/8.
- Store contract RED reported 18 missing adapter obligations across the two normal functions: no Local policy,
  five events, five reducer calls, and copied legacy watermark state. GREEN full contract is 64/64.
- Real-path characterization uses one valid persisted frame copied into independent, unmapped 512-byte segment
  fixtures. This avoids changing an unrelated existing append rollover retry and avoids Windows mapped-file write
  locking. The final normal recovery matrix passes in both standard and optimized routes.

## Frozen transition semantics

| Event | Standard | Optimized | Watermark effect |
|---|---|---|---|
| `SegmentStarted` | continue record | continue record | Standard truncate=base; optimized unchanged |
| `MessageAccepted` | continue record | continue record | Standard last=start/truncate=end; optimized end/end |
| `Blank` | next segment | next segment | unchanged |
| `InvalidRecord` | stop recovery | next segment | unchanged |
| `SourceEnded` | stop recovery | next segment | unchanged |

The reducer computes `base + relative` and `start + size` with `checked_add`. Both candidate watermarks must fit
`i64`; errors return a typed value without mutating state. Store converts every `usize`/`i32` frame offset through
`TryFrom`, logs any error, and stops the global normal scan.

## Store boundary and compatibility

Store retains `get_simple_message_bytes`, `BatchMessageIterator`, `RecoveryContext`, parser/CRC/property policy,
`DispatchRequest`, logs, segment switching, controller clamp, ConsumeQueue cleanup, and physical queue watermarks.
Normal recovery still calls dispatch with `do_dispatch=false`; the real fixture proves an absent in-memory CQ is
not rebuilt. Empty CommitLog files still take the existing destroy/reload path before reducer construction.

The four public async recovery signatures are frozen. Contract hashes prove both abnormal bodies are unchanged:

- `recover_abnormally_optimized`: `c16a626f45d97069eff88eb1370ae1195e9e71747c085b2a3fc37512aff093c6`
- `recover_abnormally`: `272dd66951ba29ec54b36e7e9706ab82241299cb5bb154da05c796363624cb9c`

## Contract and mutation resistance

The 64-case active-Rust contract proves the six Local definitions have one owner, the state has exactly three
fields, source end has no invented kind, arithmetic is checked, and Store orchestration cannot enter the reducer.
Store adapters must construct the correct policy, submit each of the five events exactly once, act on all four
record outcomes, and cannot copy mutable last-valid/truncate state or match Local policy. Mutations cover branch
bypass, event replacement, reducer deletion, start/end exchange, unchecked addition, policy/watermark copies,
signature drift, and either abnormal body changing.

## Validation

- Local full: 52 unit + 10 record + 6 kernel + 7 mapping + 10 storage + 8 normal-recovery tests = 93 passed;
  nine existing Rustdoc examples ignored.
- Store recovery: 12/12. CommitLog load: 7/7 with one stress test ignored. Record fail-closed: 13/13. Record and
  recovery-window compatibility: 3/3 and 2/2.
- Ownership/mutation contract: 64/64. Local no-default, fast-load, safe-load, fast+safe, and io_uring checks passed.
- Local/Store all-target/all-feature Clippy, workspace no-deps all-target/all-feature Clippy, and Local
  `RUSTDOCFLAGS=-D warnings` Rustdoc passed. Windows emitted only the existing linker notice and existing
  `proc-macro-error2` future-incompatibility notice.
- Architecture dependency tests 35/35, fixtures, and baseline mode passed. ArcMut tests 63/63, fixtures, and final
  guard passed without a baseline change. `cargo fmt --all -- --check` and `git diff --check` passed.

## Scope and rollback

No manifest, dependency, persisted byte, abnormal recovery algorithm, dispatch rule, checkpoint/window planner,
CQ/Index/HA, flush, or runtime owner changed. Rollback restores the two normal functions' local watermark variables
and removes the Local reducer/API/tests; no data migration is required. PR-M06-03 and the M06 Exit Checklist remain
open.
