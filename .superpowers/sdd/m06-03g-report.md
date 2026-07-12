# M06-03g bounded CommitLog record parser report

## Final outcome

M06-03g uses two ordered implementation commits. Commit A (`00c65505514231ccf89f2746c0378f5eb624c975`)
freezes fail-closed, bounded-frame, checksum-order, and transactional input-advance behavior while the neutral
parser is still owned by Store. Commit B moves that exact parser and DTO surface to
`rocketmq-store-local::commit_log::record_parser`; Store retains only Common-backed checksum and configuration,
property/duplicate/tag/delay policy, forced property CRC, inner-batch validation, `DispatchRequest` mapping, and
the final transactional `Bytes` advance.

The independent review-fix closes blank declared-size validation, removes the per-record heap allocation, adds
complete valid V1/V2 dispatch goldens, strengthens mutations, and passes all final gates. M06-03g is complete;
the PR-M06-03 parent and M06 Exit Checklist intentionally remain open.

## A-to-B semantic identity

- The parser implementation was moved from `rocketmq-store/src/log_file/commit_log_record_parser.rs` to
  `rocketmq-store-local/src/commit_log/record_parser.rs`.
- After removing Rustdoc and normalizing `pub(crate)` to `pub`, the text from `struct RecordReader` through the
  production decoder is byte-for-byte identical between A and the B candidate.
- Store's `check_message_and_return_size` body has no A-to-B diff; only parser import paths changed.
- No `Cargo.toml`, feature, or dependency changed. Local already owned the required `bytes` dependency.

## Current behavior evidence

- A RED run exposed six legacy failure classes: truncated-prefix panics, next-record bleed, negative length
  acceptance/panic, declared-overrun consumption, invalid/CRC failure consumption, and partial size-mismatch
  advancement.
- GREEN fail-closed corpus: 10/10 on A and 10/10 on B, expanded to 13/13 after review. It covers every prefix truncation, negative
  V2 topic/body/property lengths, declared-overrun, next-record isolation, blank/valid advancement, invalid magic,
  body CRC, forced property CRC before size mismatch, size mismatch before inner-batch validation, duplicate and
  inner-batch rollback, and the explicit computed-smaller-than-declared compatibility rule.
- Neutral parser tests: 3/3, including checksum call count/order and complete V1/V2 records across all IPv4/IPv6
  host-width combinations.
- Existing Store property-CRC and malformed inner-batch focused tests: 2/2.
- Local storage ownership/mutation contract: 62/62. It rejects mutable decoder input, unchecked Buf cursor reads,
  dynamic checksum ports, parser aliases/brace/glob imports, duplicate owners, a Store parser module copy, copied
  Store raw parsing, and changed transactional advance sites.

## Compatibility and ownership boundary

The Local parser reads an immutable bounded `Bytes` frame with an internal checked index. Signed lengths are
rejected before conversion, checked arithmetic prevents range overflow, and forced property CRC receives only the
bounded raw frame. The legacy Store wrapper is the sole owner of cursor advancement.

Local now owns V1 and V2 CommitLog storage magic values. Compatibility tests compare their numeric values with
the existing Common and Protocol codec surfaces without claiming whole-repository definition uniqueness.

## Independent review fixes

1. Blank markers now validate `declared >= 8` and `declared <= available` before Store advances. The matrix proves
   8/8 and 64/64 valid, 7/8 and 64/8 invalid, legal advancement remains exactly eight, and every failure advances zero.
2. `CommitLogRecordOutcome::Message` retains `CommitLogRecord` inline. A narrow `large_enum_variant` reason records
   the hot-path allocation decision, and the contract rejects reintroduction of `Box<CommitLogRecord>`.
3. Test-only `DispatchRequestProjection` freezes all 19 fields in one equality assertion and normalizes properties
   to `BTreeMap<String, String>` without adding a production `Eq`/`PartialEq` trait surface. V1 covers topic,
   queue/physical offsets, sysflag, timestamp, tag hash, keys, uniq, valid dup, and valid inner batch. V2 covers
   delay clamp+table and missing-table fallback.
4. The blank-boundary mutation fails if the declared-size check is removed or moved after the blank return.
5. Final ownership review hardens the comment/string-aware active-Rust contract to reject any active `Box`,
   fully-qualified `std::boxed::Box`, and parser type alias. Separate mutations cover direct, qualified, and
   `type HeapRecord = Box<CommitLogRecord>; Message(HeapRecord)` forms while masked comments/strings remain valid.
   The Store wrapper contract now locks the exact legacy signature, requires exactly one Local decode call, rejects
   manual `from_be_bytes` and input Buf get/copy/slice/index parsing, and accepts only the existing ordered advance
   arguments `8`, `total_size as usize`, `total_size as usize`. Removing decode while retaining three advances,
   changing an argument shape, or copying scalar parsing fails deterministically.

These fixes are intentionally isolated after the mechanical B commit.

## Final validation

- Fail-closed/transaction/whole-value integration: 13/13; Store focused property CRC/inner batch: 2/2; record
  compatibility: 3/3.
- `cargo test -p rocketmq-store-local`: 52 unit + 10 record + 6 kernel + 7 mapping + 10 storage tests passed
  (85 total); nine existing Rustdoc examples ignored.
- Ownership/mutation contract: 62/62. Local no-default, fast-load, safe-load, fast+safe, and io_uring checks passed.
- Local and Store all-target/all-feature Clippy, Local `RUSTDOCFLAGS=-D warnings` Rustdoc, and workspace no-deps
  all-target/all-feature Clippy passed. Windows emitted only the existing `linker_messages` notice that ignores
  `-D warnings`, plus the existing future-incompatibility notice for `proc-macro-error2`.
- Architecture dependency unit tests 35/35, fixtures, and baseline mode passed. ArcMut unit tests 63/63, fixtures,
  and final guard passed with no baseline change. `cargo tree` confirms no forbidden Local dependency edge.
- `cargo fmt --all -- --check` and `git diff --check` pass after final report/checklist updates.

## Scope and rollback

This slice does not move append/flush/group commit, CQ/Index, HA, Timer/POP, or Store composition. Rollback the
review-fix, then restore B's Store-owned parser path; persisted bytes and dependency manifests require no migration.
