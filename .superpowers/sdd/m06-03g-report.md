# M06-03g bounded CommitLog record parser report

## Current outcome: under review

M06-03g uses two ordered implementation commits. Commit A (`00c65505514231ccf89f2746c0378f5eb624c975`)
freezes fail-closed, bounded-frame, checksum-order, and transactional input-advance behavior while the neutral
parser is still owned by Store. Commit B moves that exact parser and DTO surface to
`rocketmq-store-local::commit_log::record_parser`; Store retains only Common-backed checksum and configuration,
property/duplicate/tag/delay policy, forced property CRC, inner-batch validation, `DispatchRequest` mapping, and
the final transactional `Bytes` advance.

This report intentionally remains under review. The M06-03g checklist, PR-M06-03 parent, and M06 Exit Checklist
stay open until the independent review-fix commit closes blank declared-size validation, removes the per-record
heap allocation, adds full valid V1/V2 dispatch goldens, strengthens mutations, and passes all final gates.

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
- GREEN fail-closed corpus: 10/10 on A and 10/10 on the B candidate. It covers every prefix truncation, negative
  V2 topic/body/property lengths, declared-overrun, next-record isolation, blank/valid advancement, invalid magic,
  body CRC, forced property CRC before size mismatch, size mismatch before inner-batch validation, duplicate and
  inner-batch rollback, and the explicit computed-smaller-than-declared compatibility rule.
- Neutral parser tests: 3/3, including checksum call count/order and complete V1/V2 records across all IPv4/IPv6
  host-width combinations.
- Existing Store property-CRC and malformed inner-batch focused tests: 2/2.
- Local storage ownership/mutation contract: 55/55. It rejects mutable decoder input, unchecked Buf cursor reads,
  dynamic checksum ports, parser aliases/brace/glob imports, duplicate owners, a Store parser module copy, copied
  Store raw parsing, and changed transactional advance sites.

## Compatibility and ownership boundary

The Local parser reads an immutable bounded `Bytes` frame with an internal checked index. Signed lengths are
rejected before conversion, checked arithmetic prevents range overflow, and forced property CRC receives only the
bounded raw frame. The legacy Store wrapper is the sole owner of cursor advancement.

Local now owns V1 and V2 CommitLog storage magic values. Compatibility tests compare their numeric values with
the existing Common and Protocol codec surfaces without claiming whole-repository definition uniqueness.

## Open review findings

1. Blank markers must validate positive declared size and availability before Store advances eight bytes.
2. `CommitLogRecordOutcome::Message(Box<CommitLogRecord>)` must not allocate on the per-message hot path.
3. Complete valid V1/V2 `DispatchRequest` whole-value goldens must freeze all mapped fields and policy behavior.
4. Mutation fixtures must fail if blank validation is removed or the heap allocation is reintroduced.

These findings are intentionally not mixed into the mechanical B commit.
