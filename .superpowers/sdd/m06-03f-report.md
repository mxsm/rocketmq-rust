# M06-03f CommitLog frame cursor and magic ownership report

## Final outcome

M06-03f makes `rocketmq-store-local::commit_log::record` the canonical storage-boundary owner of the CommitLog
V1 message magic (`-626843481`), blank magic (`-875286124`), pure blank-marker recognition, static
`CommitLogFrameSource`, and generic `CommitLogFrameCursor<S>`. The port exposes one fixed source length and exact
bounded copied reads; it contains no `dyn` dispatch or Store/Common/Remoting/Broker/Rocks/Tiered edge.

Store keeps the exact legacy `BatchMessageIterator<'a>::new(&'a Arc<DefaultMappedFile>)`, `next_message`, and
`current_offset` signatures. The wrapper contains only the Local cursor over a private generic `MappedFile`
adapter. Store's two CommitLog constants and recovery blank helper are exact Local re-exports, with no copied
cursor algorithm.

## RED -> GREEN evidence

All behavior and ownership tests were added before production edits.

- RED `python -m unittest scripts.tests.test_m06_store_local_contract`: 51 tests ran with the two expected
  failures because `record.rs` was absent and Store still defined the constants/iterator algorithm.
- RED `cargo test -p rocketmq-store-local --test commit_log_record_tests`: exit 101 with five E0432 errors because
  `commit_log::record` did not exist.
- RED `cargo test -p rocketmq-store --test m06_store_local_record_compatibility`: exit 101 with one E0432 for the
  absent Local record module.
- GREEN Local cursor goldens: 7/7. GREEN real-`DefaultMappedFile` Store compatibility: 3/3. GREEN ownership
  contract: 51/51.

The contract's first GREEN run correctly exposed that Common and Protocol already own same-valued wire-codec
constants. The owner assertion was explicitly narrowed to the Local+Store storage boundary, rather than making a
false whole-repository uniqueness claim. Common/Protocol wire constants remain unchanged compatibility surfaces.

## Cursor semantics

- A cursor snapshots `source_len` at construction and begins at offset zero.
- It requires eight bytes before interpreting a frame, peeks signed big-endian total size, and stops without
  advancing on an incomplete header or non-positive size.
- It returns `(Bytes, absolute_offset, frame_size)` and advances only after obtaining one complete frame.
- Exact 64-KiB frames remain on the batched path. Larger frames use one direct exact read and clear the buffer.
  Frames crossing the 64-KiB boundary refill from the unchanged current offset and retry.
- Checked arithmetic and the fixed length reject declared frames beyond the dirty tail. Exact-length validation on
  every successful source read also prevents an invalid short source implementation from causing a refill loop.
- `is_blank_message` requires eight bytes and compares only the second signed big-endian word with the blank magic.

No persisted byte, frame size, magic value, logging target/message, mmap behavior, or ArcMut representation changes.

## Boundary and compatibility contract

The 51-case active-Rust contract proves one Local+Store storage definition for both constants, the helper, port,
and cursor; exact Store constant/helper re-exports; a wrapper-only legacy iterator; no dynamic source port; no
V2 constant in Local record; and the existing forbidden dependency closure. Negative fixtures cover duplicate
constants/functions, aliases, brace/glob imports, copied batch constants/iterator fields, comments, strings, and a
`dyn CommitLogFrameSource` mutation.

The Local deterministic source copies each exact bounded range. The Store adapter delegates to existing
`MappedFile::get_file_size/get_bytes`; production still owns the same `DefaultMappedFile`, mmap, and Arc lifetime.
The real Store fixture creates segments through `MappedFileQueue`, avoiding new governed test debt.

## ArcMut governance

The brief expected no governed relocation because `DefaultMappedFile` itself does not change. Investigation showed
that the changed recovery wrapper contains three existing governed `DefaultMappedFile` occurrences whose AST
fingerprints necessarily change: the module import, the field reference now nested in the private generic source
adapter, and the unchanged public constructor parameter in its new delegation context. These are three direct
BASE-to-HEAD one-for-one approvals with no intermediate chain and no test approval:

- `c98c11c62d7172081fb48e8b`: `05e434792c97159e42a0ba1f` -> `fa7ebcefeae71c5caa42693f`.
- `126b2048d1dc96a60da90a3d`: `4c0ad73a5ebce8a7aa5ce5f9` -> `5640320689c5eee5cd7cf008`.
- `126b2048d1dc96a60da90a3d`: `827442feb29094b6890a4f23` -> `a0396c92d0a67c84a42c19e3`.

The promoted baseline was rebuilt from BASE to remove unrelated line-only churn. Its diff changes exactly three
occurrence objects (9 added/9 removed lines), leaving 1,232 identities and 3,377 occurrences. Final guard reports
`ARC_MUT_GUARD_OK`.

## Validation

All commands ran from the repository root.

- `cargo test -p rocketmq-store-local` - exit 0; 48 unit + 7 record + 6 kernel + 7 mapping + 10 storage tests
  passed (78 total); nine existing Rustdoc examples ignored.
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` - exit 0; 9/9.
- `cargo test -p rocketmq-store --test commitlog_load_tests` - exit 0; 7/7, one stress test ignored.
- M06 Local, CommitLog planning, and record compatibility targets - exit 0; 3/3, 2/2, and 3/3.
- `python -m unittest scripts.tests.test_m06_store_local_contract` - exit 0; 51/51.
- Local no-default, fast-load, safe-load, fast+safe, and io_uring exact checks - all exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; no forbidden owner edge.
- Architecture dependency unit tests - exit 0; 35/35. Fixtures and baseline mode - exit 0.
- ArcMut unit tests - exit 0; 63/63. Fixtures - exit 0; 24/24. Promotion/direct comparison/final guard - exit 0;
  1,232 identities, 3,377 occurrences, exactly three one-for-one production relocations.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0.
- `$env:RUSTDOCFLAGS='-D warnings'; cargo doc -p rocketmq-store-local --no-deps --all-features` - exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0. Windows emitted only
  the existing `linker_messages` notice, which explicitly ignores `-D warnings`, plus the existing
  `proc-macro-error2` future-incompatibility notice.
- `cargo fmt --all -- --check` and `git diff --check` - recorded after final documentation/report updates.

No manifest changed, so routing was not triggered. Error hygiene was not triggered because no error definition or
mapping changed. Runtime audit was not triggered because no task/thread/runtime/blocking/shutdown owner changed.

## Non-goals and remaining work

This slice does not move `MESSAGE_MAGIC_CODE_V2`, Common message/property constants, full message/property/CRC
parsing, `DispatchRequest`, recovery context/config/checkpoint, mmap/ArcMut representation, `DefaultMappedFile`
ownership, append callbacks/encoding, flush/group commit, CQ/Index, or HA. PR-M06-03 and every M06 Exit Checklist
item remain open.

## Rollback

Restore the Store-owned two constants, blank helper, and batched iterator algorithm; remove Local `record` and its
focused tests; restore the three prior governed occurrence objects. No persisted-data, public-signature, feature,
wire, or migration step is required.
