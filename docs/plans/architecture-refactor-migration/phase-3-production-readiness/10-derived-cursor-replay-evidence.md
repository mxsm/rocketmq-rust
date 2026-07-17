# M10-01 derived cursor and replay evidence

## Result

M10-01 establishes the versioned, per-engine progress contract and a deterministic CommitLog replay harness without
connecting derived progress to primary append acknowledgement. The architecture inventory advances to **60/82** work
packages completed, with 22 remaining. M10 has four remaining packages; the next package is PR-M10-02.

## Traceability

| Field | Value |
|---|---|
| Work package | `PR-M10-01` |
| Issue | [#8260](https://github.com/mxsm/rocketmq-rust/issues/8260) |
| Branch | `mxsm/architecture-refactor-derived-cursor-replay-harness` |
| Main baseline | `505c95f03d76b353c256f868f2084551ccd5bdeb` |
| Frozen implementation candidate | `1f2a7bb4ae5d6adf271fc34e63c60c956c22606e` |
| Candidate tree | `f6d1ae0250716403d54599347fc669a68821519c` |
| Runtime evidence | `target/architecture-refactor/M10/derived-cursor-8260/` |

## Contract decisions

- `DerivedRecordId(source_epoch, physical_offset, length)` is the stable idempotency key. Zero length and offset
  overflow fail closed.
- `DerivedCursor` is an exclusive physical offset. It advances only for a record beginning exactly at the current
  cursor; a complete prior record is idempotent, while epoch mismatch, gap, or partial overlap is rejected.
- `DerivedCheckpoint` version 1 is a fixed 32-byte metadata record containing magic, version, engine, reserved byte,
  source epoch, exclusive offset, and CRC32. It contains no topic, queue, message body, or second WAL payload.
- Each `DerivedCursorOwner` is bound to exactly one ConsumeQueue, Index, RocksDB, Tiered, or Compaction namespace. Its
  persistence port must atomically and durably replace the checkpoint before returning success.
- The owner publishes its in-memory cursor only after persistence succeeds. An uncertain persistence result stops
  replay; restart reloads the durable record rather than guessing whether to advance.
- A version-zero offset can be upgraded only with an explicitly proven source epoch. New writers emit version 1; an
  unsupported future version or corrupted checksum fails readiness.
- `replay_derived` reads complete frames from the single CommitLog, applies the idempotent sink before cursor commit,
  stops at a dirty tail or blank marker, and never copies payload into checkpoint metadata.

`AppendReceipt`, primary durable watermarks, feature defaults, existing CommitLog/CQ/Index/Rocks formats, and Broker ack
semantics are unchanged. Actual Tiered filesystem persistence, retry ledger, byte/count/age bounds, WAL pinning, and
provider backpressure remain PR-M10-02 scope.

## Correctness corpus

| Corpus | Result |
|---|---|
| Store API cursor/checkpoint contracts | 7/7 passed: continuity, duplicate, epoch, gap, overlap, corruption, version, upgrade, ack separation |
| Store Local replay harness | 7/7 passed: clean replay, crash after apply, uncertain checkpoint result, dirty-tail repair, corrupted cursor, upgrade, engine isolation |
| Store API complete package | passed: 26 tests across capability, progress, read, and result contracts |
| Store Local complete package | passed, including 185 library tests and all integration/doc-test targets |
| Tiered complete package | passed; existing behavior remains compatible before M10-02 integration |
| RocksDB exact profile | Store/Broker strict Clippy passed; 82 foundation, 9 semantics, Broker 20 Rocks and 4 POP tests passed |

## Public API review

The first signed comparison returned `review-required`. Only two public-path fingerprints changed:

- `rocketmq-store-api`: 86 -> 118 paths, entirely additive progress/checkpoint types, methods, constants, and re-exports;
- `rocketmq-store-local`: 770 -> 799 paths, entirely additive owner/replay types plus bounded frame-cursor methods.

Broker, Proxy, Store facade, Store Inspect, Store RocksDB, and Tiered public path counts and path hashes were unchanged;
their raw Rustdoc hashes changed only because dependency documentation was rebuilt. No item was removed or renamed. After
review, the baseline was signed at candidate `1f2a7bb4ae5d6adf271fc34e63c60c956c22606e`; the final comparison passed
31/31 with zero differences.

## Validation record

| Command | Result |
|---|---|
| focused Store API and replay tests | passed: 7/7 + 7/7 |
| `cargo test -p rocketmq-store-api` | passed: 26 |
| `cargo test -p rocketmq-store-local` | passed |
| `cargo test -p rocketmq-tieredstore` | passed |
| root `cargo fmt --all -- --check`, all-target/all-feature strict Clippy, locked metadata | passed |
| Store/Broker RocksDB strict Clippy and required test matrix | passed |
| dependency fixtures/target/baseline and release guard | passed: target ledger 35/35, dev-only 3/3 |
| ArcMut guard | final passed; zero baseline growth |
| runtime enforcing audit, typed-error hygiene, AGENTS routing, `git diff --check` | passed |
| public API snapshot | final 31/31, differences 0 |

### Disclosed baseline failure

`cargo test -p rocketmq-store` is **not recorded as passed**. It fails the existing
`file_store_vs_rocksdb_behavior_parity_after_restart` fixture because that helper always instantiates
`LocalFileMessageStore` even when its config says `StoreType::RocksDB`; the resulting pseudo-Rocks path has no logical
queue. `git diff --exit-code origin/main -- rocketmq-store/tests/commitlog_recovery_tests.rs` returns zero, proving the
failing fixture is unchanged by M10-01. A temporary real-backend helper made the parity assertion pass, but introduced
new test-only `ArcMut` syntax and was removed when the no-growth guard rejected it. The exact existing RocksDB semantics
suite remains green, and no ArcMut exception or baseline update was added.

The first combined Store command and the first ArcMut guard invocation are therefore superseded failures, not successful
evidence. The remaining risk is the pre-existing misconstructed parity fixture; fixing its factory without new ArcMut is
separate cleanup and does not affect this additive cursor/replay contract.

## Review and rollback

`[REV]` confirms each engine can write only its own 32-byte metadata and no replay path writes a payload/WAL. There is no
new background task, runtime, channel, or primary-ack dependency. Material findings for M10-01: 0.

Rollback the additive Store API/Local modules and public API baseline together. No persisted production checkpoint is
written yet, so rollback requires no data migration. Once M10-02 connects an owner, rollback is stop reader, set
readiness false, freeze the last checkpoint/ledger, pin required WAL, and replay idempotently after repair.
