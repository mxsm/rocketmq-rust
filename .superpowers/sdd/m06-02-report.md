# M06-02 neutral receipt/read results and compatibility bridge report

## Final outcome

M06-02 adds canonical runtime-neutral append, progress, health, and leased read results to
`rocketmq-store-api`. The existing `rocketmq-store` facade now projects exact legacy append,
health, Get, Query, and SelectMappedBuffer outcomes through narrow capabilities without copying or
extending the 126-method `MessageStore` trait. No physical Local module, WAL owner, persisted
layout, feature, lifecycle, runtime, or blocking boundary moved.

The hot read boundary is generic over its lease type. `rocketmq-store-api` contains no native
selected-buffer or backend type and no `Arc<dyn Trait>`. The Store compatibility lease privately
owns the unchanged `SelectMappedBufferResult`, so its existing `Drop` remains responsible for
release while neutral consumers see only `Bytes` and stable metadata. The canonical compatibility
request is deliberately unfiltered; filtered reads remain on the unchanged legacy trait, so no
per-read dynamic filter allocation or dispatch was moved into the canonical hot path.

## Design and compatibility decisions

- `AppendReceipt::try_new` and `try_rejected` enforce non-empty ordered ranges, accepted/rejected
  status use, ordered progress watermarks, append coverage, and explicit durability coverage.
  Invalid combinations return the closed `AppendReceiptError` rather than being normalized or
  panicking.
- `Durability::{Memory, Local, Replicated}` is explicit. The legacy bridge proves only `Memory` or
  `Local` from the durable watermark; it never infers replica durability from a legacy status.
- All 16 `PutMessageStatus` variants map one-to-one to distinct neutral `AppendStatus` variants.
  `LegacyAppendReceipt` still owns the original `PutMessageResult`, preserving Broker response
  codes, remarks, message IDs, timestamps, logical offsets, and public paths.
- `DerivedProgress` exposes source and derived watermarks but its primary-ack and
  primary-durability predicates are always false by contract.
- `StoreHealthSnapshot` contains only neutral error categories and low-cardinality health data.
  `LegacyStoreHealthSnapshot` retains all 12 exact compatibility tokens and provides a canonical
  projection through the exhaustive neutral error mapping.
- `LeasedBytes<L>`, `SelectResult<L>`, `GetResult<L>`, and `QueryResult<L>` preserve byte lifetime,
  navigation, accounting, cache, and index-safety fields. Bytes are dropped before their lease
  guard; consuming `into_bytes` returns independently owned `Bytes`.
- `LegacyMessageStoreReadAdapter` uses a closed unfiltered request enum and optional result enum to
  forward existing Get, size-limited Get, Query, and Select methods. Its adapter-local four-method
  `LegacyReadCallBoundary` has a blanket implementation for `MessageStore`, allowing real behavior
  tests without copying the 126-method trait. It adds no required legacy method and preserves
  legacy `None` rather than translating it into an error.
- No Serde/default envelope, feature alias/default, CommitLog/CQ/Index behavior, response mapping,
  persisted format, unsafe region, task, thread, runtime, or blocking call changed.

## RED -> GREEN evidence

Every listed RED occurred before its corresponding production edit.

1. Canonical receipt/progress/health/lease values
   - RED: `cargo test -p rocketmq-store-api --test result_contracts` exited 1 with unresolved
     imports for `AppendReceipt`, `AppendStatus`, `DerivedProgress`, `Durability`, `LeasedBytes`,
     and `StoreHealthSnapshot`.
   - GREEN: the same command passed 5/5 after the minimal value contracts were implemented.
2. Neutral Get/Query/Select results
   - RED: `cargo test -p rocketmq-store-api --test read_result_contracts` exited 1 with unresolved
     imports for `GetResult`, `GetStatus`, `QueryResult`, `ReadCacheState`, and `SelectResult`.
   - GREEN: the same command passed 3/3 after the generic read DTOs were implemented.
3. Legacy compatibility bridge
   - RED: `cargo test -p rocketmq-store --test store_api_legacy_adapter` exited 1 because exhaustive
     append/get/error mapping functions, the canonical receipt projection,
     `LegacyMessageStoreReadAdapter`, and `LegacyReadResult` did not exist.
   - GREEN: the same target passed 6/6 after the bridge and compile fixture were implemented.
4. Canonical health projection
   - RED: the focused `legacy_health_exposes_a_backend_neutral_canonical_projection` test exited 1
     because `LegacyStoreHealthSnapshot::canonical` did not exist.
   - GREEN: the focused test passed 1/1; the complete adapter target now passes 7/7.
5. Review fix: dynamic filter hidden by an alias
   - RED: `python scripts/tests/test_m06_store_api_contract.py` exited 1 because alias-aware request
     inspection found the old `ArcMessageFilter` inside `LegacyReadRequest`.
   - RED: the generic compile fixture exited 1 before the adapter/request type parameters existed.
   - An intermediate generic request passed those checks, but a stricter contract then correctly
     failed because it still allocated/coerced `ArcMessageFilter` on every read.
   - GREEN: the final source target passed 8/8 and the compile fixture passed 1/1 after removing
     filters from the canonical request entirely. Filtered calls remain only on legacy
     `MessageStore`; the canonical adapter source contains no `ArcMessageFilter` token.
6. Review fix: receipt invariants
   - RED: `cargo test -p rocketmq-store-api --test result_contracts` exited 1 because
     `AppendReceiptError`, `try_new`, and `try_rejected` did not exist.
   - GREEN: the target passed 11/11 after enforcing empty/reversed range, status/range, append
     coverage, progress ordering, and Local/Replicated/Memory durability invariants; the final
     target passes 12/12 including byte-before-lease drop ordering.
7. Review fix: fallible legacy receipt projection
   - RED: the focused legacy receipt target failed to compile because the bridge still called the
     removed infallible constructors and could not expose projection errors.
   - GREEN: both legacy receipt tests passed. Invalid legacy combinations preserve the original
     `PutMessageResult` and return the typed projection error without panic.
8. Review fix: real legacy read behavior
   - RED: `cargo test -p rocketmq-store --test store_api_legacy_read_behavior` exited 1 because
     `SelectResult::into_data` did not exist for the lease-safe consuming path.
   - GREEN: the new target passed 5/5 with actual size-limited/normal Get, Query, Select, and `None`
     dispatch; direct Get/Query/Select projection; field parity; lease retain/release; and safe
     `into_bytes` after releasing the compatibility lease.

## Final validation

All commands ran from the repository root with `GIT_CONFIG_NOSYSTEM=1`.

- `cargo test -p rocketmq-store-api` - exit 0; 17 integration tests and doc targets passed.
- `cargo test -p rocketmq-store-api --no-default-features` - exit 0; the same 17 tests and doc
  targets passed with no default features.
- `cargo doc -p rocketmq-store-api --no-deps` - exit 0.
- `cargo test -p rocketmq-store --test store_api_legacy_adapter` - exit 0; 8/8.
- `cargo test -p rocketmq-store --test store_api_legacy_read_behavior` - exit 0; 5/5.
- `cargo test -p rocketmq-broker processor::send_message_processor::tests --lib` - exit 0; 17/17
  M06-01 seam/status/health/error parity tests. Existing unrelated test imports emitted two
  ordinary warnings; the command succeeded.
- `cargo tree -p rocketmq-store-api -e normal` - exit 0; only the approved `bytes`,
  `rocketmq-error`, and `rocketmq-model` direct edges are present.
- `python scripts/tests/test_m06_store_api_contract.py` - exit 0; 8/8.
- `python scripts/tests/test_architecture_dependency_guard.py` - exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` - exit 0; one clean and six
  violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` - exit 0.
- `python scripts/arc_mut_guard.py --fixtures` - exit 0; 24 fixtures.
- `python scripts/arc_mut_guard.py` - exit 0, `ARC_MUT_GUARD_OK`.
- `cargo clippy -p rocketmq-store-api --all-targets -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets -- -D warnings` - exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0.
  Windows emitted only `linker_messages`, which explicitly ignores `-D warnings`, plus the
  existing `proc-macro-error2` future-incompatibility notice.
- `.\scripts\check-error-hygiene.ps1` - exit 1 only for the pre-existing baseline: one auth source
  stringification site, eight MCP `anyhow` sites, and two missing governance documents. All core
  surfaces, mappings, allowlists, redaction guards, and changed paths were clean.
- `cargo fmt --all -- --check` - exit 0 in the final post-report check.
- `git diff --check` - exit 0 in the final post-report check.

Routing validation was not triggered because no manifest, workflow, project boundary, routing
script, routing ADR, or `AGENTS.md` changed. Runtime ownership, RocksDB, observability, and MCP
specialized gates were not triggered by these result/adapter changes.

One initial combined focused-validation shell was interrupted by its 120-second outer timeout and
was not counted. Every command in the final evidence above was rerun to a definitive exit code.
During review-fix validation, the first package Clippy run correctly rejected a literal reversed
range in the regression fixture. The fixture now constructs `Range { start, end }` without an
allow; both package and workspace Clippy then passed.

## Remaining concerns

- Repository-wide error hygiene remains red only for the unchanged baseline listed above.
- The exact legacy linker and unrelated Broker test-import warnings remain visible; neither comes
  from an M06-02 changed path.
- Physical Local extraction and all M06-03+ work remain intentionally unimplemented.
