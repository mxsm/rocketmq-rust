# M06-01 Store capability spike report

## Final outcome

M06-01 adds the runtime-neutral `rocketmq-store-api` capability boundary and composes it into real
Broker send and admission paths without moving store ownership. The final API exposes eight traits
through associated types and statically dispatched RPITIT futures, plus only the closed
`StoreOperation`, neutral `StoreErrorKind`, and `StoreError` values. Concrete append/read/health/
replication/derived/admin DTOs remain deferred to M06-02.

The compatibility implementation remains in `rocketmq-store`. Its borrowing adapter owns the
unchanged legacy `PutMessageResult`, independent append/durable watermark observations, and exact
legacy health-error tokens. Ordinary single, batch, and transactional-prepare append paths traverse
`MessageAppender`; Broker admission traverses `StoreHealth`. No persisted layout, public
`MessageStore` method, feature, lifecycle owner, task, runtime, or blocking boundary changed.

## Final design and compatibility decisions

- `MessageAppender<M>` remains generic over consumer-owned message inputs, so store-api does not
  depend on Broker or `rocketmq-common` message types.
- The hot append path uses static dispatch. It contains no `StoreFuture`, `dyn Future`,
  `Pin<Box<_>>`, or `Box::pin` allocation.
- `StoreOperation` is a closed vocabulary. Store errors contain no native error, path, message body,
  credential, or implementation detail.
- Broker maps errors by `(StoreOperation, StoreErrorKind)`. For append, `Unavailable` and `NotFound`
  become redacted `BrokerOperationFailed` errors with `SwitchBroker` recovery. Lifecycle
  `Unavailable` remains `Service`, and read `NotFound` remains `QueryNotFound`.
- All 16 legacy `PutMessageStatus` response code/remark pairs are preserved exhaustively.
- All 12 legacy writable-rejection error tokens are preserved exactly in the compatibility layer;
  backend vocabulary is absent from store-api.
- Store-api keeps the previously approved direct dependencies `bytes`, `rocketmq-error`, and
  `rocketmq-model`; production source exposes no concrete type from them in this spike.

## Changed areas

- `rocketmq-store-api/`: minimal capability/error contracts and compile contracts.
- `rocketmq-store/src/store_api_adapter.rs` and its integration test: borrowing append/health
  compatibility adapters.
- `rocketmq-broker/src/processor/send_message_processor.rs`: append, transaction, admission,
  response-parity, health-parity, and typed-error composition.
- `scripts/tests/test_m06_store_api_contract.py`: dependency-table, public-surface, static-future,
  forbidden-vocabulary, and structural production-seam contracts.
- Architecture policy/tests: runtime-neutral store-api dependency edge.
- ArcMut baseline/relocation evidence: one reviewed Broker test glob-import relocation. The final
  fix restores the eight specified Remoting identities to their exact base-branch entries.
- Phase checklist and this report: M06-01 evidence only; later steps remain incomplete.

## Historical TDD evidence

These RED results are historical pre-fix observations; the matching GREEN results describe the
current final state.

1. Minimal capability API
   - RED: `cargo test -p rocketmq-store-api --test capability_contracts` exited 1 because the closed
     operation vocabulary, associated types, and static future signatures were absent.
   - GREEN: the same test target passed 2/2 after removing the concrete M06-02 DTOs and boxed future
     alias.
2. Legacy adapter
   - RED: `cargo test -p rocketmq-store --test store_api_legacy_adapter` exited 1 because the adapter
     referenced the removed API DTOs and did not retain exact legacy health kinds.
   - GREEN: the same command passed 3/3, including a monomorphized generic fixture, unchanged legacy
     result/watermarks, and all 12 compatibility tokens.
3. Complete production seams and parity
   - RED: focused Broker compilation failed on the removed receipt/status APIs, missing associated
     type bounds, and the transactional branch bypassing `MessageAppender`.
   - GREEN: the Broker processor module passed 17/17 with ordinary single, batch, transaction, and
     reject seams plus all status and health parity cases.
4. Append-specific typed errors (second review fix)
   - RED: `cargo test -p rocketmq-broker
     every_store_api_error_kind_preserves_typed_semantics_and_operation --lib` exited 1: append
     `Unavailable` expected `BrokerOperationFailed` but produced `Service`.
   - GREEN: the same command passed 1/1. Its exhaustive append table asserts `ErrorKind`, retry,
     redaction, operation, and boundary view for all 11 neutral kinds; lifecycle/read controls prove
     mapping is operation-sensitive. Append `NotFound` is no longer `QueryNotFound`.
5. Compound forbidden identifiers (second review fix)
   - RED: `python scripts/tests/test_m06_store_api_contract.py` exited 1 with five failures because
     `HaState`, `TimerWheel`, `RocksDbBackend`, `MappedFileHandle`, and `NativeStore` escaped the old
     exact-token comparison.
   - GREEN: the command passed 4/4 after checking normalized substrings in code identifiers while
     excluding comments and string literals. No allowlist was required.
6. Deterministic forbidden-token priority (third review fix)
   - RED: `python scripts/tests/test_m06_store_api_contract.py` exited 1 because unordered set
     iteration classified `MappedFileHandle` as the short token `ha` instead of the specific token
     `mappedfile`. The deterministic-priority assertion also failed, and focused ambiguity fixtures
     showed false `ha` matches for `HashMap`, `HandleState`, and `Chart`.
   - GREEN: the command passed 6/6 after replacing the set with an explicit longest/specific-first
     tuple. The short `ha` token now matches only a CamelCase/snake_case identifier segment; all five
     compound backend fixtures remain enforced.

The first API test draft omitted a `Future` import. That setup-only error was corrected before the
API RED evidence above was recorded.

## Final validation

All commands ran from the repository root with `GIT_CONFIG_NOSYSTEM=1`.

- `cargo test -p rocketmq-store-api` — exit 0; 2/2 integration tests plus unit/doc targets.
- `cargo test -p rocketmq-store --test store_api_legacy_adapter` — exit 0; 3/3.
- `cargo test -p rocketmq-broker processor::send_message_processor::tests --lib` — exit 0; 17/17.
- `python scripts/tests/test_m06_store_api_contract.py` — exit 0; 6/6.
- `Remove-Item Env:PYTHONHASHSEED -ErrorAction SilentlyContinue; foreach ($run in 1..20) { python
  scripts/tests/test_m06_store_api_contract.py; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }`
  — exit 0; 20 independent Python processes, 6/6 in every process
  (`CONTRACT_REPEAT_OK processes=20 tests_per_process=6`).
- `python scripts/tests/test_architecture_dependency_guard.py` — exit 0; 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` — exit 0; one clean fixture and six
  violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` — exit 0.
- `python scripts/arc_mut_guard.py --fixtures` — exit 0; 24 fixtures.
- `python scripts/arc_mut_guard.py --bootstrap
  target/m06-01-fix2-arc-bootstrap-final.json` — exit 0; 1,233 entries.
- `python scripts/arc_mut_guard.py --promote-baseline
  target/m06-01-fix2-arc-promoted-final.json` — exit 0; target-only output with 1,233 identities and
  3,378 occurrences. The output was not copied into the tracked baseline.
- `python scripts/arc_mut_guard.py --compare-baseline
  target/m06-01-fix2-arc-promoted-final.json` — exit 0, `ARC_MUT_GUARD_OK`.
- `python scripts/arc_mut_guard.py` — exit 0, `ARC_MUT_GUARD_OK`.
- A PowerShell JSON comparison of the eight identities named in the second review against
  `7ffeafdf6:scripts/arc-mut-baseline.json` — exit 0,
  `ARC_SELECTED_BASE_ENTRIES_OK identities=8`.
- `cargo clippy -p rocketmq-store-api --all-targets -- -D warnings` — exit 0.
- `cargo clippy -p rocketmq-store --all-targets -- -D warnings` — exit 0.
- `cargo clippy -p rocketmq-broker --lib -- -D warnings` — exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` — exit 0 in
  29.7s. Windows emitted only `linker_messages`, which Rust explicitly states ignores `-D
  warnings`, plus an unrelated `proc-macro-error2` future-incompatibility notice.
- `.\scripts\check-agents-routing.ps1` — exit 0; four standalone Cargo projects, three Node
  projects, eight routes.
- `.\scripts\check-error-hygiene.ps1` — exit 1 only for the pre-existing baseline: one auth source
  stringification site, eight MCP `anyhow` sites, and two missing governance documents. The changed
  send processor is clean; no allowlist or exception was added.
- `cargo fmt --all -- --check` — exit 0 in the final post-report rerun.
- `git diff --check` — exit 0 in the final post-report rerun.

## ArcMut scope and remaining drift

The tracked baseline retains only the M06-01 Broker test-import relocation and restores the exact
base entries for the four `channel.rs` and four `rocketmq_tokio_server.rs` identities named by the
review. Current Remoting source line metadata differs from those base entries; the target-only
promotion reflects that pre-existing drift, while the tracked baseline intentionally does not
absorb it. Identity and occurrence debt remains 1,233 / 3,378, and the normal guard passes because
the governed identities, occurrence IDs, and fingerprints did not expand.

## Remaining concerns

- M06-02 and later DTO/ownership extraction remains intentionally unimplemented.
- The repository error-hygiene baseline remains red only for the unrelated findings listed above.
- The pre-existing Remoting line-number drift is intentionally visible and unpromoted; it does not
  change governed ArcMut identities or occurrences.
