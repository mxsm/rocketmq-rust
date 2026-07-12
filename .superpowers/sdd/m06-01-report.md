# M06-01 Store capability spike report

## Outcome

M06-01 introduces the runtime-neutral `rocketmq-store-api` boundary, a borrowing compatibility
adapter over the existing `MessageStore`, and one real Broker send-processor composition seam.
The main single-message and batch append paths now consume `MessageAppender<M>` and the admission
path consumes `StoreHealth`; the legacy store remains the active implementation behind adapters.
No M06-02 or later ownership move was implemented.

## Changed files

- `Cargo.toml`, `Cargo.lock`: register and lock the new workspace crate.
- `rocketmq-store-api/Cargo.toml`, `rocketmq-store-api/src/lib.rs`: define the empty-default-feature
  crate, eight narrow capabilities, runtime-neutral values, append/durable watermarks, health, and
  backend-neutral typed errors.
- `rocketmq-store-api/tests/capability_contracts.rs`: compile/composition, error, writable, and
  watermark contracts.
- `rocketmq-store/Cargo.toml`, `rocketmq-store/src/lib.rs`,
  `rocketmq-store/src/store_api_adapter.rs`: expose borrowing legacy append/health adapters and
  exhaustive legacy status/error mappings without changing `MessageStore`.
- `rocketmq-store/tests/store_api_legacy_adapter.rs`: legacy compile fixture, status mapping, output,
  and watermark parity.
- `rocketmq-broker/Cargo.toml`, `rocketmq-broker/src/processor/send_message_processor.rs`: wire the
  actual send/reject paths through narrow capabilities and keep response/status/statistics behavior.
- `scripts/architecture-dependency-policy.json`,
  `scripts/tests/test_architecture_dependency_guard.py`,
  `scripts/tests/test_m06_store_api_contract.py`: enforce dependency and focused source contracts.
- `scripts/arc-mut-baseline.json`, `scripts/arc-mut-relocation-approvals.json`: record one reviewed
  one-to-one fingerprint relocation for the unchanged test-module glob import. Identity count and
  occurrence count did not increase.
- `docs/plans/architecture-refactor-migration/phase-2-core-boundaries/06-storage-boundary-extraction.md`:
  append only the M06-01 evidence checklist; later substeps remain incomplete.

## RED to GREEN evidence

1. Crate/source contract
   - RED: `python scripts/tests/test_m06_store_api_contract.py` exited 1 because the workspace member
     and `rocketmq-store-api/src/lib.rs` did not exist.
   - RED: `cargo test -p rocketmq-store-api --test capability_contracts` exited 1 with 23 E0432
     unresolved imports for the wished-for capability and value surface.
   - GREEN: the same Rust test passed 4/4; the source/manifest contract passed 3/3 after the Broker
     seam was wired.
2. Legacy compatibility adapter
   - RED: `cargo test -p rocketmq-store --test store_api_legacy_adapter` exited 1 because
     `store_api_adapter` and the API dependency did not exist.
   - GREEN: the same command passed 2/2 and compiled the generic legacy adapter fixture.
3. Broker capability seam
   - RED: `cargo test -p rocketmq-broker append_seam_depends_only_on_message_appender --lib`
     exited 1 because the API dependency and the two generic seam functions were absent.
   - GREEN: `cargo test -p rocketmq-broker seam_depends_only --lib` passed 2/2 with the production
     single/batch append and reject paths using the same seam functions.
4. Processor output parity
   - The initial test draft had tuple setup errors and was corrected before counting RED.
   - RED: the corrected parity command exited 1 only because `map_append_status_to_response` was
     absent.
   - GREEN: `cargo test -p rocketmq-broker neutral_append_status_preserves_every_legacy_processor_output --lib`
     passed 1/1 across all 16 legacy statuses, including exact response code and remark.
5. Architecture rule
   - RED: the focused architecture unit exited 1 because only the generic target-DAG finding existed
     and the dedicated `store-api-runtime-neutral` rule was missing.
   - GREEN: the focused fixture passed 1/1 after adding the narrow forbidden-edge rule; the complete
     architecture suite passed 35/35.
6. Writable compatibility
   - RED: the health corpus passed 7/8 and exposed a `mapped_file` to `io` response-token regression.
   - RED: the corrected neutral contract then failed with E0599 because `StoreErrorKind::Storage` did
     not exist.
   - GREEN: the health corpus passed 8/8 after adding neutral `Storage` and restoring `mapped_file`
     only at the Broker compatibility response layer.
7. Typed Broker error mapping
   - RED: the named test exited 1 only because `map_store_api_error` did not exist.
   - GREEN: the named test passed 1/1 after exhaustive kind-to-`RocketMQError` mapping; the error
     guard no longer reports the three new source-stringification findings.

## Validation evidence

Completed before the cold final gate:

- `cargo check -p rocketmq-store-api --no-default-features` — passed.
- `cargo check -p rocketmq-store-api` — passed.
- `cargo test -p rocketmq-store-api` — passed, 4 integration tests plus unit/doc targets.
- `cargo test -p rocketmq-store --test store_api_legacy_adapter` — passed 2/2.
- Broker focused seam/parity/error/health commands — passed 2/2, 1/1, 1/1, and 8/8.
- `cargo tree -p rocketmq-store-api -e normal` — only direct API dependencies
  `bytes`, `rocketmq-error`, and `rocketmq-model`; no forbidden implementation/runtime crate.
- `python scripts/tests/test_architecture_dependency_guard.py` — passed 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` — passed clean fixture plus six
  violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` — passed.
- `python scripts/arc_mut_guard.py --fixtures` — passed 24 fixtures.
- ArcMut bootstrap/promote/compare — 1,233 identities and 3,378 occurrences before and after;
  reviewed one-to-one relocation passed.
- `python scripts/arc_mut_guard.py` — passed.
- `cargo clippy -p rocketmq-store-api --all-targets -- -D warnings` — passed.
- `cargo clippy -p rocketmq-store --lib -- -D warnings` — passed.
- `cargo clippy -p rocketmq-broker --lib -- -D warnings` — passed after gating one test-only import.
- `scripts/check-agents-routing.ps1` — passed: four standalone Cargo projects, three Node projects,
  eight routes.
- `scripts/check-error-hygiene.ps1` — the changed send processor is clean. The command remains exit
  1 only for pre-existing findings: one auth source-stringification site, eight MCP `anyhow` sites,
  and two missing governance documents.
- `git diff --check` — passed before final cold validation.

Cold final validation is recorded below after it completes.

## Compatibility decisions

- `MessageAppender<M>` is generic over the consumer-owned message input, so the API crate does not
  depend on `rocketmq-common`; Broker continues using `MessageExtBrokerInner` and `MessageExtBatch`
  only at the legacy adapter boundary.
- `AppendReceipt` keeps the append range, appended watermark, and durable watermark independent.
  A timeout can therefore remain accepted without being reported durable.
- `StoreError` exposes only a stable kind and operation name. Native error objects, paths, and
  implementation details remain inside `rocketmq-store`.
- The existing `mapped_file` Broker rejection remark is preserved only in the compatibility response
  projection from neutral `Storage`; it is not part of the API error taxonomy.
- The adapter borrows the current store. It adds no task, runtime, blocking boundary, ownership
  container, or `ArcMut` occurrence.
- CommitLog and every persisted layout remain untouched. Existing store types, methods, deep paths,
  Serde/default behavior, feature aliases, and lifecycle owners remain in place.

## Remaining baselines and concerns

- M06-02 and later extraction work remains intentionally untouched, including physical ownership of
  Local/Rocks modules and broader MessageStore decomposition.
- The repository error-hygiene command still has the unrelated pre-existing findings listed above;
  no new allowlist or governance baseline was added.
- ArcMut debt did not increase. One existing test glob-import fingerprint moved because tests were
  added in the same module; ADR-013 relocation evidence records exactly that one-to-one move.
- Windows cold builds emit `linker_messages` about generated import libraries. Rust explicitly notes
  that this lint ignores `-D warnings`; focused Clippy otherwise completed successfully.

## Cold final gates

- `cargo fmt --all -- --check` — passed, including the final post-review rerun.
- `cargo check -p rocketmq-store-api --no-default-features` — passed.
- `cargo check -p rocketmq-store-api` — passed.
- `cargo test -p rocketmq-store-api` — passed 4/4 plus unit/doc targets.
- `cargo test -p rocketmq-store --test store_api_legacy_adapter` — passed 2/2.
- Broker cold focused tests — seam 2/2, exact response parity 1/1, typed error 1/1, health 8/8.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` — passed in
  4m44s from the cleaned target; the final post-review incremental rerun also passed.
- Final dependency/source/architecture/ArcMut guards and `git diff --check` — passed.
- The four standalone Cargo manifests contain no direct `rocketmq-store` or `rocketmq-broker` path
  dependency, so this change activates no standalone-consumer validation route.

## Review-fix wave

The review-fix wave narrows `rocketmq-store-api` to the M06-01 capability spike, preserves exact
legacy Broker behavior in the compatibility layer, and closes every blocking review finding. The
API now publishes the eight traits through associated types and static RPITIT futures, plus only a
closed `StoreOperation`, neutral `StoreErrorKind`, and `StoreError`. Concrete append, read,
replication, derived-record, health, and admin DTOs remain deferred to M06-02.

The Broker ordinary single, batch, and transactional-prepare append paths all traverse the
`MessageAppender` seam. The legacy adapter owns the unchanged `PutMessageResult`, append/durable
watermarks, and exact health-error compatibility tokens. The response mapper continues to consume
all 16 legacy `PutMessageStatus` variants exhaustively. No persisted layout, public `MessageStore`
method, feature, lifecycle owner, task, or runtime boundary changed.

### Review-fix RED to GREEN evidence

1. Minimal associated-type API and static futures
   - RED: `cargo test -p rocketmq-store-api --test capability_contracts` exited 1 because
     `StoreOperation`, the associated types, and the RPITIT signatures were absent; the old boxed
     future and concrete DTO surface did not satisfy the wished-for contract.
   - GREEN: the same command passed 2/2 after shrinking the public API and removing `StoreFuture`,
     `Pin<Box<_>>`, and the M06-02 DTO implementations.
2. Exact legacy adapter compatibility
   - RED: `cargo test -p rocketmq-store --test store_api_legacy_adapter` exited 1 because the adapter
     still referenced the deleted API DTOs/boxed future aliases and did not expose exact legacy
     health kinds.
   - GREEN: the same command passed 3/3, covering unchanged `PutMessageResult`, independent
     watermarks, a monomorphized generic fixture, and all 12 exact legacy health tokens.
3. Broker append, status, health, and typed-error behavior
   - RED: `cargo test -p rocketmq-broker
     every_store_api_error_kind_preserves_typed_semantics_and_operation --lib` exited 1 with the old
     API imports, old receipt mapper, missing associated-type bounds, missing transaction adapter,
     and missing legacy status mapper.
   - GREEN: `cargo test -p rocketmq-broker processor::send_message_processor::tests --lib` passed
     17/17. This covers the static append and health seams, transaction-capable production wiring,
     exact output parity for all 16 legacy statuses, all 12 legacy writable-rejection tokens, and
     all 11 neutral error kinds with `ErrorKind`, retry, redaction, and operation assertions.
4. Structural and dependency contracts
   - RED: `python scripts/tests/test_m06_store_api_contract.py` passed 2/3 while the transactional
     prepare branch still bypassed `MessageAppender`.
   - GREEN: the same command passed 3/3 after adding the Broker-local transaction adapter. The
     contract parses all normal/build/dev/target dependency tables, rejects aliases/case variants,
     rejects backend/runtime vocabulary and dynamic futures, and structurally inspects production
     single, batch, transaction, and reject call sites.
5. Lint feedback
   - RED: focused Clippy rejected two manual RPITIT implementations as `manual_async_fn`, then
     rejected two test-only Broker imports in the library target.
   - GREEN: the implementations use statically dispatched `async fn`, the imports are test-gated,
     and all focused plus workspace Clippy commands below exit 0.

The first API test draft omitted the `Future` import; that setup-only compile error was corrected
before recording the API RED evidence.

### Review-fix validation evidence

- `cargo fmt --all` — passed; `cargo fmt --all -- --check` is included in the final rerun below.
- `cargo test -p rocketmq-store-api` — passed 2/2 plus unit/doc targets.
- `cargo test -p rocketmq-store --test store_api_legacy_adapter` — passed 3/3.
- `cargo test -p rocketmq-broker processor::send_message_processor::tests --lib` — passed 17/17.
- `python scripts/tests/test_m06_store_api_contract.py` — passed 3/3.
- `cargo tree -p rocketmq-store-api -e normal,build,dev` — only the allowed API dependencies and
  their transitive dependencies; no forbidden direct backend/runtime edge.
- `python scripts/tests/test_architecture_dependency_guard.py` — passed 35/35.
- `python scripts/architecture_dependency_guard.py --fixtures` — passed one clean fixture and six
  violation fixtures.
- `python scripts/architecture_dependency_guard.py --mode baseline` — passed.
- `python scripts/arc_mut_guard.py --fixtures` — passed 24 fixtures.
- ArcMut bootstrap/promote/compare — passed with 1,233 identities and 3,378 occurrences before and
  after. One existing test glob-import fingerprint relocated one-to-one; the ADR-013 approval and
  promoted baseline record no debt increase.
- `python scripts/arc_mut_guard.py` — passed after promotion.
- `.\scripts\check-agents-routing.ps1` — passed: four standalone Cargo projects, three Node
  projects, eight routes.
- `.\scripts\check-error-hygiene.ps1` — the changed send processor is clean. The command remains
  exit 1 only for the same pre-existing findings: one auth source-stringification site, eight MCP
  `anyhow` sites, and two missing governance documents.
- `cargo clippy -p rocketmq-store-api --all-targets -- -D warnings` — passed.
- `cargo clippy -p rocketmq-store --all-targets -- -D warnings` — passed.
- `cargo clippy -p rocketmq-broker --lib -- -D warnings` — passed.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` — passed in 47s.
  Windows emitted only the documented `linker_messages` warning, which Rust states ignores
  `-D warnings`, plus an unrelated future-incompatibility notice for `proc-macro-error2`.

### Review-fix remaining concerns

- M06-02 and later DTO/ownership extraction remains intentionally unimplemented.
- The error-hygiene repository baseline remains red only for the unrelated findings listed above;
  no allowlist or governance exception was added.
- The API crate keeps its already-approved `bytes`, `rocketmq-error`, and `rocketmq-model` direct
  dependencies, but its production source exposes no concrete type from them in this M06-01 spike.
