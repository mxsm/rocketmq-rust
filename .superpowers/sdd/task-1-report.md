# Phase 2 Task 1 / M04 implementation report

## Outcome

`DONE_WITH_CONCERNS`: this commit lands the coherent PR-M04-01 protocol-boundary spike and the directly
applicable feature/dependency-policy work. It does not claim the full M04 exit checklist; the remaining
schema, primitive, projection, trace, and message-codec batches are listed below.

## Files changed

- Added `rocketmq-protocol/` with an empty default feature set and optional `simd` JSON codec.
- Moved the canonical request/response/broker code enums, `RemotingCommand`, custom-header traits,
  RocketMQ binary header codec, RPC request value headers, and the real `GetMinOffset` request/response
  header slice into `rocketmq-protocol`.
- Changed the corresponding `rocketmq-remoting` root/deep modules to exact re-exports, retained existing
  root/prelude consumers, and forwarded `remoting/simd` to `protocol/simd`.
- Added canonical/legacy type-identity, JSON/ext-field golden, malformed-input differential, feature, and
  dependency-closure tests.
- Registered the 25th workspace package, updated root and standalone lockfiles, added the protocol closure
  policy, updated the migration package count, and marked only PR-M04-01 complete in the checklist.
- Updated the typed-error source-boundary fixture to inspect the canonical source paths.

## TDD evidence

### RED

- `python -m unittest scripts.tests.test_m04_protocol_boundary.ProtocolBoundaryTests.test_workspace_exposes_runtime_neutral_protocol_crate`
  - Exit 1 as expected: `rocketmq-protocol` was absent from Cargo workspace metadata.
- `cargo test -p rocketmq-protocol --features simd`
  - Exit 1 during the feature cycle because `Deserialize` was accidentally gated off with `simd`; fixed by
    making the derive import feature-independent.
- First `cargo test -p rocketmq-remoting`
  - Exit 1 during facade integration because a typed-error source fixture still included the old physical
    paths; fixed by pointing the fixture at canonical sources.

### GREEN

- Boundary Python test: 1 passed.
- `cargo check -p rocketmq-protocol --no-default-features`: passed.
- `cargo test -p rocketmq-protocol --no-default-features`: 84 passed.
- `cargo test -p rocketmq-protocol --features simd`: 84 passed.
- `cargo test -p rocketmq-remoting --test protocol_extraction_compatibility --no-default-features`: 3 passed.
- `cargo test -p rocketmq-remoting --test protocol_extraction_compatibility --features simd`: 3 passed.

## Validation results

- `cargo tree -p rocketmq-protocol -e normal`: passed; no common/remoting/Tokio/TLS/transport/client/server/auth/store dependency.
- `python scripts/architecture_dependency_guard.py --mode baseline`: passed.
- `cargo test -p rocketmq-remoting`: passed (1,369 unit tests plus all integration and doctest suites; expected ignores only).
- `cargo test -p rocketmq-client-rust --lib`: passed (955 tests).
- `cargo test -p rocketmq-admin-core`: test assertions executed before the failure were green, but the aggregate
  command exited 1 when Windows refused to launch `topic_update_list_core_models` with OS error 740
  (`operation requires elevation`).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`: passed.
- `./scripts/check-agents-routing.ps1`: passed.
- `git diff --check`: passed.
- `./scripts/check-error-hygiene.ps1`: script exited 0 but reported pre-existing unrelated findings in
  `rocketmq-broker/src/auth/auth_admin_service.rs`, RocketMQ MCP `anyhow` boundaries, and missing
  `docs/07-error-hygiene-allowlist.md` / `docs/error-codes.md`; none of those paths changed here.
- Standalone formatter/Clippy commands for example, Tauri backend, and Web backend were started in parallel
  but produced no result before being terminated for handoff; their lockfiles were updated by Cargo metadata.

## Remaining M04 requirement ledger

- PR-M04-02: Java hash, retry/POP key grammar, message/topic flags, and common-path primitive re-exports.
- PR-M04-03: all remaining declarative headers, bodies/admin, heartbeat/route, subscription/static-topic schema.
- PR-M04-04: pure static-topic/route projections, epoch injection, file-I/O owner split, and remaining codec-neutral RPC values.
- PR-M04-05: message binary codec and stateless TraceRecord/codec/constants with client differential adapter.
- PR-M04-06: full remoting facade conversion and all affected direct consumer imports/tests.
- `RemotingCommand` now uses runtime-neutral fixed defaults instead of reading the legacy environment-backed
  remoting version/serialization settings. A later batch must inject those defaults from the remoting owner to
  preserve environment-configured behavior without introducing `std::env` into protocol.
- Disabled legacy enum declarations remain in the remoting facade under `#[cfg(any())]`; compiled type identity
  is canonical and tested, but a cleanup batch should physically remove the disabled source after the facade
  ledger is complete.

## Commit

- Recorded after commit below.
