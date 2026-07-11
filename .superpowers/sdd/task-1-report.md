# Phase 2 Task 1 / M04 second-review implementation report

## Outcome

`DONE`: PR-M04-01 through PR-M04-06 are implemented and the second-review compatibility findings are resolved. `rocketmq-protocol` is the canonical runtime-neutral owner for wire codes, command/schema, primitives, pure message/trace codecs, route/static-topic values, and codec-neutral RPC values. Legacy remoting/common/client paths remain exact re-exports or documented owner adapters.

## Delivered boundary

- Migrated the declarative admin/body/header/heartbeat/route/subscription/static-topic schema into `rocketmq-protocol`; removed disabled duplicate facade declarations.
- Restored exact root/deep `RemotingCommand` type identity and its complete canonical API. Remoting owns environment-backed command factories and a deprecated free-function adapter for the concrete legacy `ArcMut` setter signature.
- Replaced canonical `ArcMut`/`DashMap`/clock dependencies with owned values, `HashMap`s, zero defaults, and caller-supplied timestamps. Public legacy schema fields retain their `DashMap`, `Arc`, and `ArcMut` shapes through bidirectional owner adapters.
- Moved codec-neutral RPC values with owned boxed headers, while the remoting RPC response facade retains its public `ArcMut<Box<_>>` field contract and rejects lossy shared conversion.
- Added canonical message-frame decoding and stateless trace values/codecs. Common and client adapters delegate through the canonical codecs.
- Forwarded `remoting/simd` to `protocol/simd`, kept protocol's default feature set empty, and preserved the approved dependency direction.
- Added frozen JSON/RocketMQ binary/body/primitive/message/trace corpora, malformed and boundary cases, exact CRC-condition tests, injected/random route-selection tests, signature fixtures, and source ownership tests.
- Recorded the M03 DAG exception that keeps `RocketMqVersion` canonical in `rocketmq-model`, with exact protocol/common re-exports and guard coverage.

## TDD evidence

- RED: the compatibility fixture rejected a root/deep `RemotingCommand` wrapper, missing canonical methods, non-canonical factories, changed legacy map/RPC field types, and the absent injected route-selection API.
- RED: the message decoder checked CRC for metadata-only and empty-body frames, the static-topic `ArcMut` adapter was missing, and common `LiteSubscription` no longer owned its legacy clock behavior.
- GREEN: the remoting compatibility facade suite passes 10/10, the differential message-codec suite passes 7/7, and the common Lite compatibility suite passes 1/1.

## Validation

Passed before the final handoff gates:

- `cargo test -p rocketmq-protocol --no-default-features` (1,381 unit tests, 4 wire-primitive integration tests, doctests)
- `cargo test -p rocketmq-protocol --features simd` (same corpus)
- `cargo test -p rocketmq-remoting` (unit, integration, compatibility, signature/default, and doctest suites)
- `cargo test -p rocketmq-remoting --features simd --test protocol_extraction_compatibility` (4/4)
- `cargo test -p rocketmq-remoting --features simd --test m04_compatibility_facades` (10/10)
- `cargo test -p rocketmq-common --test protocol_message_codec_compatibility` (7/7)
- `cargo test -p rocketmq-common --test lite_subscription_compatibility` (1/1)
- `cargo test -p rocketmq-client-rust --lib` (943/943)
- Focused broker suites: Lite subscription registry (3/3), topic queue mapping manager (12/12), and topic config manager (4/4)
- `cargo test -p rocketmq-namesrv` (175 unit tests plus all integration tests and doctests)
- `cargo check --workspace --all-targets --all-features`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
- `python scripts/tests/test_m04_schema_ownership.py` (3/3)
- `python scripts/architecture_dependency_guard.py --mode baseline`
- `cargo tree -p rocketmq-protocol -e normal` (no common/remoting/Tokio/TLS/transport edge)
- `.\scripts\check-agents-routing.ps1`
- `git diff --check`
- Example standalone: fmt and `cargo clippy --all-targets -- -D warnings`
- Tauri backend standalone: fmt and all-target/all-feature Clippy
- Web backend standalone: fmt, all-target/all-feature Clippy, and all-target/all-feature build

Partially blocked by the host or repository baseline:

- `cargo test -p rocketmq-admin-core`: every launched suite/test passed, including 100 library tests and the integration groups, but Windows refused to launch `topic_update_list_core_models` with OS error 740 (elevation required). The binary never executed.
- `cargo test -p rocketmq-broker --lib`: 496 passed, 25 failed, and 1 was ignored. One failure was a fixed-port collision (OS 10048); the remaining failures are existing process-global Lite/config lifecycle tests and reproduce individually outside the changed compatibility adapters. All changed-path focused broker suites pass.
- `python scripts/error_architecture_guard.py`: the M04 redaction path was updated to the canonical protocol owner. The guard still exits 1 for documented Phase 1 baseline findings: auth source stringification, MCP `anyhow` allowlist entries, and two absent error-governance documents.

The standalone gates also caught two consumers that still assumed the canonical `HashMap` view. Their loops now use the restored legacy `DashMap` entry API, and both complete standalone profiles pass.

## Compatibility note

The root and deep `rocketmq-remoting::RemotingCommand` paths are exact canonical re-exports. Environment defaults live in remoting factories. Rust cannot add an inherent `ArcMut` method to a type defined in another crate, so the exact legacy setter signature is preserved as a deprecated remoting free function and explicitly recorded in the compatibility ledger.
