# Phase 2 Task 1 / M04 implementation report

## Outcome

`DONE`: PR-M04-01 through PR-M04-06 are implemented. `rocketmq-protocol` is the canonical runtime-neutral owner for wire codes, command/schema, primitives, pure message/trace codecs, route/static-topic values, and codec-neutral RPC values. Legacy remoting/common/client paths remain re-exports or owner adapters.

## Delivered boundary

- Migrated the declarative admin/body/header/heartbeat/route/subscription/static-topic schema into `rocketmq-protocol`; removed disabled duplicate facade declarations.
- Preserved remoting environment defaults and the concrete legacy `set_command_custom_header_origin(Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync>>>)` signature in a transparent owner wrapper.
- Moved Java hash, retry/POP key grammar, flags, compression/version/value primitives and retained common-path re-exports.
- Replaced canonical ArcMut/DashMap/clock dependencies with owned values, HashMaps, zero defaults, and caller-supplied timestamps; mutating planners, clocks, file I/O, network, and runtime state stay in remoting/admin/broker/client owners.
- Moved codec-neutral RPC request/response values, with owned boxed headers.
- Added canonical message-frame decoding and stateless trace record/codec/constants/type/transfer values; common and client adapters delegate through the canonical codecs.
- Forwarded `remoting/simd` to `protocol/simd`, activated the planned common-to-protocol compatibility baseline edge, and kept protocol's default feature set empty.
- Added frozen JSON/RocketMQ binary/body/primitive/message/trace corpora, malformed/unknown/missing/boundary cases, type/signature fixtures, and source ownership tests.

## TDD evidence

- RED: environment-backed RemotingCommand defaults returned the canonical fixed version; the exact legacy function-item signature failed generic `None` inference.
- RED: canonical primitive/schema ownership tests failed before the modules existed and enumerated 221 legacy declarative definition files.
- RED: first protocol-only schema compile reported 74 errors (owner imports, ArcMut/DashMap serde bounds, missing canonical traits).
- GREEN: protocol-only compile reached zero errors after canonical traits/owned schema conversion; remoting no-default compile then reached zero errors after owner adapters.

## Validation

Passed:

- `cargo check -p rocketmq-protocol --no-default-features`
- `cargo test -p rocketmq-protocol --no-default-features` (1,381 unit tests, 3 primitive integration tests)
- `cargo test -p rocketmq-protocol --features simd` (same corpus)
- `cargo test -p rocketmq-remoting` (unit, integration, compatibility, signature/default, and doctest suites)
- `cargo test -p rocketmq-remoting --features simd --test protocol_extraction_compatibility` (4/4)
- `cargo test -p rocketmq-common --test protocol_message_codec_compatibility` (2/2)
- `cargo test -p rocketmq-client-rust --lib` (943/943)
- `cargo check --workspace --all-targets --all-features`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
- `python scripts/tests/test_m04_schema_ownership.py` (2/2)
- `python scripts/architecture_dependency_guard.py --mode baseline`
- `cargo tree -p rocketmq-protocol -e normal` (no common/remoting/Tokio/TLS/transport/client/server/auth/store edge)
- `.\scripts\check-agents-routing.ps1`
- `.\scripts\check-error-hygiene.ps1`
- `git diff --check`
- Example standalone: fmt and `cargo clippy --all-targets -- -D warnings`
- Tauri backend standalone: fmt and all-target/all-feature Clippy
- Web backend standalone: fmt, all-target/all-feature Clippy, and all-target/all-feature build

Partially blocked by the host:

- `cargo test -p rocketmq-admin-core`: every launched suite/test passed, including 100 library tests and the integration groups, but Windows refused to launch `topic_update_list_core_models` with OS error 740 (elevation required). The binary never executed. The package compiles and passes workspace Clippy.

## Compatibility note

The root `rocketmq-remoting::RemotingCommand` is intentionally a transparent owner wrapper, rather than an exact type alias, because exact legacy environment defaults and its concrete ArcMut setter signature cannot coexist on the runtime-neutral canonical type. Declarative schema root/deep paths remain exact canonical re-exports.

## Commit

The completed milestone is committed from this task after the validation snapshot above.
