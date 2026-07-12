# Task 2 report: M05 transport extraction

## Outcome

Implemented the M05 `rocketmq-transport` ownership boundary and retained the Remoting/Common compatibility
envelope. The review fix closes the production ownership gap: normal Remoting clients and servers now delegate
socket connect/accept, TLS handshake deadlines, framed reads/writes, bounded session writer queues, pending
correlation, admission/security checks, and reader/writer/session shutdown to canonical Transport runners.

The crate depends only on Protocol, Security API, Runtime, Error, optional Observability, and third-party
primitives. Remoting forwards `tls`, `simd`, and `observability`, exact-reexports the moved value/primitives, and
keeps a narrow TLS lifecycle adapter for its legacy standalone task-group/report name. `connection_v2` was
deleted because it had no production consumer, selectable feature, or complete end-to-end lifecycle that could
pass the compatibility/maintainability prerequisites for keeping a parallel stack.

## Red-green-refactor evidence

Every behavior slice started with a failing focused test or contract compile fixture:

- Boundary/config imports failed before the new crate and legacy re-exports existed.
- Pending request tests failed before owner-scoped opaque retirement, 10,000-request expiration, close-all, and
  count-plus-byte permits were implemented.
- Codec, buffer, and connection fixtures failed before fragmented-frame handling, allocation-before-limit
  rejection, byte-pool release, half-close, and active socket shutdown existed.
- Admission tests failed before global/per-IP/per-tenant/per-session budgets, control reserve, bounded scope
  cardinality, reject/close policies, and non-blocking observer delivery existed.
- Security fixtures failed before borrowed command projection and injected policy/signer ports existed.
- Client/server fixtures failed before the single absolute deadline, send/timeout/close completion, owned
  accept/session/processor tasks, and hung-processor abort path existed.
- The runtime enforcing audit produced a final RED for a transport-owned top-level TLS task group. Transport was
  corrected to require injected ownership; only the legacy Remoting adapter retains the approved top-level
  boundary. The audit and focused compatibility test then passed.
- Workspace Clippy produced RED on reserve normalization readability; explicit branches made the final profile
  pass without lint suppression.
- Review source-contract and production-runtime tests first failed because Remoting still owned its raw
  connect/accept/read loops. The client now uses `connect_with_config` plus `run_connected_session`; the server
  uses `TransportListener`; and a public client/server request test proves response identity and lifecycle cleanup.
- A TLS reload regression test first failed because certificate filesystem work ran on the async worker. The TLS
  runtime now requires the injected `BlockingExecutor`, including explicit `reload_now` coverage.
- Workspace Clippy then exposed downstream future-layout overflow in `rocketmq-admin-cli`. Boxing the client
  compatibility connect/session boundary removed the overflow without raising recursion limits.

## Compatibility and governance

- Common and Remoting legacy `ServerConfig`, `TlsConfig`, and `TlsMode` paths preserve defaults and Serde shape.
- Remoting codec, connection, pending-table, encode-buffer, and error-helper paths resolve to transport-owned
  types/functions. TLS connector functions are exact re-exports; the runtime wrapper preserves lifecycle output.
- Wire commands remain `rocketmq-protocol` types; no schema, request code, response mapping, or body logging was
  introduced.
- ArcMut promotion is monotonic: 1,266 identities / 3,430 occurrences became 1,233 / 3,378. The 16 exact
  fingerprint relocations are consumed under ADR-013, and `rocketmq-transport` contains no ArcMut occurrence.
- The dependency policy permits Remoting and Common compatibility edges to transport and rejects forbidden
  reverse/business/provider dependencies.

## Validation

Passed:

- `cargo check -p rocketmq-transport --no-default-features`
- `cargo check -p rocketmq-transport`
- `cargo check -p rocketmq-transport --no-default-features --features simd,tls,observability`
- `cargo test -p rocketmq-transport --no-default-features`
- `cargo test -p rocketmq-transport`
- `cargo test -p rocketmq-transport --features observability`
- Remoting no-default/default/combined feature checks
- `cargo test -p rocketmq-remoting --no-default-features` (111 unit tests plus integration and doc tests)
- `cargo test -p rocketmq-remoting` (112 unit tests plus integration and doc tests)
- production transport delegation source contracts, canonical public client/server exchange, and injected TLS
  blocking reload coverage
- `cargo test -p rocketmq-client-rust --lib` (943 passed)
- `cargo tree -p rocketmq-transport -e normal`
- architecture dependency guard fixtures and baseline
- runtime audit with `-SkipBaseline -EnforceBoundaryBaseline`
- ArcMut guard, committed-baseline monotonic promotion, 24 fixtures, and 63 unit tests
- AGENTS routing check and `git diff --check`
- root `cargo fmt --all -- --check`
- root all-workspace/all-target/all-feature Clippy with `-D warnings`
- standalone `rocketmq-example` format and Clippy profile
- standalone Tauri backend format and Clippy profile
- standalone web backend format, Clippy, and all-feature build profile

Existing unrelated baseline failures:

- `cargo test -p rocketmq-broker`: 497 passed, 24 failed, 1 ignored. Failures are the existing Lite Subscription
  and lifecycle-probe baseline behaviors; no failing path is modified by M05.
- `scripts/check-error-hygiene.ps1`: existing Broker auth source-stringification finding, existing RocketMQ MCP
  anyhow allowlist findings, and the two already-missing error-governance documents. No finding is in the M05
  diff.

Windows emitted informational MSVC linker-library messages during successful builds; these are not Rust/Clippy
warnings and the repository profile completed with exit code zero.
