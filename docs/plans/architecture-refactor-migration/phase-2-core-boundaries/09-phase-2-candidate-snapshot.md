# M09-06 Phase 2 candidate snapshot

## Frozen candidate

| Field | Value |
|---|---|
| Work package | `PR-M09-06` |
| Issue | [#8258](https://github.com/mxsm/rocketmq-rust/issues/8258) |
| Branch | `mxsm/architecture-refactor-phase-2-gate` |
| Main baseline | `1e1aea44a5b31f1122c8caf2c819a768c474daea` |
| Implementation candidate | `490c583e94b31dc7ae1b83c55ed811e2b90d4cce` |
| Candidate tree | `e959367d3b4002653e4e25e5b0c19213de8766b5` |
| Evidence index | `target/architecture-refactor/M09/phase2-gate-490c583e/` |
| Freeze date | 2026-07-18 |

The implementation candidate is the immutable Rust and policy-tool snapshot reviewed and tested by the M09-06 gate.
The later evidence/checklist commit does not change Rust source, Cargo manifests, features, wire/storage fixtures, or the
candidate public surface.

## Candidate state

- [x] Root workspace contains exactly 32 packages and the ten planned boundary crates.
- [x] Target dependency guard has zero unauthorized findings; all 35 compatibility/composition and three dev-only edges
  match the signed ledger.
- [x] The complete Client allowlist is workspace two (`rocketmq-admin-core` adapter and `rocketmq-proxy-cluster`) plus
  standalone one (`rocketmq-example`).
- [x] The public API snapshot covers 31 library/proc-macro targets. MCP moved from 205 to 209 paths only by adding typed
  canonical entry points while retaining deprecated legacy `anyhow` wrappers.
- [x] Feature 24/24, wire/canonical-legacy 6/6, and storage 10/10 compatibility rows pass.
- [x] Error architecture has no remaining finding across all 14 guard categories.

## Gate fixes before freeze

The first gate run found 11 historical typed-error findings: one Broker source-stringification site, eight MCP public
`anyhow` occurrences, and two missing governance documents. The candidate resolves them by preserving structured Broker
authorization fields, adding source-preserving `McpError` variants and typed canonical MCP entry points, retaining the
old MCP calls as deprecated R0 wrappers, and adding the required error-code/allowlist governance documents.

The old and new MCP APIs are referenced together by `rocketmq-tools/rocketmq-mcp/tests/error_boundary_compat.rs`; this
prevents the compatibility wrappers from being removed before their approved next-major window.

## Writer handoff

`[DEV]` The implementation candidate and tree above are frozen. Production-source writer lease is released. Any later
production fix invalidates this snapshot and requires affected Reviewer and Tester conclusions to be rerun.
