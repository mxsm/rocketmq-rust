# M09-05 next-major removal plan

## Authorization boundary

The relative next-major window and external notification plan are approved. Destructive removal remains
`pending-next-major-evidence-gate`: this document does not approve early deletion, does not remove a public API, and does
not activate the Proxy optional mode feature in R0/R1.

## Dependency-ledger removals

Exactly four dependency edges are scheduled for next-major:

| Consumer | Target | Kind | Reason |
|---|---|---|---|
| standalone `rocketmq-example` | `rocketmq-common` | dev | examples use canonical Model/Protocol/Runtime owners |
| standalone `rocketmq-example` | `rocketmq-remoting` | dev | examples use canonical Protocol/Transport owners |
| standalone `rocketmq-example` | `rocketmq-rust` | dev | examples use `rocketmq-runtime` directly |
| `rocketmq-common` | `rocketmq-protocol` | normal | remove the Common protocol compatibility re-export after external migration |

These four manifest edges are the quantitative dependency ledger. The public removal scopes below are additional API
and feature decisions and must each have their own API/feature evidence.

## Public API and feature removal scopes

1. **admin legacy**: remove deprecated Admin facade signatures and `legacy-common-compat` only after MCP, Dashboard,
   CLI/TUI, standalone users, and external consumers compile against Admin Core canonical contracts.
2. **common compat**: remove deprecated Common model/protocol/runtime re-exports only after the four dependency edges
   above and external deep-path usages reach their gates.
3. **remoting deep path**: remove deprecated Remoting protocol/transport deep paths only after public API diff and code
   search prove the canonical Protocol/Transport replacements are adopted.
4. **Proxy optional mode feature**: replace R0’s always-present Cluster/Local adapter dependencies with the
   `cluster-mode`, `local-mode`, `compat-all-modes`, and `tieredstore` closure defined in
   `scripts/fixtures/proxy-next-major-features.toml`.
5. **MCP `anyhow` compatibility wrappers**: remove deprecated `McpApp::bootstrap`, `init_tracing`, stdio `serve`, and
   Streamable HTTP `serve`/`build_router` only after external callers adopt their `McpError`-returning `*_typed`
   replacements. The MCP process binary already uses the typed paths in R0.

The approved long-term `rocketmq-broker → rocketmq-store` and `rocketmq-store-inspect → rocketmq-store` composition
edges are explicitly outside this removal list.

## Evidence gates

Removal may begin only when all of the following are true on one frozen candidate snapshot:

- at least two deprecation releases and one major-version boundary have elapsed;
- workspace and all routed standalone internal usages are zero;
- unresolved high-impact external consumers are zero, based on the signed external usage snapshot;
- migration guide, canonical replacements, release notes, tracking issue/discussion, and rollback instructions are
  published;
- public API, feature/default, wire, storage, Serde, standalone, and Proxy closure matrices pass;
- release manager and Human approval are recorded for the destructive candidate, separately from this planning approval.

An unknown or unavailable external-usage signal fails closed. A deadline alone is not removal evidence.

## Proxy activation and rollback

Before next-major, active `rocketmq-proxy/Cargo.toml` must keep `default = []`, keep Cluster/Local dependencies
non-optional, and omit `cluster-mode`, `local-mode`, and `compat-all-modes`. The fixture is tested but inactive.

If the next-major candidate fails, restore the deprecated forwarding paths and R0 Proxy dependency closure, republish
the previous compatible major, and rerun public API/feature/wire/storage and cross-project matrices. Persisted data and
wire formats are never rolled back by conversion.

## next-major checklist

- [x] Four dependency-ledger edges are listed exactly.
- [x] admin legacy, common compat, remoting deep path, and Proxy optional mode feature scopes are explicit.
- [x] MCP public `anyhow` wrappers have typed canonical replacements and a next-major removal window.
- [x] Two long-term Store composition edges are excluded from deletion.
- [x] Evidence thresholds and a separate destructive Human approval are required.
- [x] Current R0/R1 source is guarded against early removal and early Proxy feature activation.
