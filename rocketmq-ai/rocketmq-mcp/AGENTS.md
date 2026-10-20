# AGENTS.md

## Scope

This file applies to the standalone Rust 2021 `rocketmq-ai/rocketmq-mcp/` entrypoint crate.

## Boundary

- Compose query, auth, and control through optional dependencies and additive feature forwarding.
  Disable implementation dependencies' default features explicitly.
- Defaults remain `read-only`, `diagnose`, and `stdio`; `read-only` aliases `query`. The default
  dependency closure must contain no control server or mutation adapter.
- HTTP query builds imply auth; control builds imply auth. Only `write-tools` enables the control
  crate's reviewed mutation feature, and compilation grants no runtime mutation authority.
- `ROCKETMQ_MCP_MODE` selects exactly one server (`query` by default or `control`). Delegate to its
  synchronous runtime-owning entrypoint. Never add a nested runtime or subprocess launcher, or merge
  tool catalogs, authentication middleware, audit trails, or server policies.
- Keep the query embedding API available through explicit compatibility re-exports. Export control
  and auth in their named modules.
- Combined builds unify shared Admin features and provide logical server separation. Separate dependency
  closures require independent query and control builds and deployments. Keep their local boundaries intact.
- Keep existing query configuration paths and protocol names compatible. Query owns the canonical examples;
  copies under this crate's `conf/` must match them.

## Development validation

Use `cargo fmt --all -- --check` and focused `cargo test --locked` cases for feature or composition
changes. Cover feature absence,
HTTP query, control-only, and combined builds as affected. Test builds replace a separate compile check.
Run the affected implementation crate's focused regressions and standalone boundary checks when changing it.
