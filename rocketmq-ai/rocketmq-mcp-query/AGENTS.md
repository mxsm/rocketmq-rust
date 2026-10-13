# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp-query/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace.
- The default server is read-only and diagnostic.
- Depend on `rocketmq-admin-core` only through `read-client-adapter`.
- Do not enable `client-adapter`, `mutation-client-adapter`, `admin-full`, or `admin-mutation`.
- These dependency restrictions apply to this standalone build. The entrypoint's combined query/control
  build may unify shared Admin features through control; query source must still use only the read interface.
- Query owns the canonical `conf/` examples. Keep the entrypoint's compatibility copies identical when
  changing those examples.
- Streamable HTTP is authenticated by default. Stdio is local-development only and writes protocol frames only to stdout.
- OAuth JWKS retrieval, caching, and key selection come from `../rocketmq-mcp-auth/`, an optional dependency of
  `streamable-http`. This server states its own limits in `src/guard/jwks.rs`; a private JWKS issuer stays
  supported, so do not restrict its outbound addresses without a separate decision.
- The server answers one MCP protocol revision, `MCP_PROTOCOL_VERSION`, which the HTTP build takes from
  `../rocketmq-mcp-auth/`. Keep `initialize`, `supported_protocol_versions()`, and the `MCP-Protocol-Version`
  header check of the HTTP transport on that set, and the header check after authentication. Answering another
  revision, or negotiating one in `initialize`, is a decision to take together with the control server and the
  SRE Connector.
- Tool and Resource output must use the shared authorization, audit, correlation, sanitization, row, and byte policy.
  Argument completion is an output surface too: it needs the permission of the listing Tool that returns the same
  names, and it is rate limited, audited, and sanitized.
- A Tool that looks up a message by its identifier also needs the permission for `rocketmq_get_message_metadata`.
  The identifier is redacted before audit hashing, and results name a message by its pseudonyms only.
- An audit record says what a call asked about only through `AuditTarget`, which holds Topic, Consumer Group,
  producer group, Broker, Proxy, and Controller names that passed the check of their kind. Never record a message
  identifier, a key, a filter, or a completion prefix. The file sink seals a full audit file as
  `<path>.<six-digit number>` and never replaces or deletes a sealed file.
- Prefer native async fn methods in traits. #[allow(async_fn_in_trait)] is permitted when required by the lint for an intentional public async trait API; do not add #[async_trait].

## Development validation

From this directory, use `cargo fmt --all -- --check`, `cargo check --locked`,
and focused `cargo test --locked <test_name>` cases for the changed behavior.
A test build may replace the separate compile check.

Run `python scripts/check_read_only_boundary.py` when tools, resources, authorization, adapter
dependencies, or feature boundaries change. Test the affected transport/feature combination;
changes spanning default and HTTP behavior need coverage of both.

For MCP integration, select broader tests, `cargo clippy --locked --all-targets --features streamable-http -- -D warnings`,
or `cargo doc --locked --no-deps` as relevant. Do not automatically stack root workspace checks or
run all features for an internal fix.
