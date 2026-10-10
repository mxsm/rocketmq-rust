# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp-auth/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace excluded from the root workspace. It is a library
  used by `rocketmq-ai/rocketmq-mcp-control/` and, under `streamable-http`, by `rocketmq-ai/rocketmq-mcp/`.
- It owns JWKS retrieval, caching, and parsing, RS256 key selection by `kid`, and Bearer token extraction.
  Signature and claims validation, principal mapping, HTTP responses, and telemetry stay in each server.
- Do not depend on any `rocketmq-*` crate or on a web framework such as axum.
- Verification keys are RS256 only and are always selected by `kid`. Do not add symmetric algorithms, static
  keys, or a way to accept a token without a `kid`.
- Every limit and timing comes from the caller's `JwksPolicy`. Do not add a default policy that a server could
  inherit without stating it. `OutboundAddressPolicy::PublicOnly` stays the default outbound policy.
- A JWKS entry that cannot verify RS256 tokens is skipped; a document fails only when it breaks a size or count
  limit, repeats a `kid` among usable entries, or has no usable entry. Keep both servers on these rules.
- Do not spawn tasks or threads, start subprocesses, or write to stdout. The control server's boundary check
  scans this crate's source as its own production code.
- Never log or put into `Display` output a token, key material, the JWKS URL, a resolved address, or the text
  of a source error. Source errors may be kept as typed `source()` values for the caller to inspect.
- Prefer native async trait methods; do not add `async_trait`.

## Development validation

From this directory, use `cargo fmt --all -- --check` and `cargo test --locked`; the suite is small and needs
no network. Add `cargo clippy --locked --all-targets -- -D warnings` when the change is more than a test.

A change to behavior or to the public API affects both servers. Run the control suite for the default and
`write-tools` feature sets together with `python scripts/check_control_boundary.py`, and the query suite with
`--features streamable-http`, each from its own directory. Refresh a consumer's `Cargo.lock` only when this
crate's dependencies change.
