# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp-control/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace excluded from the root workspace.
- HTTPS Streamable MCP with RS256 OAuth/JWKS is the only transport and authentication mode.
- The default feature set must not depend on RocketMQ Admin or client mutation adapters.
- `write-tools` may enable only `rocketmq-admin-core/mutation-client-adapter`; it must never enable a read or full adapter.
- The reviewed production tools are `rocketmq_upsert_topic`,
  `rocketmq_upsert_consumer_group`, `rocketmq_reset_consumer_offset`,
  `rocketmq_patch_broker_config`, and `rocketmq_set_consumer_request_mode`.
  Keep delete, skip, resend, and any free-form mutation outside this project
  until separately reviewed.
- OAuth and closed operation/cluster authorization must complete before mutation argument parsing. A durable
  `started` audit record must then complete before session creation or RPC.
- Do not add stdio, CLI, shell, subprocess, free-form RPC, arbitrary Admin commands, or stdout protocol output.
- Durable audit v2 records may contain only the validated OAuth subject and optional safe bounded request reason
  as operator evidence. Responses, errors, tracing, ordinary logs, and all other public data must exclude both;
  every surface must also exclude credentials, tokens, network addresses, raw backend errors, and message bodies.
- An audit operator is 1--128 ASCII bytes: the first byte is alphanumeric and every remaining byte is
  alphanumeric or one of `._@-`. Without `@`, it must not be endpoint-shaped. With exactly one `@`, the local
  side starts/ends alphanumeric and has no consecutive dots; the domain is a non-IP, non-rooted, valid
  multi-label hostname with an alphabetic, non-reserved top-level label. Never scan the validated domain as a
  compact token; reject a compact local side whose base64url header is a JSON object with JOSE/JWT marker fields,
  regardless of the declared algorithm, or whose signature is empty/underscore. Reject
  percent-escaped, token-shaped, path, whitespace, control, non-ASCII, and Unicode-format input. A reason is a
  trimmed 5--256 byte ASCII string containing only alphanumerics, ordinary space, and `._,#-`. These punctuation
  marks support sentences, ticket IDs, and comma-separated prose; all syntax punctuation is rejected by grammar.
  Scan each whitespace token and its comma/hash/underscore/hyphen and repeated-dot-delimited subtokens, stripping
  allowed edge punctuation before rejecting bare JWT, IP, or FQDN values. Treat 1--4 component decimal,
  hexadecimal, octal, and leading-zero IPv4 numeric notation as network addresses. Reject whole-value numeric
  operators and dotted, hexadecimal, or long octal forms embedded in non-email operators or email local parts;
  recognize valid RFC4122 UUIDs before subtoken scanning and permit plain decimal service-ID subtokens. In reasons, retain only
  explicit hash-number or uppercase-tag ticket references and decimal version tokens immediately following
  `release` or `version`.
  Apply both rules at OAuth,
  request context, and v2 recovery boundaries; v1 recovery retains its legacy shape rules.
- Normalize every reliable audit sink read, recovery, append, or timeout failure to `audit_unavailable`; never
  expose a sink-provided code or message. A failed durable `started` append must precede and prevent session/RPC.
- Each upsert accepts only 1--64 explicit broker names. All operations validate the full selected-cluster topology
  before target state RPC, keep plans sealed to one Admin session, and preserve exact-target post-read verification.
- Targeted Topic upserts must treat the complete order-Topic KV as a sealed no-write guard. Never merge, put,
  or delete that global KV from the targeted path; reject any selected entry that is not already exact.
- Request-key reuse is bounded and process-local; every invocation, including followers and cache hits, keeps
  its own durable audit pair.
- Prefer native async trait methods where object safety is not required; do not add `async_trait`.

## Mandatory validation

Run from this directory with `INSTA_UPDATE=no` and `RUST_MIN_STACK` unset:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo check --locked --features write-tools
python scripts/check_control_boundary.py
cargo test --locked
cargo test --locked --features write-tools
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo doc --locked --no-deps --all-features
```

Also run the repository AGENTS routing guard, the query MCP read-only boundary and contract snapshot checks, and
the root strict Clippy profile required by the root `AGENTS.md` before final handoff.
