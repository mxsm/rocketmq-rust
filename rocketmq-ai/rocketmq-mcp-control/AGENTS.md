# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp-control/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace excluded from the root workspace.
- HTTPS Streamable MCP with RS256 OAuth/JWKS is the only transport and authentication mode.
- JWKS retrieval, caching, and key selection come from `../rocketmq-mcp-auth/`. This service states its own
  limits in `JWKS_POLICY` (`src/auth.rs`): keys are never used past their five-minute lifetime, and the JWKS
  endpoint is reached only at public addresses. The boundary check scans that crate as production source.
- The default feature set must not depend on RocketMQ Admin or client mutation adapters.
- `write-tools` may enable only `rocketmq-admin-core/mutation-client-adapter`; it must never enable a read or full adapter.
- The reviewed production tools are `rocketmq_upsert_topic`,
  `rocketmq_upsert_consumer_group`, `rocketmq_reset_consumer_offset`,
  `rocketmq_patch_broker_config`, `rocketmq_set_consumer_request_mode`,
  `rocketmq_begin_proxy_drain`, `rocketmq_cancel_proxy_drain`,
  `rocketmq_set_broker_log_filter_ttl`, and `rocketmq_restore_broker_log_filter`,
  plus the read-only `rocketmq_get_mutation_result`.
  Keep delete, skip, resend, and any free-form mutation outside this project
  until separately reviewed.
- The Proxy drain and Broker log filter tools are operation-bound and have exactly one target. A Proxy is named
  only by an alias from `[[clusters.proxies]]`: refuse an alias the cluster does not list before the `started`
  record, and give no tool argument that could carry an address. Read the target before every write. Never
  replace or end a state that another `operation_id` holds; report it as a conflict and expose a holder only as
  `none`, `this_operation`, or `other_operation`. Never return, log, or store an `operation_id`; only the
  requested digest covers it. A log filter override stays within one lower-case `rocketmq_broker::` logger
  outside the protected modules, the levels `INFO` and `DEBUG`, and 60--900 seconds.
- `rocketmq_get_mutation_result` reads only what the audit trail remembers about the caller's own invocations on
  the authorized cluster. It opens no session, sends no RPC, writes no durable audit record, and never returns the
  operator, the reason, or a request key. It needs the write scope and cluster authorization before its arguments
  are read, and no operation: it is registered whenever `write-tools` is compiled and stays available when
  `mutations_enabled` is false. Its annotations are read-only; do not register it through the mutation path.
- OAuth and closed operation/cluster authorization must complete before mutation argument parsing. A durable
  `started` audit record must then complete before session creation or RPC.
- Do not add stdio, CLI, shell, subprocess, free-form RPC, arbitrary Admin commands, or stdout protocol output.
- Durable audit v3 records may contain only: the validated OAuth subject and optional safe bounded request reason
  as operator evidence; the validated logical names of the mutation target (Topic, Consumer Group, Broker names,
  a configured Proxy alias);
  SHA-256 digests of the requested state, the request key, the pre-change state, and the selected Broker set; and
  closed outcome values (whether a target changed, per-target result counts). Never store the requested values, a
  request key, or a name that fails argument validation. Responses, errors, tracing, ordinary logs, and all other
  public data must exclude the operator and reason; every surface must also exclude credentials, tokens, network
  addresses, raw backend errors, and message bodies.
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
  request context, and v2/v3 recovery boundaries; v1 recovery retains its legacy shape rules.
- Normalize every reliable audit sink read, recovery, append, rotation, or timeout failure to `audit_unavailable`;
  never expose a sink-provided code or message. A failed durable `started` append must precede and prevent
  session/RPC. Never delete or rewrite a sealed audit segment.
- Admission control runs after authorization and argument validation and before the `started` record. A call it
  refuses gets `rate_limited`, writes no durable audit record, and reaches no session.
- Each upsert accepts only 1--64 explicit broker names. All operations validate the full selected-cluster topology
  before target state RPC, keep plans sealed to one Admin session, and preserve exact-target post-read verification.
- Targeted Topic upserts must treat the complete order-Topic KV as a sealed no-write guard. Never merge, put,
  or delete that global KV from the targeted path; reject any selected entry that is not already exact.
- Request-key reuse is bounded and process-local; every mutation invocation, including followers and cache
  hits, keeps its own durable audit pair. A mutation response carries the `invocation_id` of the call that
  returns it, so a replayed response is never sent under the invocation of the call that produced it.
- Prefer native async trait methods where object safety is not required; do not add `async_trait`.

## Development validation

From this directory, keep `INSTA_UPDATE=no` and `RUST_MIN_STACK` unset when running tests.
Use `cargo fmt --all -- --check`, `cargo check --locked` and focused `cargo test --locked <test_name>`
cases for changed behavior; a relevant test build can replace the separate compile check.

Run `python scripts/check_control_boundary.py` when the mutation adapter, registered operations,
authorization, audit, redaction, or dependency boundary changes. Include `--features write-tools`
for changes to the adapter and verify the default no-mutation-dependency behavior when feature wiring changes.
Snapshots that specify protocol behavior must be reviewed when intentionally changed, not blindly updated.

For control-service integration, select the broader default/write-tools suites, Clippy, or Rustdoc
checks required by that scope. Do not automatically add query MCP snapshots or root strict Clippy;
validate those consumers only when the change actually affects them.
