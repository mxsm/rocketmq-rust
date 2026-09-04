# RocketMQ MCP Control

`rocketmq-mcp-control` is an isolated, deny-by-default server for supervised RocketMQ mutations. It is a
standalone Cargo project and is not part of the root workspace. The reviewed `write-tools` surface contains
exactly five typed tools: Topic and Consumer Group upsert, consumer offset reset, Broker configuration patch,
and consumer request mode. The default build contains neither Admin Core nor production mutation tools.

Read the [reviewed mutation Tool Reference](docs/tool-reference.md) before operating the server. The
[operations runbook](docs/operations-runbook.md) covers rollout, rollback, audit, and emergency stop-write
handling; the [real-cluster E2E runbook](docs/e2e-runbook.md) covers the opt-in local harness and its cleanup.

## Security boundary

- Streamable MCP is served only over an enforcing TLS listener. There is no plaintext or stdio transport.
- Every MCP request requires an OAuth bearer JWT verified with an HTTPS JWKS endpoint and RS256. The verifier
  requires a bounded `kid`, valid signature, exact HTTPS issuer, exact audience, expiry, subject, and
  `rocketmq:write`. HS algorithms, static tokens, and development authentication are not available.
- JWKS generations have a bounded five-minute lifetime. Rotation and revocation replace the whole generation;
  unknown keys use a bounded negative cache and refresh cooldown. DNS answers are rechecked at connection time,
  and any private, loopback, link-local, or reserved answer rejects the connection.
- The listener accepts at most 1 MiB per request and applies a 30-second request timeout.
- Principal scope, closed cluster claim/allowlist, closed operation claim/allowlist, runtime enablement, and
  compile/catalog availability are evaluated in that order before common argument schema parsing or
  adapter/session creation.
- The project exposes no CLI, shell, subprocess, free-form RPC, arbitrary Admin command, or stdout protocol path.

The fixed capability Resource is `rocketmq-control://capabilities`. Its independent compile-time,
runtime-enabled, and registered-operation fields make availability explicit. `mutation_supported` becomes true
only when `write-tools` is compiled, runtime mutation policy is enabled, and at least one reviewed production
operation is registered. Resource templates and prompts remain empty.

## Feature boundary

| Build | Admin dependency | Runtime policy | `tools/list` | `mutation_supported` |
|---|---|---:|---:|---:|
| default | none | off or on | 0 | false |
| `write-tools` | mutation-only adapter | off | 0 | false |
| `write-tools` | mutation-only adapter | on, empty allowlist | 0 | false |
| `write-tools` | mutation-only adapter | on, one reviewed operation | 1 | true |
| `write-tools` | mutation-only adapter | on, all reviewed operations | 5 | true |

The optional feature enables only `rocketmq-admin-core/mutation-client-adapter`, whose client dependency enables
only `admin-mutation`. It does not enable read or full Admin adapters. Delete, skip, resend, CLI, shell, and
free-form RPC remain outside this delivery.

## Configuration

Set `ROCKETMQ_MCP_CONTROL_CONFIG` to a TOML file based on
[`conf/mcp-control.example.toml`](conf/mcp-control.example.toml). All configuration structs reject unknown
fields. `mutations_enabled` defaults to `false` and `dry_run` defaults to `true`. Issuer, JWKS, and public server
URLs must use canonical public HTTPS hostnames. The listener rejects wildcard binds. Configuration is loaded at
startup; changing `mutations_enabled` or either allowlist, including an emergency disable, requires a process
restart.

The private cluster registry maps a logical alias to a NameServer endpoint, TLS policy, and optional environment
variable names for credentials. Inline credentials are rejected. Registry values and resolved credentials have
redacted debug behavior and never enter MCP responses or audit records. Every server-allowed cluster for a
registered operation must have a registry entry.

[`conf/permissions.example.toml`](conf/permissions.example.toml) documents the closed operation and cluster
claim vocabulary for an authorization server. It is an identity-provider example, not a local authorization
bypass.

The closed policy vocabulary remains `topic_upsert`, `consumer_group_upsert`,
`consumer_offset_reset`, `broker_config_patch`, and `consumer_request_mode`. Common request arguments reject
unknown fields. Omitted `dry_run` uses the configured default and omitted `confirm` is false. Dry-run requests may
omit `reason` and `request_key`; execution requires `confirm = true` and a trimmed 5–256 byte safe reason. A
reason contains only ASCII letters/digits, ordinary space, and `._,#-`. That exact punctuation set supports
sentence stops, identifiers, ticket markers, hyphenated terms, and comma-separated prose. Syntax punctuation
such as `/ \ | : = @ [ ] { } ( ) ' " % < >` and backtick, plus controls and all non-ASCII, is rejected by
grammar before audit. Whitespace tokens and their comma/hash/underscore/hyphen or repeated-dot-delimited subtokens
are checked again after stripping allowed edge punctuation; bearer/compact-JWT, IP, and FQDN values therefore fail
closed even when embedded in allowed punctuation, including `route,broker.internal,now`. IPv4 detection includes
common 1–4 component decimal, hexadecimal, octal, and leading-zero numeric forms such as `127.1` and
`127.000.000.001`. To avoid treating audit references as endpoints, hash-number or uppercase-tag ticket forms
such as `#42` and `INC_42`, plus decimal version tokens immediately following `release` or `version`, remain
allowed. Safe examples
include `CHG-1234 increase queue count` and `ticket INC_42, increase queue count`. A request key is optional and,
when present, is a bounded safe identifier.

The upsert tools accept 1–64 unique logical broker names. All five tools validate the complete selected cluster
membership, embedded broker identity, master presence, and endpoint uniqueness before reading target state.
Offset and request-mode operations use every validated master; Broker patch uses exactly one named master.
Requests use closed typed fields and bounds. The Consumer Group validator reuses Admin Core's
single canonical protected-group classifier, including RocketMQ defaults, tools/scheduler/filter/monitor groups,
ONS groups, transaction/system prefixes, and the heartbeat syncer group. System names, addresses, inline
secrets, and unknown fields are rejected before audit or session creation; Admin constructors repeat the same
check as defense in depth.

Each result uses `rocketmq-mcp-mutation.v1`, returns one of the five closed operations, and has an
operation-specific logical top-level `target`.
Top-level `before`, `requested`, and post-read `after` preserve the complete aggregate state; broker-sorted
`targets` retain per-target persistence, verification, and failure evidence rather than replacing that aggregate
contract. Schema version and operation are single-value enums in each tool output schema. Conflict, partial, and
failed outcomes set MCP `isError=true` while retaining the structured result. The required-nullable top-level
`error_code` is null for `planned` and `applied`, `partial_apply` for `partial`, `precondition_conflict` for
`conflict`, and `verification_failed` for a complete verification failure; other failed execution results use
`execution_failed`. An unchanged success is `applied` with `changed=false`; there is no separate no-op status.
Responses never include addresses, credential
references, OAuth subjects, reasons, request keys, or raw backend errors.

Targeted Topic upserts never update or delete the NameServer-wide order-Topic KV. Before any broker state read,
the server reads and strictly parses the complete KV: selected ordered entries must already equal the requested
queue count, while selected unordered entries must already be absent. The sealed value is checked again before
broker CAS and after it. A pre-change is a zero-broker-write conflict; a post-change preserves broker-applied
truth and returns `order_reconciliation_failed` as a partial result. Unselected and other-cluster KV entries are
never rewritten.

Optional request keys use an in-process 10-minute, 4096-entry singleflight/result cache scoped by principal,
operation, cluster, sorted target set, and canonical payload. Matching followers and cache hits open no Admin
session and perform no RocketMQ RPC, but every invocation writes its own started/terminal audit pair. Reusing a
key with a different payload is rejected. If all 4096 slots are in flight, a new explicit key is rejected before
audit or session creation; an unkeyed request may still run uncached. A follower's cancellation or timeout ends
only that invocation and its audit pair—the leader continues and its result remains cacheable. The cache makes no
cross-restart guarantee.

## Reliable audit and session ordering

The JSONL audit sink checks file metadata against a practical cap, parses existing records as a bounded stream,
and awaits append, flush, and storage confirmation. Payloads are synced before a newline commit, and each
transaction has an internal two-second budget. Cancellation or an incomplete transaction permanently poisons
the live trail; a partial disk tail is rejected during restart. Its records are queryable by the owning service
without adding an MCP audit tool. A bounded memory sink supports deterministic tests. New records use
`rocketmq-mcp-control.audit.v2` and contain sequence, invocation id, timestamp, event, closed operation, safe
cluster alias, validated OAuth subject as `operator`, optional validated request `reason`,
`dry_run|execute` mode, closed `started|planned|applied|partial|conflict|failed` result, terminal error code, and
terminal monotonic duration. Operator and reason are a durable-audit-only exception: they never enter MCP
responses, error envelopes, tracing, or ordinary logs. Audit records never contain tokens, credentials, network
addresses, request keys, message bodies, endpoints, or raw backend errors.

The audit operator grammar is 1–128 ASCII bytes: it starts with an ASCII letter or digit, then uses only ASCII
letters/digits plus `._@-`. Without `@`, endpoint-shaped dotted subjects are rejected. Email-like IDs contain
exactly one `@`; their local side starts and ends alphanumeric without consecutive dots, while the domain is a
non-IP, non-rooted, valid multi-label hostname whose top-level label contains a letter and is not `internal`,
`local`, `localhost`, or `lan`. A compact local side is rejected when its base64url header decodes to a JSON
object with JOSE/JWT marker fields, regardless of the declared algorithm, or its signature is empty/underscore.
This deliberately allows UUIDs, `svc-control_01`-style service IDs, and
normal email-like values such as `first.middle.last@example.test`. The validated DNS domain is never scanned as
a compact token. Non-email IDs reject every compact three-segment base64url shape. Both non-email IDs and email
local parts reject dotted, hexadecimal, or long octal numeric addresses embedded through `._-`. Valid RFC4122
UUIDs are recognized before subtoken scanning. A whole-value decimal numeric operator or email local part remains
an address, while delimiter-separated plain decimal service-ID components such as `service-2026` and `svc_1024`
remain valid. Whitespace, controls, Unicode,
paths/URLs, percent escapes, credentials, bearer material, canonical or legacy numeric IP/socket values, numeric
top-level labels, and endpoint-shaped identities remain invalid. The same operator and reason rules run during
OAuth principal construction, audit-context creation,
and version-2 recovery. Version-1 records remain readable under their original identity-free rules.

Every reliable audit sink read, recovery, append error, or two-second transaction timeout is exposed only as
`audit_unavailable`; a sink-provided code or message is never propagated. Repair and validate durable audit
storage before retrying. In particular, a failed `started` append opens no Admin session and performs no RPC;
a terminal append failure is reported only after the acquired session has completed bounded shutdown.

On restart, the trail reconstructs completed and dangling invocation state from strict version-1, version-2, or
mixed JSONL without rewriting old bytes. Version-1 `invalid_arguments` and `conflict` values recover as
`invalid_argument` and `precondition_conflict`; new writes are version 2 only. A terminal
record is accepted only for a matching active invocation from the same live trail, and only one concurrent
terminal attempt can become durable. Unknown, cross-trail, and duplicate terminal attempts fail closed; a failed
terminal write leaves the invocation active while poisoning subsequent audit operations.

Each registered tool follows this order:

1. authenticate and authorize scope/operation/cluster;
2. parse the common bounded arguments;
3. persist `started` successfully;
4. register a lifecycle-owned supervisor and create exactly one mutation session;
5. run preflight, dry-run, optional execute, and verify;
6. await a bounded shutdown of the acquired session exactly once;
7. persist `completed` or `failed`.

If `started` cannot be persisted, the stable result is `audit_unavailable` and no Admin session or RocketMQ RPC
is created. The supervisor is owned by the injected RocketMQ runtime task group, so dropping an HTTP waiter does
not drop an acquired session or its terminal audit. The synthetic session used by tests proves success,
conflict, adapter panic, failure, timeout, cancellation, caller drop, hanging shutdown, and terminal-audit
behavior without calling a live cluster.

## Validation

Follow [`AGENTS.md`](AGENTS.md). The boundary script validates Cargo dependency closure for both feature modes,
rejects prohibited production surfaces, runs the query MCP read-only boundary, and executes its four real
default/all contract snapshot tests. Snapshot checks run with `INSTA_UPDATE=no`.
