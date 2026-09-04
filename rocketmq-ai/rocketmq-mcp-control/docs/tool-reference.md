# Reviewed mutation tool reference

The `write-tools` build exposes at most five production tools. A tool is listed only when compile-time support,
runtime mutation enablement, the server operation/cluster allowlists, a configured logical cluster, and the
authenticated principal's `rocketmq:write` scope plus operation/cluster claims all intersect. Authorization is
completed before the tool-specific JSON schema is parsed.

All five tools use input schema version `rocketmq-mcp-control.arguments.v1`. Unknown fields are rejected. The
`cluster` is a closed logical alias; it is never a NameServer or broker address. `broker_names` contains 1--64
unique logical masters and is validated against the complete selected-cluster topology before a target state
RPC. Omitted `dry_run` uses the server default, and omitted `confirm` is false. Execution requires
`dry_run=false`, `confirm=true`, and a trimmed 5--256 byte ASCII `reason`. The only permitted characters are
letters, digits, ordinary space, and `._,#-`; the five punctuation marks support prose, ticket IDs, separators,
and hyphenated terms. All syntax punctuation—including `/ \ | : = @ [ ] { } ( ) ' " % < >` and backtick—is
rejected by grammar. Each whitespace token and its comma/hash/underscore/hyphen or repeated-dot-delimited
subtokens are scanned after allowed edge punctuation is stripped, so embedded bearer/compact-JWT, IP, and
FQDN-shaped values are also rejected. IPv4 detection includes common 1--4 component decimal, hexadecimal,
octal, and leading-zero numeric notation. Hash-number or uppercase-tag ticket references and decimal version
tokens immediately following `release` or `version` remain valid reason text. An optional `request_key` is an
8--64 byte safe identifier.

## `rocketmq_upsert_topic`

The request identifies one non-system Topic and supplies a complete replacement:

- `read_queue_nums` and `write_queue_nums`: integers from 1 through 127;
- `perm`: a RocketMQ permission value from 1 through 7 that grants read or write;
- `order`: whether ordered-topic configuration is required;
- `message_type`: one of `NORMAL`, `FIFO`, `DELAY`, `TRANSACTION`, or `UNSPECIFIED`.

The same sealed Admin session performs targeted preflight, optional conditional replacement, and targeted
post-read. The targeted path never writes or deletes the global order-Topic KV. Its complete value must already
represent each selected target (`order=true` means the entry equals `write_queue_nums`; `order=false` means the
entry is absent), and the sealed value is rechecked before and after broker CAS. A pre-change conflicts before
broker mutation; a post-change returns `order_reconciliation_failed` and partial while preserving applied broker
truth. Entries for unselected brokers or other clusters are never merged, replaced, or removed.

## `rocketmq_upsert_consumer_group`

The request identifies one non-system Consumer Group and supplies every replacement field:

- `consume_enable`, `consume_from_min_enable`, `consume_broadcast_enable`, and
  `consume_message_orderly`;
- `retry_queue_nums`, `retry_max_times`, `broker_id`, and `which_broker_when_consume_slowly`;
- `notify_consumer_ids_changed_enable`, `group_sys_flag`, and `consume_timeout_minute`.

The operation uses the same exact-target preflight, conditional replacement, post-read, and session-seal rules
as the Topic tool. It rejects the complete canonical RocketMQ protected Consumer Group set through the same
Admin Core classifier used by Admin request constructors, before audit or session creation.

## `rocketmq_reset_consumer_offset`

The request identifies one non-system Topic and Consumer Group, a timezone-qualified RFC3339 `timestamp`, and
`force`. Preflight resolves every validated cluster master and seals at most 1,000 unique Broker/queue targets.
Dry-run returns each queue's current offset, planned offset, and delta. Execute performs one exact expected-offset
CAS per changed queue without retry or route re-resolution, then re-reads that queue. A Broker preview failure is
retained as a target with `queue_id=null`; it is never hidden by successful queues. Without `force`, a reset does
not move an uninitialized or existing offset forward; with `force`, the timestamp-derived offset is used.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "timestamp": "2026-08-30T08:00:00+08:00",
  "force": false,
  "dry_run": true
}
```

## `rocketmq_patch_broker_config`

The request identifies exactly one logical `broker_name`. Its nested `properties` object rejects unknown keys and
must contain at least one of `autoCreateTopicEnable`, `autoCreateSubscriptionGroup`, `brokerPermission`,
`defaultTopicQueueNums`, `messageIndexEnable`, or `traceTopicEnable`. Boolean strings must be canonical lowercase;
explicit `null` and empty strings are rejected; permissions are 1--7 with read or write, and queue count is
1--128. Admin Core validates the complete selected
cluster topology before reading only that Broker. Execute performs one generation CAS with no retry and re-reads
the complete six-field projection on the same sealed endpoint. Post-read failure keeps `applied=true` and returns
a failed result.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "properties": {
    "brokerPermission": "6",
    "traceTopicEnable": "true"
  },
  "dry_run": true
}
```

## `rocketmq_set_consumer_request_mode`

The request identifies a non-system Topic and Consumer Group, `mode=pull|pop`, a non-negative
`pop_share_queue_num`, and `timeout_millis` from 1 through 24,000. Every validated master must have the Topic and
Consumer Group before its current request mode is accepted into the sealed plan. Execute forwards the request
timeout to the conditional Client RPC, performs no conflict retry, and precisely re-reads each applied target.
Endpoints remain internal.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "mode": "pop",
  "pop_share_queue_num": 4,
  "timeout_millis": 12000,
  "dry_run": true
}
```

## Result contract

Every successful tool invocation returns `rocketmq-mcp-mutation.v1` structured content. Each tool schema fixes
the schema version and operation to one value. The operation-specific top-level `target` contains only logical
resource names and sorted brokers; top-level `before`, `requested`, and post-read `after` preserve the full
aggregate state required by the mutation contract. Broker-name-sorted `targets` additionally retain per-target
persistence, verification, and failure evidence and do not replace the aggregate fields. An unchanged success
uses `status=applied` with `changed=false`; `noop` is not part of the status vocabulary.
`partial`, `conflict`, and `failed` set MCP `isError=true` while preserving structured target truth. Output never
contains endpoints, credential references or values, OAuth subjects, reasons, request keys, or raw backend
errors. Operation-specific post-read fields are required in the schema but nullable: a missing observation is
represented by JSON `null`, never by omitting the field.

Every one of the five response schemas also requires a nullable, typed top-level `error_code`:

| `status` | `error_code` |
| --- | --- |
| `planned`, `applied` | `null` |
| `partial` | `partial_apply` |
| `conflict` | `precondition_conflict` |
| `failed`, with every actual failure being verification or order reconciliation | `verification_failed` |
| other `failed` result | `execution_failed` |

Per-target failure codes remain available and more specific; for example, a mixed partial result keeps every
target's failure while its aggregate `error_code` remains `partial_apply`. Cache hits return the same complete
structured value as the original result.

## Error envelope

Errors outside a logical mutation result use the closed `rocketmq-mcp-control.error.v2` envelope. The server
applies authorization in scope, cluster, operation, runtime, then compile/catalog order and does not parse the
mutation schema before those checks pass.

| Code | Trigger | Operator action |
| --- | --- | --- |
| `unauthorized` | Bearer authentication fails, or the OAuth subject violates the audit-operator grammar | Issue a valid RS256 token with a documented operator ID; never retry a malformed or unsafe subject unchanged. |
| `permission_denied` | OAuth principal lacks `rocketmq:write` | Request the write scope; do not retry unchanged credentials. |
| `cluster_not_allowed` | Cluster alias is invalid or outside the principal/server intersection | Use an allowed logical alias or update both policies. |
| `operation_not_allowed` | Operation is invalid or outside the principal/server intersection | Use a listed reviewed operation or update both policies. |
| `mutation_disabled` | Runtime mutation policy is disabled | Enable the reviewed policy and restart only through the normal change process. |
| `confirmation_required` | Execute mode does not set `confirm=true` | Review the dry-run, then explicitly confirm. |
| `invalid_argument` | Schema, bounds, request key, or required safe reason is invalid | Use trimmed reason text containing only letters, digits, ordinary space, and `._,#-`; remove bearer/JWT, IP, and FQDN-shaped tokens. |
| `precondition_conflict` | A sealed CAS precondition changed | Re-run dry-run and review the new state; no automatic retry occurs. |
| `partial_apply` | Some, but not all, logical targets succeeded | Reconcile using returned per-target truth before another change. |
| `verification_failed` | Every actual failure is post-apply verification/order reconciliation | Inspect returned applied/persistence truth and verify through an authorized operational path. |
| `audit_unavailable` | Durable audit recovery/read/append fails or times out, including a sink returning another code | Stop mutation attempts and repair durable audit storage first. |

Distinct infrastructure codes remain narrow: `operation_unavailable` means the reviewed adapter/catalog is not
available (and can also signal bounded idempotency admission exhaustion), while `execution_failed`, `timeout`,
`cancelled`, and `shutdown_failed` describe their corresponding session/runtime failures. Authentication and
transport rejection retain `unauthorized`, `request_rejected`, and `invalid_config` where applicable. No error
envelope contains operator, reason, endpoint, credential, request key, or raw backend text.

New durable records use `rocketmq-mcp-control.audit.v2`. They store only validated operator evidence (OAuth
subject), optional safe request reason, operation, cluster, mode, event, closed result, terminal error code, and
terminal monotonic duration plus ordering identifiers/timestamp. Operator and reason are durable-audit-only and
never appear in MCP responses, errors, tracing, or ordinary logs. Version-1 JSONL, including legacy
`invalid_arguments` and `conflict`, remains recoverable in mixed files without rewriting it; all new records are
version 2.

The operator is 1--128 ASCII bytes, begins with an ASCII letter or digit, and thereafter contains only ASCII
letters/digits or `._@-`. Without `@`, endpoint-shaped dotted subjects are rejected. An email-like ID has exactly
one `@`, a local side that starts/ends alphanumeric without consecutive dots, and a non-IP/non-rooted valid
multi-label domain with an alphabetic top-level label other than `internal`, `local`, `localhost`, or `lan`.
UUID, service, and ordinary email-like IDs such as `first.middle.last@example.test` are supported. A validated
domain is not treated as a compact token. Email local parts reject compact values whose base64url header is a
JSON object with JOSE/JWT marker fields, regardless of the declared algorithm, plus empty/underscore compact
signatures; non-email IDs reject every compact three-segment base64url shape. Non-email IDs and email local
parts also reject dotted, hexadecimal, or long octal numeric addresses embedded through `._-`. Valid RFC4122
UUIDs are recognized before subtoken scanning. Whole-value decimal numeric operators or email local parts remain
addresses; delimiter-separated plain decimal service-ID components such as `service-2026` and `svc_1024` remain
valid.
Whitespace, Unicode/control/format characters, paths/URLs, percent escapes, credential/Bearer values, numeric
top-level labels, canonical or legacy numeric IP/socket values, and endpoint-shaped identities are rejected.
OAuth construction, audit-context creation, and version-2 recovery enforce the same grammar; legacy version-1
records remain identity-free.

All durable sink read/recovery/append errors and transaction timeouts map to `audit_unavailable`, regardless of
the sink's supplied code or message. Stop mutation attempts, repair durable audit storage, and retry only after
recovery succeeds. A failed `started` append creates no session/RPC; a failed terminal append is surfaced after
session shutdown.

## Request-key semantics

A request key is scoped to principal, operation, logical cluster, sorted target set, and canonical payload.
Matching calls share a process-local singleflight or reuse its result for 10 minutes; the bounded cache holds at
most 4096 entries and includes partial or failed results. A key reused with different content is rejected. Every
call, including a follower or cache hit, still receives its own durable started/terminal audit pair. When all
4096 entries are in flight, a new explicit key is rejected before audit/session/RPC; unkeyed calls may run
uncached. A follower cancellation or timeout does not cancel or evict the leader. No behavior is promised across
process restart.
