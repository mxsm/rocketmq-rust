# Reviewed mutation tool reference

The `write-tools` build exposes at most nine production mutation tools. A mutation tool is listed only when
compile-time support, runtime mutation enablement, the server operation/cluster allowlists, a configured logical
cluster, and the authenticated principal's `rocketmq:write` scope plus operation/cluster claims all intersect.
Authorization is completed before the tool-specific JSON schema is parsed. The same build exposes one read-only
tool, [`rocketmq_get_mutation_result`](#rocketmq_get_mutation_result), which has rules of its own.

All nine mutation tools use input schema version `rocketmq-mcp-control.arguments.v1`. Unknown fields are rejected. The
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

The request identifies exactly one logical `broker_name`. Its nested `properties` must be a JSON object; an array
is rejected, never read by position. The object rejects unknown keys and
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

## Operation-bound tools

The four tools below do not replace metadata. Each puts one target into a state that lasts until the operation
that started it ends it: a drain of one Proxy, or a temporary log level on one Broker. The caller chooses an
`operation_id` of 8--64 letters, digits, `_`, and `-` when it starts the state and passes the same one to end
it. The identifier is part of `requested_digest` and appears nowhere else: no response, log line, or audit
record repeats it, and the identifier that holds a target is never returned.

Every state says who holds the target in `held_by`:

| `held_by` | Meaning |
| --- | --- |
| `none` | No drain or override is active. |
| `this_operation` | The `operation_id` of this call holds the target. |
| `other_operation` | Another identifier holds the target. |

The four tools share one sequence. The session validates the complete topology of the selected cluster and
reads the target, through the mutation session itself. A dry run stops there and says in `changed` whether an
execution would change the target. An execution sends at most one write and reads the target again: `after` is
that second read, and `verification` is `verified` only when it shows the requested state.

| What the call finds | Result |
| --- | --- |
| The target is already as requested | No write. `status=applied`, `changed=false`, `verification=not_performed`. |
| Another operation holds the target | No write. `status=conflict` and `error_code=precondition_conflict`, for a dry run as well. |
| The target cannot be read | No write. `status=failed` and `error_code=execution_failed`; the target has `before=null` and `failure=unavailable` or `invalid_data`. |
| The write is refused or not answered | `applied=false`. The target has `failure=conflict` when the second read shows another holder and `failure=unavailable` otherwise; `changed=true` when that read shows the requested state all the same. |
| The write is acknowledged and the second read differs or fails | `applied=true`, `status=failed`, and `error_code=verification_failed`. |

Each of these tools has exactly one target: `targets` has one entry, `before` and `after` are objects keyed by
its name, and `partial` cannot occur. A target has no `persistence` member, because a drain and a log filter
are running state: a restarted Proxy admits work again and a restarted Broker runs its configured filter.
A cluster whose topology cannot be validated ends in the `execution_failed` envelope before a target is read.

Use a new `request_key` for every begin, cancel, set, and restore. A key replays the settled outcome of its
call for 10 minutes without reading the target again, so a key that began a drain must not be sent again to
begin the next one.

`rocketmq_begin_proxy_drain` is annotated `destructiveHint=true`, because it takes a Proxy out of service. The
other three are annotated `destructiveHint=false`: they give back or add something bounded.

### `rocketmq_begin_proxy_drain` and `rocketmq_cancel_proxy_drain`

Both tools take the same arguments:

| Argument | Required | Rules |
| --- | --- | --- |
| `schema_version` | yes | `rocketmq-mcp-control.arguments.v1`. |
| `cluster` | yes | The logical cluster whose configuration lists the Proxy. |
| `proxy_name` | yes | Alias of the Proxy: 1--64 letters, digits, `_`, and `-`. Never an address. |
| `operation_id` | yes | 8--64 letters, digits, `_`, and `-`. A cancel passes the one the drain was begun with. |
| `dry_run`, `confirm`, `reason`, `request_key` | as for every mutation | The common rules at the top of this document. |

A Proxy is named only by an alias that the server configuration lists under `[[clusters.proxies]]` of the
cluster; the endpoint stays in that configuration. An alias the cluster does not list is refused as
`invalid_argument` with the violation `{"path": "/proxy_name", "constraint": "other"}` before the `started`
record, so the call writes no audit record and opens no session.

Beginning a drain makes the Proxy stop admitting new work. Its `phase` goes from `accepting` to `draining` and
reaches `drained` once nothing is pending; a drained Proxy can be restarted. The tool does not wait for that:
`after` is the state read directly after the write. To follow a drain, repeat the begin call as a dry run with
the same `operation_id` and read `phase`, `zero_pending`, and the `pending` counters. Cancelling makes the Proxy
admit work again.

| Call | Proxy state | Outcome |
| --- | --- | --- |
| begin | `accepting` | Begins the drain. |
| begin | Draining or drained under this operation | Nothing to do: `applied` with `changed=false`. |
| begin | Held by another operation, or not accepting for another reason | `conflict`. |
| cancel | Draining or drained under this operation | Ends the drain. |
| cancel | `accepting` | Nothing to do: `applied` with `changed=false`. |
| cancel | Held by another operation, or not accepting for another reason | `conflict`. |

A drain state has `phase` (`accepting`, `draining`, or `drained`), `held_by`, `admission_open`, `routing_open`,
`readiness_published`, `zero_pending`, and `pending`, the work the Proxy still holds: `active_connections`,
`sessions`, `receipt_handles`, `prepared_transactions`, `telemetry_links`, `remoting_channels`,
`telemetry_commands`, and `rpc_in_flight`.

A Proxy answers drain requests, reads included, only for an authenticated principal. The cluster entry must
therefore reference credentials that the Proxy accepts; without them the Proxy cannot be read and the call
ends `failed`.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "proxy_name": "proxy-a",
  "operation_id": "CHG-10021-proxy-a",
  "dry_run": true
}
```

### `rocketmq_set_broker_log_filter_ttl` and `rocketmq_restore_broker_log_filter`

`rocketmq_set_broker_log_filter_ttl` gives one logger of one Broker master the level `INFO` or `DEBUG` for a
bounded time. The Broker ends the override by itself when `ttl_seconds` have passed;
`rocketmq_restore_broker_log_filter` ends it earlier.

| Argument | Tool | Rules |
| --- | --- | --- |
| `schema_version`, `cluster` | both | As for every mutation. |
| `broker_name` | both | One logical Broker of the selected cluster; its master is the target. |
| `operation_id` | both | 8--64 letters, digits, `_`, and `-`. A restore passes the one the override was set with. |
| `logger` | set | A lower-case `rocketmq_broker::` module path such as `rocketmq_broker::processor`: at most 128 bytes, matching `^rocketmq_broker(::[a-z_][a-z0-9_]*)+$`. |
| `level` | set | `INFO` or `DEBUG`. |
| `ttl_seconds` | set | 60 through 900. |
| `dry_run`, `confirm`, `reason`, `request_key` | both | The common rules at the top of this document. |

A logger whose path contains `message`, `body`, `security`, `auth`, `acl`, `secret`, `payload`, or `store` is
rejected as `invalid_argument`, because the logs of those modules can carry message content, credentials, or
storage internals. A restore names no logger, level, or time to live; there they are unknown fields.

A Broker keeps one override at a time and replaces it without asking. The server therefore never sends a set
over an active override, and never ends an override that another operation set:

| Call | Broker state | Outcome |
| --- | --- | --- |
| set | No override | Sets the override. |
| set | Override of this operation, and the logger already has the requested level | Nothing to do: `applied` with `changed=false`. The expiry is not extended. |
| set | Override of this operation with another level, or an override of another operation | `conflict`. Restore the active override first. |
| restore | Override of this operation | Ends the override. |
| restore | No override, for example after it expired | Nothing to do: `applied` with `changed=false`. |
| restore | Override of another operation | `conflict`. |

A log-filter state has `level`, `held_by`, and `expires_at_millis`. `level` is the effective level of the logger
a set names; it is `null` for a restore, which names no logger, and when that level is neither `INFO` nor
`DEBUG`. `expires_at_millis` is the Unix time in milliseconds at which the Broker ends the active override, and
`null` without one. `requested` repeats the action: `{"action": "set", "logger": ..., "level": ...,
"ttl_seconds": ...}` or `{"action": "restore"}`.

A Broker applies a remote log filter only when it runs with authentication and authorization enabled and with
`[logging.reload] enabled = true`, and only for a request that carries an AccessKey. The cluster entry must
therefore reference credentials. A Broker without the control cannot be planned: the call ends `failed` with
`failure=unavailable` and `retryable=false`.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "logger": "rocketmq_broker::processor",
  "level": "DEBUG",
  "ttl_seconds": 300,
  "operation_id": "INC-4411-broker-a",
  "dry_run": true
}
```

## `rocketmq_get_mutation_result`

The one read-only tool. It answers how one of the caller's own mutation calls ended, for a response that was
lost or a call that timed out. It reads what the audit trail remembers: it opens no Admin session, sends no
RocketMQ RPC, and writes no durable audit record. Its annotations are `readOnlyHint=true`,
`destructiveHint=false`, and `openWorldHint=false`.

The tool is listed for a principal with `rocketmq:write` and at least one allowed, configured cluster. It needs
no operation claim and is independent of `mutations_enabled`, so the result of an earlier call stays readable
after an emergency stop. The write scope and the cluster are authorized before the arguments are read.

| Argument | Required | Rules |
| --- | --- | --- |
| `schema_version` | yes | `rocketmq-mcp-control.arguments.v1`. |
| `cluster` | yes | The logical cluster the mutation was called with. |
| `invocation_id` | one of the two | The `invocation_id` of a mutation response; at least 1. |
| `request_key` | one of the two | The `request_key` the mutation was called with. It selects the newest of the caller's calls on the cluster that carried it. |

With both selectors, the identified invocation must also have carried the key. Unknown fields are rejected;
`dry_run`, `confirm`, and `reason` are not arguments of this tool.

The answer uses `rocketmq-mcp-mutation-result.v1` and is never an MCP error, whatever it says about the mutation:

| `state` | Meaning | What to do |
| --- | --- | --- |
| `finished` | The terminal audit record is written. `status`, `error_code`, `duration_millis`, `changed`, and `target_results` repeat it. | Act on `status` as on a mutation result. `changed` and `target_results` are `null` when the call ended without a result, for example after `timeout`; `null` does not mean that nothing changed. |
| `in_progress` | The `started` record is written and the mutation is still running in this process. | Ask again later. |
| `interrupted` | A previous process wrote the `started` record and stopped before the terminal record. | Nothing records whether the cluster changed. Read the current state with a dry run before repeating the call. |
| `not_found` | None of the caller's remembered calls on this cluster matches. | The call may never have started, belong to another principal or cluster, or be older than what the server remembers. |

`invocation_id`, `operation`, `mode`, and `target` describe the invocation in every state but `not_found`.
`target` holds logical names only: `topic`, `consumer_group`, `broker`, `proxy` for a drain, and for an upsert
`broker_count` with `broker_names` when the list was short enough to be recorded. The answer never contains the
operator, the reason, the request key, the requested values, an `operation_id`, or any digest.

The server remembers the 2,048 newest invocations in memory, forgetting finished ones before running ones. At
startup it rebuilds that memory from the active audit segment, so results in sealed segments are not served;
read those from the audit files. Lookups are limited per principal to `limits.dry_runs_per_minute` calls per
minute, in a budget separate from dry runs, and take none of the `limits.max_concurrent_calls` slots. A refused
lookup gets `rate_limited`. Once the audit trail has failed to persist a record, a lookup gets
`audit_unavailable` like every mutation.

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "cluster-a",
  "request_key": "orders-topic-2026-09-25"
}
```

```json
{
  "schema_version": "rocketmq-mcp-mutation-result.v1",
  "cluster": "cluster-a",
  "state": "finished",
  "invocation_id": 41,
  "operation": "topic_upsert",
  "mode": "execute",
  "target": {"topic": "orders", "broker_count": 2, "broker_names": ["broker-a", "broker-b"]},
  "status": "applied",
  "error_code": null,
  "duration_millis": 184,
  "changed": true,
  "target_results": {"applied": 2, "unchanged": 0, "conflict": 0, "failed": 0}
}
```

## Complete request examples

The following are complete `tools/call.params.arguments` values for the nine reviewed Tools. They
use only the checked-in `rocketmq-mcp-control.arguments.v1` schema. A dry-run is a plan request and
therefore uses `dry_run=true` with `confirm=false`; it does not require `reason`. Every execute
example sets `dry_run=false`, `confirm=true`, and a safe, bounded reason. The logical `cluster`
must be allowed by both the principal and server cluster allowlists, and the operation must be
allowed by both their operation allowlists. Broker names are not allowlisted in either policy; each
must instead be a member of the selected cluster topology. A Proxy alias must be one the server
configuration lists for the cluster.

### `rocketmq_upsert_topic`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "broker_names": ["broker-a"],
  "read_queue_nums": 8,
  "write_queue_nums": 8,
  "perm": 6,
  "order": false,
  "message_type": "NORMAL",
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "broker_names": ["broker-a"],
  "read_queue_nums": 8,
  "write_queue_nums": 8,
  "perm": 6,
  "order": false,
  "message_type": "NORMAL",
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10016 create orders Topic"
}
```

### `rocketmq_upsert_consumer_group`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "consumer_group": "orders-consumer",
  "broker_names": ["broker-a"],
  "consume_enable": true,
  "consume_from_min_enable": false,
  "consume_broadcast_enable": false,
  "consume_message_orderly": false,
  "retry_queue_nums": 1,
  "retry_max_times": 16,
  "broker_id": 0,
  "which_broker_when_consume_slowly": 1,
  "notify_consumer_ids_changed_enable": true,
  "group_sys_flag": 0,
  "consume_timeout_minute": 15,
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "consumer_group": "orders-consumer",
  "broker_names": ["broker-a"],
  "consume_enable": true,
  "consume_from_min_enable": false,
  "consume_broadcast_enable": false,
  "consume_message_orderly": false,
  "retry_queue_nums": 1,
  "retry_max_times": 16,
  "broker_id": 0,
  "which_broker_when_consume_slowly": 1,
  "notify_consumer_ids_changed_enable": true,
  "group_sys_flag": 0,
  "consume_timeout_minute": 15,
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10016 configure orders Consumer Group"
}
```

### `rocketmq_reset_consumer_offset`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "timestamp": "2026-09-04T12:00:00Z",
  "force": false,
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "timestamp": "2026-09-04T12:00:00Z",
  "force": false,
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10016 reset orders Consumer offset"
}
```

### `rocketmq_patch_broker_config`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "properties": {
    "traceTopicEnable": "true"
  },
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "properties": {
    "traceTopicEnable": "true"
  },
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10016 enable trace Topic"
}
```

### `rocketmq_set_consumer_request_mode`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "mode": "pop",
  "pop_share_queue_num": 4,
  "timeout_millis": 12000,
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "mode": "pop",
  "pop_share_queue_num": 4,
  "timeout_millis": 12000,
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10016 enable orders pop mode"
}
```

### `rocketmq_begin_proxy_drain` and `rocketmq_cancel_proxy_drain`

Dry-run of a begin:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "proxy_name": "proxy-a",
  "operation_id": "CHG-10021-proxy-a",
  "dry_run": true,
  "confirm": false
}
```

Execute the begin:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "proxy_name": "proxy-a",
  "operation_id": "CHG-10021-proxy-a",
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10021 drain proxy-a before restart",
  "request_key": "CHG-10021-proxy-a-begin"
}
```

Execute the cancel with `rocketmq_cancel_proxy_drain`, the same `operation_id`, and a key of its own:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "proxy_name": "proxy-a",
  "operation_id": "CHG-10021-proxy-a",
  "dry_run": false,
  "confirm": true,
  "reason": "CHG-10021 proxy-a restart finished",
  "request_key": "CHG-10021-proxy-a-cancel"
}
```

### `rocketmq_set_broker_log_filter_ttl`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "logger": "rocketmq_broker::processor",
  "level": "DEBUG",
  "ttl_seconds": 300,
  "operation_id": "INC-4411-broker-a",
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "logger": "rocketmq_broker::processor",
  "level": "DEBUG",
  "ttl_seconds": 300,
  "operation_id": "INC-4411-broker-a",
  "dry_run": false,
  "confirm": true,
  "reason": "INC-4411 debug processor logging for five minutes"
}
```

### `rocketmq_restore_broker_log_filter`

Dry-run:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "operation_id": "INC-4411-broker-a",
  "dry_run": true,
  "confirm": false
}
```

Execute:

```json
{
  "schema_version": "rocketmq-mcp-control.arguments.v1",
  "cluster": "production-a",
  "broker_name": "broker-a",
  "operation_id": "INC-4411-broker-a",
  "dry_run": false,
  "confirm": true,
  "reason": "INC-4411 investigation finished"
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

Every mutation response schema also requires a nullable, typed top-level `error_code`:

| `status` | `error_code` |
| --- | --- |
| `planned`, `applied` | `null` |
| `partial` | `partial_apply` |
| `conflict` | `precondition_conflict` |
| `failed`, with every actual failure being verification or order reconciliation | `verification_failed` |
| other `failed` result | `execution_failed` |

Per-target failure codes remain available and more specific; for example, a mixed partial result keeps every
target's failure while its aggregate `error_code` remains `partial_apply`. Cache hits return the same complete
structured value as the original result, except for `invocation_id`.

Every mutation response schema requires `invocation_id`, an integer of at least 1: the sequence number
of the call's `started` record in the durable audit log, and the value the log lines of the call carry. It
identifies the call, not the result, so a follower and a cache hit each return it for their own audit pair.
Pass it to `rocketmq_get_mutation_result` to read how the call ended. An error envelope has no `invocation_id`;
a call that ended in one is found by its `request_key`.

## Error envelope

Errors outside a logical mutation result use the closed `rocketmq-mcp-control.error.v2` envelope. The server
applies authorization in scope, cluster, operation, runtime, then compile/catalog order and does not parse the
mutation schema before those checks pass.

| Code | Trigger | Operator action |
| --- | --- | --- |
| `invalid_config` | Control startup or session creation cannot read, parse, validate, bind, or resolve configured transport, TLS, OAuth/JWKS, cluster, runtime, or credential-reference inputs. | Correct the referenced Control configuration and protected TLS/credential inputs, then restart. Do not retry a Tool until initialization succeeds. |
| `request_rejected` | The `Host` does not exactly match the configured public host, or a supplied `Origin` does not exactly match the configured public HTTPS origin. | Send the request to the configured public HTTPS origin with the exact `Host` and, when present, matching `Origin`; do not bypass the origin policy. |
| `unauthorized` | Bearer authentication fails, or the OAuth subject violates the audit-operator grammar | Issue a valid RS256 token with a documented operator ID; never retry a malformed or unsafe subject unchanged. |
| `permission_denied` | OAuth principal lacks `rocketmq:write` | Request the write scope; do not retry unchanged credentials. |
| `cluster_not_allowed` | Cluster alias is invalid or outside the principal/server intersection | Use an allowed logical alias or update both policies. |
| `operation_not_allowed` | Operation is invalid or outside the principal/server intersection | Use a listed reviewed operation or update both policies. |
| `mutation_disabled` | Runtime mutation policy is disabled | Enable the reviewed policy and restart only through the normal change process. |
| `rate_limited` | The principal used up its per-minute budget of dry runs or of execute calls, or the server is at its limit of concurrent mutation calls | Retry later; the call wrote no audit record and reached no session. Raise `[limits]` only after reviewing who is calling. |
| `confirmation_required` | Execute mode does not set `confirm=true` | Review the dry-run, then explicitly confirm. |
| `invalid_argument` | Schema, bounds, request key, or required safe reason is invalid, or a Proxy alias is not configured for the cluster | Correct the arguments that `violations` names. Without `violations`, check the rules listed under Located arguments, starting with the reason: use trimmed text containing only letters, digits, ordinary space, and `._,#-`; remove bearer/JWT, IP, and FQDN-shaped tokens. |
| `precondition_conflict` | A sealed CAS precondition changed, or another operation holds the Proxy or the Broker log filter | Re-run dry-run and review the new state; no automatic retry occurs. |
| `partial_apply` | Some, but not all, logical targets succeeded | Reconcile using returned per-target truth before another change. |
| `verification_failed` | Every actual failure is post-apply verification/order reconciliation | Inspect returned applied/persistence truth and verify through an authorized operational path. |
| `audit_unavailable` | Durable audit recovery/read/append fails or times out, including a sink returning another code | Stop mutation attempts and repair durable audit storage first. |

Distinct infrastructure codes remain narrow: `operation_unavailable` means the reviewed adapter/catalog is not
available (and can also signal bounded idempotency admission exhaustion), while `execution_failed`, `timeout`,
`cancelled`, and `shutdown_failed` describe their corresponding session/runtime failures. Authentication and
transport rejection retain `unauthorized`, `request_rejected`, and `invalid_config` where applicable. No error
envelope contains operator, reason, endpoint, credential, request key, or raw backend text.

### Located arguments

An `invalid_argument` envelope carries an optional `violations` array when the arguments break the published
input schema. It holds at most three entries, each with a `path` and a `constraint`:

```json
{
  "schema_version": "rocketmq-mcp-control.error.v2",
  "code": "invalid_argument",
  "message": "mutation argument is invalid",
  "retryable": false,
  "violations": [
    {"path": "/broker_names", "constraint": "required"},
    {"path": "/read_queue_nums", "constraint": "minimum"},
    {"path": "/*", "constraint": "additional_property"}
  ]
}
```

`path` is a JSON Pointer built only from property names the input schema declares and from array indexes; `*`
stands for a property the schema does not declare. `constraint` is one of `required`, `type`, `min_length`,
`max_length`, `minimum`, `maximum`, `enum`, `pattern`, `additional_property`, or `other`; `other` covers the
number of entries in `broker_names` and a `proxy_name` that the cluster does not configure. A violation never
repeats a value that was sent, nor a property name the caller chose. One argument reports its first broken rule
only.

The field is omitted when nothing is located. That is the case for rules the input schema cannot express: a
system Topic or protected Consumer Group, duplicate Broker names, a permission that grants neither read nor
write, an empty Broker patch or an invalid setting value, a timestamp that is not RFC 3339, a logger of a
module whose logs stay closed, the reason grammar, a missing execute reason, and a request key reused with
different content.

Arguments are located only after authorization has passed. A caller that may not run the operation on the
cluster receives `permission_denied`, `cluster_not_allowed`, or `operation_not_allowed` without `violations`,
whatever it sent. Envelope readers must accept the optional field; every other code omits it.

New durable records use `rocketmq-mcp-control.audit.v3`. A record says who changed which object to what,
without holding the requested values:

| Field | On | Content |
| --- | --- | --- |
| `operator`, `reason` | `started` and terminal | Validated OAuth subject and optional safe request reason. They are durable-audit-only and never appear in MCP responses, errors, tracing, or ordinary logs. |
| `target` | `started` and terminal | Logical object of the call, shaped like the `target` of the Tool response: `topic` and `brokers` for a Topic upsert, `consumer_group` and `brokers` for a Consumer Group upsert, `topic` and `consumer_group` for an offset reset or a request-mode change, `broker` for a Broker configuration patch or a Broker log filter, `proxy` for a Proxy drain. The names are the validated arguments, a Proxy alias included, never addresses. |
| `target.brokers` | `started` and terminal | Always the `count` and the `digest` of the selected Broker names; the sorted `names` only when the list serializes to at most 1,024 bytes. The digest is the SHA-256 of the sorted names as a JSON array without whitespace. |
| `requested_digest` | `started` and terminal | SHA-256 of the call's arguments without `reason`, `request_key`, `dry_run`, and `confirm`, with Broker names sorted, written as JSON with object keys sorted and no whitespace. The `operation_id` of an operation-bound call is covered by this digest and is not stored. |
| `request_key_digest` | `started` and terminal | SHA-256 of `request_key`, or `null` without one. The key itself is never stored. |
| `before_digest` | terminal | SHA-256 of the `before` state of the result, in the same canonical JSON; `null` when no state was read. |
| `changed` | terminal | Whether at least one target was written; `null` when the attempt ended without a result. |
| `target_results` | terminal | How many targets were `applied` (written and changed), `unchanged` (already as requested, or only planned by a dry run), in `conflict`, or `failed`; `null` when the attempt ended without a result. |

Operation, cluster, mode, event, closed result, terminal error code, terminal monotonic duration, and the
ordering identifiers and timestamp are stored as before. A terminal record repeats the `target` and both request
digests of its `started` record, and recovery refuses a file in which they differ. After a timeout, a
cancellation, or an adapter error, the three outcome fields are `null`: the record does not claim to know
whether a write happened. Version-1 and version-2 JSONL, including legacy `invalid_arguments` and `conflict`,
remains recoverable in mixed files without rewriting it; all new records are version 3.
`audit.max_record_bytes` must be at least 4,096, which holds the largest possible record.

`target.proxy` and the operation values `proxy_drain_begin`, `proxy_drain_cancel`, `broker_log_filter_set`, and
`broker_log_filter_restore` were added to version 3 together with the operation-bound tools. A build from
before them refuses to recover an active segment that holds such a record; the operations runbook describes
how to roll back to such a build.

### Audit segments

`audit.capacity` bounds one segment, not the life of the trail. When the active file at `audit.path` holds
`capacity` lines, it is synced, closed, and renamed to `<path>.<six-digit segment number>`, and a new active file
starts. The first line of every segment after the first is a header:

```json
{"schema_version":"rocketmq-mcp-control.audit-segment.v1","segment":3,"previous_last_sequence":31,"previous_segment_digest":"<SHA-256 of the sealed file>","open_invocations":[31]}
```

`open_invocations` lists the calls that were still in flight. Their `started` records follow the header as exact
copies, so the active segment alone is enough to restart and to check each later terminal record against its
start. A reader that merges segments skips those copies. Sequence numbers continue across segments.

The server never deletes or rewrites a sealed segment. It refuses to start, and to rotate, when a file already
exists under the name the active segment would be sealed to, and a rotation that fails leaves the trail
`audit_unavailable`. A call that a stopped process left without a terminal record stays that way and is not
carried into later segments.

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

A request key applies to execute calls and is scoped to principal, operation, logical cluster, sorted target
set, and canonical payload. For an operation-bound call the target is the Proxy alias or the Broker name, and
the payload includes the `operation_id`. Matching calls that overlap share one process-local attempt. What a
later call with the same key receives depends on how that attempt ended:

| Outcome of the attempt | Next call with the same key and content |
| --- | --- |
| `applied`, `partial`, or `conflict`, as a result or as `partial_apply` / `precondition_conflict` | Replays that outcome for 10 minutes without a session or RPC. |
| `failed`, `verification_failed`, `execution_failed`, `timeout`, `cancelled`, `shutdown_failed`, `audit_unavailable`, or any other error | Runs again: a new session, a new preflight read, and a new conditional write. |

Only a settled outcome is replayed. A failure leaves open what happened to the targets, so repeating it for 10
minutes would hide the current state; the retry reads it instead. Every write is conditional on the state its
preflight sealed, so running again cannot apply a change twice. Calls that were already waiting on a failed
attempt receive its error once, and the entry is then released. It is released only after the session of that
attempt has been shut down, so one key never has two attempts in flight.

A dry run is never keyed, even when it carries `request_key`: each dry run reads the cluster again, and the
execute call that follows a plan may reuse the plan's key. The bounded cache holds at most 4096 entries. A key
reused with different content is rejected. Every call, including a follower or a replay, still receives its own
durable started/terminal audit pair. When all 4096 entries are in flight, a new explicit key is rejected before
audit/session/RPC; unkeyed calls may run uncached. A follower cancellation or timeout does not cancel or evict
the leader. No behavior is promised across process restart.
