# Reviewed mutation tool reference

The `write-tools` build exposes at most five production tools. A tool is listed only when compile-time support,
runtime mutation enablement, the server operation/cluster allowlists, a configured logical cluster, and the
authenticated principal's `rocketmq:write` scope plus operation/cluster claims all intersect. Authorization is
completed before the tool-specific JSON schema is parsed.

All five tools use input schema version `rocketmq-mcp-control.arguments.v1`. Unknown fields are rejected. The
`cluster` is a closed logical alias; it is never a NameServer or broker address. `broker_names` contains 1--64
unique logical masters and is validated against the complete selected-cluster topology before a target state
RPC. Omitted `dry_run` uses the server default, and omitted `confirm` is false. Execution requires
`dry_run=false`, `confirm=true`, and a safe 5--256 byte `reason`. An optional `request_key` is an 8--64 byte safe
identifier.

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

## Request-key semantics

A request key is scoped to principal, operation, logical cluster, sorted target set, and canonical payload.
Matching calls share a process-local singleflight or reuse its result for 10 minutes; the bounded cache holds at
most 4096 entries and includes partial or failed results. A key reused with different content is rejected. Every
call, including a follower or cache hit, still receives its own durable started/terminal audit pair. When all
4096 entries are in flight, a new explicit key is rejected before audit/session/RPC; unkeyed calls may run
uncached. A follower cancellation or timeout does not cancel or evict the leader. No behavior is promised across
process restart.
