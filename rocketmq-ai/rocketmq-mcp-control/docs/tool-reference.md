# Reviewed mutation tool reference

The `write-tools` build exposes at most two production tools. A tool is listed only when compile-time support,
runtime mutation enablement, the server operation/cluster allowlists, a configured logical cluster, and the
authenticated principal's `rocketmq:write` scope plus operation/cluster claims all intersect. Authorization is
completed before the tool-specific JSON schema is parsed.

Both tools use input schema version `rocketmq-mcp-control.arguments.v1`. Unknown fields are rejected. The
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

## Result contract

Every successful tool invocation returns `rocketmq-mcp-mutation.v1` structured content. Each tool schema fixes
the schema version and operation to one value. The operation-specific top-level `target` contains `topic` or
`consumer_group` and sorted `brokers`; top-level `before`, `requested`, and post-read `after` preserve the full
aggregate state required by the mutation contract. Broker-name-sorted `targets` additionally retain per-target
persistence, verification, and failure evidence and do not replace the aggregate fields. An unchanged success
uses `status=applied` with `changed=false`; `noop` is not part of the status vocabulary.
`partial`, `conflict`, and `failed` set MCP `isError=true` while preserving structured target truth. Output never
contains endpoints, credential references or values, OAuth subjects, reasons, request keys, or raw backend
errors.

## Request-key semantics

A request key is scoped to principal, operation, logical cluster, sorted target set, and canonical payload.
Matching calls share a process-local singleflight or reuse its result for 10 minutes; the bounded cache holds at
most 4096 entries and includes partial or failed results. A key reused with different content is rejected. Every
call, including a follower or cache hit, still receives its own durable started/terminal audit pair. When all
4096 entries are in flight, a new explicit key is rejected before audit/session/RPC; unkeyed calls may run
uncached. A follower cancellation or timeout does not cancel or evict the leader. No behavior is promised across
process restart.
