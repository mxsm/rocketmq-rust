# RocketMQ MCP Tool Reference

This reference documents the checked-in Query MCP catalog. It is derived from the default and
`change-planning` Tool contracts in `src/tools/catalog.rs` and their checked-in schema snapshots. The
default catalog has 24 Tools; enabling `change-planning` adds five non-mutating planning Tools for a
total of 29. Use `tools/list` as the live authority for the caller-visible subset.

All default Tools have `readOnlyHint=true`, `destructiveHint=false`, and do not create a RocketMQ
Admin mutation session. See the authorization matrix for discovery and call-time checks. Logical
aliases such as `cluster`, `proxy_name`, and fields explicitly marked logical in the argument
tables are not network addresses.

## Authorization scope matrix

Each Tool has a risk level and requires the corresponding principal scope. For HTTP, scopes come
from verified OAuth claims. For stdio, `Principal::local(profile)` synthesizes them: every profile
gets `rocketmq:read`; `diagnose`, `diagnostic`, and `operator` also get `rocketmq:diagnose`; and
`plan` and `operator` also get `rocketmq:plan`.

| Risk level | Required scope |
| --- | --- |
| ReadOnly | `rocketmq:read` |
| Diagnose | `rocketmq:diagnose` |
| Plan | `rocketmq:plan` |

`tools/list` discovery checks only the principal scope and the role's Tool allow/deny rules. It does
not check configured clusters, the principal cluster claim, tenant binding, or runtime
`allow_change_planning`; discovery therefore does not prove that a call will be accepted. At
call-time, the effective `cluster` is checked against server configuration, the role cluster rule,
the principal `rocketmq_clusters` claim when present, and the configured tenant binding. A planning
call also checks the compiled feature and runtime `allow_change_planning=true` then.

Role rules from every role of the principal, including included roles, are combined. The more
specific rule wins and a denial wins between equally specific rules:

1. a Tool named in `deny_tools` is denied;
2. otherwise a Tool named in `allow_tools` is allowed;
3. otherwise `deny_tools = ["*"]` denies it;
4. otherwise `allow_tools = ["*"]` allows it.

A role that lists its Tools by name and adds `deny_tools = ["*"]` therefore grants exactly the
listed Tools, and a role with `allow_tools = ["*"]` removes single Tools by naming them in
`deny_tools`. A denied Tool is absent from `tools/list`, and the Resources it backs are denied too.

The two inventory Tools permit `cluster` omission. The server-default/sole-configured cluster they
then select is the effective cluster: it gets the same per-cluster authorization, tenant,
rate-limit, and concurrency checks as an explicitly named cluster.

Unless a Tool section says otherwise, input objects reject unknown fields and have no mutually
exclusive fields. Examples are `tools/call.params.arguments` objects. Each response example is a
representative JSON fragment: every actual success carries the complete common envelope.

## Common input validation

The following checked-in validation rules are referenced by every argument table below. They apply
after JSON type validation. An optional argument typed `... or null` defaults to `null` when
omitted unless the relevant table says otherwise. This maps every `cursor` and `filter` entry and
the nullable planning fields below; `limit` is the stated exception with a default of 50.

| Input category | Contract |
| --- | --- |
| `cursor` | Optional opaque continuation from `data.next_cursor`, at most 256 bytes. It is not a client-defined offset or address. |
| Inventory `filter` | Optional Topic or Consumer Group inventory filter: trim it, convert ASCII letters to lowercase, and require at most 1,024 bytes. |
| Exact identifier | Trimmed nonempty identifier of 1--255 bytes. This category applies to Topic, Consumer Group, Producer Group, and message identifiers. |
| Logical alias | Trimmed 1--100 byte ASCII token using only letters, digits, `.`, `_`, and `-`. `.` and `..` are permitted; IP literals, socket/endpoint forms, `:`, `/`, `\\`, `@`, `=`, `&`, `?`, and control characters are rejected. |
| Broker logger | Original (not case-normalized) value of at most 128 bytes which must be an allowlisted `rocketmq_broker::` module path. |
| Configuration-state Broker selection | A nonempty 1--64 element selection of logical aliases; it is sorted and deduplicated before lookup. |
| HA selection | `broker_names` is zero through 64 logical aliases. `controller_names` is zero through 32 non-duplicate logical aliases; a nonempty `controller_names` list requires `include_sync_state=true`. |
| Controller metadata selection | `controller_names` is zero through 32 non-duplicate logical aliases. |

For `rocketmq_list_topics` and `rocketmq_list_consumer_groups`, an omitted or `null` `cluster`
selects the explicit default cluster only when one is configured; otherwise it is valid only when
the server configuration contains exactly one cluster. In every other case the request is invalid.
The selected cluster is authorized exactly like an explicitly named one: the caller's cluster
allow-list, the tenant binding, the rate limit, and the per-cluster concurrency limit all apply to
it, and the response and the audit record carry its name.

Every other Tool requires `cluster`. An empty or whitespace-only `cluster` is invalid for every
Tool and never selects the default cluster.

## Common response and paging behavior

Successful calls return the `rocketmq-mcp.v2` envelope with `schema_version`, `request_id`,
`cluster`, `observed_at`, `freshness_ms`, `cache_status`, `partial`, `warnings`, and typed
`data`. `cache_status` is `hit`, `miss`, or `bypass`. `source_failures` is serialized only when it
is nonempty: entries are sanitized, sorted and deduplicated, capped at 16, make `partial=true`,
and add `source_failures_present` to `warnings`. If the cap is exceeded, `warnings` also includes
`source_failures_truncated` and the response remains partial. `warnings` can also include
`output_rows_truncated`. The controlled Query/Admin projections omit addresses, credentials,
tokens, message bodies, and other configured sensitive values.

The paginated Tools use `limit` and `cursor`. `limit` is an optional unsigned integer from 1 through
200 and defaults to 50. `cursor` follows the common 256-byte opaque-cursor rule. Paged `data`
contains `items`, `count`, `total_count`, `has_more`, and `next_cursor`. More than 1,000 output
rows are truncated, with `partial=true` and `output_rows_truncated`. A serialized response over
1 MiB instead fails with `OutputTooLarge` (`output_too_large`); it is not returned as a truncated
partial response.

A cursor continues one result that the server keeps for `cache.cursor_snapshot_ttl_ms` (60 seconds
by default, at most 5 minutes), so that every page comes from the same observation. Send it back
unchanged, with the same arguments and `limit` as the request that returned it. Paging fails in
three different ways, each with its own code and recovery:

- `cursor_expired`: the kept result is gone, because it expired, was evicted to make room, or the
  cache was invalidated. Send the request again without `cursor` and page from the start.
- `cursor_invalid`: the cursor was altered, or the arguments or `limit` differ from the request that
  returned it. Resend the cursor exactly as received with the original arguments.
- `result_too_large`: the result exceeds what the server keeps for paging, 10,000 entries or rows
  or 4 MiB. Narrow the request, for example with a more specific `filter`.

`rocketmq_get_cluster_overview` reports `topic_count` and `consumer_group_count` for inventories of
any size, including ones that `rocketmq_list_topics` and `rocketmq_list_consumer_groups` can only
return with a `filter`.

Client and message identifiers are returned as pseudonyms: `client-`, `message-`, or
`unique-message-` followed by 32 hexadecimal digits. A pseudonym is a keyed hash of the identifier,
so the same identifier always gets the same pseudonym from one server and the original cannot be
read back from it. The key is random for each process unless `security.pseudonym_key_env` names a
shared one: without it, pseudonyms change when the server restarts and differ between replicas.

```json
{
  "schema_version": "rocketmq-mcp.v2",
  "request_id": "request-opaque",
  "cluster": "production-a",
  "observed_at": "2026-09-04T12:00:00Z",
  "freshness_ms": 0,
  "cache_status": "miss",
  "partial": false,
  "warnings": [],
  "data": {
    "cluster": "production-a"
  }
}
```

## Error responses

A failed call returns `isError=true` and one text block holding a JSON object with
`schema_version`, `request_id`, `correlation_id`, `tool`, `code`, `retryable`, `message`, and
`suggestions`. It never carries addresses, credentials, or backend error text.

| `code` | Retryable | Meaning |
| --- | --- | --- |
| `invalid_arguments` | no | The arguments violate the input schema or a validation rule. The additional `violations` field locates them when the server can tell which argument is wrong. |
| `not_found` | no | The request was valid, but the selected entity does not exist. The additional `entity` field names its kind: `topic`, `consumer_group`, `broker`, `message`, `proxy`, `controller`, or `cluster`. |
| `source_unavailable` | yes | A RocketMQ source could not be reached or answered with a failure. |
| `backend_timeout` | yes | The request used up `server.request_timeout_ms` (25 seconds by default), the time it may spend on RocketMQ sources over every query the Tool runs. |
| `rate_limited` | yes | The rate limit or the per-cluster concurrency limit was reached. |
| `cancelled` | no | The client cancelled the request. |
| `unauthorized_scope` | no | The principal lacks the scope the Tool's risk level requires. |
| `permission_denied` | no | No role of the principal grants the Tool. |
| `cluster_not_allowed` | no | The cluster is not configured or not in the caller's allow-list. |
| `tenant_mismatch` | no | The principal's tenant does not match the cluster's tenant binding. |
| `change_planning_disabled` | no | Planning is not enabled by server policy. |
| `output_too_large` | no | The serialized response would exceed 1 MiB. |
| `cursor_expired` | no | The result that `cursor` continued is no longer kept. Request the first page again without `cursor`. |
| `cursor_invalid` | no | `cursor` was altered, or the arguments or `limit` differ from the request that returned it. |
| `result_too_large` | no | The result exceeds the size the server keeps for paging. Narrow the request, for example with `filter`. |
| `identifier_input_bound_exceeded` | no | An identifier is longer than the 1,024 bytes a pseudonym may cover. |
| `internal_error` | no | The server failed internally; report the `request_id`. |

`violations` holds at most three entries, each with a `path` and a `constraint`:

```json
{"code":"invalid_arguments","retryable":false,"violations":[{"path":"/topic","constraint":"required"},{"path":"/limit","constraint":"maximum"}]}
```

`path` is a JSON Pointer into the arguments. Its segments are names that the Tool's input schema
declares, or array indexes; `*` stands for a property the schema does not declare. `constraint` is
one of `required`, `type`, `min_length`, `max_length`, `minimum`, `maximum`, `enum`, `pattern`,
`additional_property`, or `other`. A violation never repeats a value that was sent, nor the name of
an undeclared property. The limits themselves are in each Tool's input schema, which `tools/list`
returns with a description of every argument.

A missing target is `not_found`, never `source_unavailable`: do not retry it, confirm the name with
the Tool named in `suggestions` instead. `rocketmq_diagnose_consumer_lag` does not fail when the
Topic or Consumer Group is missing: the report lists the lookup as `missing` evidence with
`error_code` set to `not_found`, names the missing target as the root cause, and recommends the
listing Tool. A Resource read reports a missing target as the same non-retryable
`resource_unavailable` error that it returns for a Resource the caller may not read, so the two
cases cannot be told apart.

## Default read-only catalog (24 Tools)

### `rocketmq_get_cluster_overview`

**Purpose and access.** Summarizes Brokers plus Topic and Consumer Group counts for one configured
cluster. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |

**Output.** `data` contains the logical cluster, `brokers` summaries, `topic_count`,
`consumer_group_count`, and `generated_at`.

```json
{"cluster":"production-a"}
```

```json
{"cluster":"production-a","data":{"topic_count":42,"consumer_group_count":9,"brokers":[{"broker_name":"broker-a","broker_active":true}]}}
```

### `rocketmq_list_topics`

**Purpose and access.** Lists a bounded, optionally filtered Topic page. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string or null | no | Optional logical alias; omission/`null` follows the explicit-default or sole-configured-cluster rule. |
| `filter` | string or null | no | Optional Topic inventory filter; trim, ASCII-lowercase, at most 1,024 bytes. |
| `limit` | integer or null | no | Page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation from the preceding response, at most 256 bytes. |

**Output.** Paged `data.items` contains Topic entries with their visible cluster and optional
consumer-group context.

```json
{"cluster":"production-a","filter":"orders","limit":25}
```

```json
{"cluster":"production-a","data":{"items":[{"topic":"orders","cluster":"production-a","consumer_group":null}],"count":1,"total_count":1,"has_more":false,"next_cursor":null}}
```

### `rocketmq_describe_topic`

**Purpose and access.** Describes one Topic with bounded queue-route data. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `limit` | integer or null | no | Queue-route page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque queue-route continuation, at most 256 bytes. |

**Output.** `data` identifies the Topic and returns its bounded route/queue observations with normal
page metadata. Internal Broker addresses are not serialized.

```json
{"cluster":"production-a","topic":"orders","limit":50}
```

```json
{"cluster":"production-a","data":{"topic":"orders","brokers":[],"items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_topic_route`

**Purpose and access.** Returns bounded route data for one Topic. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `limit` | integer or null | no | Route page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque route continuation, at most 256 bytes. |

**Output.** `data` contains the Topic's Broker and queue route observations with page metadata;
address-bearing source fields remain omitted.

```json
{"cluster":"production-a","topic":"orders"}
```

```json
{"cluster":"production-a","data":{"topic":"orders","brokers":[],"items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_list_consumer_groups`

**Purpose and access.** Lists a bounded, optionally filtered Consumer Group page and consumption
summaries. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string or null | no | Optional logical alias; omission/`null` follows the explicit-default or sole-configured-cluster rule. |
| `filter` | string or null | no | Optional Consumer Group inventory filter; trim, ASCII-lowercase, at most 1,024 bytes. |
| `limit` | integer or null | no | Page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation, at most 256 bytes. |

**Output.** Paged `data.items` contains the visible Consumer Group records and the standard page
metadata.

```json
{"cluster":"production-a","filter":"orders","limit":25}
```

```json
{"cluster":"production-a","data":{"items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_consumer_lag`

**Purpose and access.** Gets bounded per-queue lag for one Topic and Consumer Group. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `consumer_group` | string | yes | Exact Consumer Group name. |
| `limit` | integer or null | no | Queue page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** Paged `data` contains per-queue current, broker, and lag observations plus page
metadata.

```json
{"cluster":"production-a","topic":"orders","consumer_group":"orders-consumer","limit":50}
```

```json
{"cluster":"production-a","data":{"topic":"orders","consumer_group":"orders-consumer","items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_describe_broker`

**Purpose and access.** Describes one logical Broker's state. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Trimmed nonempty exact identifier, 1--255 bytes; used only for Broker-name lookup, not as a network endpoint. It does not apply logical-alias or endpoint-shaped rejection. |

**Output.** `data` contains the logical Broker, bounded Broker summaries, and `generated_at`; the
source NameServer address is not serialized.

```json
{"cluster":"production-a","broker_name":"broker-a"}
```

```json
{"cluster":"production-a","data":{"broker_name":"broker-a","brokers":[{"broker_name":"broker-a","broker_active":true}]}}
```

### `rocketmq_get_broker_diagnostics`

**Purpose and access.** Returns bounded readiness, store, recovery, HA, and security diagnostics for
one logical Broker. It is read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Logical Broker name. |

**Output.** `data` includes the diagnostics schema version, observed time, Broker diagnostic rows,
unavailable-Broker count, and sanitized warnings.

```json
{"cluster":"production-a","broker_name":"broker-a"}
```

```json
{"cluster":"production-a","data":{"broker_name":"broker-a","diagnostics_schema_version":"rocketmq-broker-diagnostics.v1","unavailable_brokers":0,"brokers":[{"broker_name":"broker-a","coverage":"available"}]}}
```

### `rocketmq_get_broker_config_summary`

**Purpose and access.** Reads the fixed allowlisted configuration summary for one logical Broker. It
is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Logical Broker name. |

**Output.** `data.brokers` reports generation and the fixed summary fields; it never returns a
free-form configuration dump.

```json
{"cluster":"production-a","broker_name":"broker-a"}
```

```json
{"cluster":"production-a","data":{"broker_name":"broker-a","brokers":[{"broker_name":"broker-a","generation":12,"send_message_thread_pool_nums":8}]}}
```

### `rocketmq_get_broker_log_filter_state`

**Purpose and access.** Reads temporary filter state for one allowlisted `rocketmq_broker` logger
target. It is read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Logical Broker name. |
| `logger` | string | yes | Original form, at most 128 bytes, and an allowlisted `rocketmq_broker::` module path. |

**Output.** `data.brokers` reports support, logger, optional `INFO` or `DEBUG` level, temporary
operation identifiers, and expiry only when available.

```json
{"cluster":"production-a","broker_name":"broker-a","logger":"rocketmq_broker::processor"}
```

```json
{"cluster":"production-a","data":{"broker_name":"broker-a","logger":"rocketmq_broker::processor","brokers":[{"broker_name":"broker-a","supported":true,"level":"INFO"}]}}
```

### `rocketmq_get_proxy_drain_state`

**Purpose and access.** Reads bounded drain progress for one configured logical Proxy. It is
read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `proxy_name` | string | yes | Configured logical Proxy alias. |

**Output.** `data` reports phase, admission/routing/readiness flags, zero-pending state, and bounded
pending counters.

```json
{"cluster":"production-a","proxy_name":"proxy-a"}
```

```json
{"cluster":"production-a","data":{"proxy_name":"proxy-a","admission_open":true,"routing_open":true,"readiness_published":true,"zero_pending":false}}
```

### `rocketmq_diagnose_consumer_lag`

**Purpose and access.** Correlates read-only lag, route, and Broker evidence into a diagnosis report.
It is read-only and **diagnose** risk; callers cannot supply a time range or thresholds.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `consumer_group` | string | yes | Exact Consumer Group name. |

**Output.** `data` is the versioned diagnosis report, including its evidence snapshot, findings,
recommendations, and sanitized warnings.

The report is evaluated on every queue and route row, not on a page of them, and it carries no
cursor. Each evidence entry holds a summary in `evidences[].data` and in
`evidence_snapshot.items[].payload.data`:

| Evidence | Content |
| --- | --- |
| `consumer_lag` | `total_lag`, `consume_tps`, `inflight_total`, `queue_count`, `max_queue_lag`, `brokers` (per Broker: `broker_name`, `queue_count`, `lag`, `max_queue_lag`; largest lag first), and `worst_queues` (the 10 most lagging queues, largest first). |
| `topic_description` | `broker_names`, `read_queue_count`, and `write_queue_count`. |
| `topic_route` | `brokers`, `read_queue_count`, `write_queue_count`, and `queues` (one route row per Broker that hosts the Topic). |
| `broker_description` (`broker_summary` in the snapshot) | The `rocketmq_describe_broker` output for the Broker that holds the most lag, summed over its queues. |

To see more queues than `worst_queues` lists, page `rocketmq_get_consumer_lag`.

```json
{"cluster":"production-a","topic":"orders","consumer_group":"orders-consumer"}
```

```json
{"cluster":"production-a","data":{"summary":"No lag diagnosis finding was produced","root_causes":[],"recommendations":[]}}
```

### `rocketmq_list_consumer_connections`

**Purpose and access.** Lists a bounded page of pseudonymous consumer connections for one exact
Consumer Group. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |
| `limit` | integer or null | no | Page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** Paged `data.items` has logical Broker names, pseudonymous `client_alias`, language,
version, and optional update time; client addresses are not exposed.

```json
{"cluster":"production-a","consumer_group":"orders-consumer","limit":25}
```

```json
{"cluster":"production-a","data":{"consumer_group":"orders-consumer","items":[{"broker_name":"broker-a","client_alias":"consumer-1","language":"RUST","version":1}],"count":1,"total_count":1,"has_more":false,"next_cursor":null}}
```

### `rocketmq_list_producer_connections`

**Purpose and access.** Lists a bounded page of pseudonymous producer connections for one exact
Topic and Producer Group. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `producer_group` | string | yes | Exact Producer Group name. |
| `limit` | integer or null | no | Page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** Paged `data.items` has pseudonymous connection observations and page metadata.

```json
{"cluster":"production-a","topic":"orders","producer_group":"orders-producer","limit":25}
```

```json
{"cluster":"production-a","data":{"topic":"orders","producer_group":"orders-producer","items":[{"broker_name":"broker-a","client_alias":"producer-1","language":"RUST","version":1}],"count":1,"total_count":1,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_message_metadata`

**Purpose and access.** Reads fixed body-free metadata for one message using process-lifetime aliases.
It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `message_id` | string | yes | Exact message identifier. |

**Output.** `data` supplies aliases, Topic, optional born/stored times, queue position, size, flags,
reconsume count, and transaction offset. Message body and address data are absent.

```json
{"cluster":"production-a","message_id":"message-opaque"}
```

```json
{"cluster":"production-a","data":{"message_alias":"message-1","unique_message_alias":null,"topic":"orders","queue_id":0,"queue_offset":42,"store_size":128}}
```

### `rocketmq_get_topic_config_state`

**Purpose and access.** Reads version-CAS observations for one Topic at selected logical Brokers. It
is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `broker_names` | array of strings | yes | 1--64 logical aliases; sorted and deduplicated configuration-state selection. |

**Output.** `data.brokers` returns each Broker's version, queue counts, and ordering state.

```json
{"cluster":"production-a","topic":"orders","broker_names":["broker-a"]}
```

```json
{"cluster":"production-a","data":{"topic":"orders","brokers":[{"broker_name":"broker-a","version":12,"read_queue_nums":8,"write_queue_nums":8,"order":false}]}}
```

### `rocketmq_get_consumer_group_config_state`

**Purpose and access.** Reads version-CAS Consumer Group observations at selected logical Brokers.
It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `group` | string | yes | Exact Consumer Group name. |
| `broker_names` | array of strings | yes | 1--64 logical aliases; sorted and deduplicated configuration-state selection. |

**Output.** `data.brokers` contains group configuration observations including version, retries,
timeouts, consume flags, and Broker selection fields.

```json
{"cluster":"production-a","group":"orders-consumer","broker_names":["broker-a"]}
```

```json
{"cluster":"production-a","data":{"group":"orders-consumer","brokers":[{"broker_name":"broker-a","version":12,"retry_max_times":16,"retry_queue_nums":1,"consume_enable":true}]}}
```

### `rocketmq_get_topic_stats`

**Purpose and access.** Returns a bounded snapshot page of deterministic per-queue Topic statistics
and aggregate totals. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `limit` | integer or null | no | Queue page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** `data` includes deterministic per-queue items, normal page metadata, and complete
aggregate totals.

```json
{"cluster":"production-a","topic":"orders","limit":50}
```

```json
{"cluster":"production-a","data":{"topic":"orders","items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_topic_config`

**Purpose and access.** Reads fixed address-free Topic configuration observations and semantic
differences across Brokers. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |

**Output.** `data` contains observations per logical Broker and explicit inconsistent fields, not a
free-form Broker configuration.

```json
{"cluster":"production-a","topic":"orders"}
```

```json
{"cluster":"production-a","data":{"topic":"orders","brokers":[{"broker_name":"broker-a","read_queue_nums":8,"write_queue_nums":8}],"inconsistent_fields":[]}}
```

### `rocketmq_get_consumer_group_details`

**Purpose and access.** Reads fixed address-free configuration and connection observations for one
Consumer Group. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |

**Output.** `data` contains the Group's configuration and connection observations, with endpoints
and credential material omitted.

```json
{"cluster":"production-a","consumer_group":"orders-consumer"}
```

```json
{"cluster":"production-a","data":{"consumer_group":"orders-consumer","brokers":[{"broker_name":"broker-a"}],"total_connection_count":0}}
```

### `rocketmq_get_consumer_progress`

**Purpose and access.** Returns a bounded snapshot page of deterministic per-queue progress and
complete aggregate totals. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |
| `limit` | integer or null | no | Queue page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** `data` includes per-queue progress rows, normal page metadata, and aggregate totals.

```json
{"cluster":"production-a","consumer_group":"orders-consumer","limit":50}
```

```json
{"cluster":"production-a","data":{"consumer_group":"orders-consumer","items":[],"count":0,"total_count":0,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_ha_status`

**Purpose and access.** Reads bounded HA observations for logical master Brokers, optionally with
configured Controller synchronization state. It is read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_names` | array of strings | no | Defaults to `[]`; zero through 64 logical master aliases. |
| `include_sync_state` | boolean | no | Defaults to `false`; must be `true` when `controller_names` is nonempty. |
| `controller_names` | array of strings | no | Defaults to `[]`; zero through 32 non-duplicate logical Controller aliases. |

**Output.** `data` contains bounded Broker HA observations and, when requested and available,
Controller synchronization observations.

```json
{"cluster":"production-a","broker_names":["broker-a"],"include_sync_state":true,"controller_names":["controller-a"]}
```

```json
{"cluster":"production-a","data":{"brokers":[{"broker_name":"broker-a","broker_id":0,"in_sync_slave_count":1}],"controller_sync_states":[]}}
```

### `rocketmq_get_controller_metadata`

**Purpose and access.** Reads bounded metadata for configured logical Controllers. It is read-only
and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `controller_names` | array of strings | no | Defaults to `[]`; zero through 32 non-duplicate logical Controller aliases. |

**Output.** `data.controllers` provides group, leadership, peer-count, and log-index observations
where available.

```json
{"cluster":"production-a","controller_names":["controller-a"]}
```

```json
{"cluster":"production-a","data":{"controllers":[{"controller_name":"controller-a","is_leader":true,"peer_count":3}]}}
```

### `rocketmq_get_nameserver_config_summary`

**Purpose and access.** Reads fixed allowlisted configuration values and differences for configured
NameServers. It is read-only.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |

**Output.** `data.nameservers` contains the fixed summary fields and `inconsistent_fields`; it never
returns a free-form NameServer configuration dump.

```json
{"cluster":"production-a"}
```

```json
{"cluster":"production-a","data":{"nameservers":[{"nameserver_name":"nameserver-a","values":{"cluster_test":false}}],"inconsistent_fields":[]}}
```

## Optional `change-planning` catalog (5 Tools)

`change-planning` is compiled separately and remains disabled until runtime policy permits it. Each
Tool has `readOnlyHint=true` and produces a plan only: none has Apply mode, confirmation, operator
identity, or a RocketMQ mutation API call. The `ToolResponse<ChangePlan>.data` object reports
`mutates_cluster=false`, planned changes, impact analysis, and rollback suggestions. Do not treat a
generated plan as authorization to change a cluster.

All five inputs reject unknown fields. Each has required string `cluster`, required string `reason`,
and required object `desired`. The Guard authorizes `cluster` against a configured cluster before
the current-state query; planning does not add a separate logical-alias normalization. `reason` is
an ordinary required string. There are no mutually exclusive fields declared by these contracts.
The rules column identifies the current-state query validation that applies after decoding, if any.

Planning output is intentionally not a redacted Resource projection. It returns `reason`, and
`planned_changes` can echo `config_value` and other desired strings. Never put credentials, tokens,
secrets, or other sensitive values in any planning input.

### `rocketmq_plan_create_topic`

**Purpose.** Generates a non-mutating Topic creation plan.

| `desired` field | Type | Required | Rules |
| --- | --- | --- | --- |
| `topic` | string | yes | Current planner does no extra runtime normalization; schema type is only string. |
| `read_queue_nums` | integer (`u32`) or null | no | Defaults to `null`; minimum 0. |
| `write_queue_nums` | integer (`u32`) or null | no | Defaults to `null`; minimum 0. |
| `perm` | string or null | no | Defaults to `null`; no additional scalar bound in this schema. |

```json
{"cluster":"production-a","reason":"capacity preparation","desired":{"topic":"orders","read_queue_nums":8,"write_queue_nums":8,"perm":"read_write"}}
```

```json
{"schema_version":"rocketmq-mcp.v2","request_id":"request-opaque","cluster":"production-a","observed_at":"2026-09-04T12:00:00Z","freshness_ms":0,"cache_status":"bypass","partial":false,"warnings":[],"data":{"mutates_cluster":false,"plan_type":"create_topic"}}
```

### `rocketmq_plan_update_topic_config`

**Purpose.** Generates a non-mutating Topic configuration update plan.

| `desired` field | Type | Required | Rules |
| --- | --- | --- | --- |
| `topic` | string | yes | Passed to `describe_topic`: trimmed nonempty exact identifier, 1--255 bytes. |
| `config_key` | string | yes | No additional scalar bound in this schema. |
| `config_value` | string | yes | No additional scalar bound in this schema. |

```json
{"cluster":"production-a","reason":"review config change","desired":{"topic":"orders","config_key":"message.type","config_value":"normal"}}
```

```json
{"schema_version":"rocketmq-mcp.v2","request_id":"request-opaque","cluster":"production-a","observed_at":"2026-09-04T12:00:00Z","freshness_ms":0,"cache_status":"bypass","partial":false,"warnings":[],"data":{"mutates_cluster":false,"plan_type":"update_topic_config"}}
```

### `rocketmq_plan_update_topic_permissions`

**Purpose.** Generates a non-mutating Topic permission update plan.

| `desired` field | Type | Required | Rules |
| --- | --- | --- | --- |
| `topic` | string | yes | Passed to `describe_topic`: trimmed nonempty exact identifier, 1--255 bytes. |
| `perm` | string | yes | No additional scalar bound in this schema. |

```json
{"cluster":"production-a","reason":"review access change","desired":{"topic":"orders","perm":"read_write"}}
```

```json
{"schema_version":"rocketmq-mcp.v2","request_id":"request-opaque","cluster":"production-a","observed_at":"2026-09-04T12:00:00Z","freshness_ms":0,"cache_status":"bypass","partial":false,"warnings":[],"data":{"mutates_cluster":false,"plan_type":"update_topic_permissions"}}
```

### `rocketmq_plan_update_broker_config`

**Purpose.** Generates a non-mutating Broker configuration update plan.

| `desired` field | Type | Required | Rules |
| --- | --- | --- | --- |
| `broker_name` | string | yes | Passed to `describe_broker`: trimmed nonempty exact identifier, 1--255 bytes. |
| `config_key` | string | yes | No additional scalar bound in this schema. |
| `config_value` | string | yes | No additional scalar bound in this schema. |

```json
{"cluster":"production-a","reason":"review broker configuration","desired":{"broker_name":"broker-a","config_key":"flushDiskType","config_value":"ASYNC_FLUSH"}}
```

```json
{"schema_version":"rocketmq-mcp.v2","request_id":"request-opaque","cluster":"production-a","observed_at":"2026-09-04T12:00:00Z","freshness_ms":0,"cache_status":"bypass","partial":false,"warnings":[],"data":{"mutates_cluster":false,"plan_type":"update_broker_config"}}
```

### `rocketmq_plan_reset_consumer_offset`

**Purpose.** Generates a non-mutating Consumer offset reset plan.

| `desired` field | Type | Required | Rules |
| --- | --- | --- | --- |
| `topic` | string | yes | Passed to `query_consumer_lag`: trimmed nonempty exact identifier, 1--255 bytes. |
| `consumer_group` | string | yes | Passed to `query_consumer_lag`: trimmed nonempty exact identifier, 1--255 bytes. |
| `target_offset` | integer (`i64`/int64) or null | no | Defaults to `null`; no additional scalar bound in this schema. |
| `timestamp_millis` | integer (`i64`/int64) or null | no | Defaults to `null`; no additional scalar bound in this schema. |

`target_offset` and `timestamp_millis` are both optional in the schema; the schema declares no
mutual-exclusion rule for them.

```json
{"cluster":"production-a","reason":"review offset recovery","desired":{"topic":"orders","consumer_group":"orders-consumer","target_offset":42}}
```

```json
{"schema_version":"rocketmq-mcp.v2","request_id":"request-opaque","cluster":"production-a","observed_at":"2026-09-04T12:00:00Z","freshness_ms":0,"cache_status":"bypass","partial":false,"warnings":[],"data":{"mutates_cluster":false,"plan_type":"reset_consumer_offset"}}
```
