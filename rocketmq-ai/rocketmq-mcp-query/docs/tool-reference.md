# RocketMQ MCP Tool Reference

This reference documents the checked-in Query MCP catalog. It is derived from the default,
`change-planning`, and `security-admin` Tool contracts in `src/tools/catalog.rs` and their checked-in
schema snapshots. The default catalog has 36 Tools; enabling `change-planning` adds five non-mutating
planning Tools, and enabling `security-admin` adds one read-only Tool, for a total of 42 with both.
Use `tools/list` as the live authority for the caller-visible subset.

All default Tools have `readOnlyHint=true`, `destructiveHint=false`, and do not create a RocketMQ
Admin mutation session. See the authorization matrix for discovery and call-time checks. Logical
aliases such as `cluster`, `proxy_name`, and fields explicitly marked logical in the argument
tables are not network addresses.

## Authorization scope matrix

Each Tool has a risk level and requires the corresponding principal scope. For HTTP, scopes come
from verified OAuth claims. For stdio, `Principal::local(profile)` synthesizes them: every profile
gets `rocketmq:read`; `diagnose`, `diagnostic`, and `operator` also get `rocketmq:diagnose`; `plan`
and `operator` also get `rocketmq:plan`; and `operator` also gets `rocketmq:message-search`. Only
the profile `security_admin` gets `rocketmq:security-admin`.

| Risk level | Required scope |
| --- | --- |
| ReadOnly | `rocketmq:read` |
| Diagnose | `rocketmq:diagnose` |
| Plan | `rocketmq:plan` |
| MessageSearch | `rocketmq:message-search` |
| SecurityAdmin | `rocketmq:security-admin` |

`MessageSearch` is the level of `rocketmq_find_messages_by_key`. It reads only. It has a scope of its
own because a search by a business key finds messages the caller could not name, so the permission
to read does not include it. A Tool that looks messages up, by an identifier or by a key, also needs
the role permission for `rocketmq_get_message_metadata` on the cluster.

`SecurityAdmin` is the level of `rocketmq_list_acl_users`, which exists only in a build with the
`security-admin` feature. It reads only. It has a scope of its own because the list of who may sign
requests to a cluster is for the people who administer access to it: no other scope includes it, and
it includes no other scope.

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

Every other Tool requires `cluster`, except `rocketmq_list_clusters`, which takes no argument at
all. An empty or whitespace-only `cluster` is invalid for every Tool and never selects the default
cluster.

## Common response and paging behavior

Successful calls return the `rocketmq-mcp.v2` envelope with `schema_version`, `request_id`,
`cluster`, `observed_at`, `freshness_ms`, `cache_status`, `partial`, `warnings`, and typed
`data`. `cache_status` is `hit`, `miss`, or `bypass`. `source_failures` is serialized only when it
is nonempty: entries are sanitized, sorted and deduplicated, capped at 16, make `partial=true`,
and add `source_failures_present` to `warnings`. If the cap is exceeded, `warnings` also includes
`source_failures_truncated` and the response remains partial. `warnings` can also include
`output_rows_truncated`. The controlled Query/Admin projections omit addresses, credentials,
tokens, message bodies, and other configured sensitive values.

`warnings` includes `stale_cache_value` when the source was unavailable or did not answer in time
and the server answered with the result it had kept: `cache_status` is `hit`, and `observed_at`
and `freshness_ms` tell how old the result is. This happens only on a server whose operator set
`cache.stale_if_error_ms`; by default such a request fails with `source_unavailable` or
`backend_timeout`.

`rocketmq_list_clusters` is not bound to a cluster and answers in the same envelope without
`cluster` and `source_failures`.

The paginated Tools use `limit` and `cursor`. `limit` is an optional unsigned integer from 1 through
200 and defaults to 50. `cursor` follows the common 256-byte opaque-cursor rule. Paged `data`
contains `items`, `count`, `total_count`, `has_more`, and `next_cursor`. More than 1,000 output
rows are truncated, with `partial=true` and `output_rows_truncated`. A serialized response over
1 MiB instead fails with `OutputTooLarge` (`output_too_large`); it is not returned as a truncated
partial response.

### List formats

The list Tools `rocketmq_list_topics`, `rocketmq_list_consumer_groups`, `rocketmq_list_brokers`,
`rocketmq_list_consumer_connections`, and `rocketmq_list_producer_connections` take the optional
argument `response_format`:

- `detailed`, the default, returns every field of each item.
- `concise` returns only the fields that name an item. Use it to find a name, then ask for the one
  item with the Tool that describes it.

| Tool | A `concise` item has |
| --- | --- |
| `rocketmq_list_topics` | `topic` |
| `rocketmq_list_consumer_groups` | `group` |
| `rocketmq_list_brokers` | `broker_name`, `broker_id`, `broker_active` |
| `rocketmq_list_consumer_connections`, `rocketmq_list_producer_connections` | `broker_name`, `client_alias` |

Everything outside the items is the same in both formats, including the page metadata, `partial`,
and `source_failures`. The output schema of a list Tool therefore requires only the naming fields
of an item, and a client that validates results accepts both formats. A cursor is not bound to the
format: a page read as `concise` can be continued as `detailed`, and the other way round.

For `rocketmq_list_consumer_groups`, `concise` also skips the Broker reads that fill in the
details of each group, so it is faster and cannot be partial because of them. For the other list
Tools the format changes only what is returned. Any other value is an invalid argument, and a Tool
that is not a list Tool does not accept the argument.

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
| `not_found` | no | The request was valid, but the selected entity does not exist. The additional `entity` field names its kind: `topic`, `consumer_group`, `broker`, `message`, `proxy`, `controller`, `client`, or `cluster`. |
| `source_unavailable` | yes | A RocketMQ source could not be reached or answered with a failure. Identical requests get the same answer for up to one second without asking the source again. |
| `backend_timeout` | yes | The request used up `server.request_timeout_ms` (25 seconds by default), the time it may spend on RocketMQ sources over every query the Tool runs. |
| `rate_limited` | yes | The principal used up its per-minute budget for this Tool on this cluster. The additional `retry_after_ms` field is the wait after which one call is due. |
| `concurrency_limited` | yes | The cluster already serves as many requests as `security.max_concurrent_requests_per_cluster` allows at once. Retry shortly; no wait is named, because a permit is free as soon as a request in flight ends. |
| `cancelled` | no | The client cancelled the request. |
| `unauthorized_scope` | no | The principal lacks the scope the Tool's risk level requires. |
| `permission_denied` | no | No role of the principal grants the Tool, or an argument needs the permission for another Tool; `violations` then names that argument. |
| `cluster_not_allowed` | no | The cluster is not configured or not in the caller's allow-list. |
| `tenant_mismatch` | no | The principal's tenant does not match the cluster's tenant binding. |
| `change_planning_disabled` | no | Planning is not enabled by server policy. |
| `output_too_large` | no | The serialized response would exceed 1 MiB. |
| `cursor_expired` | no | The result that `cursor` continued is no longer kept. Request the first page again without `cursor`. |
| `cursor_invalid` | no | `cursor` was altered, or the arguments or `limit` differ from the request that returned it. |
| `result_too_large` | no | The result exceeds the size the server keeps for paging. Narrow the request, for example with `filter`. |
| `identifier_input_bound_exceeded` | no | An identifier is longer than the 1,024 bytes a pseudonym may cover. |
| `internal_error` | no | The server failed internally; report the `request_id`. |

The mutation control server (`rocketmq-mcp-control`) has its own error codes and spells the argument
error `invalid_argument`. A client of both servers accepts both spellings. One spelling for both is
left to a later contract version, because renaming a code breaks every client that matches on it.

A budget holds one minute of calls (`security.rate_limit_per_minute`, 60 by default) and refills
evenly, so a caller that used it up gets one call back every `60 / rate_limit_per_minute` seconds
instead of all of them a minute later. `retry_after_ms` is that wait, rounded up to a whole
millisecond:

```json
{"code":"rate_limited","retryable":true,"retry_after_ms":830}
```

Budgets are kept per principal, cluster, and operation. Over HTTP a principal also has one budget for
all its requests, `security.http_rate_limit_per_minute`, which defaults to ten times the budget of a
single operation. A request beyond it is refused before it reaches a Tool, with HTTP 429 and a
`Retry-After` header in whole seconds.

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
the Tool named in `suggestions` instead. A diagnosis Tool does not fail when its Topic, Consumer
Group or Broker is missing: the report lists the lookup as `missing` evidence with `error_code` set
to `not_found`, names the missing target as the root cause, and recommends the listing Tool. A
Resource read reports a missing target as the same non-retryable
`resource_unavailable` error that it returns for a Resource the caller may not read, so the two
cases cannot be told apart.

## Argument completion

The server declares the `completions` capability and answers `completion/complete` for Prompt
arguments (`ref/prompt`) and Resource template variables (`ref/resource`, whose `uri` is one of the
published templates, not a concrete URI).

| Names | Prompt argument | Template variable | Permission needed |
| --- | --- | --- | --- |
| Cluster | `cluster` | `cluster` | `rocketmq_list_clusters` |
| Topic | `topic` | `topic` | `rocketmq_list_topics` |
| Consumer Group | `consumer_group` | `group` | `rocketmq_list_consumer_groups` |
| Broker | `broker_name` | `broker` | `rocketmq_list_brokers` |

`check_level`, `message_id`, `filter`, `limit`, and `cursor` take free values and have no
candidates. An unknown Prompt or template, or an argument it does not declare, has none either.

`completion.values` holds the names that start with `argument.value`, ignoring ASCII case, in sorted
order and at most 100 of them. `completion.total` is the number of matching names and
`completion.hasMore` is `true` when it exceeds 100. Names an argument would reject, or that the
output sanitizer would rewrite, are not offered.

A cluster is completed from the server configuration, limited to the clusters the caller may list.
A Topic, Consumer Group, or Broker is completed inside the logical cluster in
`context.arguments.cluster`, with the same cluster, tenant, role, and scope checks as the listing
Tool. Without that argument the result is empty: the default cluster is never assumed. Topic and
Consumer Group names come from the inventory snapshot the listing Tools page through, and Broker
names from the `rocketmq_list_brokers` read, so repeated completions reuse one observation. An
inventory of more than 10,000 names has candidates only once a value is typed.

A completion never fails. The result is empty when the argument has no candidates, when the caller
may not list the names, when the request is rate limited, and when the names cannot be read. Each
admitted or refused request is audited as `completion:cluster`, `completion:topic`,
`completion:consumer_group`, or `completion:broker`; the typed value is recorded as a hash only.
Completions have a per-principal, per-cluster rate limit of their own, separate from Tool calls, and
a cluster-bound completion holds one of the cluster's concurrency permits while it runs.

```json
{"ref":{"type":"ref/resource","uri":"rocketmq://clusters/{cluster}/topics/{topic}"},"argument":{"name":"topic","value":"ord"},"context":{"arguments":{"cluster":"production-a"}}}
```

```json
{"completion":{"values":["orders","orders-retry"],"total":2,"hasMore":false}}
```

## Default read-only catalog (36 Tools)

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
| `response_format` | string | no | `detailed` (default) or `concise`; see [List formats](#list-formats). |

**Output.** Paged `data.items` contains Topic entries with their visible cluster and optional
consumer-group context. With `response_format = "concise"` an entry is its `topic` alone.

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
| `response_format` | string | no | `detailed` (default) or `concise`; see [List formats](#list-formats). |

**Output.** Paged `data.items` contains the visible Consumer Group records and the standard page
metadata. With `response_format = "concise"` a record is its `group` alone, and the server reads
nothing about the groups beyond their names.

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

**Purpose and access.** Explains the backlog of one Consumer Group on one Topic. It reads the lag of
every queue, the connections and configuration of the group, the route and permissions of the Topic, the
Broker that holds the most lag, and the retry and dead-letter Topics of the group, then grades the backlog
and names its likely causes. It is read-only and **diagnose** risk; callers cannot supply a time range or
thresholds.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `consumer_group` | string | yes | Exact Consumer Group name. |

**Output.** `data` is the versioned diagnosis report: `severity`, `summary`, `root_causes`,
`recommendations`, `risks`, `follow_up_metrics`, and the evidence the conclusions rest on.
`rules_version` is `rocketmq-mcp.rules.consumer-lag.v3` and `evidence_version` is
`rocketmq-mcp.evidence.consumer-lag.v3`.

All evidence is read in one admin session inside the request budget. The report is evaluated on every
queue and route row, not on a page of them, and it carries no cursor.

**Evidence.** The report holds each piece of evidence once, in `evidence_snapshot.items[]`. Every other
part names an item by its `id`: `evidence_refs` lists the items that were collected, `missing_evidence`
the ones that were not, and `root_causes[].evidence_refs` the ones a conclusion rests on.

| `id` | `kind` | `payload.data` |
| --- | --- | --- |
| `consumer_lag` | `consumer_lag` | `total_lag`, `consume_tps`, `inflight_total`, `consume_delay_ms`, `estimated_produce_tps`, `queue_count`, `max_queue_lag`, `brokers` (per Broker: `broker_name`, `queue_count`, `lag`, `max_queue_lag`; largest lag first), and `worst_queues` (the 10 most lagging queues, largest first). |
| `topic_route` | `topic_route` | `brokers`, `read_queue_count`, `write_queue_count`, and `queues` (one route row per Broker that hosts the Topic, with the permission in effect). |
| `broker_summary` | `broker_summary` | The `rocketmq_describe_broker` output for the Broker that holds the most lag, summed over its queues. |
| `consumer_group` | `consumer_group` | `configured_broker_count`, `online_consumer_count`, `online_broker_count`, `offline_broker_count` with `offline_brokers`, `consume_disabled_broker_count` with `consume_disabled_brokers`, `connections_complete`, `consume_types`, and `message_models`. |
| `topic_config` | `topic_config` | `broker_count`, `read_denied_broker_count` with `read_denied_brokers`, and `inconsistent_fields`. |
| `retry_topic_stats` | `topic_stats` | For `%RETRY%<group>`: `topic`, `exists`, `queue_count`, `message_count`, `last_message_at`, and `last_message_age_ms`. |
| `dead_letter_topic_stats` | `topic_stats` | The same for `%DLQ%<group>`. |

A list of Broker names holds at most 10 names; the count next to it covers every Broker. To see more
queues than `worst_queues` lists, page `rocketmq_get_consumer_lag`.

`consume_delay_ms` is how far behind production the group is. A queue reports the store time of the last
message the group consumed from it, so on a queue with lag everything stored since then is waiting. The
delay of the report is the delay that at least half of the backlog has, which keeps one quiet queue from
making a busy Topic look hours behind. It bounds how long the oldest waiting message has waited; it is
`null` when the queues that hold most of the backlog report no consumed message.
`estimated_produce_tps` is the rate at which the backlog built up over that time.

A read that fails never fails the diagnosis. The item is kept with the status `missing`, `unavailable`,
`timeout`, `unauthorized`, or `invalid` and no payload, it is listed in `missing_evidence`, `partial` is
`true`, and the rules that need it stay silent. Consumer lag and the Topic route may use the whole request
budget, because nothing can be concluded without them. The other reads share a budget that ends a tenth of
the request budget earlier: when it is spent, the remaining items are recorded as `timeout` and the report
is written from what was collected. A retry or dead-letter Topic that does not exist is `present`
evidence with `exists: false`, because nothing was ever written to it.

**Severity.** A backlog is graded by its size and by its consume delay.

| `severity` | Condition |
| --- | --- |
| `unknown` | Consumer lag could not be read, or the group reports no consume progress on the Topic. |
| `healthy` | `total_lag` is at or below the lag threshold. |
| `low` | `total_lag` is above the lag threshold, and the consume delay is below the delay threshold. At the rate the messages arrive, the backlog is less than the delay threshold of traffic, however many messages it holds. This grade is not given while the evidence shows that the group cannot consume at all. |
| `medium`, `high`, `critical` | Otherwise the higher of two grades: `total_lag` against the lag threshold, and the consume delay against the delay threshold. Each is `medium`, `high` from 10 times its threshold, and `critical` from 20 times. Without a measured delay the size decides alone. |

The same number of waiting messages is therefore `low` on a Topic that receives them within the delay
threshold and `critical` on a Topic that took days to produce them.

**Rules.** Each root cause carries the `rule_id` of the rule that drew it. Identifiers are stable; the
wording of `cause` and `reasoning` may change with `rules_version`. Root causes are ordered by confidence.

| `rule_id` | Fires when | Evidence |
| --- | --- | --- |
| `no_online_consumer` | Every Broker that holds the configuration of the group reports no connected consumer. | `consumer_group` |
| `consumption_disabled` | `consume_enable` is `false` for the group on at least one Broker. | `consumer_group` |
| `read_permission_missing` | A Broker that hosts the Topic denies reads. With the Topic configuration at hand, the reasoning says whether the Topic permission or the Broker permission denies them. | `topic_route`, `topic_config` |
| `consumer_missing_on_broker` | Consumers are online, but a Broker that holds lag reports none of them. | `consumer_lag`, `consumer_group` |
| `consume_throughput_low` | The group can consume, and `consume_tps` is below 1. | `consumer_lag` |
| `lag_concentrated_on_broker` | One Broker holds at least 70% of the backlog and no single queue does. | `consumer_lag` |
| `lag_concentrated_on_queue` | One queue holds at least 70% of the backlog. | `consumer_lag` |
| `consume_delay` | The consume delay is at or above the delay threshold. | `consumer_lag` |
| `pop_inflight_high` | The group consumes in POP mode and `inflight_total` is at or above the in-flight threshold. | `consumer_lag`, `consumer_group` |
| `retry_queue_growing` | The retry Topic holds messages and received one within the activity window. | `retry_topic_stats` |
| `dead_letter_queue_growing` | The dead-letter Topic holds messages and received one within the activity window. | `dead_letter_topic_stats` |

The first three rules are evaluated with or without a backlog, because they describe a group that cannot
consume. While the backlog is within the lag threshold they are reported under `risks` instead of
`root_causes`. The other rules explain a backlog and are evaluated only above the lag threshold.

Four `rule_id` values are conclusions that no rule drew: `backlog_within_threshold`,
`cause_not_isolated` (a backlog, and no rule fired), `no_queue_rows`, and `target_not_found` (the Topic
or the Consumer Group does not exist).

**Thresholds.** The server owns the thresholds; see `[diagnosis]` in the configuration. `data.target`
states the values a report was evaluated against, after overrides: `lag_threshold`,
`consume_delay_threshold_ms`, `pop_inflight_threshold`, and `retry_activity_window_ms`.

```json
{"cluster":"production-a","topic":"orders","consumer_group":"orders-consumer"}
```

```json
{"cluster":"production-a","data":{"severity":"high","summary":"Consumer group orders-consumer on topic orders has 12000 total lag across 8 queues; severity is High.","root_causes":[{"rule_id":"no_online_consumer","cause":"No consumer of the group is online","confidence":0.9,"evidence_refs":["consumer_group"],"reasoning":"All 2 Brokers that hold the configuration of the group report no connected consumer."}],"partial":false,"missing_evidence":[]}}
```

### `rocketmq_diagnose_consumer_group`

**Purpose and access.** Explains the state of one Consumer Group over every Topic it consumes: whether
consumers are online, whether consumption is enabled, how large and how old the backlog is, which Topic,
Broker or queue holds it, and whether messages are retried or dead-lettered. Use it when a group misbehaves
and the Topic is not known yet; `rocketmq_diagnose_consumer_lag` adds the route and the permissions of one
Topic. It is read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |

**Output.** `data` is a diagnosis report with the shape, the evidence handling and the failure handling
described for `rocketmq_diagnose_consumer_lag`. `report_type` is `consumer_group`, `rules_version` is
`rocketmq-mcp.rules.consumer-group.v1` and `evidence_version` is `rocketmq-mcp.evidence.consumer-group.v1`.
The result carries no Resource link.

| `id` | `kind` | `payload.data` |
| --- | --- | --- |
| `consumer_group` | `consumer_group` | The same summary as in the lag diagnosis. |
| `consumer_lag` | `consumer_progress` | The backlog over every Topic: `state`, `topic_count`, `total_lag`, `consume_tps`, `inflight_total`, `consume_delay_ms`, `estimated_produce_tps`, `queue_count`, `max_queue_lag`, `brokers`, `worst_queues`, `topics` (the 10 Topics with the most lag: `topic`, `queue_count`, `lag`, `max_queue_lag`), and `truncated`. |
| `retry_topic_stats` | `topic_stats` | As in the lag diagnosis. |
| `dead_letter_topic_stats` | `topic_stats` | As in the lag diagnosis. |

The group details and the progress may use the whole request budget; the retry and dead-letter Topics share
the supporting budget. When `truncated` is `true`, the group has more queues than the 10,000 rows that were
read: the totals cover every queue, and the figures per Topic, per Broker and per queue cover the rows that
were read.

**Severity and rules.** The backlog of the group over all its Topics is graded like the backlog of one
Topic, against the same thresholds. An override that names only the group applies; one that names a Topic
does not. The rules of the lag diagnosis are evaluated on the group and on its backlog. `read_permission_missing`
needs the route of a Topic and stays silent. One rule is added:

| `rule_id` | Fires when | Evidence |
| --- | --- | --- |
| `lag_concentrated_on_topic` | The group consumes at least two Topics and one of them holds at least 70% of the backlog. The recommendation names the Topic to pass to `rocketmq_diagnose_consumer_lag`. | `consumer_lag` |

A Consumer Group that does not exist is reported as `target_not_found`, and its progress is not asked for.

```json
{"cluster":"production-a","consumer_group":"orders-consumer"}
```

```json
{"cluster":"production-a","data":{"report_type":"consumer_group","severity":"high","root_causes":[{"rule_id":"lag_concentrated_on_topic","cause":"Lag is concentrated on topic orders","confidence":0.66,"evidence_refs":["consumer_lag"],"reasoning":"orders holds lag=11000 on 8 queues, 92% of total_lag=12000 across 3 topics."}],"partial":false,"missing_evidence":[]}}
```

### `rocketmq_diagnose_broker_health`

**Purpose and access.** Checks one Broker, which is a master and its replicas: whether every instance
answers and is ready, whether the store accepts writes and keeps up, whether the replicas are in sync, and
whether the instances are configured alike. `rocketmq_get_broker_diagnostics` returns the figures the
conclusions rest on. It is read-only and **diagnose** risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Logical Broker name. It stands for the master and its replicas. |

**Output.** `data` is a diagnosis report with the shape, the evidence handling and the failure handling
described for `rocketmq_diagnose_consumer_lag`. `report_type` is `broker_health`, `rules_version` is
`rocketmq-mcp.rules.broker-health.v1` and `evidence_version` is `rocketmq-mcp.evidence.broker-health.v1`.
The result carries no Resource link.

| `id` | `kind` | `payload.data` |
| --- | --- | --- |
| `broker_summary` | `broker_summary` | The `rocketmq_describe_broker` output: one row per instance of the Broker. |
| `broker_diagnostics` | `broker_diagnostics` | The `rocketmq_get_broker_diagnostics` output for the Broker. |
| `ha_status` | `ha_status` | The `rocketmq_get_ha_status` output for the master of the Broker, without the Controller sync state. |
| `broker_config` | `broker_config` | The `rocketmq_get_broker_config_summary` output for the Broker. |

The instance rows are read first and may use the whole request budget; the other three reads share the
supporting budget. A Broker that does not exist is reported as `target_not_found`, and nothing else is asked
about it.

**Severity.** This is a health report: every finding has its own severity, and the report is as severe as
its worst finding. It is `healthy` without a finding and `unknown` when the instance rows could not be read.
`impacts[]` lists each finding with its severity and the area it affects, and `root_causes[]` lists the
findings worst first.

| `rule_id` | Severity | Fires when | Evidence |
| --- | --- | --- | --- |
| `master_missing` | `critical` | No instance of the Broker is registered with id 0. | `broker_summary` |
| `broker_instance_inactive` | `critical` for the master, `medium` for a replica | An instance is registered and did not answer. | `broker_summary` |
| `broker_not_ready` | `critical` for the master, `high` for a replica | An instance reports that it is not ready, not active, or shutting down. | `broker_diagnostics` |
| `store_not_writeable` | `critical` | A store is not writeable, failed its last flush, or is shut down. | `broker_diagnostics` |
| `store_under_pressure` | `medium` | A store reports a busy page cache, or a transient store pool that ran out of buffers. | `broker_diagnostics` |
| `dispatch_behind` | `medium` | At least 256 MiB of commit log are not dispatched to the consume queues yet. | `broker_diagnostics` |
| `sync_replication_stalled` | `high` | A write has waited at least 5 seconds for a replica. | `broker_diagnostics`, `ha_status` |
| `no_in_sync_replica` | `high` | The Broker has replicas, and none of them is in sync with the master. | `ha_status`, `broker_summary` |
| `replica_not_in_sync` | `medium` | A replica is in sync and another one is not. | `ha_status` |
| `recovery_failed` | `high` | The last store recovery of an instance reports failed phases. | `broker_diagnostics` |
| `index_rebuild_failed` | `medium` | The background index rebuild of an instance failed. | `broker_diagnostics` |
| `config_inconsistent` | `low` | The instances differ in `send_message_thread_pool_nums`, `pull_message_thread_pool_nums`, `flush_delay_offset_interval_ms`, or `max_client_event_count`. | `broker_config` |

The two limits are part of `rules_version` and cannot be configured.

```json
{"cluster":"production-a","broker_name":"broker-a"}
```

```json
{"cluster":"production-a","data":{"report_type":"broker_health","severity":"high","summary":"Broker broker-a is High: No replica is in sync with the master.","root_causes":[{"rule_id":"no_in_sync_replica","cause":"No replica is in sync with the master","confidence":0.9,"evidence_refs":["ha_status","broker_summary"],"reasoning":"Replicas of broker-a: 1 known, 1 connected to the master, 0 in sync."}],"partial":false,"missing_evidence":[]}}
```

### `rocketmq_get_cluster_health`

**Purpose and access.** Checks a cluster at a glance and names the Brokers to look at first: which
instances are registered, which do not answer, which Brokers lack a master or a replica, whether the
replicas keep up, and whether the Brokers run one version. It reads the instance rows and the replication
state of every master. It does not look inside a Broker, so an instance that answers can still be
unhealthy: run `rocketmq_diagnose_broker_health` for a Broker it names. It is read-only and **diagnose**
risk.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |

**Output.** `data` is a health report as described for `rocketmq_diagnose_broker_health`. `report_type` is
`cluster_health`, `rules_version` is `rocketmq-mcp.rules.cluster-health.v1` and `evidence_version` is
`rocketmq-mcp.evidence.cluster-health.v1`. The result carries no Resource link.

| `id` | `kind` | `payload.data` |
| --- | --- | --- |
| `broker_inventory` | `broker_inventory` | `broker_count`, `instance_count`, `master_count`, `replica_count`, `inactive_instance_count`, `inactive_master_count` with `inactive_brokers`, `brokers_without_master_count` with `brokers_without_master`, `masters_without_replica_count` with `masters_without_replica`, and `versions` (per version: `version`, `instance_count`; most instances first). |
| `ha_status` | `ha_status` | `master_count`, `masters_without_in_sync_replica_count` with `masters_without_in_sync_replica`, `masters_with_lagging_replica_count` with `masters_with_lagging_replica`, and `max_replica_lag_bytes`. |

A list of Broker names holds at most 10 names; the count next to it covers every Broker. The instance rows
may use the whole request budget, and without them the health is `unknown`. An instance that did not answer
the row read is also listed in `source_failures`.

| `rule_id` | Severity | Fires when | Evidence |
| --- | --- | --- | --- |
| `no_broker_registered` | `critical` | The cluster has no Broker instance registered. | `broker_inventory` |
| `master_missing` | `critical` | A Broker has no instance registered with id 0. | `broker_inventory` |
| `broker_instance_inactive` | `critical` when a master is among them, `medium` otherwise | An instance is registered and did not answer. | `broker_inventory` |
| `no_in_sync_replica` | `high` | A master has replicas, and none of them is in sync. | `ha_status` |
| `replica_not_in_sync` | `medium` | A master has a replica in sync and another one that is not. | `ha_status` |
| `master_without_replica` | `low` | A master has no replica registered. | `broker_inventory` |
| `version_skew` | `low` | The instances run more than one Broker version. | `broker_inventory` |

```json
{"cluster":"production-a"}
```

```json
{"cluster":"production-a","data":{"report_type":"cluster_health","severity":"medium","summary":"Cluster production-a is Medium: A replica does not answer.","root_causes":[{"rule_id":"broker_instance_inactive","cause":"A replica does not answer","confidence":0.9,"evidence_refs":["broker_inventory"],"reasoning":"1 of 4 instances are registered but did not answer, 0 of them masters. Brokers: broker-b."}],"partial":false,"missing_evidence":[]}}
```

### `rocketmq_diagnose_message_delivery`

**Purpose and access.** Checks the path from one Topic to one Consumer Group, in the order a message
travels: whether the Brokers accept writes and serve reads for the Topic, whether consumers of the group are
online and allowed to consume, whether the group has progress on the Topic and keeps up, and whether the
Topic holds messages. With `message_id` it also says where that message stands for the group. Use it first
when a message does not arrive. It is read-only and **diagnose** risk, and it never reads or returns a
message body.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `consumer_group` | string | yes | Exact Consumer Group name. |
| `message_id` | string or null | no | Exact message identifier, as for `rocketmq_get_message_metadata`. Omit it to check the path only. |

**Access to a message.** A call with `message_id` looks one message up, which is what
`rocketmq_get_message_metadata` does. The caller therefore needs the permission for
`rocketmq_get_message_metadata` on the cluster as well. Without it the call fails with `permission_denied`
before any RocketMQ source is read, and `violations` names `/message_id`. A call without `message_id` needs
the permission for this Tool only. The identifier is never recorded: it is redacted before the arguments
are hashed for the audit trail, and the report names the message by its pseudonyms.

**Output.** `data` is a health report as described for `rocketmq_diagnose_broker_health`. `report_type` is
`message_delivery`, `rules_version` is `rocketmq-mcp.rules.message-delivery.v1` and `evidence_version` is
`rocketmq-mcp.evidence.message-delivery.v1`. `data.target` holds `topic`, `consumer_group`, `message_alias`
(the pseudonym of the identifier that was sent, or `null`), and the thresholds the report was evaluated
against: `lag_threshold` and `consume_delay_threshold_ms`. The result carries no Resource link.

| `id` | `kind` | `payload.data` |
| --- | --- | --- |
| `topic_route` | `topic_route` | As in the lag diagnosis. |
| `consumer_group` | `consumer_group` | As in the lag diagnosis. |
| `consumer_lag` | `consumer_lag` | As in the lag diagnosis, for the Topic. |
| `topic_stats` | `topic_stats` | For the Topic: `topic`, `exists`, `queue_count`, `message_count`, `last_message_at`, and `last_message_age_ms`. |
| `message_metadata` | `message_metadata` | Only for a call with `message_id`: `message_alias`, `unique_message_alias`, `topic`, `queue_id`, `queue_offset`, `born_at`, `stored_at`, `stored_age_ms`, `reconsume_times`, and `position`. |

The Topic route may use the whole request budget. The other reads share the supporting budget, in this
order: the group, its lag on the Topic, the message, and the Topic statistics. What does not exist is not
asked about: a Topic that does not exist has no lag and no statistics, and a group that does not exist has
no lag.

`position` says where the message stands for the group: `consumed`, `waiting`, or `undetermined`. It is
`null` for a message that is stored on another Topic. A message names its queue by number and not by Broker,
and every Broker that hosts the Topic has a queue with that number. A queue that ends at or before the
offset of the message cannot hold it. The position is `consumed` or `waiting` when the queues that can hold
the message agree, and `undetermined` otherwise: the offsets of the group were not read from every Broker,
no queue with that number reaches the offset, or two Brokers disagree.

**Severity.** The report is as severe as its worst finding and `healthy` without one. It is `unknown` when
nothing was found and a half of the path was not observed: the Topic route is missing, or the group and its
lag are both missing. A Topic or a Consumer Group that does not exist leads the report as `target_not_found`
with the severity `unknown`; what the rules found on the rest of the path follows it.

| `rule_id` | Severity | Fires when | Evidence |
| --- | --- | --- | --- |
| `write_permission_missing` | `critical` when no Broker accepts writes, `high` otherwise | A Broker that hosts the Topic denies writes. | `topic_route` |
| `read_permission_missing` | `critical` when no Broker serves reads, `high` otherwise | A Broker that hosts the Topic denies reads. | `topic_route` |
| `no_online_consumer` | `critical` | Every Broker that holds the configuration of the group reports no connected consumer. | `consumer_group` |
| `consumption_disabled` | `critical` when it is disabled on every Broker, `high` otherwise | `consume_enable` is `false` for the group on at least one Broker. | `consumer_group` |
| `group_without_progress` | `high` | No Broker holds an offset of the group for the Topic: the group is not subscribed to it, or has not consumed from it yet. | `consumer_lag` |
| `consumer_backlog` | The grade of the lag diagnosis, `low` to `critical` | `total_lag` is above the lag threshold. `rocketmq_diagnose_consumer_lag` explains the backlog. | `consumer_lag` |
| `topic_has_no_messages` | `low` | The Topic holds no message. | `topic_stats` |
| `message_not_found` | `medium` | No Broker holds a message with the identifier. | `message_metadata` |
| `message_dead_lettered` | `high` | The message is stored on `%DLQ%<group>`: the group gave up on it. | `message_metadata` |
| `message_waiting_for_retry` | `medium` | The message is stored on `%RETRY%<group>`: a consumer of the group failed to consume it. | `message_metadata` |
| `message_on_other_topic` | `low` | The message is stored on another Topic. | `message_metadata` |
| `message_waiting` | `medium` when the message was stored at least the delay threshold ago, `low` otherwise | `position` is `waiting`. | `message_metadata`, `consumer_lag` |
| `message_delivered` | `healthy` | `position` is `consumed`: the message was delivered to a consumer of the group, or the subscription of the group filtered it out. | `message_metadata`, `consumer_lag` |
| `message_position_undetermined` | `unknown` | `position` is `undetermined`. | `message_metadata`, `consumer_lag` |

A finding that is `healthy` or `unknown` is a conclusion and not a problem: it is listed in `root_causes[]`
under its `rule_id`, and not in `impacts[]`.

**Thresholds.** The lag threshold and the delay threshold are those of `[diagnosis]`, after the overrides
for the Topic and the Consumer Group.

```json
{"cluster":"production-a","topic":"orders","consumer_group":"orders-consumer","message_id":"message-opaque"}
```

```json
{"cluster":"production-a","data":{"report_type":"message_delivery","severity":"low","summary":"Delivery from topic orders to group orders-consumer is Low: The group has not consumed the message yet.","root_causes":[{"rule_id":"message_waiting","cause":"The group has not consumed the message yet","confidence":0.9,"evidence_refs":["message_metadata","consumer_lag"],"reasoning":"The message is at offset 42 of queue 0, and the offset of the group on that queue has not passed it. It was stored 45 s ago."}],"partial":false,"missing_evidence":[]}}
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
| `response_format` | string | no | `detailed` (default) or `concise`; see [List formats](#list-formats). |

**Output.** Paged `data.items` has logical Broker names, pseudonymous `client_alias`, language,
version, and optional update time; client addresses are not exposed. With
`response_format = "concise"` an item is its `broker_name` and `client_alias`.

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
| `response_format` | string | no | `detailed` (default) or `concise`; see [List formats](#list-formats). |

**Output.** Paged `data.items` has pseudonymous connection observations and page metadata. With
`response_format = "concise"` an item is its `broker_name` and `client_alias`.

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

### `rocketmq_list_clusters`

**Purpose and access.** Lists the logical clusters the caller may query. It is read-only and is the
one Tool that is not bound to a cluster: it reads the server configuration, never a RocketMQ source,
and its `openWorldHint` is `false`. A call needs the `rocketmq:read` scope and a role that grants
the Tool. Each configured cluster is then checked the way a call that names it would be, against
the role cluster rule, the principal `rocketmq_clusters` claim when present, and the tenant binding,
and a cluster that fails a check is left out. An empty list is a successful answer.

The Tool takes no argument. Any property, including `cluster`, is rejected as `invalid_arguments`
with the violation `{"path": "/*", "constraint": "additional_property"}`. The call counts against
the caller's rate limit without a cluster and takes no per-cluster concurrency permit.

**Output.** `data.clusters` lists the clusters in configuration order. Each entry has `name`, the
value to pass as `cluster`; `default`, whether a Tool that permits `cluster` omission selects it;
`proxy_names`, the aliases `rocketmq_get_proxy_drain_state` accepts as `proxy_name`; and
`controller_names`, the aliases `rocketmq_get_ha_status` and `rocketmq_get_controller_metadata`
accept in `controller_names`. NameServer addresses, Proxy and Controller endpoints, the physical
cluster name, and tenant bindings are never returned. The envelope has no `cluster` and no
`source_failures`.

```json
{}
```

```json
{"data":{"clusters":[{"name":"production-a","default":true,"proxy_names":["proxy-a"],"controller_names":["controller-a"]}]}}
```

### `rocketmq_list_brokers`

**Purpose and access.** Lists the Broker instances of one configured cluster. It is read-only. It
reads the Broker rows only, so unlike `rocketmq_get_cluster_overview` it does not size the Topic and
Consumer Group inventories.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `response_format` | string | no | `detailed` (default) or `concise`; see [List formats](#list-formats). |

**Output.** `data` contains the logical cluster, `brokers`, and `generated_at`. `brokers` has one
row for each Broker instance, masters and slaves alike, with the same fields as the overview's
`brokers`: `broker_name`, `broker_id`, `version`, TPS, and `broker_active`. With
`response_format = "concise"` a row is its `broker_name`, `broker_id`, and `broker_active`. A Broker
that did not answer makes the response partial through `source_failures`. The
`rocketmq://clusters/{cluster}/brokers` Resource publishes the same rows from the same read; it
stays authorized as `rocketmq_get_cluster_overview`, so the Tool result carries no link to it.

```json
{"cluster":"production-a"}
```

```json
{"cluster":"production-a","data":{"cluster":"production-a","brokers":[{"broker_name":"broker-a","broker_id":0,"broker_active":true}]}}
```

### `rocketmq_list_topic_consumers`

**Purpose and access.** Lists the Consumer Groups that consume one Topic. It is read-only. A Broker
answers with the groups subscribed to the Topic now and the groups that hold a consume offset on it,
and it knows only the groups it has seen. The Tool therefore asks every master Broker that hosts the
Topic in the selected cluster and joins the answers.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `limit` | integer or null | no | Page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

**Output.** `data` contains the logical cluster, the `topic`, `truncated`, a page of `items` with the
normal page metadata, and `generated_at`. An item is `{"group": "<name>"}`; the items are in ascending
order and without duplicates. Pass a name as `consumer_group` to `rocketmq_get_consumer_lag` or
`rocketmq_get_consumer_group_details`. Every page of one cursor chain comes from one read. A Broker
that did not answer makes the response partial through `source_failures` with the source
`subscription_groups`, and the groups that only this Broker knows are then missing. A Topic with more
than 10,000 Consumer Groups sets `truncated` and adds the warning `topic_subscribers_truncated`. A
Topic that nobody consumes returns an empty page, and a Topic without a route in the cluster is
`not_found`. The Tool takes no `response_format`, because its items are names already, and it has no
Resource.

```json
{"cluster":"production-a","topic":"orders","limit":50}
```

```json
{"cluster":"production-a","data":{"cluster":"production-a","topic":"orders","truncated":false,"items":[{"group":"billing"}],"count":1,"total_count":1,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_consumer_running_info`

**Purpose and access.** Asks one connected client of a Consumer Group what it reports about itself.
It is read-only and requires the `rocketmq:diagnose` scope. The Broker that the client is connected to
relays the request, so the client answers for itself and the answer is as current as the call. The
thread dump of the client is never requested.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |
| `client_alias` | string | yes | Pseudonym of the client: the `client_alias` of an item of `rocketmq_list_consumer_connections`. It matches `^client-[0-9a-f]{32}$`. |
| `limit` | integer or null | no | Queue page limit, 1--200; defaults to 50. |
| `cursor` | string or null | no | Opaque continuation. |

The client is named by its pseudonym only. The server matches the pseudonym against the connections
of the group in the session that then asks the client, so a client identifier or address is neither
accepted nor returned. A pseudonym that no connection has is `not_found` with `entity = "client"`
when every Broker answered for the group; when a Broker did not answer, or the group has more
connections than one read keeps, the call fails with `source_unavailable` instead, because the client
may still be connected. Without `security.pseudonym_key_env` a pseudonym is valid only in the server
process that issued it and until that process restarts.

**Output.** `data` contains the logical cluster, `consumer_group`, `client_alias`, the `broker_name`
that relayed the request, and what the client reported:

- `consume_type`, `consume_orderly`, `thread_pool_core_size`, `client_version`, and `started_at`. These
  are read from fixed members of the client's property table. No other property is returned, so the
  NameServer address the client holds and anything an application added stay out. A member the client
  did not report is null.
- `subscriptions`: the Topic, the `expression_type` (`TAG` or `SQL92`), the filter `expression` and
  the `sub_version` of each subscription the client itself holds, at most 256. An expression longer
  than 1,024 bytes is null. The clients of one group must hold the same subscriptions, so comparing
  two clients shows an inconsistent group.
- `topic_status`: pull and consume response times in milliseconds, rates, and the number of messages
  the listener failed, for each Topic, at most 256.
- `queue_count`, `truncated`, and a page of `items` with the normal page metadata. An item is one
  queue the client holds, in ascending order of Topic, Broker, queue and mode. Its `mode` is `pull`
  or `pop`. A `pull` item has `commit_offset`, `cached_message_count`, `cached_message_size_mib`,
  `locked`, `try_unlock_times`, `last_lock_at`, and `last_consume_at`; a `pop` item has
  `wait_ack_count`. Both have `dropped` and `last_pull_at`. A member the mode does not have is null,
  and so is a time the client never recorded.

Every page of one cursor chain comes from one answer of the client. `truncated` is true when the
client reported more than 256 subscriptions or Topics or more than 10,000 queues. A name in the
answer that no Broker would accept fails the call instead of being passed on. The Tool has no
Resource.

```json
{"cluster":"production-a","consumer_group":"order-service","client_alias":"client-0123456789abcdef0123456789abcdef","limit":50}
```

```json
{"cluster":"production-a","data":{"consumer_group":"order-service","client_alias":"client-0123456789abcdef0123456789abcdef","broker_name":"broker-a","consume_type":"CONSUME_PASSIVELY","subscriptions":[{"topic":"orders","expression_type":"TAG","expression":"*","sub_version":1700000000000}],"topic_status":[],"queue_count":1,"truncated":false,"items":[{"topic":"orders","broker_name":"broker-a","queue_id":0,"mode":"pull","dropped":false,"commit_offset":40,"cached_message_count":5,"locked":false}],"count":1,"total_count":1,"has_more":false,"next_cursor":null}}
```

### `rocketmq_get_dlq_summary`

**Purpose and access.** Returns how many dead letters one Consumer Group holds and when the newest
one was stored. It is read-only. A Broker moves a message to the dead-letter Topic of a group,
`%DLQ%` followed by the group name, when the group gave up retrying it. The Tool reads the statistics
of that Topic. It reads no message, so it returns counts and times only: no message identifier, key,
tag, property, or body.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `consumer_group` | string | yes | Exact Consumer Group name. |

**Output.** `data` contains the logical cluster, `consumer_group`, `dlq_topic`, `exists`,
`queue_count`, `message_count`, `last_message_at`, `last_message_age_ms`, `brokers`, `truncated`, and
`generated_at`.

- `exists` is false when the dead-letter Topic does not exist, which means that no message of the
  group was dead-lettered in this cluster. The counts are then zero and `brokers` is empty. This is a
  complete answer, not a failure.
- `message_count` is the number of dead letters the Topic holds now. A Broker deletes them when their
  retention time ends, so it is not the number that was ever dead-lettered.
- `last_message_at` is the store time of the newest dead letter and `last_message_age_ms` its age at
  the time of the observation. Both are null when the Topic holds none.
- `brokers` has `broker_name`, `queue_count`, `message_count`, and `last_message_at` for each Broker
  that holds a part of the Topic, in ascending order of Broker name.
- `truncated` is true when the Topic has more than 10,000 queues. The per-Broker rows then cover the
  queues that were read; the totals cover every queue.

A dead-letter Topic that is absent and a name that is no Consumer Group look the same to a Broker.
The Tool therefore looks the group up when the Topic is absent: an unknown group is `not_found` with
`entity = "consumer_group"` instead of a zero count. When that lookup itself fails, the response is
partial with a `consumer_group_config` source failure, and the zero count then holds only if the
name is a group. A Broker that did not answer for the statistics makes the response partial through
`source_failures` with the source `topic_stats`. The Tool has no Resource. To find out where one
message stands, use `rocketmq_diagnose_message_delivery`.

```json
{"cluster":"production-a","consumer_group":"order-service"}
```

```json
{"cluster":"production-a","data":{"consumer_group":"order-service","dlq_topic":"%DLQ%order-service","exists":true,"queue_count":1,"message_count":12,"last_message_at":"2026-10-01T00:00:05.000Z","last_message_age_ms":60000,"brokers":[{"broker_name":"broker-a","queue_count":1,"message_count":12,"last_message_at":"2026-10-01T00:00:05.000Z"}],"truncated":false}}
```

### `rocketmq_query_message_trace`

**Purpose and access.** Returns the trace records clients wrote about one message: when it was
published, and when each Consumer Group received it and finished it. It is read-only. Because it
looks a message up, the caller also needs the role permission for `rocketmq_get_message_metadata`;
without it the call fails with `permission_denied` and a violation at `/message_id`. The identifier
is replaced before the arguments are hashed for the audit record.

Trace records exist only where clients are configured to write them. Clients write them to a trace
Topic, and Brokers index them by the identifier the producing client gave the message. A record is
therefore a claim of a client and not a fact a Broker verified.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `message_id` | string | yes | Identifier the producing client gave the message, 1--255 bytes without whitespace. The offset message identifier does not find a trace. |
| `trace_topic` | string or null | no | Name of the trace Topic; defaults to `RMQ_SYS_TRACE_TOPIC`. |

**Output.** `data` contains the logical cluster, `unique_message_alias`, `message_alias`,
`trace_topic`, `trace_topic_found`, `redacted`, `records`, `truncated`, `queried_broker_count`,
`unanswered_brokers`, and `generated_at`.

- `unique_message_alias` is the pseudonym of the queried identifier and `message_alias` the
  pseudonym of the offset message identifier the records name. They are the pseudonyms
  `rocketmq_get_message_metadata` reports under the same names. The identifiers themselves are never
  returned.
- `trace_topic_found` is false when the trace Topic has no route in the cluster: no client traces
  messages there, or the clients use another trace Topic. `records` is then empty. This is a
  complete answer, not a failure.
- `records` holds at most 128 records in ascending order of time. A record has `step` (`publish`,
  `consume_start`, `consume_end`, `end_transaction`, or `recall`), `at`, `group`, `cost_time_ms`,
  `success`, `topic`, `retry_times`, `body_length`, `from_transaction_check`, and
  `store_broker_name`. `body_length` is a size; the body is never read. `store_broker_name` is the
  logical name of the Broker the record names as the store and is null when that is no Broker of
  the cluster.
- `client_host_alias`, `tags`, and `key_aliases` describe a business message and its sender, so they
  are returned by level. With the `rocketmq:diagnose` scope a record carries the pseudonym of the
  client host, the tags, and the pseudonyms of at most 4 keys. Without it they are null, null, and
  empty, and `redacted` is true. A host or a key is never returned as it was written. The two
  answers are cached apart.
- A text a client wrote into a record that is empty, longer than 255 bytes, or holds a control
  character is null.

A Broker that hosts the trace Topic and did not answer is named in `unanswered_brokers`; the response
is then partial with the warning `message_index_brokers_unanswered`, and the call fails with
`source_unavailable` only when no Broker answered. More than 128 records set `truncated` and the
warning `message_trace_records_truncated`. The Tool has no Resource.

```json
{"cluster":"production-a","message_id":"7F0000010000B4AAC21828A0B9B50000"}
```

```json
{"cluster":"production-a","data":{"unique_message_alias":"unique-message-0123456789abcdef0123456789abcdef","message_alias":"message-fedcba9876543210fedcba9876543210","trace_topic":"RMQ_SYS_TRACE_TOPIC","trace_topic_found":true,"redacted":true,"records":[{"step":"publish","at":"2026-10-01T00:00:05.000Z","group":"order-producers","cost_time_ms":12,"success":true,"topic":"orders","retry_times":0,"body_length":128,"from_transaction_check":false,"store_broker_name":"broker-a","client_host_alias":null,"tags":null,"key_aliases":[]}],"truncated":false,"queried_broker_count":2,"unanswered_brokers":[]}}
```

### `rocketmq_find_messages_by_key`

**Purpose and access.** Finds the messages of one Topic that carry an exact message key. It reads
only, and it requires the scope `rocketmq:message-search` instead of `rocketmq:read`. The caller also
needs the role permission for `rocketmq_get_message_metadata`; without it the call fails with
`permission_denied` and a violation at `/key`. A key is a business identifier, such as an order
number. It is replaced before the arguments are hashed for the audit record, and it is repeated
neither in the result nor in an error.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `topic` | string | yes | Exact Topic name. |
| `key` | string | yes | One whole key, 1--255 bytes without whitespace, matched exactly and case-sensitively. It is neither a prefix nor a pattern. |
| `end_at` | string or null | no | End of the searched store-time window as an RFC 3339 timestamp; defaults to now. |
| `window_minutes` | integer or null | no | Length of the window, 1--1440; defaults to 60. |

The search is bounded on purpose. The window ends at `end_at` and is at most 24 hours long, and a
call returns at most 32 messages. There is no paging: to reach more matches, search a shorter or an
earlier window. Every call reads the Brokers; nothing is answered from a cache.

**Output.** `data` contains the logical cluster, `topic`, `window_begin_at`, `window_end_at`,
`messages`, `truncated`, `queried_broker_count`, `unanswered_brokers`, and `generated_at`. `messages`
is ordered newest first. A message has the same fixed metadata `rocketmq_get_message_metadata`
returns, by pseudonyms only: `message_alias`, `unique_message_alias`, `born_at`, `stored_at`,
`queue_id`, `queue_offset`, `store_size`, `reconsume_times`, `sys_flag`, `flag`, and
`prepared_transaction_offset`, plus the `broker_name` that stores it. No body, key, tag, property, or
address is returned.

A Broker finds a key by its hash, so the server keeps only the messages of the Topic that really
carry the key and were stored inside the window. More than 32 matches set `truncated` and the warning
`message_key_matches_truncated`. A Broker that hosts the Topic and did not answer is named in
`unanswered_brokers`; the response is then partial with the warning
`message_index_brokers_unanswered`, and the call fails with `source_unavailable` only when no Broker
answered. A Topic without a route in the cluster is `not_found`. The Tool has no Resource.

```json
{"cluster":"production-a","topic":"orders","key":"order-42","end_at":"2026-10-01T08:30:00Z","window_minutes":120}
```

```json
{"cluster":"production-a","data":{"topic":"orders","window_begin_at":"2026-10-01T06:30:00.000Z","window_end_at":"2026-10-01T08:30:00.000Z","messages":[{"message_alias":"message-fedcba9876543210fedcba9876543210","unique_message_alias":"unique-message-0123456789abcdef0123456789abcdef","broker_name":"broker-a","stored_at":"2026-10-01T07:12:44.120Z","queue_id":3,"queue_offset":42,"store_size":128,"reconsume_times":0}],"truncated":false,"queried_broker_count":2,"unanswered_brokers":[]}}
```

### `rocketmq_get_lite_broker_info`

**Purpose and access.** Returns what one Broker holds for Lite Topics. It is read-only. A Lite Topic
is a lightweight child queue of a Topic whose message type is `LITE`. The Tool asks the master of
the logical Broker, because the master serves Lite Topics.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |
| `broker_name` | string | yes | Logical Broker name. It is a name, not a network address. |

**Output.** `data` contains the logical cluster, `broker_name`, and what the master reported:

- `store_type`, the kind of store that keeps Lite Topic queues; `max_lmq_count`, the most queues the
  Broker accepts; and `current_lmq_count`, the queues it holds now. A Broker close to its limit
  refuses new Lite Topics.
- `lite_subscription_count`, `order_info_count`, `consume_queue_count`, `offset_count`, and
  `event_count`, the sizes of the tables the Broker keeps for Lite Topics.
- `lite_topic_count` and `lite_topics`: the Topics of message type `LITE`, in ascending order of
  name and at most 100. A row has `topic`, `expiration_minutes` (how long an idle Lite Topic is kept;
  `-1` when no expiration is configured), `bound_group_count`, and `bound_groups`, the Consumer
  Groups bound to the Topic in ascending order and at most 8.
- `truncated` is true when the Broker knows more than 100 Lite Topics or a Topic has more than 8
  bound groups. The counts still cover what was left out.

A Broker name that the cluster does not have is `not_found` with `entity = "broker"`. A Broker
without a master that can be asked, and a master that does not answer, fail the call with
`source_unavailable`. The Tool has no Resource.

```json
{"cluster":"production-a","broker_name":"broker-a"}
```

```json
{"cluster":"production-a","data":{"broker_name":"broker-a","store_type":"RocksDB","max_lmq_count":100000,"current_lmq_count":42,"lite_subscription_count":7,"lite_topic_count":1,"lite_topics":[{"topic":"lite-orders","expiration_minutes":30,"bound_group_count":1,"bound_groups":["order-service"]}],"truncated":false}}
```

## Optional `change-planning` catalog (5 Tools)

**Deprecated.** The `change-planning` feature, its five Tools, and `security.allow_change_planning` will be
removed. There is one place to plan a change: a dry run (`dry_run=true`, the default) of the control MCP
server `rocketmq-mcp-control`. A dry run reads the state the change would be applied to, takes the arguments
the execution takes, and is audited; a plan produced here is none of these. Each planning Tool's description
names its replacement, and a server that serves the planning Tools logs a deprecation warning at startup.

| Deprecated planning Tool | Dry run on the control MCP server |
| --- | --- |
| `rocketmq_plan_create_topic` | `rocketmq_upsert_topic` |
| `rocketmq_plan_update_topic_config` | `rocketmq_upsert_topic` |
| `rocketmq_plan_update_topic_permissions` | `rocketmq_upsert_topic` |
| `rocketmq_plan_update_broker_config` | `rocketmq_patch_broker_config` |
| `rocketmq_plan_reset_consumer_offset` | `rocketmq_reset_consumer_offset` |

The five Prompts never lead to a planning Tool: where a change is needed, they tell the model to name it in
its report for an operator to plan with a control dry run.

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

## Optional `security-admin` catalog (1 Tool)

`security-admin` is compiled separately and is not a default feature: a default build has no code
path that lists ACL users. With the feature, three checks still stand between a caller and the list:

1. the scope `rocketmq:security-admin`. No other scope includes it. Over HTTP the identity provider
   must issue it; for stdio only the local profile `security_admin` gets it.
2. a role that holds `rocketmq_list_acl_users`. No role of the example permissions file does; the
   file shows a role to add. A role with `allow_tools = ["*"]` and no `deny_tools = ["*"]` holds
   every Tool, this one included, so the scope is then the only check left. Name the Tool in
   `deny_tools` of such a role when its holders must not list users.
3. the cluster, tenant, rate-limit, and concurrency checks of every Tool.

A caller without the scope gets `unauthorized_scope`, a caller without the role `permission_denied`,
and neither sees the Tool in `tools/list` or in the capability Resource. Every call is audited with
the risk level `security_admin`.

### `rocketmq_list_acl_users`

**Purpose and access.** Lists the ACL users of one cluster. It is read-only and requires the scope
`rocketmq:security-admin` instead of `rocketmq:read`. Each Broker keeps its own users, so the Tool
asks the master of every Broker of the cluster and names, for each user, the Broker that holds it.
Use it to check who may sign requests to a cluster and whether every Broker holds the same users in
the same state.

| Argument | Type | Required | Rules |
| --- | --- | --- | --- |
| `cluster` | string | yes | Configured logical cluster alias. |

There is no argument that names a user or a Broker: the Tool answers for the whole cluster, and it
cannot be used to confirm guessed user names one by one.

**Output.** `data` contains the logical cluster, `user_count`, `users`, `truncated`,
`queried_broker_count`, `unanswered_brokers`, and `generated_at`.

- `users` has one row for each user on each Broker, in ascending order of user name and Broker and
  at most 500. A row has `username`, the logical `broker_name`, `user_type` (`Super` or `Normal`),
  and `user_status` (`enable` or `disable`). A type or status that is not a short label is null.
- `user_count` is the number of distinct user names over every Broker that answered. It also covers
  the rows that `truncated` reports as left out, with the warning `acl_users_truncated`.
- A Broker without a master that can be asked, and a master that did not answer, are named in
  `unanswered_brokers`; the response is then partial with the warning
  `acl_user_brokers_unanswered`, and the users of those Brokers are missing. The call fails with
  `source_unavailable` only when no Broker answered.

The result never contains a password, an access key, an access rule, or an address. A Broker sends
the password of every user it lists; the RocketMQ client drops it before the read is handed on, so
it reaches neither this server's code, nor its cache, nor its logs. Every call reads the Brokers:
nothing is answered from a cache, and the Tool has no Resource. A cluster with more than 64 Brokers
is not read in part: the call fails with `invalid_arguments`.

```json
{"cluster":"production-a"}
```

```json
{"cluster":"production-a","data":{"user_count":2,"users":[{"username":"ops","broker_name":"broker-a","user_type":"Super","user_status":"enable"},{"username":"ops","broker_name":"broker-b","user_type":"Super","user_status":"disable"},{"username":"reader","broker_name":"broker-a","user_type":"Normal","user_status":"enable"}],"truncated":false,"queried_broker_count":2,"unanswered_brokers":[]}}
```
