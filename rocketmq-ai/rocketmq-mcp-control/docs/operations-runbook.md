# RocketMQ MCP Control operations runbook

## Prepare and enable

1. Build the standalone project with `write-tools`; the default build intentionally has no Admin dependency or
   mutation tools.
2. Configure TLS and the canonical HTTPS public origin, remote RS256 OAuth issuer/JWKS/audience, and the durable
   JSONL audit path.
3. Add a private cluster-registry entry for each logical cluster. Reference credentials only through protected
   environment-variable names; inline credentials are rejected.
4. Configure `mutations_enabled=true` and allowlist only the required subset of `topic_upsert`,
   `consumer_group_upsert`, `consumer_offset_reset`, `broker_config_patch`, `consumer_request_mode`,
   `proxy_drain_begin`, `proxy_drain_cancel`, `broker_log_filter_set`, and `broker_log_filter_restore`, plus
   their logical clusters. The identity provider must issue matching closed claims and `rocketmq:write`.
5. For Proxy drains, list each Proxy of a cluster under `[[clusters.proxies]]` with an alias and its remoting
   `host:port`. Callers name a Proxy only by that alias. See [Proxy drains and temporary log
   levels](#proxy-drains-and-temporary-log-levels) for what the Proxy and the Broker need.
6. Optionally set `mutations.emergency_stop_file` to a path the operators on call can write to. It is the one
   control that takes effect without a restart; see [Emergency stop-write
   procedure](#emergency-stop-write-procedure).
7. Restart the process. Configuration changes are startup-time controls and do not hot reload.

Verify authenticated `tools/list`: a principal can see zero through nine mutation tools according to the
intersection, and `rocketmq_get_mutation_result` whenever it holds `rocketmq:write` and an allowed, configured
cluster.
The capability Resource must report compile/runtime/registered state consistent with that catalog. Resource
templates and prompts remain empty.

## Deployment inside a private network

By default the server accepts only a specific bind address, a public canonical origin, and an
identity provider with a public name whose JWKS endpoint answers from a public address. Three
switches replace one of those defaults each. All are off unless the configuration sets them to
`true`, and each one only admits what its own row names. A server that starts with any of them on
logs one warning, `rocketmq-mcp-control starts with deployment switches that relax its network
defaults`, whose `switches` field lists their configuration keys.

| Switch | What it accepts | What you take on |
| --- | --- | --- |
| `server.allow_wildcard_bind` | A `bind` on every interface: `0.0.0.0:<port>` or `[::]:<port>`, as a container usually needs. | The server listens on interfaces you may not have meant to expose. Limit who can connect with a network policy or a firewall. Requests still have to name the configured origin in `Host`. |
| `server.allow_internal_base_url` | A `public_base_url` with a port, or with a host name that only resolves inside your network: one ending in `.internal`, `.local`, or `.home.arpa`, or a single-label service name. Example: `https://control.mq.svc.cluster.local:8443`. | No public certificate authority vouches for such a name. Your own name service and certificates decide who answers under it, and the same name can mean another server in another network. |
| `oauth.allow_private_network` | An `issuer` and a `jwks_url` with a port or an internal host name, and a JWKS endpoint that answers from a private address. The JWKS request then follows the resolver and proxy settings of the process. | The keys that decide who may change a cluster come from wherever that name leads. Set `oauth.jwks_ca_path` to the CA of your identity provider, so that the certificate check of the JWKS connection still identifies it. |

What stays fixed with every switch on: the origin and both OAuth URLs are canonical HTTPS URLs
without credentials, query, or fragment; a host is never an address literal or a loopback name; a
request must send the configured origin as `Host`, with the port when the origin has one, and as
`Origin` when it sends that header; tokens are RS256 OAuth JWTs for the exact issuer and audience.

Turn on only the switches a deployment needs, and treat the startup warning as an alert in
environments that are not meant to run with one.

## Safe operating sequence

Start with dry-run and inspect the operation-specific top-level `target`, aggregate `before` and `requested`,
then the broker-sorted per-target evidence. For execution, preserve the same complete replacement and target
set, set `dry_run=false` and `confirm=true`, and provide a safe operator reason. Inspect aggregate `after` as
well as each target's persistence/verification evidence. That response is where to read the result: the query
MCP server answers from caches of a few seconds, so a read through it right after the execution can still show
the earlier state. Use a request key when retry or concurrent delivery is
possible. Do not change the payload for a reused key. A repeated key replays an applied, partial, or conflicting
outcome; after any failure it runs preflight again, so retrying with the same key is the way to learn the current
state.

Pass the `plan_digest` of the dry run as `expected_plan_digest` of the execution. The execution then writes
only if the plan it reads is still that one, and otherwise ends with `precondition_conflict` and writes
nothing. Run the dry run again, review the plan it reports, and execute it under a new request key. Set
`mutations.require_plan_digest = true` to refuse every execution that names no plan; the item is read at
startup like the rest of the policy, and it is off unless set. Before you turn it on, check the callers of
`rocketmq_reset_consumer_offset`: the offsets of a group that is consuming move between two reads, so its
plans go stale until the consumers are stopped. A Proxy that serves does not have that problem, because the
work it holds is not part of a drain plan. The [tool reference](tool-reference.md#executing-a-reviewed-plan)
lists what a plan covers for each tool.

The server persists `started` before opening an Admin session. One lifecycle-owned supervisor then opens one
mutation-only session, performs targeted preflight, optionally executes the sealed conditional plan, performs a
targeted post-read, awaits exactly one bounded shutdown, and persists a terminal record. A caller disconnect,
timeout, cancellation, or contained adapter panic does not abandon an acquired session.

Interpret results conservatively:

- `planned` and `applied` are non-error statuses; unchanged success is `applied` with `changed=false`;
- `conflict` has `error_code=precondition_conflict`; the conditional expected state no longer matched and is not retried;
- `partial` has `error_code=partial_apply`, retains per-target applied/persistence/verification truth, and requires operator review;
- `failed` uses `verification_failed` when every actual failure is a verification or order-reconciliation failure,
  otherwise `execution_failed`; it may still contain an applied target whose persistence or post-read verification failed;
- `order_reconciliation_failed` means the sealed NameServer-wide order value changed after broker CAS; the
  targeted path never tries to repair that global value and preserves the broker-applied truth.

Never infer success from HTTP delivery alone; consume the closed structured result and `isError` value.

## Proxy drains and temporary log levels

`rocketmq_begin_proxy_drain`, `rocketmq_cancel_proxy_drain`, `rocketmq_set_broker_log_filter_ttl`, and
`rocketmq_restore_broker_log_filter` each start or end a state on one target instead of replacing metadata.
The state belongs to the `operation_id` that started it, and only a call with the same identifier ends it. The
tool reference lists the arguments and every outcome.

Prepare the targets before you allowlist an operation:

- A Proxy answers drain requests, reads included, only for an authenticated principal. Run the Proxy with
  authentication, and give the cluster entry credentials that the Proxy accepts.
- A Broker applies a remote log filter only with authentication and authorization enabled and with
  `[logging.reload] enabled = true`, and only for a request that carries an AccessKey. Give the cluster entry
  credentials that the Broker authorizes to update its configuration.
- Grant `proxy_drain_cancel` to every principal that holds `proxy_drain_begin`, and `broker_log_filter_restore`
  to every principal that holds `broker_log_filter_set`. A principal that can start a state should be able to
  end it.

Drain a Proxy before a restart:

1. Call `rocketmq_begin_proxy_drain` as a dry run and check that `before` shows `phase=accepting` and
   `held_by=none`. A `conflict` means another operation already drains the Proxy.
2. Execute it with a new `operation_id`, and write that identifier into the change record. The Control
   server stores it nowhere and never returns it, so your own record is what tells you which identifier
   cancels the drain.
3. Repeat the dry run with the same `operation_id` until `phase=drained` and `zero_pending=true`. The `pending`
   counters show what the Proxy still holds.
4. Restart the Proxy. The new process admits work again, so no cancel is needed.
5. To abandon the drain instead, execute `rocketmq_cancel_proxy_drain` with the same `operation_id`.

A drain whose identifier is lost cannot be cancelled through the Control server. Restart that Proxy, or cancel
the drain through a separately authorized operational path.

Raise a log level for an investigation:

1. Call `rocketmq_set_broker_log_filter_ttl` as a dry run. `held_by=other_operation` means an override is
   already active, and the server will not replace it.
2. Execute it with the shortest `ttl_seconds` that covers the investigation, 60 through 900.
3. Call `rocketmq_restore_broker_log_filter` with the same `operation_id` when you are done. Without it the
   Broker ends the override by itself at `expires_at_millis`.

An override covers one `rocketmq_broker::` logger at `INFO` or `DEBUG`. Loggers of modules that handle message
content, credentials, or storage are refused. `DEBUG` output still grows the Broker log volume, so prefer one
narrow logger and a short time to live.

Send a new `request_key` with every one of these calls. A key replays the outcome of its call for 10 minutes
without reading the target again.

## Logs and metrics

The process installs its log subscriber before it reads the configuration and writes plain-text events to
stderr, one event per line. Only events of this service are written at `info` and above. Dependency events
stay off because they can carry network addresses and backend error text; `RUST_LOG` does not change that.
A log line holds closed labels, counts, durations, the audit invocation number, and the authorized logical
cluster alias. It never holds the operator, the request reason, addresses, tokens, target names, or backend
error text.

| Message | Level | Fields | Meaning |
| --- | --- | --- | --- |
| `rocketmq-mcp-control is starting` | info | `version`, `write_tools_compiled` | Logging is installed; the configuration is read next. |
| `control configuration was rejected` | error | `stage`, then `reason` or `variable` | See the configuration table below. |
| `control telemetry could not start` | error | `stage="telemetry"`, `operation` | The OTLP variables are invalid or the exporter could not start. |
| `rocketmq-mcp-control starts with deployment switches that relax its network defaults` | warn | `switches` | One or more [deployment switches](#deployment-inside-a-private-network) are on. `switches` lists their configuration keys, separated by commas. |
| `control server failed` | error | `stage`, `code` | `stage` is `audit`, `oauth_keys`, `tls`, `listener`, `mutation_tools`, or `serve`. |
| `rocketmq-mcp-control authenticated HTTPS transport is ready` | info | `version`, `write_tools_compiled`, `mutations_runtime_enabled`, `registered_operations` | The listener accepts requests. The fields repeat the capability Resource. |
| `request was rejected before authentication` | warn | `site="origin"`, `code`, `suppressed` | The Host or Origin header did not match the public origin. |
| `request was rejected by authentication` | warn | `site="authentication"`, `code`, `reason`, `suppressed` | `reason` is `invalid_token`, `insufficient_scope`, or `keys_unavailable`. |
| `request named an unsupported protocol version` | warn | `site="protocol"`, `code`, `suppressed` | An authenticated request sent an `MCP-Protocol-Version` header other than `2025-11-25`. The client speaks another MCP revision or skipped the handshake. |
| `mutation call was rejected before execution` | warn | `site="call"`, `tool`, `code`, `suppressed` | Authorization, argument, or admission failure. No Admin session was opened. |
| `mutation finished` | info | `operation`, `cluster`, `mode`, `result`, `invocation_id`, `elapsed_ms` | A supervised dry run was `planned` or an execution was `applied`. |
| `mutation finished without a clean result` | warn | the same fields plus `code` and `audit_recorded` | The result is `partial`, `conflict`, or `failed`, or the terminal audit record was not persisted. |
| `mutation result was looked up` | info | `tool`, `cluster`, `state`, `elapsed_ms` | `rocketmq_get_mutation_result` answered. `state` is `finished`, `in_progress`, `interrupted`, or `not_found`. A lookup writes no audit record. |
| `durable audit record is unavailable` | error | `site="audit"`, `stage`, `suppressed` | `started`: no session began. `terminal`: the mutation ran without a terminal record. `supervisor`: the terminal state is unknown. |
| `rocketmq-mcp-control stopped` | info | none | Normal shutdown. |
| `rocketmq-mcp-control stopped with an error` | error | `code` | The stage was logged earlier. |
| `control telemetry did not shut down cleanly` | warn | `metrics_flushed` | The final metrics export failed or did not finish within 10 seconds. |

`invocation_id` is the `invocation_id` of the matching `started` and terminal records in the audit file, so a
log line leads to the operator evidence without repeating it. `tool` is a reviewed tool name or `unknown`;
the caller's text is never logged. Each `site` and `code` pair (`reason` for authentication, `stage` for
audit) is logged at most once every 10 seconds, and `suppressed` on the next line is the number of identical
events that were skipped. Metrics count every event.

Configuration rejections name the stage and position. Values, paths, and quoted file content are never
logged.

| `stage` | `reason` or `variable` | Action |
| --- | --- | --- |
| `locate` | `variable="ROCKETMQ_MCP_CONTROL_CONFIG"` | Set the variable to the configuration file path. |
| `read` | `file was not found`, `file is not readable by this process`, `file is not valid UTF-8`, or `file could not be read` | Correct the path, permissions, or encoding. |
| `parse` | `file is not valid TOML`, ``unknown key `<key>` ``, ``required key `<key>` is missing``, or `a value has the wrong type or is not allowed`, followed by `at line <n>, column <n>` when known | Correct the file at that position. A quoted key that is not a plain name is reported without its text. |
| `validate` | `` `<table.field>` has a value that is not allowed ``, followed by ``in `[[clusters]]` entry <n>`` for the nth cluster table | Correct that value. For `mutations.allowed_clusters`, every allowed cluster must also be a registered cluster. |

Metrics are optional. Build with the `otlp` feature next to `write-tools`, then set
`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`. A build without `otlp` ignores both
variables. In an `otlp` build, an endpoint without the `grpc` protocol stops startup. Only metrics are
exported: traces and OTLP log records stay off, and no metrics listener is opened. The service name is
`rocketmq-mcp-control`; `deployment.environment.name` and `rocketmq.cluster` keep the shared observability
defaults, so set them at the collector when needed. Export failures after startup are dependency events and
are therefore not logged; watch for missing data on the collector side. An unreachable collector does not
block startup. At shutdown the process waits up to 10 seconds for the final export and logs a warning when it
fails.

The service records the shared MCP instruments.

| Instrument | Attributes | Recorded for |
| --- | --- | --- |
| `rocketmq_mcp_requests_total`, `rocketmq_mcp_request_latency` | `operation_kind="tool"`, `operation`, `result` | Every tool call. `operation` is the tool name for a rejected call and the tool name plus `.dry_run` or `.execute` for a supervised mutation. `result` is `success`, `failure`, or `denied`. |
| `rocketmq_mcp_errors_total` | `operation_kind="tool"`, `operation`, `result` | Every failed call, plus `operation="authentication"` and `operation="http_request"` for requests rejected before a tool was selected. |
| `rocketmq_mcp_cache_operations_total` | `result` | Request-key admission: `miss` leads, `coalesced_waiter` follows an in-flight call, `hit` replays a settled one, `bypass` has no request key or is a dry run. |
| `rocketmq_mcp_rate_limit_total` | `result` | Admission control for an authorized, valid call: `accepted` or `rejected`. |
| `rocketmq_mcp_audit_failures_total` | `reason="sink"` | Every durable audit record that could not be persisted or confirmed. |

`rocketmq_mcp_errors_total` folds the control codes into the shared failure classes:

| `result` | Control codes |
| --- | --- |
| `permission_denied` | `unauthorized`, `permission_denied`, `cluster_not_allowed`, `operation_not_allowed`, `mutation_disabled` |
| `invalid_request` | `request_rejected`, `confirmation_required`, `invalid_argument`, `precondition_conflict` |
| `rate_limited` | `rate_limited` |
| `source_unavailable` | `operation_unavailable`, `audit_unavailable`, `partial_apply`, `verification_failed`, `timeout`, `shutdown_failed` |
| `cancelled` | `cancelled` |
| `internal` | `execution_failed`, `invalid_config` |

The exact code of each call is in the log line and in the audit record.

## Audit capacity and archiving

Every admitted call writes two audit records, a dry run included: `started` before any session, and one
terminal record. Each record is one line of the active file at `audit.path`.

- `audit.capacity` is the number of lines in one segment. A full segment is sealed as
  `<path>.<six-digit number>` and a new active file starts, so the trail has no lifetime limit and a full segment
  does not fail a call. With the default `capacity = 4096`, a segment holds about 2,000 calls.
- `audit.max_record_bytes` is at least 4,096. A segment is at most `capacity` times that size and never more
  than 64 MiB.
- `[limits]` bounds how fast segments fill. Each OAuth subject may start `dry_runs_per_minute` dry runs and
  `executes_per_minute` execute calls, and at most `max_concurrent_calls` calls run at once. A call over a limit
  gets `rate_limited`, writes no audit record, and counts as `rejected` in `rocketmq_mcp_rate_limit_total`. At
  the defaults, one subject can fill at most one default segment in about 25 minutes.
- `audit.capacity` must be at least four times `max_concurrent_calls`: a new segment starts with a copy of every
  call still in flight.

The server logs `audit segment was sealed` with the segment number, its record count, and its last sequence at
each rotation. It exports no gauge for the number of sealed segments or the fill of the active one, so plan disk
space from the log line and the file sizes.

To archive, copy or move sealed segments, the files with a numeric suffix, to long-term storage. The service can
keep running: it never reads a sealed segment again. Never touch the active file or a `<path>.next` file while
the service runs. Keep the sealed segments in order; each header names the last sequence and the SHA-256 of the
segment before it, which lets an auditor prove that none is missing or altered.

When calls fail with `audit_unavailable`:

1. Read the service log. `durable audit record is unavailable` names the stage, `started` or `terminal`. At
   startup, `a sealed audit segment already has the number of the active segment` means a file already exists
   under the name the active segment would be sealed to; move that file to the archive.
2. Check that the volume of `audit.path` is writable and has free space, and that no other process holds or
   has modified the active file.
3. Restart the service after the cause is fixed. A failed write poisons the live trail on purpose, so it does
   not recover without a restart. Startup validates the active segment and refuses a file that is torn,
   reordered, or altered.
4. A call that was `started` and has no terminal record after the restart stays that way. Compare its `target`
   and `requested_digest` with the RocketMQ state to decide whether it took effect.

## Failure and emergency handling

If audit `started` is unavailable, the operation opens no session and performs no RocketMQ RPC. Treat audit
poison, shutdown failure, persistence failure, or verification failure as fail-closed and investigate the
durable audit plus RocketMQ state using separately authorized operational systems. Version-3 audit records
contain the validated OAuth subject and optional safe request reason strictly as durable operator evidence,
together with the logical target names, digests, and outcome counts described in the tool reference.
Operator and reason appear in no response, error, trace, or ordinary log. Audit records contain no credentials,
tokens, endpoints, request keys, message bodies, or backend error text. Existing version-1, version-2, and
mixed-version JSONL is recovered in place and is never rewritten; new writes are version 3 only.

To stop new mutations at once, create the emergency stop file; to stop them for good, set
`mutations_enabled=false` and restart. Both are described under [Emergency stop-write
procedure](#emergency-stop-write-procedure). Removing an operation or cluster from the
server allowlist also requires restart. Keep the service unavailable until incomplete or partial targets are
reconciled explicitly; the control server never retries conflicts or invents compensating mutations.

## Controlled rollout

Use a narrow, reversible rollout. The configuration is read only during startup; neither
`mutations_enabled` nor either allowlist hot reloads.

1. Build the `write-tools` variant and deploy it with the checked-in default configuration:
   `mutations_enabled=false`, `dry_run=true`, and empty mutation allowlists.
2. Confirm TLS and the remote OAuth resource-server configuration. Authentication uses RS256 JWT signatures
   verified against HTTPS JWKS; this identity verification is unrelated to, and does not introduce, a
   change-plan signature or approval provider.
3. Configure the private logical cluster registry, durable audit destination, and only the intended
   operation and cluster allowlists. After those prerequisites are in place, set
   `mutations_enabled=true` and restart the service. The initial `false` value is a pre-deployment
   safety state; no write Tool can be discovered until this enable-and-restart step completes.
4. Authenticate an authorized principal and inspect `tools/list` plus
   `rocketmq-control://capabilities`. Confirm that the visible catalog is the intended subset, not merely
   that the feature was compiled.
5. Send a dry-run for each operation being introduced. Review the complete aggregate `before`, `requested`,
   ordered target evidence, warnings, and any `partial` signal before considering an execute call.
6. Execute only the reviewed dry-run payload with `dry_run=false`, `confirm=true`, and a safe reason. Check
   the structured result rather than HTTP delivery alone.
7. Read the durable audit trail for a `started` record and its matching terminal v2 record. Confirm the
   operation, cluster alias, mode, result, terminal error code, sequence ordering, and duration. Operator
   and reason are audit-only evidence and must not be copied from public responses or logs.
8. Expand to another allowed operation or cluster only by editing the startup configuration and repeating
   the same restart, discovery, dry-run, execute, and audit review sequence.

This is intentionally an operational sequence, not an approval system. A plan digest holds an execution to
the plan that a dry run reported; it is no signature for a change plan, no approval, and no release gate, and
the server does not record who reviewed a plan.

## Rollback and result handling

Keep the complete pre-change state from the dry-run/result before execution. Use a new typed restore request
with the normal explicit confirmation and reason only when that Tool can express the recorded `before` state.
Topic and Consumer Group creation has no delete Tool, an offset reset has no exact per-queue restore, and an
absent Consumer request mode cannot be restored through a set operation. For those cases, preserve the
evidence and use a separately authorized operational path; do not invent a generic compensating command,
CLI, RPC, or delete operation. Do not reuse a request key with different content.

A Proxy drain and a temporary log level are undone by their own counterpart: `rocketmq_cancel_proxy_drain` and
`rocketmq_restore_broker_log_filter` with the `operation_id` that started the state.

| Result or code | Meaning | Required operator action |
| --- | --- | --- |
| `planned` | Dry-run produced a sealed candidate without mutation. | Review it; no rollback is needed. |
| `applied` | Every target verified, including `changed=false` idempotent success. | Record the terminal audit evidence and continue normal monitoring. |
| `precondition_conflict` / `conflict` | Sealed state changed before the conditional write, or the plan read now is not the one the execution named. | Re-run dry-run, compare new state, and choose a new explicit action; never automatic-retry. A plan that was refused needs a new `request_key` for the next one. |
| `partial_apply` / `partial` | Some targets persisted or verified and others did not. | Treat returned `targets`, `before`, `after`, persistence, and verification fields as source of truth; reconcile each target explicitly before retrying. |
| `verification_failed` | A write may have persisted but complete post-read verification or order reconciliation failed. | Preserve the result, inspect the affected logical target using an authorized operational path, then restore or complete the intended state with a new typed request. |
| `audit_unavailable` | Durable audit recovery, append, or timeout failed. | Stop mutation attempts and repair/recover the audit sink before retrying. A failed `started` record means no session/RPC began. |
| `timeout`, `cancelled`, or `shutdown_failed` | The request/session lifecycle did not reach a normal terminal path. | Call `rocketmq_get_mutation_result` with the call's `request_key` until it is `finished`, then reconcile the durable terminal audit state and target state before any new request. |
| lost response | The client never received a result or an error. | Call `rocketmq_get_mutation_result` with the `request_key`. `in_progress`: wait. `interrupted`: the server restarted mid-call, so read the current state with a dry run. `not_found`: the call did not start, or it is no longer remembered. |
| `execution_failed` or `operation_unavailable` | The typed operation/session cannot complete or is not registered. | Correct deployment/runtime policy or the underlying authorized service condition; do not substitute a CLI, shell, or arbitrary RPC. |

For rollback of server policy rather than a RocketMQ resource, restore the prior configuration file and restart
the Control process. After restart, repeat authenticated `tools/list` and capability-resource verification;
configuration edits do not take effect in a running process.

Rolling back the Control binary needs one more step once a Proxy drain or a log filter call has been recorded.
A build from before those four operations rejects their operation values and the `proxy` target member, so it
refuses to start on an active audit segment that holds such a record. Stop the service, leave the whole trail
in place as an archive, and start the older build with a new `audit.path`. Its configuration must not contain
`[[clusters.proxies]]` or one of the four operation values either: that build rejects them as unknown.

## Emergency stop-write procedure

There are two stops. The stop file acts within one call and needs no restart, and it is the first step in an
incident. The configuration stop takes a restart, removes the mutation Tools from discovery, and is how a
stop is made to last. Neither erases audit records or changes any RocketMQ state.

### Stop file: no restart

This stop needs `mutations.emergency_stop_file` in the configuration the server was started with. Set it when
you deploy: it cannot be added during an incident without the restart it is meant to avoid.

1. Create the file that `mutations.emergency_stop_file` names, on the host or in the volume the server reads.
   An empty file is enough; the server reads no content.
2. From the next call on, every mutation Tool call is refused with `mutation_disabled`, a dry run included. A
   refused call writes no audit record and opens no session. The server looks the file up for each call, so
   there is nothing to reload.
3. Verify with an authorized dry run: it must return `mutation_disabled`. `tools/list` and
   `rocketmq-control://capabilities` do not change with the stop file: they report the configuration.
4. `rocketmq_get_mutation_result` keeps answering, so the outcome of the calls made before the stop can be
   read.
5. Remove the file only after the incident is resolved. The next call is then decided by the configuration
   again.

What the stop file does not do:

- It does not interrupt a call that was admitted before the file appeared. Such a call runs to its end, at
  most `mutations.operation_timeout_seconds` later. Read its outcome with `rocketmq_get_mutation_result`.
- It never enables anything. Without the file, `mutations_enabled` and the allowlists decide alone.
- It is not an authorization. Anyone who can create the file can stop mutations and anyone who can remove it
  can lift the stop, so give the directory to the operators on call and to nobody else. The server needs
  only to look the file up, not to write there.
- If the server cannot look the file up, for example because the directory became unreadable, it refuses
  mutation calls as if the file existed. A failed lookup is never read as "no stop".

A server that starts while the file exists logs the warning `rocketmq-mcp-control starts with the emergency
stop engaged: mutation calls are refused`. Keep the path on storage that survives a restart if the stop has to
survive one, or follow up with the configuration stop below.

### Configuration stop: with a restart

Use this to make a stop last, and whenever no stop file was configured. It stops new mutation Tool
registration.

1. Edit the active Control configuration and set `mutations_enabled=false`. Keep `dry_run=true`; reducing
   `allowed_operations` and `allowed_clusters` is optional defense in depth, not a substitute for disabling
   mutations.
2. Restart the Control process. There is no hot reload for this setting.
3. With an authenticated request, call `tools/list`. Verify that no reviewed write Tool is present:
   `rocketmq_upsert_topic`, `rocketmq_upsert_consumer_group`, `rocketmq_reset_consumer_offset`,
   `rocketmq_patch_broker_config`, `rocketmq_set_consumer_request_mode`, `rocketmq_begin_proxy_drain`,
   `rocketmq_cancel_proxy_drain`, `rocketmq_set_broker_log_filter_ttl`, and
   `rocketmq_restore_broker_log_filter` must all be absent. The read-only
   `rocketmq_get_mutation_result` stays listed on purpose: it is how the outcome of the calls made before the
   stop is read, for every invocation whose records are still in the active audit segment.
4. Read `rocketmq-control://capabilities`. Verify
   `mutations_runtime_enabled=false`, `registered_operations=0`, and `mutation_supported=false`.
   In a `write-tools` binary, `write_tools_compiled` can remain true; compile availability is intentionally
   distinct from runtime enablement and registration.
5. Preserve and inspect existing durable audit records. Reconcile any already-started, partial, failed, or
   verification-incomplete operations through the returned target evidence before permitting new writes.

The stop also removes `rocketmq_cancel_proxy_drain` and `rocketmq_restore_broker_log_filter`. A drain that is
active at that moment stays active: cancel it before the stop, restart the Proxy, or cancel it once mutations
are enabled again. A log filter override ends by itself when its time to live runs out, at most 15 minutes
after it was set.

To restore writes, reverse the configuration change only after the incident is resolved: set
`mutations_enabled=true`, restore only the necessary closed operation/cluster allowlists, restart, verify the
capability fields and intended `tools/list` catalog, then begin again with dry-run. Remove the stop file as
well if one was created. There is no emergency override, generic delete Tool, shell, or free-form RPC.
