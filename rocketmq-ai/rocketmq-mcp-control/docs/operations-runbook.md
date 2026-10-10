# RocketMQ MCP Control operations runbook

## Prepare and enable

1. Build the standalone project with `write-tools`; the default build intentionally has no Admin dependency or
   mutation tools.
2. Configure TLS and the canonical HTTPS public origin, remote RS256 OAuth issuer/JWKS/audience, and the durable
   JSONL audit path.
3. Add a private cluster-registry entry for each logical cluster. Reference credentials only through protected
   environment-variable names; inline credentials are rejected.
4. Configure `mutations_enabled=true` and allowlist only the required subset of `topic_upsert`,
   `consumer_group_upsert`, `consumer_offset_reset`, `broker_config_patch`, and `consumer_request_mode`, plus
   their logical clusters. The identity provider must issue matching closed claims and `rocketmq:write`.
5. Restart the process. Configuration and emergency-disable changes are startup-time controls and do not hot
   reload.

Verify authenticated `tools/list`: a principal can see zero through five tools according to the intersection.
The capability Resource must report compile/runtime/registered state consistent with that catalog. Resource
templates and prompts remain empty.

## Safe operating sequence

Start with dry-run and inspect the operation-specific top-level `target`, aggregate `before` and `requested`,
then the broker-sorted per-target evidence. For execution, preserve the same complete replacement and target
set, set `dry_run=false` and `confirm=true`, and provide a safe operator reason. Inspect aggregate `after` as
well as each target's persistence/verification evidence. Use a request key when retry or concurrent delivery is
possible. Do not change the payload for a reused key.

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
| `control server failed` | error | `stage`, `code` | `stage` is `audit`, `oauth_keys`, `tls`, `listener`, `mutation_tools`, or `serve`. |
| `rocketmq-mcp-control authenticated HTTPS transport is ready` | info | `version`, `write_tools_compiled`, `mutations_runtime_enabled`, `registered_operations` | The listener accepts requests. The fields repeat the capability Resource. |
| `request was rejected before authentication` | warn | `site="origin"`, `code`, `suppressed` | The Host or Origin header did not match the public origin. |
| `request was rejected by authentication` | warn | `site="authentication"`, `code`, `reason`, `suppressed` | `reason` is `invalid_token`, `insufficient_scope`, or `keys_unavailable`. |
| `mutation call was rejected before execution` | warn | `site="call"`, `tool`, `code`, `suppressed` | Authorization, argument, or admission failure. No Admin session was opened. |
| `mutation finished` | info | `operation`, `cluster`, `mode`, `result`, `invocation_id`, `elapsed_ms` | A supervised dry run was `planned` or an execution was `applied`. |
| `mutation finished without a clean result` | warn | the same fields plus `code` and `audit_recorded` | The result is `partial`, `conflict`, or `failed`, or the terminal audit record was not persisted. |
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
| `rocketmq_mcp_cache_operations_total` | `result` | Request-key admission: `miss` leads, `coalesced_waiter` follows an in-flight call, `hit` replays a completed one, `bypass` has no request key. |
| `rocketmq_mcp_audit_failures_total` | `reason="sink"` | Every durable audit record that could not be persisted or confirmed. |

`rocketmq_mcp_errors_total` folds the control codes into the shared failure classes:

| `result` | Control codes |
| --- | --- |
| `permission_denied` | `unauthorized`, `permission_denied`, `cluster_not_allowed`, `operation_not_allowed`, `mutation_disabled` |
| `invalid_request` | `request_rejected`, `confirmation_required`, `invalid_argument`, `precondition_conflict` |
| `source_unavailable` | `operation_unavailable`, `audit_unavailable`, `partial_apply`, `verification_failed`, `timeout`, `shutdown_failed` |
| `internal` | `execution_failed`, `cancelled`, `invalid_config` |

The exact code of each call is in the log line and in the audit record.

## Failure and emergency handling

If audit `started` is unavailable, the operation opens no session and performs no RocketMQ RPC. Treat audit
poison, shutdown failure, persistence failure, or verification failure as fail-closed and investigate the
durable audit plus RocketMQ state using separately authorized operational systems. Version-2 audit records
contain the validated OAuth subject and optional safe request reason strictly as durable operator evidence.
Neither value appears in responses, errors, tracing, or ordinary logs. Audit records contain no credentials,
tokens, endpoints, request keys, message bodies, or backend error text. Existing version-1 and mixed-version
JSONL is recovered in place and is never rewritten; new writes are version 2 only.

To stop new mutations, set `mutations_enabled=false` and restart. Removing an operation or cluster from the
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

This is intentionally an operational sequence, not an approval system: it adds no plan hash, fingerprint,
signature for a change plan, or release gate.

## Rollback and result handling

Keep the complete pre-change state from the dry-run/result before execution. Use a new typed restore request
with the normal explicit confirmation and reason only when that Tool can express the recorded `before` state.
Topic and Consumer Group creation has no delete Tool, an offset reset has no exact per-queue restore, and an
absent Consumer request mode cannot be restored through a set operation. For those cases, preserve the
evidence and use a separately authorized operational path; do not invent a generic compensating command,
CLI, RPC, or delete operation. Do not reuse a request key with different content.

| Result or code | Meaning | Required operator action |
| --- | --- | --- |
| `planned` | Dry-run produced a sealed candidate without mutation. | Review it; no rollback is needed. |
| `applied` | Every target verified, including `changed=false` idempotent success. | Record the terminal audit evidence and continue normal monitoring. |
| `precondition_conflict` / `conflict` | Sealed state changed before the conditional write. | Re-run dry-run, compare new state, and choose a new explicit action; never automatic-retry. |
| `partial_apply` / `partial` | Some targets persisted or verified and others did not. | Treat returned `targets`, `before`, `after`, persistence, and verification fields as source of truth; reconcile each target explicitly before retrying. |
| `verification_failed` | A write may have persisted but complete post-read verification or order reconciliation failed. | Preserve the result, inspect the affected logical target using an authorized operational path, then restore or complete the intended state with a new typed request. |
| `audit_unavailable` | Durable audit recovery, append, or timeout failed. | Stop mutation attempts and repair/recover the audit sink before retrying. A failed `started` record means no session/RPC began. |
| `timeout`, `cancelled`, or `shutdown_failed` | The request/session lifecycle did not reach a normal terminal path. | Wait for/reconcile the durable terminal audit state and target state before any new request. |
| `execution_failed` or `operation_unavailable` | The typed operation/session cannot complete or is not registered. | Correct deployment/runtime policy or the underlying authorized service condition; do not substitute a CLI, shell, or arbitrary RPC. |

For rollback of server policy rather than a RocketMQ resource, restore the prior configuration file and restart
the Control process. After restart, repeat authenticated `tools/list` and capability-resource verification;
configuration edits do not take effect in a running process.

## Emergency stop-write procedure

Use this procedure for an immediate policy stop. It stops new mutation Tool registration; it does not erase
audit records or mutate any RocketMQ state.

1. Edit the active Control configuration and set `mutations_enabled=false`. Keep `dry_run=true`; reducing
   `allowed_operations` and `allowed_clusters` is optional defense in depth, not a substitute for disabling
   mutations.
2. Restart the Control process. There is no hot reload for this setting.
3. With an authenticated request, call `tools/list`. Verify that no reviewed write Tool is present:
   `rocketmq_upsert_topic`, `rocketmq_upsert_consumer_group`, `rocketmq_reset_consumer_offset`,
   `rocketmq_patch_broker_config`, and `rocketmq_set_consumer_request_mode` must all be absent.
4. Read `rocketmq-control://capabilities`. Verify
   `mutations_runtime_enabled=false`, `registered_operations=0`, and `mutation_supported=false`.
   In a `write-tools` binary, `write_tools_compiled` can remain true; compile availability is intentionally
   distinct from runtime enablement and registration.
5. Preserve and inspect existing durable audit records. Reconcile any already-started, partial, failed, or
   verification-incomplete operations through the returned target evidence before permitting new writes.

To restore writes, reverse the configuration change only after the incident is resolved: set
`mutations_enabled=true`, restore only the necessary closed operation/cluster allowlists, restart, verify the
capability fields and intended `tools/list` catalog, then begin again with dry-run. There is no emergency
override, generic delete Tool, shell, or free-form RPC.
