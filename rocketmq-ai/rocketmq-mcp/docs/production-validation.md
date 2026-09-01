# RocketMQ MCP production validation

This document is a release-gate checklist for operating `rocketmq-mcp`. It is not a production-readiness
certificate. Local compilation, unit tests, contract snapshots, and in-process HTTP tests do not prove that a
RocketMQ deployment is reachable, correctly secured, or ready for an operator workload. Record live-cluster
evidence separately and keep the limitation next to the result.

The checked-in implementation and local evidence for this revision are listed in
[implementation-baseline.md](implementation-baseline.md).

## Deployment boundaries

### stdio

Use stdio for a local desktop MCP client. The process reads and writes MCP protocol frames on standard streams;
application logs go to stderr when `server.stdio.log_to_stderr = true`. Stdio uses the local identity derived from
`security.profile` and is intended for a trusted local process. It is not a network authentication boundary and
must not be exposed through a pipe, wrapper, or service that accepts untrusted remote input.

### Streamable HTTPS

Streamable HTTPS is compiled only with the `streamable-http` feature. The listener is TLS-enforcing, requires a
certificate and private key, applies an HTTP request-body limit of 1 MiB and a 30-second request timeout, and
validates configured hosts and browser origins. The MCP endpoint is protected by authentication; the OAuth
protected-resource metadata path remains unauthenticated for client discovery.

The example configuration binds to loopback. `development-token` authentication is valid only for reviewed
loopback development. It must not be used as production authentication or with a non-loopback bind address.
Production HTTPS must use `oauth-jwt`, an absolute HTTPS issuer, an absolute HTTPS JWKS URL, RS256, and a
non-empty required-scope set. The server validates the issuer, audience, expiry, `sub`, `kid`, signature, and
required scopes, and never forwards the incoming bearer token to RocketMQ.

The MCP process is outside the broker, NameServer, store, and dashboard runtime paths. It queries RocketMQ through
the read-only admin adapter and does not provide a mutation or Apply endpoint.

## External dependencies and test data

Before a live-cluster smoke test, provide all of the following from the environment in which the MCP process will
run:

- A reachable RocketMQ NameServer address in `clusters[].namesrv_addr`, plus a reachable broker for every operation
  being tested.
- A least-privilege RocketMQ request-signing identity. Configure it by a bounded mounted YAML file or paired
  access-key/secret-key environment references; do not put secret values in TOML, command lines, logs, or tickets.
- A trusted server certificate and private key for HTTPS. If the issuer uses a private CA, provide a readable PEM
  bundle through `server.http.auth.jwks_ca_path`.
- A production OAuth issuer, audience, HTTPS JWKS endpoint, RS256 signing key with a `kid`, and refresh/staleness
  windows. The issuer and JWKS endpoint must be reachable from the MCP process.
- A test principal with the intended roles, scopes, optional exact `rocketmq_tenant`, and optional
  `rocketmq_clusters` allow-list.
- A non-sensitive test cluster, topic, consumer group, and broker name that are known to exist. The external test
  also requires `ROCKETMQ_MCP_E2E_NAMESRV_ADDR`, `ROCKETMQ_MCP_E2E_TOPIC`, `ROCKETMQ_MCP_E2E_CONSUMER_GROUP`, and
  `ROCKETMQ_MCP_E2E_BROKER`.

Do not substitute a successful local test for any missing dependency. If one is unavailable, mark the live check
blocked or not run and retain the reason.

## Security and bounded-output gate

Confirm each control in the deployed configuration before exposing the endpoint:

| Control | Required production condition | Evidence to retain |
| --- | --- | --- |
| TLS | `server.http.tls.cert_path` and `key_path` resolve to the intended certificate and key; the listener rejects plaintext. | Startup log with the certificate generation (without key material) and an HTTPS-only probe. |
| Authentication | `server.http.auth.mode = "oauth-jwt"`; development tokens are not used. | Redacted configuration review and probes for missing, invalid, expired, wrong-issuer, wrong-audience, wrong-`kid`, and wrong-algorithm tokens. |
| JWKS | HTTPS JWKS is reachable, contains bounded RS256 verification keys, and refresh failure retains only a verified last-known-good generation within the configured stale window. | Issuer/JWKS health evidence and key-rotation test, with tokens and key material omitted. |
| RBAC | `security.permissions_file` defines the intended roles and tool patterns. Claims are intersected with policy rather than trusted as an unrestricted grant. | Policy review plus allowed and denied tool/resource probes. |
| Scope | Read-only, diagnosis, and planning operations require `rocketmq:read`, `rocketmq:diagnose`, and `rocketmq:plan`, respectively. | Principal claims and redacted authorization results. |
| Tenant and cluster boundary | A configured `clusters[].tenant` requires an exact `rocketmq_tenant` claim; `rocketmq_clusters` and role allow-lists both constrain cluster access. | Matching and mismatching tenant/cluster probes. |
| Rate and concurrency | Per-principal, per-cluster, per-operation rate limits and the configured per-cluster concurrency bound are sized for the workload. | 429/denial probe and saturation result; do not increase limits solely to hide client retry problems. |
| Redaction | `security.sanitize_output = true` in production. Internal addresses, credentials, bearer tokens, and sensitive assignments do not leave through tool/resource output or audit records; audit records may contain bounded principal and client identifiers for accountability. | Sanitized sample output and audit record review. |
| Row and byte bounds | Arrays are capped at 1,000 rows and structured output at 1 MiB. Row truncation reports `partial = true` and `output_rows_truncated`; oversized output returns `output_too_large`. HTTP request bodies are also capped at 1 MiB. | Boundary probes that verify the stable warning/error without retaining sensitive payloads. |
| Audit | Keep `audit.enabled = true`; choose `file` or `tracing` for durable operations evidence and size the count and byte queues deliberately. Records are redacted, bounded, and drained FIFO during shutdown. | Sink-health, accepted/written/drop, pending-record, and flush evidence. |
| Cache and visibility | Every verified request maps to the closed `standard` or `sensitive` visibility class. Read-only HTTP principals and local read-only stdio profiles use `standard`; diagnosis/planning principals and local diagnose/operator profiles use `sensitive`. Cache keys include only the stable class name, schema version, query kind, resolved cluster, and normalized parameters. Ordinary entries, snapshots, cursors, and singleflight work are shared within a class and isolated across classes. Principal, tenant, role, scope, client, and bearer-token values are not retained in query state. Failures are not cached; TTLs and capacity are reviewed for the workload. | Allowed Tool and live Resource probes for both classes; same-class hit/coalescing evidence; cross-class miss and cursor-context rejection without a backend reload; query-state review with identity and credential values omitted. |
| ResourceLink compatibility | A Resource-backed Tool adds a link only for a canonical, safely representable target. Existing Tool-compatible targets outside the Resource identifier contract retain their Tool success/error and data behavior and omit only the link; genuinely sensitive target values remain sanitized. Discovery ignores configured cluster names that cannot be represented as closed logical aliases. | Probe a safe target, benign unrepresentable `.`, `:`, and overlength targets, IP/socket targets, raw and encoded assignments, plus unsafe-only and mixed safe/unsafe cluster configurations. Retain link counts, unchanged non-target fields, sanitized output/audit, discovery counts, and cursor continuity. |
| Mutation boundary | Keep the RocketMQ user least-privileged and retain the `read-client-adapter` dependency boundary. No Apply path, mutation feature, or mutation admin API is part of the MCP binary. | Read-only boundary check and dependency review. |

## Validation stages

### 1. Build and static checks

From the repository root, change to `rocketmq-ai/rocketmq-mcp`. Run the commands in the evidence table in
[implementation-baseline.md](implementation-baseline.md). A successful local command proves only the property
tested by that command and the checked-in revision; it does not qualify a live cluster.

### 2. Configuration review

Start with `conf/mcp.example.toml`, then use a copied, access-restricted configuration for a real deployment.
Check that:

1. Relative permission, TLS, JWKS CA, audit, and credential paths resolve from the configuration file's directory,
   not from an incidental caller working directory.
2. Inline credentials and mixed file/environment credential sources are rejected.
3. Production HTTP uses an HTTPS public origin and `oauth-jwt`; development-token is loopback-only.
4. The permission file allows only the intended tools and clusters, and planning remains disabled unless the
   feature, runtime opt-in, policy, and principal scope are all intentionally enabled.
5. Audit and cache capacities, TTLs, and diagnosis thresholds are explicit and reviewed.
6. The same verified principal maps to the same visibility class for Tool and live Resource requests; a `standard` cursor is rejected in `sensitive` context, and conversely, without a RocketMQ query.

### 3. Startup checks

For stdio, start the binary with `--config conf/mcp.example.toml --transport stdio` and verify that no wrapper or
logger writes anything except MCP frames to stdout. For HTTPS, build with `streamable-http`, start with the copied
configuration, and verify all of the following before sending MCP traffic:

- certificate and key validation succeeds and a certificate generation is active;
- OAuth JWKS warm-up succeeds in production mode;
- the protected-resource metadata path returns metadata without a bearer token;
- the MCP endpoint rejects missing or invalid authentication;
- the listener is reachable only through the intended TLS and network boundary.

Startup failure is fail-closed. Do not bypass certificate, issuer, JWKS, or authorization errors to get a smoke
request through.

### 4. MCP smoke checks

Use a test principal and non-sensitive test data. Verify initialization with protocol version `2025-11-25`, then
check discovery and execution in this order:

1. `tools/list`, `resources/list`, `resources/templates/list`, and `prompts/list` show only the caller-authorized
   surface.
2. The 24 default read-only/diagnosis tools listed in the implementation baseline return the `rocketmq-mcp.v2`
   envelope or a stable sanitized error. The all-features catalog contains those 24 plus five non-mutating plans.
3. The five planning tools appear only when compiled and policy-enabled; every result has `mutates_cluster = false`.
   There is no Apply request to test.
4. Read the five cluster roots, all ten parameterized forms (including broker diagnostics/configuration, Topic
   statistics/configuration, and Consumer Group progress), and the two system resources only when the principal has
   the required authorization. Verify unknown, incomplete, unauthorized, noncanonical, and unsupported forms fail
   closed without revealing target existence.
5. Verify the cache status and freshness fields, bounded rows/bytes, redaction, correlation IDs, and corresponding
   audit records.
6. For every Resource-backed Tool family, verify a canonical safe target returns the exact ResourceLink. Verify Tool-
   compatible but Resource-unrepresentable `.`, `:`, and non-sensitive overlength targets keep the same Tool result,
   evidence, schema version, and RFC 3339 time while returning no link. Verify IP/socket and raw/percent-encoded
   assignment targets return no link and expose no sensitive value in content, structured data, errors, or audit.
7. With only unsafe configured cluster names, verify cluster Resource/template/Prompt discovery is empty while
   authorized system Resources remain available. With mixed safe/unsafe names, verify only safe cluster roots are
   listed, all otherwise-authorized templates and Prompts remain available through the safe cluster, cursors/counts
   match the safe-only configuration, and direct unauthorized/invalid reads keep the same oracle-safe envelope.

The checked-in external-cluster integration test covers the first eight tools and a parameterized resource when
run with all four `ROCKETMQ_MCP_E2E_*` variables. The exact command in the evidence table uses Cargo's default
features only (`read-only`, `diagnose`, and `stdio`); it is ignored by default and remains a separate live-cluster
qualification gate. The all-features local suite also reports the test as ignored, but no all-features live command
was attempted here.

### 5. Shutdown and rollback

Request a normal process shutdown after a smoke run. Confirm that admission closes, accepted audit records drain in
FIFO order, the sink flushes, query/transport tasks stop before the common deadline, and no pending records or
bytes remain. Treat a timed-out or unhealthy audit drain as a failed operational check.

For rollback, stop the MCP process, preserve the failing evidence, restore the last known-good binary and copied
configuration, and restart only after rechecking certificate, issuer/JWKS, permissions, cluster identity, and
credential references. Re-run initialization, authorization, one read-only query, one resource read, and shutdown.
Because the MCP surface has no Apply path, rollback is a process/configuration rollback; it does not undo a
cluster mutation.

## Troubleshooting

| Symptom | Checks |
| --- | --- |
| HTTPS listener will not start | Verify absolute HTTPS `public_base_url`, loopback/network policy, certificate/key readability, and certificate validity. |
| 401 or bearer challenge | Verify `Authorization: Bearer`, RS256 signature, `kid`, issuer, audience, expiry, required scopes, and JWKS reachability. |
| 403 | Check role includes, `allow_tools`/`deny_tools`, required scope, exact tenant, `rocketmq_clusters`, configured cluster name, and allowed origins. |
| 429 | Inspect per-principal rate and per-cluster concurrency limits, then inspect caller retry behavior. |
| No cluster data | Verify NameServer reachability, broker reachability, cluster name resolution, least-privilege RocketMQ credentials, and that the test data exists. |
| Empty or invalid stdio response | Ensure logs and banners go to stderr and that the wrapper does not write to stdout. |
| JWKS rotation or refresh failure | Verify HTTPS CA trust, key `kid`/algorithm, refresh and stale windows, and last-known-good behavior; do not install a static-key fallback. |
| Output is partial or rejected | Inspect `partial`, `warnings`, `output_rows_truncated`, and `output_too_large`; reduce query scope rather than removing bounds. |
| Audit file or drain failure | Check directory permissions, sink health, count/byte drops, oversized records, flush failures, and pending records/bytes. |

## Evidence record

The following command-level evidence was captured on 2026-09-01 from the uncommitted Issue #9955 worktree based on
`3b7cad90d53b2bf134dbf7289056fd19c978b372`. The working directory is relative to the repository root. The local
environment had no reachable RocketMQ test cluster and no `ROCKETMQ_MCP_E2E_*` variables.

| Exact command | Working directory | Feature set | Status | Environment | Limitation |
| --- | --- | --- | --- | --- | --- |
| `cargo fmt --all -- --check` | `rocketmq-ai/rocketmq-mcp` | Cargo defaults | **failed (exit 1)** | Windows local checkout | Reported `文件名或扩展名太长。 (os error 206)` and printed usage before formatting. |
| `cargo fmt -p rocketmq-mcp -- --check` | `rocketmq-ai/rocketmq-mcp` | Package `rocketmq-mcp` only | **passed (exit 0)** | Issue #9955 worktree on Windows | This narrower check does not replace the failed mandatory all-package formatter command; it validates every Rust file in the MCP package. |
| `cargo check --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | No cluster connection was attempted. |
| `cargo check --locked --all-features` | `rocketmq-ai/rocketmq-mcp` | All declared features | **passed (exit 0)** | Windows local checkout | Validates the 29-Tool feature combination without a live cluster. |
| `python scripts/check_read_only_boundary.py` | `rocketmq-ai/rocketmq-mcp` | Metadata/dependency closure | **passed (exit 0)** | Windows local checkout | Printed `MCP read-only dependency boundary passed`; this is not live-cluster evidence. |
| `cargo test --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0): 268 passed, 0 failed, 1 ignored** | Windows local checkout; `RUST_MIN_STACK` and `INSTA_UPDATE` unset | 259 library + 5 binary + 1 compatibility + 3 non-E2E integration tests passed; doc-tests reported 0. |
| `cargo test --locked --all-features` | `rocketmq-ai/rocketmq-mcp` | All declared features | **passed (exit 0): 313 passed, 0 failed, 1 ignored** | Windows local checkout; `RUST_MIN_STACK` and `INSTA_UPDATE` unset | 304 library + 5 binary + 1 compatibility + 3 non-E2E integration tests passed; doc-tests reported 0. |
| `cargo clippy --locked --all-targets --features streamable-http -- -D warnings` | `rocketmq-ai/rocketmq-mcp` | `streamable-http` plus defaults | **passed (exit 0)** | Windows local checkout | No warning was accepted. |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | `rocketmq-ai/rocketmq-mcp` | All declared features | **passed (exit 0)** | Windows local checkout | No warning was accepted. |
| `cargo doc --locked --no-deps` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | Generated docs successfully; this does not exercise a cluster. |
| `git diff --check` | repository root | Not applicable | **passed (exit 0)** | Windows local checkout | Whitespace check only; it does not qualify a deployment. |
| `cargo test --locked --test integration external_cluster_exercises_mvp_tools_and_resources -- --ignored` | `rocketmq-ai/rocketmq-mcp` | **Default features only**: `read-only`, `diagnose`, `stdio` | **not run** | Required external variables and a reachable cluster were absent | This exact command is not an all-features run; it remains a separate live-cluster qualification gate. |

The mandatory all-package formatter failed in this worktree with `文件名或扩展名太长。 (os error 206)`. The
package-scoped formatter passes, but it does not change the failed mandatory gate.

The complete baseline, including test-suite breakdown and snapshot links, is in
[implementation-baseline.md](implementation-baseline.md). These local results do not imply that OAuth, TLS,
JWKS, RBAC, RocketMQ connectivity, or production rollback has passed.

## Evidence status vocabulary

Use these values in release records:

- **passed**: the exact command or probe completed successfully in the named environment.
- **failed**: it ran and returned a failure; retain the exact error and revision.
- **ignored**: the test harness deliberately did not run the test, normally because it is marked ignored.
- **skipped**: an operator intentionally did not run a possible check.
- **blocked**: a required dependency or authorization was unavailable.
- **environment-dependent**: the result is valid only for the named local toolchain, OS, configuration, or cluster.
- **not run**: no attempt was made.

Never convert a passed local check into a live-cluster or production-readiness claim.
