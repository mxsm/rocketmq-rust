# Phase 02 diagnostic evidence sources

P2-02 extends the Phase 01 read-only Connector without adding a second runtime
endpoint or a mutation-capable client. Every adapter emits canonical
`EvidenceSnapshot` data with an explicit `exposure`, bounded content,
freshness, coverage, partial state, stable warnings, and content hash.

## Query surface

| Source | Resources | Read boundary |
| --- | --- | --- |
| `prometheus` | `instant/<metric>`, `range/<metric>`, `trend/7d/<metric>`, `trend/30d/<metric>` | Fixed metric name plus mandatory cluster matcher; arbitrary PromQL is rejected |
| `alertmanager` | Active alert summaries | `GET /api/v2/alerts`; annotations, routing URLs, and unknown labels are dropped |
| `kubernetes` | Pods, Nodes, Events, Deployments, StatefulSets, PVCs, PDBs, Certificates, change timeline | Kubernetes `GET` only; workload queries use the cluster label and Events use the configured namespace boundary |
| `runtime` | Runtime and observability views | Existing authenticated MCP System Resources only |
| `admin-query` | Broker, Store, RocksDB, Tiered Store, and Auth diagnostics | Versioned `BrokerQueryAdmin` DTO backed by read-only Admin RPC |
| `prometheus` diagnostic projections | Proxy and Remoting summaries | Fixed allowlisted metrics; unavailable metrics become missing signals |

Prometheus trend resources use exact 7-day or 30-day windows. Range responses
are projected into finite numeric samples with allowlisted labels and bounded
row counts. The Connector never forwards the raw Prometheus response to rules.

Kubernetes deployment evidence keeps rollout counts, generations, conditions,
safe configuration summaries, feature flags, release metadata, and image
digests. It omits image repositories, environment values, Secret references,
node addresses, Event messages, and reporting identities. Certificate evidence
contains readiness and expiry/renewal timestamps but no Secret name, PEM
material, or issuer identity.

## Component diagnostics

The Broker publishes `rocketmq.broker-diagnostics.v1` through its existing
runtime-info read RPC. `rocketmq-admin-core` converts only known keys into
`rocketmq.admin-broker-diagnostics.v1`; unknown keys and the backing `KVTable`
cannot cross the DTO boundary.

The DTO includes:

- Broker readiness, shutdown and registration state.
- The real atomic runtime configuration generation and its matching
  configuration summary.
- Store health, recovery summary, and background index rebuild state.
- RocksDB maintenance and Tiered Store dispatch readiness.
- Authentication/authorization readiness, ACL generation, and reload counters.

Credential rotation is explicitly reported as `not_production_verified` until
a runtime-owned rotation source exists. Proxy receipt handles, prepared
transactions, admission internals, and Remoting pool/circuit internals follow
the same rule: a missing process-only signal is never represented as zero or
healthy.

Runtime diagnostics reuse the existing MCP resources:

- `rocketmq://system/runtime/v1`
- `rocketmq://system/observability/v1`

The runtime view exposes bounded TaskGroup/task summaries and Blocking lane
maximum concurrency, queue capacity, running, queued, and timeout counts. Raw
task names, arguments, configuration, credentials, and message content remain
excluded.

## Configuration and failure behavior

Optional backends are configured with:

- `ROCKETMQ_SRE_PROMETHEUS_URL`
- `ROCKETMQ_SRE_ALERTMANAGER_URL`
- `ROCKETMQ_SRE_LOKI_URL`
- `ROCKETMQ_SRE_TEMPO_URL`
- the existing read-only Admin and Kubernetes environment settings

An unavailable optional backend produces missing Evidence and a stable warning;
it does not abort the incident diagnostic run. Unsupported schema major
versions fail closed. The Connector dependency graph continues to enable only
`read-client-adapter`, and Kubernetes access is limited to `get` and `list`
RBAC verbs.
