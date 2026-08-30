# rocketmq-sre-connector

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../../LICENSE-APACHE)

`rocketmq-sre-connector` is the read-only integration boundary between the AI
SRE control plane and a managed RocketMQ environment. It authenticates with
OAuth2 client credentials, establishes a TLS-protected MCP Streamable HTTP
session, verifies the advertised capability surface, and converts validated
wire responses into canonical Evidence.

## Responsibilities

- Perform the fail-closed MCP protocol, business-schema, tool-digest, tenant,
  scope, and cluster-allowlist handshake.
- Actively register with the control plane over a versioned mTLS HTTP/2 reverse
  channel and handle heartbeat, query, cancellation, response, and bounded
  inventory upload commands.
- Query MCP, the compile-time read-only Admin adapter, Prometheus, Loki, Tempo,
  Kubernetes metadata, the Phase 00 Runtime diagnostics contract, and topology.
- Enforce deadline/cancellation, tenant and cluster scope, time range, label
  allowlist, concurrency/rate, row/byte, sanitization and short-cache bounds.
- Expose the legacy protected evidence API only when the reverse channel is not
  configured. Normal Phase 01 mode has no inbound evidence endpoint.
- Refresh an expired token once for an idempotent read request.
- Project supported Wave A resources from their validated MCP, Admin,
  Prometheus, Kubernetes, Runtime, or topology wire contracts. Fields without a
  real read source are omitted and the snapshot is marked partial; unsupported
  resources remain explicitly `not_production_verified`.

## Read-only boundary

- MCP is consumed only through Streamable HTTP; this crate does not import the
  MCP server crate or its Rust DTOs.
- The Admin dependency enables only `read-client-adapter`; the dependency graph
  contains neither the mutation adapter nor the full compatibility adapter.
- A server advertising mutation support, an unknown schema major, or capability
  drift is rejected.
- There is no anonymous fallback, cross-cluster scope expansion, write Tool, or
  RocketMQ management implementation.
- Message bodies and Kubernetes workload specifications are never collected.
  Message IDs, keys and trace IDs are pseudonymized; credential and address
  fields are removed before canonical Evidence capture.
- Inventory upload is initiated only by the connector through its registered
  session path. Every snapshot is row/byte/query bounded and carries
  `source`, `observed_at`, `freshness_seconds`, `partial`, and an embedded
  `rocketmq-sre.inventory-coverage.v1` matrix. Missing sources remain
  `not_production_verified`; no placeholder connection or topology edge is
  invented.

## Phase 01 inventory and topology

The inventory collector prefers public MCP read tools and falls back to the
compile-time read-only Admin adapter only for the same query. Kubernetes
metadata is supplementary and is never used to guess a RocketMQ identity.

| Asset | Verified source | Behavior when unavailable |
| --- | --- | --- |
| Broker, embedded Store | MCP cluster overview or Admin broker list | `not_production_verified` |
| Topic | MCP/Admin bounded topic list | `not_production_verified` |
| Queue | MCP topic description or Admin topic route | `partial` or `not_production_verified`; queue IDs are materialized only from the observed RocketMQ route count |
| Consumer | MCP/Admin consumer-group list and observed topic association | `partial` or `not_production_verified` |
| NameServer, Controller, Proxy | Kubernetes Pod with the repository's `rocketmqrust.com/service` label | `not_production_verified` |
| Pod, Node, PVC, PDB | Kubernetes core/policy read APIs | `not_production_verified` |
| Producer, per-client Connection | Read-only Admin broker producer table and consumer-connection query | `partial` or `not_production_verified`; raw client IDs and addresses are replaced with a tenant/cluster-scoped keyed pseudonym |

`Topic -> Queue -> Broker -> Store` is formed from RocketMQ route/runtime
observations. It is extended to `Pod -> Node/PVC` only when Kubernetes supplies
the matching metadata. A Broker Pod joins the RocketMQ logical Broker only when
it has an explicit `rocketmqrust.com/broker-name` label; otherwise the Pod
component remains a partial, Pod-scoped asset. The
`Producer/Consumer -> Connection -> Broker` path is formed only from a
successful read-only Admin observation that names both the client and Broker.
The connector derives one stable tenant/cluster-scoped connection pseudonym
from the raw identity and discards the raw client ID and address before
creating inventory assets or canonical evidence. When Admin is disabled,
unavailable, truncated, or returns per-Broker failures, coverage and warnings
state that limitation explicitly; no placeholder connection or edge is
fabricated.

At most 32 topic/consumer detail queries are issued per inventory pass. Asset
and edge collections are deterministically truncated to the configured row
budget. Client-connection collection additionally caps each pass at 200 rows
and 32 consumer groups. Dangling edges are removed, and the encoded upload is
capped at 512 KiB even when a larger general response limit is configured.

## Phase 01 configuration

When `ROCKETMQ_SRE_CONTROL_PLANE_URL` is set, HTTPS endpoints also require
`ROCKETMQ_SRE_CONTROL_PLANE_CA_PATH` and
`ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH`. The identity file is a
combined PEM certificate/private-key identity. Plain HTTP is accepted only for
a loopback development endpoint and still uses HTTP/2 prior knowledge.

Optional evidence sources are enabled with:

- `ROCKETMQ_SRE_ADMIN_NAMESRV_ADDR` for the read-only Admin adapter.
- `ROCKETMQ_SRE_PROMETHEUS_URL`, `ROCKETMQ_SRE_LOKI_URL`, and
  `ROCKETMQ_SRE_TEMPO_URL` for observability backends.
- `ROCKETMQ_SRE_KUBERNETES_API_URL`,
  `ROCKETMQ_SRE_KUBERNETES_TOKEN_PATH`,
  `ROCKETMQ_SRE_KUBERNETES_CA_PATH`, and a fixed namespace for Kubernetes
  Pod/Event/Node/PVC/PDB metadata. The identity must have only `get`/`list`
  access to those resources. Every query includes the mandatory
  `rocketmqrust.com/cluster=<cluster>` selector.

The Kubernetes token path must be an absolute path inside its dedicated
read-only projected-volume mount. The Connector retains that configured path
and performs a bounded metadata-I/O read for every request, so kubelet's
atomic `..data` symlink rotation is observed without restarting the process.
The resolved target must remain under the configured mount and be a regular
file no larger than 64 KiB. A static
`ROCKETMQ_SRE_KUBERNETES_TOKEN_ENV` token is accepted only when
`ROCKETMQ_SRE_KUBERNETES_ALLOW_ENV_TOKEN=true` and the Kubernetes API endpoint
is loopback; that mode is for explicit local development only.

The default label allowlist includes `rocketmqrust.com/cluster`,
`rocketmqrust.com/service`, `rocketmqrust.com/broker-name`, and the
standard `app.kubernetes.io/*` identity labels. Custom allowlists must retain
the cluster label or Kubernetes collection fails closed.

`ROCKETMQ_SRE_SOURCE_MAX_ROWS`,
`ROCKETMQ_SRE_MAX_RESPONSE_BYTES`,
`ROCKETMQ_SRE_SOURCE_MAX_TIME_RANGE_SECONDS`,
`ROCKETMQ_SRE_SOURCE_MAX_REQUESTS_PER_MINUTE`,
`ROCKETMQ_SRE_MAX_CONCURRENCY`, and
`ROCKETMQ_SRE_SOURCE_LABEL_ALLOWLIST` define fail-closed collection budgets.

## Validation

Run from `rocketmq-ai/rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-connector
cargo test --locked -p rocketmq-sre-connector
cargo clippy --locked -p rocketmq-sre-connector --all-targets -- -D warnings
```

The tests cover capability verification, wire-schema validation, bounded
responses, OAuth2 recovery, tenant isolation, reverse-channel isolation,
cancellation, pseudonymization, Kubernetes projection, the verified
Topic/Queue/Broker/Store/Kubernetes path, real pseudonymous
Producer/Consumer-to-Connection-to-Broker edges, explicit unavailable-source
coverage, and offboarded-cluster rejection.
