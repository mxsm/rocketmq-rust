# Phase 00 Kind acceptance environment

This directory is a bounded local acceptance overlay. It reuses the canonical
`distribution/helm/rocketmq-rust/values-dev-single.yaml` profile and locally
loaded images. It is not a production Helm chart and does not replace the
production Kubernetes assets under `distribution/`.

The runner uses the repository-pinned versions:

- Kind `v0.27.0`
- Kubernetes `v1.32.2`
- Helm `v4.2.3`
- `kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f`

PostgreSQL runs inside the cluster with an ephemeral `emptyDir`. The overlay
also starts the SRE Control Plane and UI, places the Connector beside MCP as a
loopback-only sidecar, and adds OTel Collector, Prometheus, Loki, and Tempo.
The Broker uses a 10 GiB `standard` StorageClass PVC named
`data-rocketmq-broker-0`. Its StatefulSet retains the claim across Pod
replacement so the DR acceptance can verify message-history RPO and RTO.
This single-node local-path fixture does not claim node-loss, Kind-cluster
recreation, replicated commit-log, or production backup/restore coverage.
The development Control Plane alone mounts a private 1 GiB `emptyDir` for
large Evidence objects. It survives a Control Plane container restart within
the Pod, but is intentionally ephemeral with the Kind cluster; production
deployments must configure the HTTPS S3-compatible backend instead.
The Control Plane Pod has a non-root mTLS Nginx sidecar on `8444`. The
Connector trusts a dedicated Control Plane server CA and presents a separate
combined client identity. Nginx validates the client certificate, derives and
overwrites the connector subject/issuer headers, and proxies only the bounded
Connector POST surface to Axum on loopback. A NetworkPolicy allows the
Connector Pod to reach `8444` but prevents it from bypassing the proxy on
`8090`. The public listener does not mount Connector routes; the sidecar is the
only process that can reach the separate `127.0.0.1:8093` upstream.
Development credentials are generated into `target/phase00-kind`; the shared
RocketMQ and Connector-channel certificates are generated into
`target/phase00-certs`. They are not production credentials.
The runner creates separate MCP reader, Agent reader, Agent mutation, bounded
Probe, and one-shot bootstrap identities. The MCP Pod receives only its reader
credential secret; the Broker ACL and bootstrap credential remain in the
RocketMQ namespace secret. The Agent receives its two identities through
individual Secret keys in the SRE namespace. Neither reader can perform Topic,
Group, or cluster mutations, and only the Execution Agent receives the mutation
identity.
Although MCP and Connector share a Pod, the Connector mounts a separate Secret
projection containing only MCP's public `ca.crt`; it cannot read MCP's TLS
private key, admin identity, request policy, or RocketMQ reader credential
file. Its RocketMQ Admin source receives the same read-only RocketMQ identity
through individual Secret keys, without mounting MCP's credential file.

The Connector uses the dedicated `rocketmq-sre-connector` ServiceAccount with
automatic token mounting disabled. A bounded projected volume supplies only a
rotating Kubernetes API token and the cluster CA. Namespaced RBAC permits
`get`/`list` for Pods, Events, PVCs, and PDBs in `rocketmq-system`; cluster
RBAC permits only `get`/`list` for Nodes. It has no watch, Secret access, or
mutation verb. The token is reread on every bounded Kubernetes request, so
projected-token rotation does not require a Pod restart.
The runner also keeps a dedicated kubeconfig there and passes it explicitly to
Kind, kubectl, and Helm, so it does not replace the user's current kubectl
context.

From the repository root:

```powershell
.\rocketmq-sre\scripts\kind.ps1 -Action Up
.\rocketmq-sre\scripts\kind.ps1 -Action Status
.\rocketmq-sre\scripts\kind.ps1 -Action Smoke
.\rocketmq-sre\scripts\kind.ps1 -Action Down
```

The cluster does not publish host ports. To inspect the read-only UI:

```powershell
kubectl --kubeconfig .\target\phase00-kind\kubeconfig `
  --context kind-rocketmq-sre-phase00 `
  --namespace rocketmq-sre `
  port-forward service/sre-ui 3004:3004
```

The internal SRE ports are Control Plane API `8090`, Connector mTLS `8444`,
Connector-only loopback upstream `8093`, Connector diagnostics `8091`, Change
Executor `8094`, Execution Agent `8095`, and UI `3004`; MCP remains
loopback-only on `8089` inside its Pod. The Phase 01 model
fixture is a separate ClusterIP-only workload on `8094`. Its NetworkPolicy
allows ingress only from the Control Plane and denies all egress.

Broker, NameServer, Controller, and Proxy each expose the fixed
`/internal/v1/runtime/diagnostics` contract on port `8087`. This listener is
separate from `/livez` and `/readyz`, requires both the mounted bearer
credential and the `rocketmq:diagnose` scope, returns only the bounded
`RuntimeDiagnosticsViewV1`. When chart network policies are enabled, the chart
renders a Connector-only ingress policy for this listener. The local Kind
profile validates bearer/scope enforcement but does not claim CNI-level
NetworkPolicy enforcement. It explicitly opts into non-loopback HTTP for this
local fixture. Production deployments must terminate TLS before the listener
and production-verify credential rotation and multi-replica discovery.

After `kind.ps1 Up`, validate the four protected endpoints and all six required
Runtime metric families with:

```powershell
$env:ROCKETMQ_SRE_KIND_KUBECONFIG = `
  (Resolve-Path .\target\phase00-kind\kubeconfig)
.\rocketmq-sre\scripts\phase00-runtime-diagnostics-smoke.ps1
```

The smoke rejects artifact and kubeconfig locations outside D: or F:, never
prints the bearer credential, and writes only a sanitized JSON report plus
`kubectl port-forward` logs below
`target/phase00-runtime-diagnostics-smoke`.

The Executor and Agent run as separate Deployments with different
ServiceAccounts. NetworkPolicy permits only Control Plane → Executor,
Executor → Control Plane Lease Authority, and Executor → Agent. Executor has no
target namespace/Kubernetes API path and mounts no target credential. Only the
Agent ServiceAccount is bound to the closed workload mutation Role; the Agent
still requires an active epoch grant and durable shared/exclusive PostgreSQL
barrier before invoking any registered handler. The Kind test profile explicitly
enables the generation-checked Broker configuration adapter and the
version-checked Topic configuration adapter against `rocketmq-namesrv:9876`;
their read and mutation access keys are distinct. Topic precheck resolves every
route Broker and fails closed on configuration/version drift. A production
deployment must additionally use TLS and production secret delivery.

`Up` builds and loads all local RocketMQ/SRE images. Use `-SkipBuild` only when
every required image already exists in the local Docker engine. Re-running
`Up` reuses the generated credential fixtures and replays the idempotent
onboarding Job, so live Pods and PostgreSQL keep the same credentials. If an
existing cluster has only a partial fixture set, the runner fails closed and
requires `Down` before regeneration. `Smoke`
verifies workload readiness, performs real Prometheus/Loki/Tempo queries,
requires non-empty RocketMQ service metrics, log entries, and MCP traces,
checks the persisted `ready_read_only`/`read_only` onboarding state, validates
all six required data sources (including the versioned MCP Runtime and
Observability resources), confirms the mutation-disabled capability contract,
and confirms that the Connector is online through the Control Plane. Connector
readiness is not reported until its HTTP/2 mTLS registration succeeds, so this
smoke verifies the deployed reverse channel without reopening Connector inbound
query endpoints. The Phase 01 live smoke below exercises a real MCP query and
persists its canonical Evidence through that channel. The longer
Probe/Lag, PostgreSQL and Control Plane restart, Collector outage, token
rotation, and offboarding lifecycle remains the Compose smoke; this Kind check
is its bounded deployment-parity subset.

## Phase 01 live read-only E2E

The Phase 01 acceptance builds on the same runner and cluster. `kind.ps1 Up`
also builds and loads the test-only fault driver and the SRE probe image. After
the Phase 00 parity Job passes, run:

```powershell
.\rocketmq-sre\scripts\phase01-kind-smoke.ps1
```

For a fast local contract check that does not require a cluster:

```powershell
.\rocketmq-sre\scripts\phase01-kind-smoke.ps1 -ValidateOnly
```

The live runner opens one temporary loopback-only `kubectl port-forward` for
the Control Plane and always terminates it before returning. It never calls the
Connector diagnostics service or disabled inbound query endpoints. It then
runs `phase01-live-probe-job.yaml`, whose service-account token is disabled.
Its bootstrap init container can create or update exactly one fixed
`SRE_PROBE_` Topic. Its separate probe identity can only register the matching
`SRE_PROBE_G_` Group and send at most 10 messages of 64 bytes within 60
seconds. Neither identity is shared with MCP, Connector, Control Plane, or any
business Topic.

The acceptance requires a positive real Consumer Lag result from MCP converted
to canonical Evidence, normalized inventory, an online mTLS Connector,
Conversation → Investigation → Incident diagnosis with persisted citations,
inspection plus Markdown/HTML reports, Diagnostic Coverage, cross-cluster
denial, append-only read audit, and a mutation-disabled MCP/OpenAPI surface.
In addition, the deployed acceptance now requires a `model_assisted` result,
persists the selected OpenAI-compatible fixture lineage, and verifies that its
conclusion cites the positive live Consumer Lag Evidence. The model fixture is
read-only and cannot make an outbound network call.
Re-running it deletes and recreates only the named probe Job; the Topic update
is idempotent and the consumer registration advances any prior probe backlog
before the next bounded send.

For static checks without a cluster:

```powershell
.\rocketmq-sre\scripts\verify-mtls-deployment.ps1
kubectl kustomize .\rocketmq-sre\deploy\kind > $null
helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq-system `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml `
  -f .\rocketmq-sre\deploy\kind\helm-values.yaml > $null
```

## Phase 01 Shadow Job

After the Kind cluster exists, the Phase 01 evaluator can be built, loaded, and
run as a one-shot Job:

```powershell
.\scripts\phase01-shadow.ps1 -Target Kind -Provider Mock
```

Its manifests are under `phase1-shadow/`. The Job has no service-account token,
RBAC, ingress, egress, or Executor reference. It evaluates committed Evidence
only and succeeds only when all 24 Wave A runs pass with
`mutation_calls=0` and `executor_calls=0`.
