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
All development credentials and certificates are generated into
`target/phase00-kind`; they are not production credentials.
The runner creates separate MCP reader, bounded Probe, and one-shot bootstrap
identities. The MCP Pod receives only its reader credential secret; the Broker
ACL and bootstrap credential remain in the RocketMQ namespace secret, and the
reader cannot perform Topic, Group, or cluster mutations.
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

The internal SRE ports are Control Plane `8090`, Connector `8091`, and UI
`3004`; MCP remains loopback-only on `8089` inside its Pod.

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
and requests a canonical inline Topic-list Evidence response. The longer
Probe/Lag, PostgreSQL and Control Plane restart, Collector outage, token
rotation, and offboarding lifecycle remains the Compose smoke; this Kind check
is its bounded deployment-parity subset.

For static checks without a cluster:

```powershell
kubectl kustomize .\rocketmq-sre\deploy\kind > $null
helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq-system `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml `
  -f .\rocketmq-sre\deploy\kind\helm-values.yaml > $null
```
