---
title: "Run RocketMQ Rust with Kubernetes"
permalink: /docs/run-rocketmq-rust-k8s/
excerpt: "Run RocketMQ Rust with Kubernetes"
last_modified_at: 2026-07-24T00:00:00+08:00
redirect_from:
  - /theme-setup/
toc: true
classes: wide
---

# Run RocketMQ Rust with Kubernetes

The Helm chart has two explicit deployment profiles:

- `dev-single` is a one-pod-per-service development topology. Controller state and
  service data may be ephemeral.
- `production-controller-ha` is the production topology. It requires three or
  more Brokers, an odd Controller quorum of at least three members, stable
  Controller endpoints, and persistent storage.

The examples below use images built into the local Docker image store. They do
not log in to a registry and do not push images.

## Prerequisites

- Docker with BuildKit/buildx
- Helm 4.2.3 or a compatible Helm release
- Kubernetes 1.32 or newer
- `kubectl` configured for the target cluster
- a `ReadWriteOnce` StorageClass for the production profile

For production, reserve one unused ClusterIP per Controller member from the
cluster Service CIDR. The example values use `10.96.0.201` through
`10.96.0.203`; replace them if the cluster uses another Service CIDR or those
addresses are already allocated.

## Build local images

Run these commands from the repository root. `--load` writes each result to the
local Docker image store; there is intentionally no `--push`.

```powershell
docker buildx build --load --file .\docker\Dockerfile.base --target broker `
  --tag rocketmq-rust/broker:local .
docker buildx build --load --file .\docker\Dockerfile.base --target namesrv `
  --tag rocketmq-rust/namesrv:local .
docker buildx build --load --file .\docker\Dockerfile.base --target controller `
  --tag rocketmq-rust/controller:local .
docker buildx build --load --file .\docker\Dockerfile.base --target proxy `
  --tag rocketmq-rust/proxy:local .
docker buildx build --load --file .\docker\Dockerfile.base --target mcp `
  --tag rocketmq-rust/mcp:local .

docker image inspect `
  rocketmq-rust/broker:local `
  rocketmq-rust/namesrv:local `
  rocketmq-rust/controller:local `
  rocketmq-rust/proxy:local `
  rocketmq-rust/mcp:local > $null
```

Docker Desktop Kubernetes can use the host image store directly. A cluster
running in containers has a separate image store, so load the images without a
registry:

```powershell
# kind
kind load docker-image --name <cluster-name> `
  rocketmq-rust/broker:local `
  rocketmq-rust/namesrv:local `
  rocketmq-rust/controller:local `
  rocketmq-rust/proxy:local `
  rocketmq-rust/mcp:local

# k3d
k3d image import --cluster <cluster-name> `
  rocketmq-rust/broker:local `
  rocketmq-rust/namesrv:local `
  rocketmq-rust/controller:local `
  rocketmq-rust/proxy:local `
  rocketmq-rust/mcp:local
```

Both profiles set `imagePullPolicy: Never`. An `ErrImageNeverPull` Pod therefore
means that the image was not loaded into the image store used by that node.

## Prepare storage and secrets

The production values use the placeholder StorageClass
`rocketmq-retain`. Either create a class with that name and an appropriate
retention policy, or override all four persistent services:

```yaml
services:
  broker:
    persistence:
      storageClassName: standard
  namesrv:
    persistence:
      storageClassName: standard
  controller:
    persistence:
      storageClassName: standard
  mcp:
    persistence:
      storageClassName: standard
```

The chart never generates credentials or private keys. Create the namespace and
the externally managed Secret before installing. Keep the source files outside
the repository.

```powershell
kubectl create namespace rocketmq --dry-run=client -o yaml | kubectl apply -f -
kubectl -n rocketmq create secret generic rocketmq-runtime-secrets `
  --from-file=broker-acl.yml=<path-to-broker-acl> `
  --from-file=proxy-acl.yml=<path-to-proxy-acl> `
  --from-file=tls.crt=<path-to-tls-certificate> `
  --from-file=tls.key=<path-to-tls-private-key>
```

Use a Secrets Store CSI provider instead by clearing
`global.secretRefs.existingSecret` and setting
`global.secretRefs.secretProviderClassName`.

## Validate locally

```powershell
helm lint .\distribution\helm\rocketmq-rust --strict
helm lint .\distribution\helm\rocketmq-rust --strict `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml
helm lint .\distribution\helm\rocketmq-rust --strict `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml

helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml > $null
helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml > $null
```

The production schema rejects an even or undersized Controller set, fewer than
three Brokers, missing Controller endpoints, disabled required persistence, and
a Controller backend other than RocksDB.

## Install `dev-single`

```powershell
helm upgrade --install rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml `
  --wait --timeout 10m
```

This profile is for local development only. It uses one Controller with the
in-memory backend and does not promise recovery after Pod or node loss.

## Install `production-controller-ha`

First copy `values-production-controller-ha.yaml` to a local, uncommitted
override file and set:

- the Controller ClusterIPs reserved for this cluster;
- the StorageClass used by Broker, NameSrv, Controller, and MCP;
- production MCP URLs and OAuth values;
- resource limits appropriate for the nodes.

Then install:

```powershell
helm upgrade --install rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml `
  -f <local-production-overrides.yaml> `
  --atomic --wait --timeout 15m
```

The profile creates:

- three stable Controller ordinals, stable Raft/Remoting ClusterIPs, RocksDB
  PVCs, and a two-member disruption quorum;
- one three-replica Controller-managed Broker group, with stable Pod DNS and an
  independent retained PVC per ordinal;
- three persistent NameServers, two Proxies, and one persistent MCP audit
  instance;
- required host anti-affinity, topology spread constraints, PDBs, default-deny
  networking, and service-specific ingress/egress rules.

Broker identity metadata is stored on the Broker PVC. A new ordinal requests a
Broker ID from Controller and commits it before becoming ready; a restarted
ordinal recovers the same identity file.

## Readiness and rollout

The HTTP probe on port `8088` is intentionally not exposed by a Service.
Startup readiness is published only after:

- Controller has observed a Raft leader and applied committed state;
- Broker has a writable Store, bound listeners, active processors, initialized
  security, Controller registration, and an assigned runtime role;
- Proxy has a working NameServer/Broker metadata path, initialized
  authentication/authorization, and all configured listeners bound.

Use `kubectl rollout status` and inspect `/readyz` through the Pod network when
diagnosing a rollout:

```powershell
kubectl -n rocketmq rollout status statefulset/rocketmq-controller --timeout=10m
kubectl -n rocketmq rollout status statefulset/rocketmq-broker --timeout=10m
kubectl -n rocketmq rollout status deployment/rocketmq-proxy --timeout=10m
kubectl -n rocketmq get pods,pvc,pdb
```

Upgrade one release at a time with the same production override file. Do not
change Controller node IDs, Controller ClusterIPs, Broker group names, or PVC
ownership during an ordinary upgrade.

## Recovery

- Keep Controller and Broker PVC retention set to `Retain`. Never replace a
  failed Pod by deleting its PVC as a first response.
- Restore a Controller ordinal with its original PVC and the same node ID and
  ClusterIP. A majority of the configured voters must be available before the
  cluster can accept writes.
- Restore a Broker ordinal with its original PVC so that its Controller-issued
  Broker ID is recovered. Do not attach one ordinal's PVC to another ordinal.
- If a Controller member is permanently lost, recover quorum first and use an
  explicit Raft membership operation before changing the Helm replica count or
  endpoint list.
- Take storage-provider snapshots of Controller and Broker volumes. Restore a
  mutually consistent set; do not combine unrelated Controller state,
  Broker identity, and message-store snapshots.
- After recovery, wait for Controller, then Broker, then Proxy readiness before
  reopening client traffic.

Uninstalling the Helm release does not authorize deletion of retained data.
Review PVCs individually before any manual removal.
