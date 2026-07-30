# RocketMQ Rust Kubernetes assets

`base/manifest.yaml` is the rendered `production-controller-ha` profile from
the canonical Helm chart. The base uses five locally loaded images:

```text
rocketmq-rust/broker:local
rocketmq-rust/namesrv:local
rocketmq-rust/controller:local
rocketmq-rust/proxy:local
rocketmq-rust/mcp:local
```

Every workload sets `imagePullPolicy: Never`. The committed `local` tags are a
rendering fixture; deployable images use `rocketmq-rust/<service>:<commit>`.
`scripts/build-production-images.ps1 -Load` builds all five images from a clean
checkout, records their image and binary digests, and writes a complete local
`ReleaseState` under `.rocketmq/candidate/`. No registry login or remote push is
part of this deployment mode.

`ReleaseState` binds the five images to one configuration digest, Secret
reference and version, rollout nonce, and storage generation.
`scripts/set-architecture-release-state.ps1` validates that complete state,
imports the same image set with Kind or K3d, applies it in policy order, waits
for readiness, and compensates completed steps in reverse order after failure.

The three `10.96.0.20x` Controller ClusterIPs are example reservations. Replace
them with unused addresses from the target cluster's Service CIDR before
deployment.

## Profiles

- `values-dev-single.yaml` runs one ephemeral Controller, Broker, NameServer,
  Proxy, and MCP instance for development.
- `values-production-controller-ha.yaml` runs an odd Controller quorum backed
  by RocksDB PVCs, a Controller-managed three-member Broker replica group,
  persistent NameServers, two Proxies, and persistent MCP audit storage.

The production schema rejects unsafe replica counts, missing Controller
endpoints, disabled required persistence, and a non-RocksDB Controller backend.

## State, readiness, and security

- Broker, NameServer, and Controller are StatefulSets with retained PVCs.
- Stable Controller ordinals own stable Raft/remoting endpoints and RocksDB
  paths. Stable Broker ordinals own separate message-store and identity PVCs.
- Controller readiness requires an observed Raft leader and applied committed
  state. Broker readiness requires Store, registration, assigned role,
  listeners, processors, and security. Proxy performs its Cluster
  route/security metadata preflight before binding listeners.
- PDBs, host anti-affinity, topology spread constraints, rolling updates, and
  default-deny NetworkPolicies follow the production replica model.
- Pods run as UID/GID 10001 with a read-only root filesystem, dropped
  capabilities, no privilege escalation, and no mounted service-account token.
- Every process completes the same `secure-enforced` bootstrap before its first
  listener bind. Missing or unreadable trust anchor, TLS identity, mounted-file
  provider, administrator identity, or request policy stops startup.
- No Kubernetes Secret is generated. Operators supply `ca.crt`, `tls.crt`,
  `tls.key`, `admin.identity`, `request-policy.json`, `broker-acl.yml`, and
  `proxy-acl.yml` through an existing Secret or a Secrets Store CSI provider.
  The `dev-single` profile also uses secure bootstrap because Pod listeners are
  necessarily non-loopback.

## Local validation

Build and validate a local candidate from the repository root:

```powershell
.\scripts\build-production-images.ps1 -Load
.\scripts\set-architecture-release-state.ps1 -ValidateOnly
helm lint .\distribution\helm\rocketmq-rust --strict
helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml > $null
```

The chart defaults are local validation fixtures. The reconciler replaces the
complete release identity and every image reference from the validated
candidate; operators must not deploy the fixture values directly.

## Dynamic fault evidence

The static policy check validates only the fault contract and committed fixture
shape. Release evidence requires the `Kubernetes architecture fault matrix`
workflow to run from `workflow_dispatch` or its weekly schedule with
digest-pinned baseline, candidate, and collector images. The isolated evidence
job generates run-scoped synthetic runtime and driver credentials, authenticates
and preloads every image digest, executes the Kind or K3d fault driver, preserves
the per-scenario reports, and uploads an artifact whose name contains the tested
commit SHA. Generated evidence credentials are never production credentials and
are never uploaded. A skipped dynamic job or a fixture-only report is not release
evidence.

The committed policy currently requires 16 ordered scenarios: rolling upgrade,
node eviction, NameServer minority partition and majority unavailability,
collector outage, disk pressure, disk-full admission, synchronous-write
contention, Controller leader and quorum loss, latency/loss/half-open network
impairment, HA lag and promotion, interrupted Raft snapshot install, Proxy
long-poll/slow-Broker overload, secret rotation, and acknowledged-message
recovery. Every scenario declares a precondition, observable, RPO/RTO, abort
condition, cleanup action, and cleanup verification. Production promotion
accepts only the dynamic report for the current candidate; the committed
fixture exists solely to test the guard.

For local-only validation, build and load the candidate images with
`scripts/build-production-images.ps1 -Load`, use a loopback registry (or
digest-pinned images already present in the Docker Engine), and pass those local
digest references to `kind-architecture-refactor-e2e.ps1`. This route does not
log in to, push to, or publish an external registry.

To update `base/manifest.yaml`, render the production profile as UTF-8 and
replace the file only after the local lint and schema checks pass. The committed
base remains a non-deployable local rendering fixture. A deployable render must
come from a complete validated `ReleaseState`.

See `rocketmq-doc/en/01-run-rocketmq-rust-k8s.md` for local image builds,
installation, upgrade, and recovery procedures.
