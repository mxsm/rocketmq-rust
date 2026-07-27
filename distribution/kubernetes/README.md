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

Every workload sets `imagePullPolicy: Never`. Build with
`docker buildx build --load` and, when the Kubernetes nodes do not share the
host Docker image store, load the images with the cluster-specific `kind` or
`k3d` command. No registry login or remote push is part of this deployment
mode.

Service image builds require an explicit non-zero 40-character
`SOURCE_REVISION`. Capture it once with `git rev-parse HEAD`, use it for every
Docker build, and pass the same value to Helm as
`--set-string releaseIdentity.commit=...`. A rollout nonce is also required;
pass it as `--set-string releaseIdentity.nonce=...`. All five processes reject
a null or mismatched release identity before readiness.

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

Run from the repository root:

```powershell
$sourceRevision = (git rev-parse HEAD).Trim()
$rolloutNonce = $env:ROCKETMQ_ROLLOUT_NONCE
if (
  $sourceRevision -notmatch '^[0-9a-f]{40}$' -or
  $sourceRevision -match '^0{40}$' -or
  [string]::IsNullOrWhiteSpace($rolloutNonce) -or
  $rolloutNonce -notmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'
) {
  throw "A real source revision and canonical ROCKETMQ_ROLLOUT_NONCE are required"
}
$releaseIdentityArgs = @(
  '--set-string', "releaseIdentity.commit=$sourceRevision",
  '--set-string', "releaseIdentity.nonce=$rolloutNonce"
)

helm lint .\distribution\helm\rocketmq-rust --strict `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml @releaseIdentityArgs
helm lint .\distribution\helm\rocketmq-rust --strict `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml @releaseIdentityArgs

helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-dev-single.yaml `
  @releaseIdentityArgs > $null
helm template rocketmq .\distribution\helm\rocketmq-rust `
  --namespace rocketmq `
  -f .\distribution\helm\rocketmq-rust\values-production-controller-ha.yaml `
  @releaseIdentityArgs > $null
```

The bare chart defaults intentionally remain fail-closed until deployable
images, a real release commit, and a rollout nonce are supplied.

To update `base/manifest.yaml`, render the production profile as UTF-8 and
replace the file only after the local lint and schema checks pass. The committed
base uses the non-deployable secure rendering fixture's explicit release
identity; a deployable render must carry the exact revision embedded in its
service images and a deliberate rollout nonce.

See `rocketmq-doc/en/01-run-rocketmq-rust-k8s.md` for local image builds,
installation, upgrade, and recovery procedures.
