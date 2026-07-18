# RocketMQ Rust Kubernetes assets

`base/manifest.yaml` is the deterministic output of the canonical Helm chart with the non-publishable secure rendering
fixture. The base Kustomization replaces every image with an all-zero digest, so applying the base directly fails closed.
The `overlays/secure` digests are also non-publishable test fixtures; a release process must replace all five with
verified, signed `ghcr.io/mxsm/rocketmq-rust/<service>@sha256:<64 hex>` references. The three `10.96.0.20x`
Controller ClusterIPs are validation fixtures as well. A production overlay must replace them with unused, reserved
addresses from that cluster's Service CIDR in both the ordinal Services and the three Controller config entries.

The chart and Kustomize assets use the shared M11-10 HTTP lifecycle contract: `/readyz` rejects new work before drain,
`/livez` reports progress, `/drainz` initiates the same absolute 45-second shutdown deadline as SIGTERM, and the Pod
termination grace is 60 seconds. TCP-open and startup probes remain forbidden because they do not prove service
readiness or durable drain completion.

## State and security boundaries

- Broker, NameServer, and Controller are StatefulSets with explicit storage classes and retained PVCs.
- Controller has three ordinal-specific configs, a two-member PDB quorum, hostname/zone spread, and stable peer
  endpoints.
  Because the current Controller peer contract is a typed socket address, each ordinal gets a stable ClusterIP with
  separate remoting `60109` and Raft `60110` ports. Only the lowest node ID may run the explicit multi-member bootstrap;
  replicas/configuration alone are still not recorded as proof that a quorum was formed.
- Proxy is a stateless Deployment with bounded `emptyDir` data. MCP is a single-replica Deployment with a retained audit
  PVC; its file-audit writer is not horizontally shared.
- All Pods use UID/GID 10001, restricted Pod Security, read-only root filesystems, dropped capabilities, no service-account
  token, default-deny networking, explicit DNS/OTel/client/JWKS paths, and an existing Secret or SecretProviderClass mount.
- No Kubernetes Secret object is generated. Operators provide `broker-acl.yml`, `proxy-acl.yml`, `tls.crt`, and `tls.key`
  through the selected external secret reference.

## Validation

```powershell
.\scripts\kubernetes-assets-contract.ps1
python -m unittest scripts.tests.test_m11_kubernetes_assets -v
python scripts/fault_matrix_guard.py --policy-only
python scripts/fault_matrix_guard.py --evidence scripts/tests/fixtures/m11-fault-matrix/pass --allow-fixture
python -m unittest scripts.tests.test_m11_fault_matrix -v
.\scripts\kind-architecture-refactor-e2e.ps1 -Mode Validate
```

To regenerate the committed Kustomize base after an intentional chart change:

```powershell
.\scripts\kubernetes-assets-contract.ps1 -UpdateGeneratedBase
```

The script downloads Helm, Kustomize, and Kubeconform only from pinned upstream releases, verifies their SHA-256 hashes,
requires the default chart to reject unpublished digests, checks deterministic chart/base parity, validates both secure
renders against Kubernetes 1.32 schemas, and runs the deployment policy guard.

## Dynamic fault evidence

`scripts/kind-architecture-refactor-e2e.ps1 -Mode Run` is the only path that may emit M11-11 dynamic PASS evidence. It
requires Docker plus Kind or K3d, five baseline and candidate `image@sha256` references, an explicitly pinned collector
image, and operator-supplied baseline/rotated runtime and fault-driver Secret manifests. It executes rolling upgrade,
node eviction, collector outage, scheduling-level disk pressure, Controller leader loss, credential rotation, and
acknowledged-message recovery. Its evidence includes cluster/tool profiles, chart/overlay hashes, events, exact Queue and
CommitLog offsets, PVC UIDs, rollback results, and artifact hashes. Missing prerequisites or any failed durability,
drain, SLO, quorum, secret-redaction, cleanup, or rollback assertion fails closed and leaves no `run.json` PASS record.

The committed positive fixture is intentionally marked `fixture=true` and `dynamic_execution=false`; it is accepted only
with `--allow-fixture` and can test the evidence parser but can never satisfy the real Kind/K3d Gate. The dynamic workflow
is manually dispatched with protected secret inputs and uploads the immutable evidence directory for independent review.
