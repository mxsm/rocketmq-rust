# RocketMQ Rust Kubernetes assets

`base/manifest.yaml` is the deterministic output of the canonical Helm chart with the non-publishable secure rendering
fixture. The base Kustomization replaces every image with an all-zero digest, so applying the base directly fails closed.
The `overlays/secure` digests are also non-publishable test fixtures; a release process must replace all five with
verified, signed `ghcr.io/mxsm/rocketmq-rust/<service>@sha256:<64 hex>` references. The three `10.96.0.20x`
Controller ClusterIPs are validation fixtures as well. A production overlay must replace them with unused, reserved
addresses from that cluster's Service CIDR in both the ordinal Services and the three Controller config entries.

The chart and Kustomize assets deliberately contain no readiness/liveness probe, `preStop`, or grace-period budget.
Those lifecycle semantics belong to PR-M11-10 and must not be approximated with TCP-open checks.

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
```

To regenerate the committed Kustomize base after an intentional chart change:

```powershell
.\scripts\kubernetes-assets-contract.ps1 -UpdateGeneratedBase
```

The script downloads Helm, Kustomize, and Kubeconform only from pinned upstream releases, verifies their SHA-256 hashes,
requires the default chart to reject unpublished digests, checks deterministic chart/base parity, validates both secure
renders against Kubernetes 1.32 schemas, and runs the deployment policy guard.
