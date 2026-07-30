# Phase 5 immutable release supply chain

The `RocketMQ AI SRE Release` workflow publishes the versioned binary set that
future current/N-1 acceptance consumes. It is separate from normal CI and is
triggered by an `sre-v<semver>` tag or an explicit manual dispatch.

## Released components

One release contains immutable Linux AMD64 images for:

| Component | Registry repository |
| --- | --- |
| Control Plane | `ghcr.io/<owner>/rocketmq-sre-control-plane` |
| Connector | `ghcr.io/<owner>/rocketmq-sre-connector` |
| Executor | `ghcr.io/<owner>/rocketmq-sre-executor` |
| Execution Agent | `ghcr.io/<owner>/rocketmq-sre-execution-agent` |
| Probe | `ghcr.io/<owner>/rocketmq-sre-probe` |
| AI SRE UI | `ghcr.io/<owner>/rocketmq-sre-ui` |
| RocketMQ MCP | `ghcr.io/<owner>/rocketmq-mcp` |

The release version must equal the version shared by every SRE crate. MCP keeps
its independent binary version, and both versions are recorded in the component
manifest.

## Publication flow

1. Build each component from the tagged commit.
2. Push only a commit-scoped staging tag.
3. Resolve and validate the registry digest.
4. Generate a CycloneDX SBOM.
5. Use the GitHub Actions OIDC identity to create a keyless Cosign signature,
   CycloneDX attestation, and SLSA provenance attestation.
6. Verify all three records against the exact workflow identity and GitHub OIDC
   issuer.
7. Upload a redacted component manifest.
8. Promote all seven version tags only after every component succeeds.
9. Refuse promotion when an existing SemVer tag points at a different digest.
10. Publish a release index and evidence archive with SHA-256 checksums.

Stable releases also update `latest`; prereleases never do. A release failure
can leave a commit-scoped staging tag, but it cannot partially move a SemVer
release because promotion waits for the complete matrix.

## Triggering a release

Normal publication uses an annotated, reviewed tag:

```powershell
git tag -a sre-v0.1.0 -m "RocketMQ AI SRE 0.1.0"
git push origin sre-v0.1.0
```

Creating or pushing a tag is an external release action and is not part of local
Phase 5 validation. It must be performed only after the normal CI, security
review, and release approval complete.

The manual workflow dispatch supports a non-publishing build by leaving
`publish=false`. A manual `publish=true` run has the same signing and immutable
promotion rules, but it does not create a GitHub Release because no reviewed
tag is present.

## N-1 retention

The first SRE release records:

```json
{
  "status": "not_available_first_release",
  "actual_binary_matrix_required": true
}
```

For later releases, the workflow locates the immediate preceding SRE SemVer
release, downloads its release index, validates all seven component digests,
and confirms every image digest still exists in the registry. The current
release index then records the retained N-1 tag and the previous index hash.

Retention proves that the actual binaries are available. It does not replace
the current/N-1 runtime matrix: that acceptance must deploy the retained
Connector, Execution Agent, and MCP images against the current Control Plane
and verify full, read-only-degraded, and incompatible fail-closed behavior.

## Verification and rollback

Operators select images by digest from
`rocketmq-sre-release-index.json`, never by a mutable tag. A representative
verification is:

```powershell
cosign verify `
  --certificate-identity "https://github.com/<owner>/<repo>/.github/workflows/rocketmq-sre-release.yml@refs/tags/sre-v0.1.0" `
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" `
  "ghcr.io/<owner>/rocketmq-sre-connector@sha256:<digest>"
```

Rollback selects the retained N-1 digest from its signed release index. It does
not rebuild old source, reuse `latest`, or change an existing SemVer tag.

## Current validation boundary

The workflow definition is locally checked with `actionlint`, ShellCheck,
Cargo metadata for both standalone workspaces, AGENTS routing control, and
`git diff --check`. A real registry publication, keyless certificate, and
current/N-1 binary deployment require a pushed release tag and therefore remain
external acceptance evidence until that release is authorized and executed.
