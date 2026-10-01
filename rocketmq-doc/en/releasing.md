# Releasing crates and container images

Use **Release crates and Docker Hub images** (`.github/workflows/release.yml`)
from the `main` branch. This workflow is manually dispatched; creating a tag or
publishing a GitHub Release does not trigger Docker Hub or crates.io publication.
After Docker Hub publication succeeds, the workflow calls **Publish release
images to GHCR** (`.github/workflows/release-ghcr.yml`). Both registries use the
same 13-component catalog in `docker/release-images.json`. GHCR copies the exact
qualified Docker Hub image digest; services are not rebuilt independently.
GHCR publishes only the version tag, for example
`ghcr.io/mxsm/rocketmq-rust/broker:1.0.0`.

The older **Publish signed service images** workflow is manual only, for the
five-service architecture qualification process. Publishing a GitHub Release
no longer triggers it. Use the release workflows for formal releases.

## Configure GitHub Actions

Set the following repository **Secrets**:

| Secret | Purpose |
| --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io API token with permission to publish the core crates, including creating new crate names on the first release |
| `DOCKERHUB_TOKEN` | Docker Hub personal access token with Read, Write, Delete permission for all selected repositories; tag cleanup requires Delete |

Set the following repository **Variables**:

| Variable | Purpose |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub login user |
| `DOCKERHUB_NAMESPACE` | Optional Docker Hub organization; defaults to the login user |

SRE UI OIDC settings are supplied when starting the container, so publishing
the generic `sre-ui` image requires no GitHub OIDC variables. The release always
selects OIDC mode and supplies no development token or development identity
build arguments. Login remains unavailable until deployment settings are provided.

Enable GitHub Actions and allow the workflow's `id-token: write` permission for
keyless Cosign signing. Create all selected Docker Hub repositories before the
first publication and grant the login user read/write access. Docker Hub may
report a nonexistent repository as insufficient authorization; the release
intentionally stops on that response rather than assuming its tags are absent.
Secret and variable presence is checked before any selected crate or image is
uploaded.

## Release 1.0.0

1. Merge the release tooling and intended release source into `main`. Keep all
   selected workspace crate versions at `1.0.0` and commit their lockfiles.
2. Create and push `v1.0.0` pointing to that source commit. The workflow requires
   an existing stable `vX.Y.Z` tag reachable from `main`, with a version matching
   the root workspace manifest. The tagged source must include the release tools.
3. In **Actions → Release crates and Docker Hub images → Run workflow**, select
   `main`, enter `v1.0.0`, and leave `dry_run` enabled. Keep both publication
   selections enabled and `image_group=all` to validate the full release.
4. Inspect the crate verification records, image SBOMs, and vulnerability scans
   in the workflow artifacts. Resolve build or CRITICAL vulnerability failures.
5. Run the same workflow with `dry_run` disabled to publish.

The crates job uses the repository toolchain and publishes all 28 root workspace
members: the 27 `registry-publish` entries in `scripts/core-release-scope.json`
plus `rocketmq-dashboard-common`. Dashboard common retains its separate core
architecture classification; the publication helper explicitly includes it as
an additional workspace package. Dashboard, MCP, and SRE standalone projects
are built into images and remain outside this crates.io publication list.
Cargo verifies package archives and publishes missing crates in dependency
order with `--locked`; archive verification is never disabled.

Crate and Docker Hub publication tooling and their tests come from the trusted
workflow revision. Package archives and image builds still use the immutable
tagged source and release policy.
This allows publication fixes to resume an existing release without changing
its tag or service binaries; crate and image publication records include both
source and tooling commits. Rerunning crate publication verifies and skips
already published matching versions, including Dashboard common.

## Docker Hub image repositories

Every image is currently built for **linux/amd64**. With namespace `example`,
the release produces these repositories:

| Component | Image |
| --- | --- |
| NameServer | `example/rocketmq-rust-namesrv:1.0.0` |
| Controller | `example/rocketmq-rust-controller:1.0.0` |
| Broker | `example/rocketmq-rust-broker:1.0.0` |
| Proxy | `example/rocketmq-rust-proxy:1.0.0` |
| MCP | `example/rocketmq-rust-mcp:1.0.0` |
| Dashboard Web backend | `example/rocketmq-rust-dashboard-web-backend:1.0.0` |
| Dashboard Web frontend | `example/rocketmq-rust-dashboard-web-frontend:1.0.0` |
| SRE Control Plane | `example/rocketmq-rust-sre-control-plane:1.0.0` |
| SRE Connector | `example/rocketmq-rust-sre-connector:1.0.0` |
| SRE Executor | `example/rocketmq-rust-sre-executor:1.0.0` |
| SRE Execution Agent | `example/rocketmq-rust-sre-execution-agent:1.0.0` |
| SRE Probe | `example/rocketmq-rust-sre-probe:1.0.0` |
| SRE UI | `example/rocketmq-rust-sre-ui:1.0.0` |

Each image receives only the stable version tag and records the full source
commit in its OCI labels. The release does not write commit aliases, `latest`, `main`, or `master`.
Development issuers, model mocks, and qualification drivers are excluded.
`docker/release-images.json` owns the six build groups and repository names.

Each group builds and scans all missing images before its first upload. Syft
generates CycloneDX SBOMs; Trivy blocks every CRITICAL finding, including those
without a fix. New images are pushed to per-run staging tags, scanned again by
registry digest, signed with Cosign, and accompanied by verified SBOM
attestations. Only then is the stable version tag promoted while
preserving the digest. Newly uploaded signatures and attestations may take time
to become discoverable. Temporary discovery failures are retried up to nine times
with bounded backoff (at most 375 seconds of waiting per verification).
Authorization, certificate, issuer, and cryptographic failures stop immediately;
tags are promoted only after successful verification.
The publisher deletes its temporary tag after promotion and on handled publication
failures. An `always()` workflow step also removes only the current run's staging
tags, including failed or cancelled runs when the runner remains available.
Successful publication removes matching legacy commit/staging aliases only after
checking their digest against the stable release. Other releases and Cosign
referrers remain intact. Cleanup uses Docker Hub's tag-only API: deleting a shared
registry manifest would also break the stable tag. Authorization or cleanup
failures fail the release rather than reporting complete publication.

## Matching GHCR release images

GHCR repository names are `ghcr.io/mxsm/rocketmq-rust/<component>`, with the same
components as the Docker Hub table above. Each version is a single linux/amd64
manifest with the same digest in both registries. The mirror does not create
Buildx attestation indexes, staging tags, commit aliases, or Cosign referrer tags
in GHCR. Existing matching version tags are verified and skipped; a conflicting
digest stops publication instead of overwriting a release.

Before copying any selected image, the mirror verifies the immutable tag/source
and OCI labels, the Docker Hub keyless signature with its release workflow
identity and source/component annotations, and the signed CycloneDX SBOM's exact
repository/digest subject. It accepts in-toto Statement v0.1 and v1 and rejects
unknown versions. All selected images receive fresh SBOMs and Trivy scans with
zero CRITICAL findings required before the first copy.

The complete publication record binds the image digests and hashes of the SBOMs,
scans, and source proofs. Cosign signs this record as a blob; evidence is uploaded
to the Actions artifact (90 days) and to the existing GitHub Release as
`ghcr-images-<version>-<group>-<run>-<attempt>.tar.gz`. If the GitHub Release has
not been created yet, download the Actions artifact and attach it when creating
the Release, or rerun the mirror after the Release exists. Evidence stays outside
the image registry, so it does not add package versions.

After extracting the evidence archive, verify its publication record:

```bash
cosign verify-blob --bundle publication.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/mxsm/rocketmq-rust/\.github/workflows/(release-ghcr|release)\.yml@refs/heads/main$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com publication.json
```

Then compare the pulled image digest and each evidence file's SHA-256 with the
signed record. The `GITHUB_TOKEN` handles GHCR publication; no additional PAT is
required. New GHCR packages initially have private visibility: set them public
in **Package settings** for anonymous pulls. Their source label links them to
this repository and grants its Actions workflows access.

To mirror an already published Docker Hub release without rebuilding images or
rerunning crates, dispatch **Publish release images to GHCR** from `main` with
the same tag and `image_group=all`. Its default dry-run verifies and scans sources
without any registry writes; disable `dry_run` to copy them.

## Retry a partial release

Publication across crates.io and 13 repositories is not atomic. Crates finish
before image jobs start; different image groups may finish independently.
Rerun the **same tag** after a failure:

- To reuse legacy images left by an older or interrupted image run, set `staging_run` to its run ID
  and attempt (for example `36800241284-1`). Matching staged images are checked
  against the release source, rescanned by digest, signed, and verified before
  promotion. Missing staged images are rebuilt; source conflicts stop publication.
- Existing crate versions are skipped only when their published archive records
  the same clean source commit. Registry errors and yanked versions stop publication.
- A terminal crates.io HTTP 429 publication error is retried up to five times,
  respecting its advertised UTC cooldown (60 seconds when no date is supplied).
  Every retry rechecks existing archives and publishes only missing versions.
  Other failures stop immediately. Cooldowns longer than ten minutes require a
  later manual rerun; the release tag and source commit remain unchanged.
- Existing image version tags must match the release source.
  Their digest is reused, rescanned, and signed again; conflicting tags stop the
  group instead of being overwritten.
- Disable `publish_crates` to retry only images. Select one `image_group` to
  retry a failed group; keep `all` to retry all groups.

Use a new version and tag for source changes. Release workflow runs are
serialized to prevent two runs from modifying the same release simultaneously.

## Runtime configuration

Dashboard Web backend and the five SRE backend images use a digest-pinned
Debian 13 (`trixie-slim`) runtime. Its patched Perl and zlib packages resolve
the CRITICAL reports that blocked the Debian 12 images. Dashboard installs
Debian 13's `libssl3t64` runtime package; the Rust 1.95 Bookworm builders remain
unchanged. Validate new runtime digests with the image build and scan dry-run
before releasing them.

Images contain binaries and static assets; mount deployment configuration,
credentials, and persistent storage separately. Core services expect their
configuration under `/etc/rocketmq/`. MCP defaults to authenticated Streamable
HTTP and expects `/etc/rocketmq/mcp.toml`; start from the MCP configuration
examples and configure listener addresses, TLS, permissions, and secrets for
your deployment. Stdio remains available through an explicit command override.

Dashboard frontend Nginx proxies `/api/` to network alias
`rocketmq-dashboard-backend:8082`, while SRE UI proxies `/v1/` to
`sre-control-plane:8090`. Provide these aliases or mount a deployment-specific
Nginx configuration. Configure the SRE services' existing authentication,
authorization, storage, and execution controls for the deployment.

Set these public environment variables on the **SRE UI container**:

| Variable | Purpose |
| --- | --- |
| `VITE_SRE_OIDC_AUTHORITY` | Required OIDC issuer URL |
| `VITE_SRE_OIDC_CLIENT_ID` | Required registered browser application client ID |
| `VITE_SRE_OIDC_REDIRECT_URI` | Optional; defaults to `<UI origin>/auth/callback` |
| `VITE_SRE_OIDC_POST_LOGOUT_REDIRECT_URI` | Optional; defaults to the UI origin |
| `VITE_SRE_OIDC_SCOPE` | Optional; defaults to `openid profile rocketmq:read rocketmq:diagnose rocketmq:model-governance` |

For example, connect the UI to the same Docker network as its Control Plane:

```bash
docker run --rm --network rocketmq-sre -p 3004:3004 \
  -e VITE_SRE_OIDC_AUTHORITY=https://sso.example.com/realms/rocketmq \
  -e VITE_SRE_OIDC_CLIENT_ID=rocketmq-sre-ui \
  example/rocketmq-rust-sre-ui:1.0.0
```

Replace the example namespace, issuer, and client ID with your deployment's
values and register the UI callback URL with the provider. The Control Plane
must have the `sre-control-plane` network alias and its own OIDC issuer,
audience, and JWKS settings configured.

At startup the image generates `/runtime-config.js` from only these five public
settings. The browser loads it before initializing authentication; Nginx serves
it with `Cache-Control: no-store`. Recreate the container to change its environment.
Runtime values override legacy OIDC build arguments, which remain available for
existing custom builds. Authentication mode, development identities, and tokens
cannot be supplied through the runtime configuration file.

## Local tooling checks

```bash
python -m unittest discover -s scripts/tests -p 'test_publish_*.py'
actionlint .github/workflows/release.yml .github/workflows/release-ghcr.yml .github/workflows/release-check.yml .github/workflows/service-image-publish.yml
git diff --check
```

The **Release tooling checks** workflow runs these release regression tests and
validates workflow syntax without access to publication secrets. Actual Linux
image builds and scans run in the release workflow's default dry-run mode.
