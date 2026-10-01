# Releasing crates and Docker Hub images

Use **Release crates and Docker Hub images** (`.github/workflows/release.yml`)
from the `main` branch. This workflow is manually dispatched; creating a tag or
publishing a GitHub Release does not trigger Docker Hub or crates.io publication.
The existing **Publish signed service images** workflow continues to publish its
five GHCR images independently.

For a release source, GHCR images receive both `<version>` (for example,
`ghcr.io/mxsm/rocketmq-rust/broker:1.0.0`) and
`<version>-<12-character-commit>` tags after all five images pass qualification.
The plain version tag is enabled only when `v<version>` resolves to the exact
published source commit. Manual builds of later `main` commits retain only the
commit tag. Existing version or commit tags with a different digest stop
publication; neither tag is overwritten. `staging-...` tags are temporary build
references and do not indicate a completed release.

## Configure GitHub Actions

Set the following repository **Secrets**:

| Secret | Purpose |
| --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io API token with permission to publish the core crates, including creating new crate names on the first release |
| `DOCKERHUB_TOKEN` | Docker Hub access token with read/write access to all selected repositories |

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
   selected core crate versions at `1.0.0` and commit their lockfiles.
2. Create and push `v1.0.0` pointing to that source commit. The workflow requires
   an existing stable `vX.Y.Z` tag reachable from `main`, with a version matching
   the root workspace manifest. The tagged source must include the release tools.
3. In **Actions → Release crates and Docker Hub images → Run workflow**, select
   `main`, enter `v1.0.0`, and leave `dry_run` enabled. Keep both publication
   selections enabled and `image_group=all` to validate the full release.
4. Inspect the crate verification records, image SBOMs, and vulnerability scans
   in the workflow artifacts. Resolve build or CRITICAL vulnerability failures.
5. Run the same workflow with `dry_run` disabled to publish.

The crates job uses the repository toolchain and the 27 `registry-publish`
entries in `scripts/core-release-scope.json`. Dashboard, MCP, and SRE standalone
projects are built into images but are not added to the core crates.io release.
Cargo verifies package archives and publishes missing crates in dependency
order with `--locked`; archive verification is never disabled.

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

Each image also receives `1.0.0-<12-character-commit>` and records the full source
commit in its OCI labels. The release does not write `latest`, `main`, or `master`.
Development issuers, model mocks, and qualification drivers are excluded.
`docker/release-images.json` owns the six build groups and repository names.

Each group builds and scans all missing images before its first upload. Syft
generates CycloneDX SBOMs; Trivy blocks every CRITICAL finding, including those
without a fix. New images are pushed to per-run staging tags, scanned again by
registry digest, signed with Cosign, and accompanied by verified SBOM
attestations. Only then are the stable version and commit tags promoted while
preserving the digest. Staging tags may remain after successful or failed runs;
retain or remove them according to your Docker Hub retention policy.

## Retry a partial release

Publication across crates.io and 13 repositories is not atomic. Crates finish
before image jobs start; different image groups may finish independently.
Rerun the **same tag** after a failure:

- Existing crate versions are skipped only when their published archive records
  the same clean source commit. Registry errors and yanked versions stop publication.
- Existing image version/commit tags must match the release source and each other.
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
actionlint .github/workflows/release.yml .github/workflows/release-check.yml
git diff --check
```

The **Release tooling checks** workflow runs these release regression tests and
validates workflow syntax without access to publication secrets. Actual Linux
image builds and scans run in the release workflow's default dry-run mode.
