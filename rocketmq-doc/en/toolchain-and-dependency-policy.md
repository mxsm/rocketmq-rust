# Toolchain and Dependency Trust Policy

## Purpose and scope

This policy defines the reproducible Rust toolchain and third-party dependency
trust boundary for the RocketMQ Rust repository. It applies to the root Cargo
workspace, every standalone Cargo project, and production container builds.

The policy is enforced locally. It does not require a GitHub CI workflow, and
local validation scripts, logs, reports, and build output must not be committed.

## Stable toolchain and MSRV

The repository uses stable Rust `1.95.0` as both its minimum supported Rust
version (MSRV) and its pinned build toolchain.

| Build surface | Authoritative declaration | Required value |
|---|---|---|
| Root workspace | `Cargo.toml` and `rust-toolchain.toml` | `1.95.0` |
| Standalone Cargo projects | each standalone `Cargo.toml` | `1.95.0` |
| Production containers | `docker/Dockerfile.base` and `docker/container-policy.json` | `1.95.0` |

`rust-toolchain.toml` uses the minimal profile and installs only `rustfmt` and
`clippy` in addition to the compiler. Repository commands should use the
automatically selected toolchain or name `+1.95.0` explicitly.

Rust editions remain an independent language-compatibility choice. The root
workspace continues to use Rust 2021 by default; the admin CLI and standalone
projects that already use Rust 2024 keep that edition. Toolchain alignment does
not authorize edition normalization.

Changing the MSRV requires all of the following in one reviewed change:

1. update every authoritative declaration above;
2. resolve the root and standalone manifests with the proposed version;
3. build the production container base with the same version;
4. update user-facing current documentation;
5. complete the local validation described below.

## Nightly exceptions

Nightly is not a general build toolchain. It is allowed only when stable Rust
cannot provide the required diagnostic interface, and every use must name an
exact dated toolchain.

The current approved exception is `nightly-2026-07-05`:

- `scripts/arc_mut_soundness_probe.py` uses it for Miri;
- `scripts/public_api_snapshot.py` uses it for rustdoc JSON.

Adding `+nightly`, an unversioned `nightly` channel, or a nightly feature to a
production crate is prohibited. Updating the dated nightly requires regenerating
and reviewing the affected baseline rather than silently accepting drift.

## Dependency source, license, and advisory policy

`deny.toml` is the repository-wide dependency admission policy. It evaluates the
full feature graph for the supported Windows and Linux targets and enforces:

- crates.io as the only default registry source;
- no unknown registry or Git source;
- no wildcard dependency requirements;
- an explicit permissive-license allowlist;
- exact crate/version license exceptions instead of global license grants;
- all known vulnerabilities and all unsound advisories across the full graph;
- all unmaintained advisories unless an explicit exception exists;
- visible warnings for yanked releases and duplicate versions.

When an advisory has a patched compatible release, the dependency must be
upgraded. An advisory exception is permitted only when the latest usable upstream
version still requires the dependency. Every exception must record:

- the responsible owner;
- the exact dependency path and exposure;
- why an upgrade or replacement is not currently possible;
- the production restriction, when the risk affects a target;
- a deadline and an upstream-update trigger for re-review.

An exception for an unsound or vulnerable runtime dependency blocks production
certification of the affected target unless the exposure is otherwise eliminated.
For example, Tauri's current GTK3/glib dependency means Linux Tauri packaging is
not production-certified while the corresponding exception remains; it does not
weaken the server or Windows dependency boundary.

## cargo-vet trust boundary

The versioned `supply-chain/` store certifies the root Cargo workspace, which owns
the production server binaries and libraries. It combines locked upstream audit
imports, exact-version exemptions, and local reviews.

The local criterion `rocketmq-critical-dependency-reviewed` records focused
source review of critical unsafe, native, storage, runtime, serialization, and
TLS/crypto dependencies. It intentionally does **not** imply
`safe-to-run` or `safe-to-deploy`. The normal cargo-vet criteria remain required
for a complete trust chain.

The focused review currently covers the critical families containing Tokio and
tokio-uring, bytes and serialization primitives, bytemuck, ring/rustls/aws-lc,
rcgen, RocksDB and its native bindings, memory mapping, and native compression
bindings.

Exact-version exemptions are review debt, not permanent approval. Each exemption
contains an owner note, stops applying after a version change, and must be
reconsidered whenever the lockfile changes. First-party path crates are not
treated as crates.io packages.

Standalone projects have independent lockfiles and much larger platform-specific
graphs. They use the same `deny.toml` policy and pinned stable toolchain, but the
root `supply-chain/` store does not claim to certify those separate lockfiles.
Creating hundreds of generated standalone exemptions is not an acceptable
substitute for review.

## Local validation

Keep Cargo build output and temporary files on a non-system drive with at least
15 GiB free. The following PowerShell setup is an example; choose a drive that is
available locally:

```powershell
$env:CARGO_TARGET_DIR = "E:\cargo-target-rocketmq-rust"
$env:TEMP = "E:\rocketmq-rust-temp"
$env:TMP = $env:TEMP
$env:CARGO_INCREMENTAL = "0"
```

From the repository root, validate the production workspace:

```powershell
rustc --version
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
cargo deny check
cargo vet --locked
```

On Windows, the current workspace can exceed the operating system command-line
length limit when `cargo fmt --all -- --check` expands every target (OS error
206). In that case, run `cargo fmt -p <package> -- --check` for every root
workspace package and run the formatter from each standalone Cargo root. This is
an equivalent per-package coverage path, not permission to skip formatting.

Run the applicable formatter, check, test, Clippy, and `cargo deny check` commands
from each changed standalone project root. The root cargo-vet result must not be
presented as certification of a standalone lockfile.

Build the pinned base image into the local Docker image store:

```powershell
docker build `
  --file .\docker\Dockerfile.base `
  --target builder-base `
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" `
  --tag rocketmq-rust/builder-base:local `
  .
docker run --rm rocketmq-rust/builder-base:local rustc --version
```

The explicit target is required because the Dockerfile's default runtime-smoke
stage does not depend on the Rust builder. Inspect and test the local image as
required by the owning task. Do not push it to a remote registry.

If free space falls below 15 GiB, clean only the dedicated target directory that
belongs to the current validation run. `cargo clean --target-dir <path>` removes
all reusable artifacts in that target directory, so it affects other worktrees
only when they were deliberately configured to share the same directory.

## Change review

A toolchain or dependency-policy change is complete only when:

- manifests, lockfiles, container policy, and current documentation agree;
- every cargo-deny graph passes with no unrecorded source, license, vulnerability,
  unsoundness, or maintenance exception;
- `cargo vet --locked` passes for the root workspace;
- the affected Rust projects pass their local stable-toolchain validation;
- a local Docker build proves the production container toolchain;
- no temporary validation artifact or build output is staged.
