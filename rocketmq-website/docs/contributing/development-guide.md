---
title: "Development guide"
---

# Development guide

A useful development loop starts with the package that owns the behavior. This repository contains the main Cargo workspace, independent Cargo applications, and Node projects; one root build does not cover all of them. Use the [module map](../architecture/module-map.md) to locate the owner before selecting a command.

## Prepare the toolchain and working directory

Install Git and rustup, clone the repository, and run the following from its root:

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
rustup toolchain install 1.95.0 --profile minimal --component rustfmt,clippy
rustc --version
cargo --version
git status --short
```

The checked-in `rust-toolchain.toml` selects Rust 1.95.0. Keep the edition in the manifest of the package being edited; the root workspace defaults to Rust 2021, while some independent projects and Admin CLI use Rust 2024. An editor's rust-analyzer should open the relevant Cargo root, especially for a standalone project.

Native prerequisites depend on the selected target. RocksDB-enabled builds need a C++ toolchain and Clang/libclang; Admin CLI enables its RocksDB export dependency even when the tutorial Broker uses LocalFile. Proxy's generated protocol code needs `protoc`. See [installation](../getting-started/installation.md) for setup and avoid treating a missing native dependency as a Rust API failure.

## Select the project boundary

| Change | Where to select commands | What this covers |
| --- | --- | --- |
| A member listed in root `Cargo.toml` | Repository root, with `-p` and the manifest's package name | The selected package and required dependencies |
| Producer/consumer examples | `rocketmq-example/` and its local guide | The independent example workspace |
| First-message tutorial application | Repository root with its explicit `--manifest-path` | The website's self-contained sample |
| Dashboard common models | Root workspace | Shared models, not all dashboard applications |
| Web, GPUI, or Tauri Dashboard | The corresponding application directory; Web has separate backend/frontend roots | That application's build or frontend bundle |
| MCP, MCP Control, or SRE | The matching independent project root | That service; SRE UI and TypeScript SDK are separate Node projects |
| Website | `rocketmq-website/` | Docusaurus pages, routes, translations, and site assets |

Inspect the nearest `AGENTS.md`, package manifest, README, and existing tests before editing. The local guide selects the relevant validation profile; its commands are not an additional full-workspace checklist.

For example, these read-only commands from the repository root show the current root members and the client package's feature dependency tree:

```bash
cargo metadata --no-deps --format-version 1
cargo tree -p rocketmq-client-rust -e features
```

`rocketmq-client` is a directory name; `rocketmq-client-rust` is the Cargo package name used with `-p`. A package may also have a differently named binary or several binaries. Inspect `[[bin]]` or the CLI's `--help` instead of assuming that bare `cargo run` selects the intended application.

## Use a focused feedback loop

Suppose the change is in the `rocketmq-model` package. From the repository root:

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

The test command compiles that target and runs its library tests. For a smaller behavior change, select an existing test name or module; inspect the result's test count so a misspelled filter does not become a misleading success. Use `cargo check -p rocketmq-model` when a compilation-only check is appropriate. Add focused regression coverage for changed behavior, rather than tests that merely repeat the implementation.

For another package, substitute its actual manifest name and choose the relevant features and target. Keep optional features optional: an all-features run does not demonstrate that a default or feature-disabled configuration works. Package-scoped Clippy is useful when the change warrants it:

```bash
cargo clippy -p rocketmq-model --no-deps -- -D warnings
```

A standalone Cargo project uses commands from its own root. Do not run a mutating workspace-wide formatter over unrelated edits. Full integration, interoperability, fault recovery, performance, or platform matrices belong to work that needs those observations; they are not a prerequisite for every local edit.

## Run and debug a real message path

Use [local source setup](../getting-started/local-source.md) to start NameServer and Broker with isolated tutorial data, then [quick start](../getting-started/quick-start.md) to create the topic/group and run the paired application. This avoids debugging a consumer subscribed to a different topic from the producer's built-in example.

The sample manifest is `rocketmq-website/examples/first-message/Cargo.toml`. It explicitly owns and closes the producer or consumer, client runtime, process runtime, and telemetry. Keep those lifecycle boundaries when extracting a small reproducer; leaving an application-owned task behind changes the failure being investigated.

For a debugger, select the real package/binary and supply the same working directory, configuration, and environment as the successful command line. Observe the affected boundary first:

| Symptom | First evidence |
| --- | --- |
| Client cannot find a route | NameServer address, broker registration, advertised broker address, topic route |
| Send times out | Client deadline, broker response, storage/replica acknowledgement state; a timeout alone does not prove no append |
| Consumer receives nothing | Subscription, group, mode, queue assignment, stored offset, filter |
| Shutdown hangs | Task ownership, drain deadline, running blocking work, shutdown report |

Use [first diagnosis](../operations/first-diagnosis.md) and the [runtime explanation](../architecture/runtime.md) for the corresponding sequence. Keep logs bounded and redact credentials, message bodies, and complete request/configuration objects.

## Prepare a reviewable change

Keep code, relevant tests, and explanation focused on the behavior being changed. Public wire fields, persistence formats, configuration names, and public APIs are compatibility surfaces; explain their migration when changing them. Document cancellation and shutdown for background work, and preserve product authentication and authorization boundaries.

The repository's issue and PR templates supply the public fields to fill in. Describe the concrete trigger, resulting behavior, and checks actually run, with relevant features and project roots. Distinguish an observed result from an untested scenario. Preserve unrelated local changes.

Documentation-only work follows the [documentation guide](./documentation.md). It needs ordinary content checks and the existing website build when rendered content changes; it does not require source fingerprints, a clean checkout, approval stages, or a new CI gate.

## Source references

- [Root workspace manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml) and [selected toolchain](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml).
- [Repository engineering guidance](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md) and [example guidance](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/AGENTS.md).
- [Runnable first-message application](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message).
