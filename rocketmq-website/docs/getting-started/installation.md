---
title: "Install and build from source"
---

This page prepares the current Rust source for the local tutorial. It builds Rust NameServer and Broker binaries; the tutorial does not substitute Java server images. If you need an older published release, use its release instructions and APIs consistently.

## Choose the source and toolchain

The repository pins Rust **1.95.0** in `rust-toolchain.toml` and declares the same root MSRV. Use Git, rustup/Cargo, and the linker/toolchain for your operating system. Windows MSVC builds need the Visual C++ build tools and Windows SDK; Unix builds need a working native compiler and linker. Optional native dependencies depend on the selected Cargo graph, so inspect the first missing-tool error rather than installing every optional product's dependencies.

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
rustup show active-toolchain
cargo --version
```

Run subsequent commands from this repository root unless a different directory is stated. rustup uses the repository's toolchain selection. The root source package version is 1.0.0; consult [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases) before assuming there is a published crate, archive or image with that tag.

## Build only the components you need

For the single-machine tutorial:

```bash
cargo build -p rocketmq-namesrv --bin rocketmq-namesrv-rust
cargo build -p rocketmq-broker --bin rocketmq-broker-rust
cargo build -p rocketmq-admin-cli --bin rocketmq-admin-cli
```

The default output is `target/debug/`, with `.exe` suffixes on Windows. A configured `CARGO_TARGET_DIR` changes that location. Add `--release` for optimized binaries and use `target/release/` consistently afterward.

| Target | Why it is included |
| --- | --- |
| `rocketmq-namesrv-rust` | Topic-route discovery and Broker registration |
| `rocketmq-broker-rust` | Local file storage and message processing |
| `rocketmq-admin-cli` | Explicitly create the tutorial Topic and Consumer Group and inspect routes |

The Admin CLI currently enables Admin Core's `rocksdb-export` dependency, even when the running Broker uses `LocalFile`. Building this CLI therefore needs the native RocksDB build prerequisites, including a C++ compiler and Clang/libclang for bindings. `--no-default-features` on the CLI does not remove this explicitly enabled dependency. Check [the CLI manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/Cargo.toml) when preparing a build machine.

The first-message client is a small standalone package under `rocketmq-website/examples/first-message/`. It uses path dependencies to this checkout:

```bash
cargo build --manifest-path rocketmq-website/examples/first-message/Cargo.toml
```

Its code adapts the existing producer and LitePull example patterns to the same Topic and Group. It is outside the root workspace and is not published as a crate. Keep it inside the source checkout so its relative dependency paths remain valid.

## Other examples and products

The larger example collection is also standalone:

```bash
cd rocketmq-example
cargo build --example producer-simple
cargo build --example consumer-lite-pull
```

Those two examples use different built-in Topics; running them unchanged is not a matched send/receive tutorial. Use [the paired first-message application](quick-start.md), or deliberately align the constants and provision the resources in each selected example.

Proxy builds involve protobuf generation and require `protoc`. RocksDB and desktop UI choices can introduce additional native tools. The website requires Node/npm, but neither is required for this minimal Rust service/client path. Build each independent Dashboard or AI project from its own manifest rather than adding it to this initial installation.

## Confirm what the build established

Run the built service or Cargo target with `--help` to inspect its actual options:

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- --help
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --help
cargo run -p rocketmq-admin-cli -- --help
```

Compilation and help output establish that the executable is available. They do not prove Broker registration, message delivery, TLS configuration, or recovery. [Local source setup](local-source.md) covers the running processes and their data directories; [quick start](quick-start.md) then follows the message path.

If disk space runs low, Cargo build products can be removed with `cargo clean` in the relevant workspace or with the example's `--manifest-path`. This removes build products, not message data. Do not confuse cleaning the compiler output with deleting a Broker's storage directory.

Sources: [root toolchain](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml), [workspace manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml), [example manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/Cargo.toml).
