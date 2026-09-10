---
title: "Build features and platform boundaries"
---

# Build features and platform boundaries

Select a product, its Cargo dependency graph, and its runtime configuration together. A Cargo feature makes an implementation available; it does not start a listener, select a store, grant permission, or demonstrate a deployment on every operating system. The [capability matrix](../overview/capability-matrix.md) describes these additional conditions.

## Toolchains and workspace boundaries

The root [toolchain file](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml) selects Rust `1.95.0`. Root workspace packages declare the same minimum version and normally use Rust 2021. Individual manifests can select another edition: the Admin CLI and standalone SRE/GPUI projects use Rust 2024. An edition is a source-language setting, not a server wire-protocol version.

Use the root `Cargo.toml` member list for root package commands. GPUI, the Tauri backend, the Web Dashboard backend, the examples workspace, and AI SRE have independent build boundaries. A successful root workspace build does not build all those products. Frontends and `rocketmq-website` are Node projects; the website's `.nvmrc` selects Node `24.13.0`, and its normal build emits English and Chinese pages.

Features are additive across a dependency graph. Disabling defaults on one dependency edge cannot remove a feature enabled through another edge. In particular, the workspace's Client and Transport dependencies disable their crate defaults, while its Store dependency retains defaults. A package's published default list alone therefore does not describe every application that embeds it.

## Core service and client feature map

| Package | Direct default features | Useful optional selections | Runtime condition |
| --- | --- | --- | --- |
| [`rocketmq-broker`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/Cargo.toml) | `local_file_store` | `rocksdb_store`, alias `rocksdb-store`; `extended_timeline`; `tieredstore` | Select a compatible store/timer configuration; build selection does not migrate existing data. |
| [`rocketmq-namesrv`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/Cargo.toml) | `tls` | `embedded-controller` | TLS still needs endpoint configuration. Embedded Controller additionally needs `enableControllerInNamesrv` and Controller configuration. |
| [`rocketmq-controller`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/Cargo.toml) | `storage-rocksdb` | `storage-file`; `dev-single` enables `storage-file` | Match `storageBackend` to the available implementation. `dev-single` does not itself change the default `RocksDB` configuration value. |
| [`rocketmq-proxy`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/Cargo.toml) | `cluster-mode` and `local-mode` | `tls`; `tieredstore` also enables Local mode | Choose `mode = "cluster"` or `"local"`. Built-in gRPC TLS requires both the TLS feature and TLS material. |
| [`rocketmq-client-rust`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml) | `admin-full`, expanding to `admin-read` and `admin-mutation` | `nameserver-dns-discovery`; separate telemetry features below | Inject an owned client runtime and configure discovery. Compiled admin APIs do not bypass authentication or authorization. |

Broker `production` combines local storage with `production-observability`. The latter enables Prometheus metrics, OTLP traces, and OTLP logs through its feature aliases. It is a build convenience, not a certification of the machine, topology, or workload as production-ready. `test-support` features expose testing facilities and are not required by ordinary messaging applications.

The client has no standalone `tls` feature. An application enables `rocketmq-transport/tls` through its dependency graph and configures the client connection. Ordinary producer and consumer code can disable Client default features if it does not need the admin surface.

## Storage and transport selections

| Package / feature | Compilation effect | Platform or runtime boundary |
| --- | --- | --- |
| [Store](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/Cargo.toml) defaults | `local_file_store` and `fast-load` | Broker's Store dependency can enable these even when a top-level feature list looks smaller. |
| Store `safe-load` | Enables conservative local loading support | Remove `fast-load` from the effective graph to select safe-load by features; when both are present, fast-load wins. `ROCKETMQ_SAFE_LOAD=true` is a separate runtime override. |
| Store `rocksdb_store` / `extended_timeline` | Adds the RocksDB foundation dependency | Native RocksDB toolchain requirements apply. Store/backend selection and timer-store ownership remain separate. |
| Store `tieredstore` | Adds secondary tiered storage integration | Current adapters and their recovery contracts determine behavior; this does not imply an S3 message-store backend or stronger primary acknowledgments. |
| Store `io_uring` | Forwards to `rocketmq-store-local/io_uring` | The dependency and optimized path are Linux-specific; kernel/runtime eligibility still matters. |
| [Transport](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/Cargo.toml) defaults | `tls` and `socks` for direct default-enabled consumers | The root workspace dependency disables these defaults. Inspect the final graph. |
| Transport `linux-sendfile` | Adds the optional Linux file-region write path | Linux and an eligible plaintext connection are required. TLS and ineligible cases use the portable path; local writer completion is not peer acknowledgment. |
| Transport `simd` | Forwards protocol SIMD support | CPU, target, and dependency support determine applicability; it does not alter RocketMQ's wire contract. |

See [storage backends](../architecture/storage-backends.md) and [transport design](../architecture/protocol-transport.md) before changing backend, loading, or file-transfer strategy. Compiling several backends together does not authorize sharing one data directory between incompatible configurations.

## Observability feature names are package-specific

| Consumer | Metrics | Traces | Exporter selection |
| --- | --- | --- | --- |
| Broker | `otel-metrics` | `otel-traces` | `otlp-metrics`, `otlp-traces`, `otlp-logs`, `prometheus` / `metrics-prometheus` |
| NameServer | `otel-metrics` | `otel-traces` | Corresponding `otlp-*` features; do not copy Broker's Prometheus alias into this manifest. |
| Controller | `metrics` | `otel-traces` | `metrics-otlp`, `metrics-prometheus`, `otlp-traces`, `otlp-logs` |
| Proxy | `observability` | `otel-traces` | `otlp-metrics`, `otlp-traces`, `otlp-logs` |
| Client | `observability-metrics` | `observability` | `otlp-traces` forwards trace export; OTLP metrics require the observability dependency's `otlp-metrics` in the graph. |
| [Observability library](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/Cargo.toml) | `otel-metrics` | `otel-traces` | No defaults; `otlp-metrics` / `otlp-traces` / `otlp-logs` include the appropriate signal and gRPC exporter; `prometheus` includes metrics. |

Log-signal support uses `otel-logs` and is distinct from ordinary console logging. A runtime request for an uncompiled exporter produces a typed error. Configure actual endpoints, resource identity, label policy, and lifecycle ownership through [observability configuration](../configuration/observability.md).

## Admin, Dashboard, MCP, SRE, and website

| Product | Build selection | External or platform needs |
| --- | --- | --- |
| [Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/Cargo.toml) / [Admin TUI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/Cargo.toml) | Root packages; no product `[features]` switchboard in these manifests | TUI needs an interactive terminal. Dependency adapter features and runtime credentials determine actual operations. |
| [GPUI Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-gpui/README.md) | Standalone Cargo project; no product feature switchboard | Graphical desktop and native GUI build dependencies. Windows uses MSVC/Windows SDK; macOS uses Xcode tools; Linux requires its GUI development libraries. |
| [Tauri Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-tauri/README.md) | Node frontend plus standalone `src-tauri` Cargo project | Tauri platform prerequisites and native packaging environment; backend compilation alone does not produce the complete desktop application. |
| [Web Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/README.md) | Separate frontend and backend commands; no backend product feature switchboard | Browser, configured backend, and selected database deployment. Database mode is runtime configuration, not a Cargo feature. |
| [MCP](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/Cargo.toml) | Defaults `read-only`, `diagnose`, `stdio`; optional `streamable-http`, `observability`, `otlp`, `change-planning` | HTTP transport enables its auth support and needs the corresponding server configuration. Planning does not add mutation execution to the read-only product. |
| [MCP Control](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp-control/Cargo.toml) | Empty defaults; `write-tools` opts into the admin dependency | Independently configured mutation service with its own authorization and audit boundary. |
| [AI SRE](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/Cargo.toml) | Standalone multi-crate workspace plus UI and SDK projects; select actual member packages | Connector, control plane, model gateway, and execution agent have different dependencies. The read connector uses the read adapter; execution-agent dependencies include mutation support. A workspace dependency does not enable every executor. |
| [Website](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/package.json) | Docusaurus Node project; `npm run build` | Installed Node dependencies and the pinned Node environment; no Cargo features. |

The absence of a product feature table does not mean the product has no optional capabilities or native dependencies. Follow its own manifest, package scripts, and deployment guide; do not run a root Cargo command and report all standalone products as covered.

## Native tools and platform limits

RocksDB-backed builds compile native dependencies and can require a C/C++ compiler and binding-generation tooling. Diagnose the actual selected dependency's build output; a pure Rust client build does not exercise that path. Controller's [build script](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/build.rs) explicitly selects `protoc-bin-vendored`. Proxy Core's [build script](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/build.rs) invokes protobuf generation through the external compiler path, so provide `protoc` through the build environment. These are different prerequisites.

Some security-provider behavior is platform-specific. The portable Windows secret-file permission projection currently fails closed until an adapter supplies owner-only ACL information; successful Windows compilation does not demonstrate that every secure secret-file deployment profile runs there. Consult [security boundaries](../architecture/security.md) for the provider you intend to use.

Linux-only optimizations, desktop packaging, storage recovery, and multi-node deployment need their own relevant environment. This page records manifest and source conditions, not a claim that every feature/platform combination was executed during documentation work.

## Inspect the graph you actually build

From the root workspace:

```bash
cargo tree -p rocketmq-broker -e features
cargo tree -p rocketmq-client-rust -e features
cargo build -p rocketmq-proxy --no-default-features --features cluster-mode
cargo build -p rocketmq-controller --no-default-features --features dev-single
```

The last command selects file-storage support; pair it with `storageBackend = "File"` and the intended single-node development configuration. These are alternative build examples, not a requirement to build every profile. For an embedded application, inspect that application's manifest and feature graph, because it can differ from the package-level commands above.

The Controller `File` backend is accepted only with `dev-single`; selecting `storage-file` alone does not enable the runtime File path. Select `storageBackend = "File"` explicitly for that development profile.
