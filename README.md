<p align="center">
    <img src="resources/RocketMQ-Rust.png" width="30%" height="auto"/>
    <img src="resources/logo.png" width="30%" height="auto"/>
</p>

<div align="center">

[![GitHub last commit](https://img.shields.io/github/last-commit/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/commits/main)
[![Crates.io](https://img.shields.io/crates/v/rocketmq-rust.svg)](https://crates.io/crates/rocketmq-rust)
[![Docs.rs](https://docs.rs/rocketmq-rust/badge.svg)](https://docs.rs/rocketmq-rust)
[![CI](https://github.com/mxsm/rocketmq-rust/workflows/CI/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions)
[![Website Deploy](https://github.com/mxsm/rocketmq-rust/workflows/Deploy%20to%20GitHub%20Pages/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions/workflows/deploy.yml)
[![Website Check](https://github.com/mxsm/rocketmq-rust/workflows/Website%20Deploy%20Check/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions/workflows/website-check.yml)
[![CodeCov][codecov-image]][codecov-url] [![GitHub contributors](https://img.shields.io/github/contributors/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/graphs/contributors) [![Crates.io License](https://img.shields.io/crates/l/rocketmq-rust)](#license)
<br/>
![GitHub repo size](https://img.shields.io/github/repo-size/mxsm/rocketmq-rust)
![Static Badge](https://img.shields.io/badge/MSRV-1.85.0%2B-25b373)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mxsm/rocketmq-rust)

</div>

<div align="center">
  <a href="https://trendshift.io/repositories/12176" target="_blank"><img src="https://trendshift.io/api/badge/repositories/12176" alt="mxsm%2Frocketmq-rust | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
  <a href="https://trendshift.io/developers/3818" target="_blank"><img src="https://trendshift.io/api/badge/developers/3818" alt="mxsm | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

# RocketMQ-Rust

🚀 A high-performance, reliable, and feature-rich **unofficial Rust implementation** of [Apache RocketMQ](https://github.com/apache/rocketmq), designed to bring
enterprise-grade message middleware to the Rust ecosystem.

<div align="center">

[![Overview](https://img.shields.io/badge/📖_Overview-4A90E2?style=flat-square&labelColor=2C5F9E&color=4A90E2)](#-overview)
[![Quick Start](https://img.shields.io/badge/🚀_Quick_Start-50C878?style=flat-square&labelColor=2D7A4F&color=50C878)](#-quick-start)
[![Documentation](https://img.shields.io/badge/📚_Documentation-FF8C42?style=flat-square&labelColor=CC6A2F&color=FF8C42)](#-documentation)
[![Components](https://img.shields.io/badge/📦_Components-9B59B6?style=flat-square&labelColor=6C3483&color=9B59B6)](#-components--crates)
<br/>
[![Contributing](https://img.shields.io/badge/🤝_Contributing-F39C12?style=flat-square&labelColor=B9770E&color=F39C12)](#-contributing)
[![Community](https://img.shields.io/badge/👥_Community-8E44AD?style=flat-square&labelColor=633974&color=8E44AD)](#-community--support)

</div>

---

## ✨ Overview

**RocketMQ-Rust** is a complete reimplementation of Apache RocketMQ in Rust, leveraging Rust's unique advantages in memory safety, zero-cost abstractions, and
fearless concurrency. This project aims to provide Rust developers with a production-ready distributed message queue system that delivers exceptional
performance while maintaining full compatibility with the RocketMQ protocol.

### 🎯 Why RocketMQ-Rust?

- **🦀 Memory Safety**: Built on Rust's ownership model, eliminating entire classes of bugs like null pointer dereferences, buffer overflows, and data races at
  compile time
- **⚡ High Performance**: Zero-cost abstractions and efficient async runtime deliver exceptional throughput with minimal resource overhead
- **🔒 Thread Safety**: Fearless concurrency enables safe parallel processing without the risk of race conditions
- **🌐 Cross-Platform**: First-class support for Linux, Windows, and macOS with native performance on each platform
- **🔌 Ecosystem Integration**: Seamlessly integrates with the rich Rust ecosystem including Tokio, Serde, and other modern libraries
- **📦 Production Ready**: Battle-tested architecture with comprehensive error handling and observability

## 🏗️ Architecture

<p align="center">
  <img src="resources/architecture.png" alt="RocketMQ-Rust Architecture" width="80%"/>
</p>

RocketMQ-Rust implements a distributed architecture with the following core components:

- **Name Server**: Lightweight service discovery and routing coordination
- **Broker**: Message storage and delivery engine with support for topics, queues, and consumer groups
- **Producer Client**: High-performance message publishing with various sending modes
- **Consumer Client**: Flexible message consumption with push and pull models
- **Store**: Efficient local storage engine optimized for sequential writes
- **Controller**: Advanced high availability and failover capabilities

## 📚 Documentation

- **📖 Official Documentation**: [rocketmqrust.com](https://rocketmqrust.com) - Comprehensive guides, API references, and best practices
- **🤖 AI-Powered Docs**: [DeepWiki](https://deepwiki.com/mxsm/rocketmq-rust) - Interactive documentation with intelligent search
- **📝 API Docs**: [docs.rs/rocketmq-rust](https://docs.rs/rocketmq-rust) - Complete API documentation
- **📋 Examples**: [rocketmq-client/examples](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples) - Ready-to-run code samples

## 🚀 Quick Start

### Prerequisites

- Rust toolchain 1.85.0 or later
- A shell with `cargo` available
- Separate terminals for the NameServer, Broker, and client examples

### 1. Build the Workspace

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
cargo build --workspace
```

If you only want to use the client SDK from your own application, add the current release to `Cargo.toml`:

```toml
[dependencies]
rocketmq-client-rust = "1.0.0"
rocketmq-common = "1.0.0"
```

### 2. Start the NameServer

```bash
cargo run --bin rocketmq-namesrv-rust
```

The default NameServer endpoint is `127.0.0.1:9876`. To bind explicitly:

```bash
cargo run --bin rocketmq-namesrv-rust -- --ip 127.0.0.1 --port 9876
```

### 3. Start the Broker

The Broker requires `ROCKETMQ_HOME`. Point it at an existing RocketMQ home or create a local runtime directory for quick testing.

Linux/macOS:

```bash
export ROCKETMQ_HOME="$(pwd)/.rocketmq"
mkdir -p "$ROCKETMQ_HOME/conf"
cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876
```

Windows PowerShell:

```powershell
$env:ROCKETMQ_HOME = "$PWD\.rocketmq"
New-Item -ItemType Directory -Force "$env:ROCKETMQ_HOME\conf" | Out-Null
cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876
```

Use `cargo run --bin rocketmq-broker-rust -- --help` to inspect configuration flags such as `--configFile`, `--namesrvAddr`, and config printing options.

### 4. Send and Receive Messages

Start the consumer example first:

```bash
cargo run -p rocketmq-client-rust --example consumer
```

Then send messages from another terminal:

```bash
cargo run -p rocketmq-client-rust --example producer
```

The quickstart examples use `127.0.0.1:9876` and `TopicTest` by default. For more messaging patterns, see:

- [Send single messages](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-a-single-message)
- [Send batch messages](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-batch-messages)
- [RPC messaging](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-rpc-messages)
- [All examples](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples)

## 📦 Components & Crates

RocketMQ-Rust is organized into deployable services, reusable protocol/runtime crates, and operational applications. The tables below focus on responsibility and integration boundaries instead of per-crate maturity labels.

### Core Runtime Services

| Crate | Responsibility |
|-------|----------------|
| [rocketmq](./rocketmq) | Public foundation crate and shared runtime entry points. |
| [rocketmq-namesrv](./rocketmq-namesrv) | NameServer implementation for broker registration, topic routing, and service discovery. |
| [rocketmq-broker](./rocketmq-broker) | Broker implementation for message storage, dispatch, delivery, and consumer coordination. |
| [rocketmq-controller](./rocketmq-controller) | Controller service for broker coordination and high availability workflows. |
| [rocketmq-proxy](./rocketmq-proxy) | Proxy layer for gateway-style client access and protocol integration. |

### Client, Protocol, and Shared Libraries

| Crate | Responsibility |
|-------|----------------|
| [rocketmq-client](./rocketmq-client) | Async producer, consumer, and admin SDK for application integration. |
| [rocketmq-remoting](./rocketmq-remoting) | RocketMQ remoting protocol, command encoding/decoding, and network integration. |
| [rocketmq-common](./rocketmq-common) | Shared message models, configuration types, constants, and utility code. |
| [rocketmq-auth](./rocketmq-auth) | Authentication, authorization, ACL evaluation, and request context support. |
| [rocketmq-filter](./rocketmq-filter) | Message filtering support, including tag and expression-based filtering. |

### Storage, Runtime, and Observability

| Crate | Responsibility |
|-------|----------------|
| [rocketmq-store](./rocketmq-store) | Durable local storage engine for commit logs, consume queues, and message indexes. |
| [rocketmq-tieredstore](./rocketmq-tieredstore) | Tiered storage abstractions for extending message data beyond local disks. |
| [rocketmq-runtime](./rocketmq-runtime) | Async runtime abstractions and runtime-friendly coordination utilities. |
| [rocketmq-error](./rocketmq-error) | Shared error types and result conventions across workspace crates. |
| [rocketmq-macros](./rocketmq-macros) | Procedural macros used by RocketMQ-Rust crates and examples. |
| [rocketmq-observability](./rocketmq-observability) | Metrics and tracing integration for service and client instrumentation. |

### Tools, Examples, and Dashboards

| Project | Responsibility |
|---------|----------------|
| [rocketmq-example](./rocketmq-example) | Standalone examples covering producer, consumer, request/reply, ordering, delay, and transaction flows. |
| [rocketmq-tools](./rocketmq-tools) | Command-line tools and operational utilities. |
| [rocketmq-admin-cli](./rocketmq-tools/rocketmq-admin/rocketmq-admin-cli) | Command-line administration interface for cluster and broker operations. |
| [rocketmq-admin-core](./rocketmq-tools/rocketmq-admin/rocketmq-admin-core) | Shared admin functionality used by CLI and terminal interfaces. |
| [rocketmq-admin-tui](./rocketmq-tools/rocketmq-admin/rocketmq-admin-tui) | Terminal UI for interactive administration workflows. |
| [rocketmq-store-inspect](./rocketmq-tools/rocketmq-store-inspect) | Storage inspection utilities for broker data files. |
| [rocketmq-dashboard](./rocketmq-dashboard) | Dashboard workspace for desktop, web, and shared management UI components. |
| [rocketmq-dashboard-common](./rocketmq-dashboard/rocketmq-dashboard-common) | Shared dashboard models and reusable dashboard infrastructure. |
| [rocketmq-dashboard-gpui](./rocketmq-dashboard/rocketmq-dashboard-gpui) | GPUI-based desktop dashboard. |
| [rocketmq-dashboard-tauri](./rocketmq-dashboard/rocketmq-dashboard-tauri) | Tauri-based cross-platform dashboard shell and backend. |
| [rocketmq-dashboard-web](./rocketmq-dashboard/rocketmq-dashboard-web) | Web dashboard frontend and backend project. |

## 💡 Capabilities

RocketMQ-Rust focuses on RocketMQ-compatible messaging services and Rust-native integration points.

| Area | What it provides |
|------|------------------|
| Messaging services | NameServer, Broker, Controller, and Proxy services for routing, storage, delivery, coordination, and gateway access. |
| Client integration | Async producer, consumer, admin, request/reply, batch, ordered, delayed, and transactional messaging APIs. |
| Protocol compatibility | RocketMQ remoting command models, headers, serialization, route discovery, and client/broker interoperability. |
| Storage engine | Durable commit log, consume queue, index, checkpoint, and tiered storage building blocks. |
| Security and governance | Authentication, authorization, ACL evaluation, request context, and broker/client-side integration points. |
| Operations | Metrics, tracing, admin tools, storage inspection utilities, and dashboard projects for cluster visibility. |

## 🧪 Build & Validation

Quick Start covers the first local run. For regular development and review, use the root workspace commands below.

| Task | Command |
|------|---------|
| Build the workspace | `cargo build --workspace` |
| Run workspace tests | `cargo test --workspace` |
| Run a focused crate test | `cargo test -p rocketmq-client` |
| Format Rust code | `cargo fmt --all` |
| Run clippy with workspace features | `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` |
| Build local API documentation | `cargo doc --workspace --no-deps` |

Standalone projects under `rocketmq-example/` and `rocketmq-dashboard/` are validated from their own project roots.

## 🤝 Contributing

We welcome contributions from the community! Whether you're fixing bugs, adding features, improving documentation, or sharing ideas, your input is valuable.

### How to Contribute

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Contribution Guidelines

- Follow Rust best practices and idiomatic patterns
- Add tests for new functionality
- Update documentation as needed
- Ensure CI passes before submitting PR
- Use meaningful commit messages

For detailed guidelines, please read our [Contribution Guide](https://rocketmqrust.com/docs/contribute-guide/).

### Repository Activity

![Repository Activity](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

## ❓ FAQ

<details>
<summary><b>Is RocketMQ-Rust production-ready?</b></summary>

Yes. The core services and client SDK are designed for production-oriented deployments and are actively maintained.
</details>

<details>
<summary><b>Is it compatible with Apache RocketMQ?</b></summary>

Yes, RocketMQ-Rust implements the RocketMQ protocol and can interoperate with Apache RocketMQ Java clients and servers.
</details>

<details>
<summary><b>What's the minimum supported Rust version (MSRV)?</b></summary>

The minimum supported Rust version is 1.85.0 (stable or nightly).
</details>

<details>
<summary><b>How does performance compare to Java RocketMQ?</b></summary>

RocketMQ-Rust leverages Rust's zero-cost abstractions and efficient async runtime to deliver comparable or better performance with lower memory footprint.
Benchmarks are available in individual component documentation.
</details>

<details>
<summary><b>Can I use it with existing RocketMQ deployments?</b></summary>

Yes, you can deploy RocketMQ-Rust components alongside Java RocketMQ. For example, you can use Rust clients with Java brokers, or vice versa.
</details>

<details>
<summary><b>How can I migrate from Java RocketMQ to RocketMQ-Rust?</b></summary>

Migration can be done incrementally:

1. Start by using Rust client SDK with existing Java brokers
2. Gradually replace brokers with Rust implementation
3. Both implementations can coexist during migration

Refer to our [migration guide](https://rocketmqrust.com) for detailed steps.
</details>

## 👥 Community & Support

- **💬 Discussions**: [GitHub Discussions](https://github.com/mxsm/rocketmq-rust/discussions) - Ask questions and share ideas
- **🐛 Issues**: [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues) - Report bugs or request features
- **📧 Contact**: Reach out to [mxsm@apache.org](mailto:mxsm@apache.org)

### Contributors

Thanks to all our contributors! 🙏

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>

### Star History

[![Star History Chart](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://star-history.com/#mxsm/rocketmq-rust&Date)

## 📄 License

RocketMQ-Rust is licensed under the **Apache License 2.0**.

See [LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0.

## 🙏 Acknowledgments

- **Apache RocketMQ Community** for the original Java implementation and design
- **Rust Community** for excellent tooling and libraries
- **All Contributors** who have helped make this project better

---

<p align="center">
  <sub>Built with ❤️ by the RocketMQ-Rust community</sub>
</p>

[codecov-image]: https://codecov.io/gh/mxsm/rocketmq-rust/branch/main/graph/badge.svg

[codecov-url]: https://codecov.io/gh/mxsm/rocketmq-rust

