# RocketMQ-Rust Dashboard

Modern, high-performance dashboard implementations for RocketMQ-Rust, compatible with Apache RocketMQ. Built with cutting-edge UI frameworks to provide multiple deployment options.

## 🎯 Overview

This workspace provides multiple dashboard implementations using different UI technologies, all sharing the same core business logic. Whether you prefer native desktop performance or web-based flexibility, we've got you covered.

**Compatibility**: All implementations are compatible with Apache RocketMQ clusters, allowing seamless management of both RocketMQ-Rust and Apache RocketMQ deployments.

## 📦 Structure

This is a nested workspace containing:

- **[rocketmq-dashboard-common](./rocketmq-dashboard-common)**: Shared code, data models, API clients, and business logic
- **[rocketmq-dashboard-gpui](./rocketmq-dashboard-gpui)**: Native desktop UI using [GPUI](https://www.gpui.rs/) - GPU-accelerated, pure Rust
- **[rocketmq-dashboard-tauri](./rocketmq-dashboard-tauri)**: Cross-platform desktop UI using [Tauri](https://tauri.app/) - Rust backend with web frontend

## 🤔 Why Multiple Implementations?

Different UI frameworks excel in different scenarios:

| Framework | Strengths | Best For |
|-----------|-----------|----------|
| **GPUI** | • Native performance<br>• GPU-accelerated rendering<br>• Pure Rust (no web stack)<br>• Low memory footprint | Power users, developers comfortable with native apps |
| **Tauri** | • Familiar web technologies (React/TypeScript)<br>• Rich UI component ecosystem<br>• Easier to customize and extend<br>• Cross-platform (Windows, macOS, Linux) | Teams with web development experience, rapid prototyping |

Both implementations share the same core functionality through `rocketmq-dashboard-common`.

## 🚀 Quick Start

### Prerequisites

- Rust toolchain 1.85.0 or later
- For Tauri: Node.js 24.x and npm

### Development Mode

For rapid development iteration with faster compile times:

```bash
cd rocketmq-dashboard

# Run GPUI version in dev mode
cargo run -p rocketmq-dashboard-gpui

# Run Tauri version in dev mode
cd rocketmq-dashboard-tauri
cargo tauri dev
```

### Production Builds

For optimized builds with full performance:

```bash
# Build all implementations
cargo build --workspace --release

# Build specific implementation
cargo build -p rocketmq-dashboard-gpui --release
cargo build -p rocketmq-dashboard-tauri --release

# Run in release mode
cargo run -p rocketmq-dashboard-gpui --release

# For Tauri, build distributable package
cd rocketmq-dashboard-tauri
cargo tauri build
```

**⚡ Performance Tip**: Dev mode compiles ~10x faster but runs slower. Use dev mode for development, release mode for performance testing and distribution.

## 🛠️ Development

### Code Quality Checks

Run comprehensive checks before committing:

```bash
# Check compilation
cargo check --workspace

# Format code
cargo fmt --all

# Run Clippy (linter) - all implementations
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run Clippy on specific package
cargo clippy -p rocketmq-dashboard-common --all-targets -- -D warnings
cargo clippy -p rocketmq-dashboard-gpui --all-targets -- -D warnings
cargo clippy -p rocketmq-dashboard-tauri --all-targets -- -D warnings

# Run tests
cargo test --workspace
```

### Full CI-like Validation

Run all checks exactly as CI does:

```bash
# Format check (non-modifying)
cargo fmt --all -- --check

# Clippy with all features and strict warnings
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings

# Build everything in release mode
cargo build --workspace --release

# Run comprehensive tests
cargo test --workspace --all-features
```

## 📚 Documentation

Each implementation has detailed documentation:

- **[Common Library](./rocketmq-dashboard-common/README.md)** - Shared APIs, data models, and business logic
- **[GPUI Implementation](./rocketmq-dashboard-gpui/README.md)** - Native desktop UI with GPU acceleration
- **[Tauri Implementation](./rocketmq-dashboard-tauri/README.md)** - Web-based cross-platform UI

## 🔌 Apache RocketMQ Compatibility

All dashboard implementations are fully compatible with Apache RocketMQ:

- **Protocol Compatibility**: Uses standard RocketMQ admin APIs
- **Cluster Management**: Manage Apache RocketMQ brokers and name servers
- **Mixed Deployments**: Monitor both RocketMQ-Rust and Apache RocketMQ in the same dashboard
- **Feature Parity**: Supports all standard dashboard operations (topic management, consumer groups, message queries, etc.)

## 🤝 Contributing

Contributions are welcome! Whether you want to improve the shared common library or enhance a specific UI implementation, please:

1. Check existing issues or create a new one
2. Fork the repository
3. Create a feature branch
4. Make your changes with appropriate tests
5. Run the full CI check suite
6. Submit a pull request

## 📄 License

This project inherits the dual-license from the parent RocketMQ-Rust project:
- Apache License 2.0
- MIT License
