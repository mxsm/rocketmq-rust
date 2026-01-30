# RocketMQ Dashboard

Multiple dashboard implementations for RocketMQ, built with different UI frameworks.

## Structure

This is a nested workspace containing:

- **rocketmq-dashboard-common**: Shared code, data models, and business logic
- **rocketmq-dashboard-gpui**: Native desktop UI using [GPUI](https://www.gpui.rs/)
- **rocketmq-dashboard-tauri**: Web-based UI using [Tauri](https://tauri.app/) (planned)

## Why Multiple Implementations?

Different UI frameworks have different strengths:

- **GPUI**: Native performance, GPU-accelerated rendering, Rust-only
- **Tauri**: Web technologies (HTML/CSS/JS), smaller bundle size, easier for web developers

## Quick Start

### Development Mode

For rapid development iteration with faster compile times:

```bash
cd rocketmq-dashboard

# Run GPUI version in dev mode
cargo run -p rocketmq-dashboard-gpui

# Run Tauri version in dev mode (when available)
cargo run -p rocketmq-dashboard-tauri
```

### Release Mode

For production builds with full optimizations:

```bash
# Build all implementations
cargo build --workspace --release

# Build specific implementation
cargo build -p rocketmq-dashboard-gpui --release
cargo build -p rocketmq-dashboard-tauri --release

# Run in release mode
cargo run -p rocketmq-dashboard-gpui --release
cargo run -p rocketmq-dashboard-tauri --release
```

**Performance Note**: Dev mode is ~10x faster to compile but runs slower. Use dev mode for development, release mode for testing performance or distribution.

## Development

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

### Full CI-like Check

Run all checks as CI would:

```bash
# Format check (don't modify files)
cargo fmt --all -- --check

# Clippy with all features
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings

# Build all in release mode
cargo build --workspace --release

# Run tests
cargo test --workspace --all-features
```

## Documentation

Each implementation has its own README with specific development instructions:

- [Common Library README](rocketmq-dashboard-common/README.md) - Shared code and APIs
- [GPUI Implementation README](rocketmq-dashboard-gpui/README.md) - Native desktop UI
- [Tauri Implementation README](rocketmq-dashboard-tauri/README.md) - Web-based UI (planned)
