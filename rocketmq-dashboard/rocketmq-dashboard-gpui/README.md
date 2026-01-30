# RocketMQ Dashboard (GPUI)

A RocketMQ Dashboard GUI implemented with [GPUI](https://www.gpui.rs/).

## Features

- Native desktop application with GPU-accelerated rendering
- Cross-platform support (Linux, macOS, Windows)
- Written entirely in Rust
- Low memory footprint and high performance

## Quick Start

### Development Mode

```bash
# From project root
cargo run -p rocketmq-dashboard-gpui

# From rocketmq-dashboard directory
cd rocketmq-dashboard
cargo run -p rocketmq-dashboard-gpui

# From current directory (rocketmq-dashboard-gpui)
cargo run
```

### Release Mode

```bash
# From project root
cargo run -p rocketmq-dashboard-gpui --release

# From rocketmq-dashboard directory
cd rocketmq-dashboard
cargo run -p rocketmq-dashboard-gpui --release

# From current directory
cargo run --release
```

### With Logging

```bash
# Enable debug logging
RUST_LOG=debug cargo run -p rocketmq-dashboard-gpui

# Enable trace logging
RUST_LOG=trace cargo run -p rocketmq-dashboard-gpui
```

## Building

```bash
# Development build
cargo build -p rocketmq-dashboard-gpui

# From current directory
cargo build

# Release build
cargo build -p rocketmq-dashboard-gpui --release

# From current directory
cargo build --release
```

**Build Comparison:**
- **Dev build**: ~10x faster to compile, slower runtime, includes debug symbols
- **Release build**: Slower to compile, optimized performance, smaller binary size

## Development

### Code Quality

```bash
# Format code
cargo fmt -p rocketmq-dashboard-gpui

# Run Clippy
cargo clippy -p rocketmq-dashboard-gpui --all-targets -- -D warnings

# Run tests
cargo test -p rocketmq-dashboard-gpui
```

## Platform-Specific Notes

### macOS
Requires Xcode command line tools for building.

### Linux
Requires development libraries:
```bash
sudo apt-get install libxcb1-dev libxkbcommon-dev libxkbcommon-x11-dev
```

### Windows
No additional dependencies required.

## License

Licensed under Apache-2.0 or MIT.
