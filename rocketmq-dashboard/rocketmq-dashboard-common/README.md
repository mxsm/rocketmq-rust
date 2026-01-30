# RocketMQ Dashboard Common

Common shared library for RocketMQ Dashboard implementations.

## Overview

This crate provides the foundational building blocks for different UI implementations:

- **Data Models**: Shared data structures (brokers, topics, consumer groups)
- **API Traits**: `DashboardClient` trait for RocketMQ API clients
- **Service Layer**: Business logic for dashboard operations

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
rocketmq-dashboard-common = { path = "../rocketmq-dashboard-common" }
```

## Development

```bash
# Run tests
cargo test -p rocketmq-dashboard-common

# Format and lint
cargo fmt -p rocketmq-dashboard-common
cargo clippy -p rocketmq-dashboard-common --all-targets -- -D warnings
```

## License

Licensed under Apache-2.0 or MIT.
