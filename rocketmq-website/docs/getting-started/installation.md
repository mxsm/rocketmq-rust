---
sidebar_position: 1
title: Installation
---

# Installation

Get RocketMQ-Rust up and running on your system.

## Prerequisites

Before installing RocketMQ-Rust, ensure you have the following:

- **Rust**: 1.70.0 or later ([Install Rust](https://www.rust-lang.org/tools/install))
- **Cargo**: Comes with Rust
- **Operating System**: Linux, macOS, or Windows

### Verify Installation

```bash
rustc --version
cargo --version
```

## Install from Crates.io

The easiest way to use RocketMQ-Rust is to add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
rocketmq = "0.3"
```

Then run:

```bash
cargo build
```

## Install from Source

For the latest features and bug fixes, you can build from source:

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
cargo build --release
```

## Running Examples

RocketMQ-Rust includes various examples to help you get started:

```bash
# Clone the repository
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust

# Run a simple producer example
cargo run --example simple_producer

# Run a simple consumer example
cargo run --example simple_consumer
```

## Docker Setup

For development and testing, you can use Docker:

```bash
# Pull the RocketMQ nameserver image
docker pull apache/rocketmq:nameserver

# Pull the RocketMQ broker image
docker pull apache/rocketmq:broker

# Start nameserver
docker run -d -p 9876:9876 --name rmqnamesrv apache/rocketmq:nameserver

# Start broker
docker run -d -p 10911:10911 -p 10909:10909 --name rmqbroker \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  --link rmqnamesrv:rmqnamesrv \
  apache/rocketmq:broker
```

## Next Steps

- [Quick Start Guide](./quick-start) - Create your first producer and consumer
- [Basic Concepts](./basic-concepts) - Understand RocketMQ's core concepts
- [Configuration](../category/configuration) - Configure your RocketMQ instance

## Troubleshooting

### Rust Version Too Old

If you see an error about Rust version, update Rust:

```bash
rustup update stable
```

### Build Failures

If the build fails, try:

```bash
cargo clean
cargo build
```

### Port Already in Use

If the default ports are already in use, you can configure different ports in your broker settings. See [Configuration](../category/configuration) for details.
