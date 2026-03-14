# RocketMQ-Rust Dashboard

Modern dashboard implementations for RocketMQ-Rust and Apache RocketMQ.

## Overview

This directory contains multiple dashboard implementations that share similar domain goals but are built as separate projects:

- [rocketmq-dashboard-common](./rocketmq-dashboard-common): shared models and common logic
- [rocketmq-dashboard-gpui](./rocketmq-dashboard-gpui): native desktop UI built with GPUI
- [rocketmq-dashboard-tauri](./rocketmq-dashboard-tauri): cross-platform desktop UI built with Tauri, Rust, React, and TypeScript

Important: `rocketmq-dashboard` itself is not a Cargo workspace root. You cannot run `cargo build --workspace` or `cargo build -p rocketmq-dashboard-tauri` from this directory.

## Prerequisites

- Rust toolchain 1.85.0 or later
- For `rocketmq-dashboard-tauri`: Node.js and npm
- Tauri platform prerequisites for your OS

## Correct Build And Packaging For `rocketmq-dashboard-tauri`

### Development

Run the Tauri desktop app in development mode:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run tauri dev
```

### Frontend-only build check

This only builds the React frontend and is useful for verifying TypeScript and Vite output:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run build
```

### Desktop package build with npm

This is the default way to produce the Tauri desktop package in this repo:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run tauri build
```

### Desktop package build with Cargo

If you prefer the Cargo-based Tauri CLI, install it first and then build from the same directory:

```bash
cargo install tauri-cli

cd rocketmq-dashboard/rocketmq-dashboard-tauri
cargo tauri build
```

The generated desktop bundle is placed under:

```text
rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/target/release/bundle/
```

Notes:

- `npm run build` does not produce the Tauri installer/package. It only builds the frontend assets.
- `cargo build` inside `src-tauri` does not produce the Tauri installer/package either. It only builds the Rust side.
- `npm run tauri build` and `cargo tauri build` both produce the Tauri desktop package.
- `cargo tauri build` requires the Tauri CLI to be installed first.

## Correct Build For `rocketmq-dashboard-gpui`

### Development

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo run
```

### Release build

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo build --release
```

## Verification

### `rocketmq-dashboard-tauri`

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm run build

cd src-tauri
cargo check
cargo test
```

### `rocketmq-dashboard-gpui`

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo check
cargo test
```

## Documentation

- [Common Library](./rocketmq-dashboard-common/README.md)
- [GPUI Implementation](./rocketmq-dashboard-gpui/README.md)
- [Tauri Implementation](./rocketmq-dashboard-tauri/README.md)

## License

This project inherits the dual-license from the parent RocketMQ-Rust project:

- Apache License 2.0
- MIT License
