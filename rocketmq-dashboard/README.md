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
- [Web Implementation](./rocketmq-dashboard-web/README.md)

## Web Dashboard

`rocketmq-dashboard-web` is the browser-based Dashboard implementation. It is split into:

- `rocketmq-dashboard-web/backend`: standalone Rust 2024 + Axum HTTP API project
- `rocketmq-dashboard-web/frontend`: React + TypeScript + Vite frontend project

The Web backend is intentionally not added to the root Cargo workspace, matching the standalone project model already used by GPUI and Tauri desktop implementations.

Backend development:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-web/backend
cargo run
```

Frontend development:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-web/frontend
npm install
npm run dev
```

Configuration can be provided with:

```bash
DASHBOARD_WEB_HOST=127.0.0.1
DASHBOARD_WEB_PORT=8082
NAMESRV_ADDR=127.0.0.1:9876
DASHBOARD_WEB_STORAGE_BACKEND=file
DASHBOARD_WEB_STORAGE_PATH=data/dashboard-config.json
```

Use `DASHBOARD_WEB_STORAGE_BACKEND=sqlite` with a `.db` storage path to persist configuration in SQLite.

Current Web capabilities include health/config APIs, optional auth/session APIs with protected API middleware, file or SQLite config persistence, live RocketMQ Admin queries for Dashboard overview, Topic, Broker, Consumer, Producer, Message lookup, message trace, ACL user/policy read workflows, Topic create/update/delete for explicit cluster or broker targets, Broker config update for explicit broker names or addresses, Consumer reset offset by topic and timestamp, Message resend through direct consume, ACL user create/update/delete, ACL policy create/update/delete, DLQ key/messageId query plus bounded page scan with batch resend/export payloads, local Monitor rule persistence, in-memory Dashboard history collection, a feature-gated common `DashboardAdminFacade` adapter for core Web admin services, plus a Vite UI for Dashboard, Topic, Consumer, Producer, Broker, Message, DLQ, ACL, Monitor, Login, and Config routes. Remaining hardening work is deeper Tauri Admin Manager extraction into common and browser E2E coverage.

## License

This project inherits the license from the parent RocketMQ-Rust project:

- Apache License 2.0
