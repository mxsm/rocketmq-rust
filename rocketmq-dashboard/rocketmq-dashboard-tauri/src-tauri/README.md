# RocketMQ Dashboard Tauri backend

Rust backend for the [Tauri desktop dashboard](../README.md). This directory is
a standalone Cargo workspace. The `app_lib` library supplies Tauri integration
and the executable hosts it; this package does not expose a standalone HTTP API.

## Responsibilities

- Register typed Tauri commands for authentication, NameServer and Proxy address
  configuration, cluster and broker inspection, topics, consumers, producers,
  messages and dashboard metrics.
- Persist local authentication and saved NameServer/Proxy configuration in
  SQLite `dashboard.db`, under Tauri's application configuration directory.
- Own the RocketMQ client runtime and admin manager for the desktop process;
  shutdown closes the admin manager, shared client and runtime owner.

[src/lib.rs](src/lib.rs) is the application composition and command registration
entry point. The frontend invokes these commands through Tauri IPC.

The application identifier is `com.rocketmqrust.dashboard`. The initial
administrator password comes from `ROCKETMQ_DASHBOARD_INIT_PASSWORD`, falling
back to `admin123` only when the administrator is first created. First login
requires changing that password. Sessions are held in memory and end when the
backend process exits. Deleting the database also deletes saved NameServer and
Proxy configuration; see the parent README before resetting local state.

## Build and validate

Use the frontend project directory and `npm run tauri dev` to run the desktop
application. For Rust-only checks, run from this directory:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked <test_name>
```

`cargo build` builds Rust artifacts. Desktop packaging and frontend assets
require the parent project's Tauri build workflow and platform prerequisites.
See [AGENTS.md](AGENTS.md) and the [application README](../README.md).
