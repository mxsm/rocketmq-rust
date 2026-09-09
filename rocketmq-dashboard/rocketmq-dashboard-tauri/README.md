# RocketMQ-Rust Dashboard (Tauri)

A RocketMQ-Rust desktop dashboard built with Tauri, React, and Rust.

## Features

- Live cluster, broker, topic, consumer, producer and message inspection, with
  explicit topic/consumer administration and message actions
- Saved NameServer and Proxy address configuration (Proxy entries do not control
  Proxy server processes)
- Native desktop packaging with a web UI
- Rust backend and React frontend
- Local embedded authentication with SQLite
- Argon2 password hashing
- Forced bootstrap password change on first login
- In-memory session management with session restore while the backend process remains alive

## Authentication Quick Start

On first startup the application bootstraps a local administrator account:

- Username: `admin`
- Initial password:
  - `ROCKETMQ_DASHBOARD_INIT_PASSWORD`, if provided
  - otherwise `admin123`

After the first successful login, the user must change the password before entering the dashboard.

Authentication and saved NameServer/Proxy configuration share `dashboard.db` in
Tauri's application configuration directory (identifier `com.rocketmqrust.dashboard`):

- Windows: `%APPDATA%\com.rocketmqrust.dashboard\dashboard.db`
- macOS: `~/Library/Application Support/com.rocketmqrust.dashboard/dashboard.db`
- Linux: `$XDG_CONFIG_HOME/com.rocketmqrust.dashboard/dashboard.db`, or
  `~/.config/com.rocketmqrust.dashboard/dashboard.db` when unset

For more detail, see [doc/AUTH_CONFIG.md](./doc/AUTH_CONFIG.md).

## Development

### Prerequisites

- Rust toolchain
- Node.js and npm
- Tauri platform prerequisites for your OS

### Install

From the repository root:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm ci
```

### Run in development mode

```bash
npm run tauri dev
```

If you prefer Cargo:

```bash
cargo install tauri-cli
cargo tauri dev
```

### Build

```bash
npm run build
npm run tauri build
```

If you prefer Cargo for packaging:

```bash
cargo install tauri-cli
cargo tauri build
```

Notes:

- `npm run build` only builds the frontend assets.
- `npm run tauri build` or `cargo tauri build` produces the desktop package.
- `cargo build` inside `src-tauri` only builds the Rust backend and does not generate the installer bundle.

## Verification

Backend:

```bash
cd src-tauri
cargo test
```

Frontend:

```bash
npm run build
```

## Reset local state

Deleting `dashboard.db` resets the administrator **and saved NameServer/Proxy
configuration**. To reset all of this local state:

1. Stop the application.
2. Back up `dashboard.db`, then remove it from the application config directory.
3. Restart the application.
4. Sign in again with the bootstrap password source.

## License

Licensed under:

- Apache License, Version 2.0
