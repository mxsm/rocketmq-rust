# RocketMQ Dashboard Web backend

Standalone Rust 2024 workspace for the Axum dashboard API. It composes shared
Dashboard models and the optional admin facade with a RocketMQ admin adapter,
application-owned client runtime, and persistent dashboard repositories.

## Executables

Run from `rocketmq-dashboard/rocketmq-dashboard-web/backend`:

```bash
cargo run --bin rocketmq-dashboard-web-backend
# Read the selected persistence backend's status
cargo run --bin rocketmq-dashboard-storage -- status --json
```

The package has two binaries, so use an explicit `--bin`. The HTTP service
defaults to `127.0.0.1:8082`. The storage utility uses the same configuration
as the server; backup and restore have additional offline requirements described
in the [storage operations guide](../docs/storage-operations.md).

## Service boundaries

- [Configuration](src/config/app_config.rs) validates the environment, credentials
  and selected File, SQLite, MySQL or PostgreSQL backend before startup. Backend
  selection is strict; an unavailable database does not trigger a File fallback.
- [Routes](src/api/router.rs) expose health, login/session, cluster inspection,
  explicit administration, history, monitor and persistence-status APIs.
- Services coordinate admin calls and repositories; shared contracts live in
  [rocketmq-dashboard-common](../../rocketmq-dashboard-common).
- Client work, history collection and shutdown belong to the application's
  runtime lifecycle. HTTP handlers do not create independent client runtimes.

`DASHBOARD_WEB_LOGIN_REQUIRED` controls operator login. RocketMQ ACL access and
secret keys configure outbound cluster authentication separately. See the
[parent README](../README.md) for the complete environment configuration,
frontend proxy, deployment and persistence choices.

## Development

Select relevant checks from this directory:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked <test_name>
```

Use [AGENTS.md](AGENTS.md) for validation boundaries. Database integration and
browser acceptance use the parent project's dedicated runners.
