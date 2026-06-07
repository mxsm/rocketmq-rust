# RocketMQ Dashboard Web

Web implementation of the RocketMQ-Rust Dashboard.

## Project Layout

```text
rocketmq-dashboard-web/
  backend/   # Rust 2024 + Axum HTTP API
  frontend/  # React + TypeScript + Vite UI
```

## Relationship To Other Dashboard Modules

- `rocketmq-dashboard-common` provides shared Dashboard models, reusable configuration logic, and an optional `admin` facade contract used by the Web backend.
- `rocketmq-dashboard-gpui` is the GPUI native desktop implementation.
- `rocketmq-dashboard-tauri` is the Tauri desktop implementation.
- `rocketmq-dashboard-web` is the browser-based Web implementation.

The Web backend is a standalone Cargo project and is not added to the root workspace.

## Backend Development

```powershell
cd D:\Github\Rust\rocketmq-rust\rocketmq-dashboard\rocketmq-dashboard-web\backend
cargo run
```

Default backend address:

```text
http://127.0.0.1:8082
```

Useful environment variables:

```powershell
$env:DASHBOARD_WEB_HOST="127.0.0.1"
$env:DASHBOARD_WEB_PORT="8082"
$env:NAMESRV_ADDR="127.0.0.1:9876"
$env:DASHBOARD_WEB_STORAGE_BACKEND="file"
$env:DASHBOARD_WEB_STORAGE_PATH="data/dashboard-config.json"
$env:DASHBOARD_WEB_LOGIN_REQUIRED="false"
$env:DASHBOARD_WEB_USERNAME="admin"
$env:DASHBOARD_WEB_PASSWORD="rocketmq"
$env:DASHBOARD_WEB_HISTORY_INTERVAL_SECS="60"
```

For SQLite storage:

```powershell
$env:DASHBOARD_WEB_STORAGE_BACKEND="sqlite"
$env:DASHBOARD_WEB_STORAGE_PATH="data/dashboard.db"
```

## Frontend Development

```powershell
cd D:\Github\Rust\rocketmq-rust\rocketmq-dashboard\rocketmq-dashboard-web\frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` to `http://127.0.0.1:8082`.

## Production Build

Backend:

```powershell
cd D:\Github\Rust\rocketmq-rust\rocketmq-dashboard\rocketmq-dashboard-web\backend
cargo build --release
```

Frontend:

```powershell
cd D:\Github\Rust\rocketmq-rust\rocketmq-dashboard\rocketmq-dashboard-web\frontend
npm install
npm run build
```

## Completed In The Initial Web Version

- Rust Axum backend project scaffold.
- Unified `ApiResponse<T>` response shape.
- Health API: `GET /api/health`.
- Config API with file and SQLite persistence.
- REST route surface for Dashboard, Topic, Consumer, Producer, Broker, Message, and Config.
- Live read-only RocketMQ Admin wiring for:
  - Dashboard overview with `DOWN` fallback when the configured NameServer is unreachable.
  - Topic list, detail, route, and stats.
  - Broker list, runtime stats, and broker config.
  - Consumer group list and consumption progress.
  - Producer group list and producer connections.
  - Message query by topic/key and by topic/messageId.
- Live Topic create/update/delete write operations for explicit cluster or broker targets.
- Live Broker config update for explicit broker names or addresses.
- Live Consumer reset offset by topic and timestamp.
- Live Message resend through direct consume when topic and consumer group are provided.
- Live Message trace lookup through RocketMQ track-detail admin when topic and message ID are provided.
- ACL user list/create/update/delete, ACL policy list/create/update/delete, and local JSON consumer monitor rule storage.
- DLQ query by key/messageId, bounded page-scan query, batch direct-consume resend, and JSON/CSV export payloads.
- Web service modules use the feature-gated common `DashboardAdminFacade` for core Dashboard, Topic, Consumer, Producer, Broker, and Message operations.
- Auth/session API with optional environment-driven login requirement, protected API middleware, and frontend login flow.
- Dashboard history APIs backed by an in-memory background collector; set `DASHBOARD_WEB_HISTORY_INTERVAL_SECS=0` to disable collection.
- React + TypeScript + Vite frontend.
- Dashboard app shell with sidebar, top status bar, light/dark theme, dense tables, search, pagination, loading/error/empty states, confirmation dialogs, and message detail drawer.
- Frontend routes for Dashboard, Topics, Consumers, Producers, Brokers, Messages, DLQ, ACL, Monitors, and Config.

## TODO

- Move the mature Tauri admin manager internals into reusable common/admin implementation modules beyond the current shared facade contract.
- Move ACL and DLQ operations from Web-only code into the shared common admin facade once GPUI/Tauri need the same surface.
- Add browser E2E tests once live admin behavior is wired.
