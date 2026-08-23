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
$env:DASHBOARD_WEB_STORAGE_PATH="data/dashboard"
$env:DASHBOARD_WEB_LOGIN_REQUIRED="false"
$env:DASHBOARD_WEB_USERNAME="admin"
$env:DASHBOARD_WEB_PASSWORD="rocketmq"
$env:DASHBOARD_WEB_HISTORY_INTERVAL_SECS="60"
$env:DASHBOARD_WEB_USE_VIP_CHANNEL="false"
$env:DASHBOARD_WEB_USE_TLS="false"
$env:DASHBOARD_WEB_ROCKETMQ_ACCESS_KEY="<access-key>"
$env:DASHBOARD_WEB_ROCKETMQ_SECRET_KEY="<secret-key>"
```

The RocketMQ access and secret key variables are optional, but they must be
configured together when the target cluster enforces ACL authentication.

## Persistence configuration

The storage backend is selected strictly at startup. Valid values are `file`,
`sqlite`, `mysql`, and `postgres`; an unknown backend or missing server
database URL prevents startup. Configuration and monitor rules are stored only
by the selected backend; the dashboard never falls back to local File storage.

File storage uses a data **directory** and takes an exclusive lock for the
process lifetime:

```powershell
$env:DASHBOARD_WEB_STORAGE_BACKEND="file"
$env:DASHBOARD_WEB_STORAGE_PATH="data/dashboard"
```

SQLite storage uses an on-disk database file. In-memory SQLite locations such
as `:memory:` and `file::memory:` are intentionally rejected:

```powershell
$env:DASHBOARD_WEB_STORAGE_BACKEND="sqlite"
$env:DASHBOARD_WEB_STORAGE_PATH="data/dashboard.db"
```

MySQL and PostgreSQL use `DASHBOARD_WEB_DATABASE_URL`; do not place database
credentials in source files or logs:

```powershell
$env:DASHBOARD_WEB_STORAGE_BACKEND="mysql"
$env:DASHBOARD_WEB_DATABASE_URL="mysql://<user>:<password>@127.0.0.1:3306/rocketmq_dashboard"

$env:DASHBOARD_WEB_STORAGE_BACKEND="postgres"
$env:DASHBOARD_WEB_DATABASE_URL="postgres://<user>:<password>@127.0.0.1:5432/rocketmq_dashboard"
```

Production MySQL and PostgreSQL endpoints must use TLS with certificate
verification. Configure a verified server name and a CA path in the URL, for
example `mysql://...?...&ssl-mode=verify_identity&ssl-ca=/run/secrets/mysql-ca.pem`
or `postgres://...?sslmode=verify-full&sslrootcert=/run/secrets/postgres-ca.pem`.
Keep CA material and credentials in a platform secret store or mounted secret,
not in the repository or dashboard configuration response.

Optional pool controls are `DASHBOARD_WEB_DB_MIN_CONNECTIONS`,
`DASHBOARD_WEB_DB_MAX_CONNECTIONS`, `DASHBOARD_WEB_DB_CONNECT_TIMEOUT_MS`,
`DASHBOARD_WEB_DB_ACQUIRE_TIMEOUT_MS`, `DASHBOARD_WEB_DB_IDLE_TIMEOUT_SECS`,
and `DASHBOARD_WEB_DB_MAX_LIFETIME_SECS`. All values must be positive and the
minimum may not exceed the maximum.

Storage-aware readiness is exposed through `GET /api/health/ready`; `GET
/api/health/live` only reports process liveness, and `GET /api/health` remains
the readiness endpoint.

## Storage integration test environment

`docker-compose.storage-test.yml` is the single local integration-test entry
point. It verifies fresh and repeated initialization against a File volume, a
real SQLite file, MySQL 8.4, and PostgreSQL 15:

```powershell
cd rocketmq-dashboard/rocketmq-dashboard-web
docker compose -f docker-compose.storage-test.yml config
docker compose -f docker-compose.storage-test.yml up -d mysql postgres
docker compose -f docker-compose.storage-test.yml run --build --rm storage-test-runner
docker compose -f docker-compose.storage-test.yml down -v --remove-orphans
```

The final command intentionally removes the four named test volumes so the
next run again exercises an empty database and the `0001` migrations. To run
all services as one command while preserving the runner's exit code, use:

```powershell
docker compose -f docker-compose.storage-test.yml up --build --abort-on-container-exit --exit-code-from storage-test-runner
```

## Frontend Development

```powershell
cd D:\Github\Rust\rocketmq-rust\rocketmq-dashboard\rocketmq-dashboard-web\frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` to `http://127.0.0.1:8082`.

## Local Kind Deployment

The repository Kind acceptance runner builds the backend and frontend images,
deploys them to the `rocketmq-dashboard` namespace, provisions a generated
login password, and validates a live RocketMQ cluster overview:

```powershell
.\rocketmq-sre\scripts\kind.ps1 -Action Up
```

See `rocketmq-sre/deploy/kind/README.md` for the Dashboard port-forward and
generated-password commands. The frontend remains a ClusterIP Service and
proxies `/api` to the backend inside the cluster.

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
- Config and monitor APIs backed by the Environment/Endpoint/Monitor
  repositories, with File, SQLite, MySQL, and PostgreSQL implementations.
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
- ACL user list/create/update/delete, ACL policy list/create/update/delete, and environment-scoped consumer monitor rules.
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
