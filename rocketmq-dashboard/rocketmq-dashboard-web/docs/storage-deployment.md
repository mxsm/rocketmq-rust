# Storage deployment and failure handling

## Choose the backend before first use

The storage backend is selected at startup and is not changed in place. Create
a new target and use the supported backup/restore process for any future move.

| Backend | Deployment mode | Persistent location | Replica policy |
| --- | --- | --- | --- |
| File | Single-node | Dashboard data directory | Exactly one process owns the directory lock. |
| SQLite | Single-node | Dashboard database file | Exactly one writer process owns the database file. |
| MySQL 8.4+ | Multi-node capable | External MySQL database | Multiple dashboard replicas may use one database. |
| PostgreSQL 15+ | Multi-node capable | External PostgreSQL database | Multiple dashboard replicas may use one database. |

Never mount the same File directory or SQLite database file into multiple
dashboard replicas. The File lock and SQLite write semantics are safety
boundaries, not a replication protocol.

## Compose examples

The production example is `deploy/docker-compose.storage.yml`. Start one
profile at a time:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-web/deploy
docker compose --profile file up --build -d
docker compose --profile sqlite up --build -d
docker compose --profile mysql up --build -d
docker compose --profile postgres up --build -d
```

For File and SQLite, Compose provisions separate named volumes. For MySQL and
PostgreSQL, it intentionally provisions no database container: production SQL
is managed by the platform team, independently backed up, and reachable only
through the required network policy. Use a load balancer only with the MySQL or
PostgreSQL profiles.

The image runs as an unprivileged user, drops Linux capabilities, and provides
`/api/health/live` as its container health check. Terminate TLS at the approved
edge or configure the platform ingress; do not expose a dashboard port directly
to the public internet.

## SQL secrets and TLS

The SQL environment examples set:

```text
DASHBOARD_WEB_DATABASE_URL_FILE=/run/secrets/dashboard_database_url
```

The deployment must mount a mode-0600 URL secret at that path and a verified
CA file at the backend-specific path. The secret contains the complete URL and
is never copied to an environment file, Docker label, shell command, log, or
dashboard API response.

MySQL URL requirements:

- `ssl-mode=verify_identity`
- `ssl-ca=/run/secrets/mysql-ca.pem`
- a host name that matches the certificate identity

PostgreSQL URL requirements:

- `sslmode=verify-full`
- `sslrootcert=/run/secrets/postgres-ca.pem`
- a host name that matches the certificate identity

Avoid `ssl-mode=disabled`, `sslmode=disable`, `verify-ca` without hostname
verification, trust-all certificates, or a plaintext URL environment variable
in production. Rotate the URL secret and CA through the platform secret system,
then restart one replica at a time after a health check succeeds.

## Least-privilege database accounts

Use a dedicated application account per dashboard deployment. The account owns
only the dashboard tables and migration metadata in its designated database or
schema. It needs data `SELECT`, `INSERT`, `UPDATE`, and `DELETE`, plus the DDL
needed for the current forward-only migrations (`CREATE`, `ALTER`, `INDEX`, and
foreign-key references). It must not be a server administrator and must not
have global `ALL PRIVILEGES`, `FILE`, `SUPER`, `PROCESS`, replication,
user-management, database-creation, or cross-application schema privileges.

For MySQL, scope grants to the dashboard database. For PostgreSQL, grant
`CONNECT` on the database, `USAGE` and `CREATE` on the dedicated schema, table
data privileges, and sequence `USAGE`/`SELECT` in that schema. Create tables
with the application migration owner so ordinary forward migrations can alter
only dashboard-owned objects. Apply account changes through database change
management, not through the dashboard application.

## Startup, upgrade, and rollback

1. Validate the selected environment file and secret mounts with `docker
   compose config` before deployment.
2. Verify the SQL endpoint and TLS certificate from the deployment network.
3. Start one dashboard replica. Startup applies only current new-architecture
   migrations; it does not import or preserve experimental legacy layouts.
4. Wait for the container health check and `GET /api/health/ready`.
5. Authenticate and confirm the sanitized OPS storage status is Available.
6. For MySQL/PostgreSQL, roll out remaining replicas only after the first
   replica is healthy. Keep total configured SQL pools within database capacity.

Take a verified storage package before a schema-changing release. Rollback is
an application-image rollback only when the target schema is accepted by the
older image. Do not manually delete migration rows or reverse a migration in a
live production database. If a schema rollback is not supported, restore the
verified same-backend package into a new empty target and direct a new instance
to it.

## Failure handling

| Symptom | Safe response |
| --- | --- |
| File directory lock cannot be acquired | Stop duplicate replicas; verify the intended owner before removing any process or lock. |
| SQLite reports lock, I/O, or capacity failure | Keep one writer only, verify volume health and safe capacity, then recover from a verified package into a new file if needed. |
| SQL connection or TLS failure | Verify DNS, network policy, URL secret rotation, CA chain, certificate identity, and database account status without logging the URL. |
| Storage status is Degraded | Treat capacity reserve or bounded status reason as actionable; expand capacity or retention headroom before writes fail. |
| Storage status is Unavailable | Stop configuration-changing operations, preserve logs and metrics, repair the platform dependency, then validate readiness and OPS status. |
| Migration/startup failure | Leave the target intact, capture the bounded failure class, and follow the database change process. Do not retry against a partially altered unsupported target. |
| Restore was rejected | Confirm the package verifies, the backend types match, every dashboard process is stopped, and the target is new and empty. |

See [storage-operations.md](storage-operations.md) for backup, verify, restore,
and offline rules. See [storage-observability.md](storage-observability.md) for
alerts and incident evidence.
