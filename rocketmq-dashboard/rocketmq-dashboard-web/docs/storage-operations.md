# Dashboard storage operations

The `rocketmq-dashboard-storage` command is the supported administrative
surface for inspecting, backing up, verifying, and restoring dashboard
persistence. It uses the same `DASHBOARD_WEB_STORAGE_*` and database
configuration as the Web backend. It does not contact RocketMQ brokers or
NameServers while it operates on storage.

Run the commands from `rocketmq-dashboard/rocketmq-dashboard-web/backend`:

```bash
cargo run --bin rocketmq-dashboard-storage -- status --json
cargo run --bin rocketmq-dashboard-storage -- backup --output ./backup/dashboard-2026-08-24
cargo run --bin rocketmq-dashboard-storage -- verify --input ./backup/dashboard-2026-08-24
```

The container image includes the same binary. For SQL profiles, use an
ephemeral operation container that receives the same environment file and
Docker secrets as the server; do not copy the database URL into a command
line, shell history, or Compose file.

## Status

`status` reports the selected backend, supported topology mode, health,
schema/format version, safe capacity when it is meaningful, and observed
write state. `--json` is intended for automation. It never prints a filesystem
path, database URL, database user, token, password, or TLS material.

The OPS page uses the authenticated `GET /api/ops/storage/status` equivalent.
It is not a replacement for database-level monitoring: it reports what this
dashboard process can safely observe.

## Backup package

`backup --output <directory>` creates a new directory. The destination must
not already contain dashboard backup files. A package contains only ordinary,
human-inspectable data files:

```text
<output>/
  manifest.json
  environments.ndjson
  endpoints.ndjson
  monitors.ndjson
  history.ndjson
  sessions.ndjson
  audit.ndjson
```

`manifest.json` identifies the storage backend, backup format version,
creation time, and the included collections. Each NDJSON file has one record
per line. This first format intentionally has no checksum, fingerprint, CRC,
custom binary frame, compressed archive, or compatibility reader for
experimental layouts.

The backup scope is limited to the new persistence architecture:

- Environment and Endpoint configuration
- Consumer Monitor rules
- Dashboard History samples
- Session records (never plaintext session tokens)
- Management Audit records (with their existing redaction behavior)

Treat a package as operationally sensitive: session token hashes, endpoint
addresses, operator identifiers, and audit facts can still be confidential.
Write packages to an access-controlled location, restrict permissions, and use
the platform's encrypted backup storage where required.

### Offline rules and consistency

- **File:** stop every dashboard process that references the data directory
  before backup. The File backend owns an exclusive process lock, so an online
  backup is unsupported.
- **SQLite:** run one operation process against the selected database file. Do
  not run a second dashboard writer during the operation window.
- **MySQL and PostgreSQL:** backup uses a consistent database read snapshot;
  the dashboard may remain online. The backup tool only uses the configured
  least-privilege application account.
- **All backends:** `restore` is offline. Stop dashboard replicas and other
  storage-operation processes before starting a restore.

## Verify

`verify --input <directory>` reads a package without modifying a storage
target. It rejects unknown format versions, missing required collections,
malformed JSON/NDJSON records, duplicate identities, missing required fields,
invalid revisions or timestamps, and broken environment/endpoint/monitor
relationships. A successful verify proves the package has the structure the
current tool can restore; it does not test a broker connection or replace a
full disaster-recovery exercise.

## Restore

Restore is intentionally constrained to prevent accidental overwrite:

```bash
cargo run --bin rocketmq-dashboard-storage -- \
  restore --input ./backup/dashboard-2026-08-24 --confirm-empty-target
```

Before restore:

1. Verify the package with the same command version.
2. Stop all dashboard and storage-operation processes that use the target.
3. Create a new, empty target in the **same backend type** recorded by the
   manifest. File is restored into a new empty data directory; SQLite into a
   new empty database file; SQL into a new empty database/schema.
4. Supply `--confirm-empty-target`. A non-empty target is rejected and is not
   modified.
5. Start one dashboard instance, check `GET /api/health/ready`, authenticate,
   and inspect the OPS storage status before allowing normal traffic.

The first release does not provide cross-backend restore. A File package is not
restorable into SQLite, MySQL, or PostgreSQL, and the reverse combinations are
also rejected. It does not import prior experimental dashboard storage layouts.

SQL restore writes the package as one transaction. File restore writes a staged
new directory and publishes it atomically only after validation completes.
If either operation fails, the empty target remains the only supported retry
target; do not attempt an in-place repair of a partially managed target.

## Recovery boundaries

The command does not create database accounts, alter network firewall rules,
repair a damaged production database, merge a package into existing data, or
recover records that were never included in the package. Escalate any of those
cases through the platform database or incident-response process. Record the
backup creation time, command version, backend, package location, and restore
result in the change record.
