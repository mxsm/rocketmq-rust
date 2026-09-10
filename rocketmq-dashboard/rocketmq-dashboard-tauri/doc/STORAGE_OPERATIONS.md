# Local storage backup and restore (S24)

Run from `src-tauri`. The tool is synchronous and does not start the desktop or
an HTTP server. Cargo's default binary remains the desktop app.

```powershell
# Point explicitly to the directory containing the desktop's dashboard.db.
$env:DASHBOARD_TAURI_DATA_DIR = 'D:\DashboardData\development'
cargo run --bin rocketmq-dashboard-storage -- status
cargo run --bin rocketmq-dashboard-storage -- backup --output 'D:\DashboardBackups\snapshot-01'
cargo run --bin rocketmq-dashboard-storage -- verify --input 'D:\DashboardBackups\snapshot-01'
cargo run --bin rocketmq-dashboard-storage -- restore --input 'D:\DashboardBackups\snapshot-01' --target 'D:\DashboardData\restored-01' --confirm-empty-target

# Start an isolated instance using the restored data; sign in again.
$env:DASHBOARD_TAURI_DATA_DIR = 'D:\DashboardData\restored-01'
cargo run --bin rocketmq-dashboard-tauri
```

`status` and `backup` require `DASHBOARD_TAURI_DATA_DIR`; they never guess which
desktop data directory to operate on. Parent directories must exist. Backup
requires a new output directory. Restore accepts a new or empty target directory
and the explicit confirmation flag. Existing files are never overwritten.

Snapshots use SQLite's backup API, including committed WAL content. The output
contains `dashboard.db` and `metadata.json` with format/schema versions, creation
time, and content scope. Verify uses read-only access, supported metadata/schema
checks, and `PRAGMA quick_check`. Busy sources fail explicitly; retry into another
new directory. Do not substitute a raw copy of an active database file.

The snapshot contains accounts and password hashes, connection settings, monitor
rules, audit, history, and sessions. Treat it as private account data. Credentials
supplied through environment variables are excluded. Restore revokes all snapshot
sessions and commits a local restore audit before publishing `dashboard.db`.
Publication uses an atomic hard link within the target filesystem and requires
hard-link support. It never replaces a running desktop's database.

Interrupted operations may leave an incomplete output directory. Verify rejects
an incomplete backup; failed restores do not publish a startable database with
active snapshot sessions. Inspect that directory and use another empty target to
retry. This tool does not migrate older schemas or restore over existing data.
