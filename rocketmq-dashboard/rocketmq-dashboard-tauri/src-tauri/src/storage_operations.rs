// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod tests;
use crate::{
    audit::{AuditAction, AuditContext},
    error::{DashboardError, DashboardResult},
    persistence::{open_read_only, schema::SCHEMA_VERSION},
};
use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const DATABASE: &str = "dashboard.db";
const METADATA: &str = "metadata.json";
const CONTENT_SCOPE: &str = "accounts, connections, monitor rules, audit, history, sessions (revoked on restore); excludes environment credentials";
const USAGE: &str = "Usage: rocketmq-dashboard-storage status | backup --output <new-dir> | verify --input <dir> | restore --input <dir> --target <empty-dir> --confirm-empty-target\nSet DASHBOARD_TAURI_DATA_DIR for status and backup.";

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    format_version: u32,
    schema_version: i64,
    created_at_ms: i64,
    content_scope: String,
}

enum Operation {
    Status,
    Backup(PathBuf),
    Verify(PathBuf),
    Restore { input: PathBuf, target: PathBuf },
}

fn parse(args: &[OsString]) -> DashboardResult<Operation> {
    match args {
        [command] if command == "status" => Ok(Operation::Status),
        [command, flag, path] if command == "backup" && flag == "--output" => Ok(Operation::Backup(path.into())),
        [command, flag, path] if command == "verify" && flag == "--input" => Ok(Operation::Verify(path.into())),
        [command, input_flag, input, target_flag, target, confirm]
            if command == "restore"
                && input_flag == "--input"
                && target_flag == "--target"
                && confirm == "--confirm-empty-target" =>
        {
            Ok(Operation::Restore {
                input: input.into(),
                target: target.into(),
            })
        }
        _ => Err(DashboardError::Validation(USAGE.into())),
    }
}

fn configured_database() -> DashboardResult<PathBuf> {
    let directory = std::env::var_os("DASHBOARD_TAURI_DATA_DIR")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DashboardError::Configuration("Set DASHBOARD_TAURI_DATA_DIR to the desktop data directory.".into())
        })?;
    Ok(PathBuf::from(directory).join(DATABASE))
}

// This synchronous CLI entry owns all blocking filesystem and SQLite work. No app runtime is started.
pub(crate) fn run(args: Vec<OsString>) -> i32 {
    if args.as_slice() == [OsString::from("--help")] {
        println!("{USAGE}");
        return 0;
    }
    match execute(args) {
        Ok(output) => {
            println!("{output}");
            0
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

fn execute(args: Vec<OsString>) -> DashboardResult<String> {
    match parse(&args)? {
        Operation::Status => {
            let mut connection = open_read_only(&configured_database()?)?;
            Ok(serde_json::to_string_pretty(&crate::ops::read_status(
                &mut connection,
                chrono::Utc::now().timestamp_millis(),
            )?)?)
        }
        Operation::Backup(output) => {
            backup(&configured_database()?, &output)?;
            Ok("Backup complete. Keep the snapshot private; it contains account data.".into())
        }
        Operation::Verify(input) => {
            verify(&input)?;
            Ok("Backup metadata, schema, and SQLite integrity verified.".into())
        }
        Operation::Restore { input, target } => {
            restore(&input, &target)?;
            Ok("Restore complete. All restored sessions are revoked. Set DASHBOARD_TAURI_DATA_DIR to the target and sign in again.".into())
        }
    }
}

fn verify_database(connection: &Connection) -> DashboardResult<()> {
    let version: i64 = connection.query_row("SELECT version FROM dashboard_schema WHERE id=1", [], |row| row.get(0))?;
    if version != SCHEMA_VERSION {
        return Err(DashboardError::UnsupportedStorageVersion);
    }
    let mut check = connection.prepare("PRAGMA quick_check")?;
    let results = check
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if results != ["ok"] {
        return Err(DashboardError::Validation(
            "SQLite integrity verification failed.".into(),
        ));
    }
    // Schema version alone is insufficient when a snapshot was truncated or manually edited.
    for table in [
        "users",
        "sessions",
        "nameserver_addresses",
        "nameserver_settings",
        "proxy_addresses",
        "connection_metadata",
        "endpoint_identity",
        "consumer_monitor_rules",
        "history_samples",
        "audit_events",
        "storage_activity",
    ] {
        connection.prepare(&format!("SELECT * FROM {table} LIMIT 0"))?;
    }
    Ok(())
}

fn snapshot(source: &Connection, path: &Path) -> DashboardResult<Connection> {
    // Reserve the exact destination atomically; never open an existing database for replacement.
    OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut destination = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    {
        let backup = Backup::new(source, &mut destination)?;
        // SQLite copies the committed snapshot, including WAL content. Busy/locked results fail
        // explicitly instead of leaving an unbounded retry loop in an operational command.
        if !matches!(backup.step(-1)?, StepResult::Done) {
            return Err(DashboardError::Configuration(
                "The source is busy; retry backup into another new directory.".into(),
            ));
        }
    }
    destination.pragma_update(None, "journal_mode", "DELETE")?;
    destination.pragma_update(None, "foreign_keys", "ON")?;
    verify_database(&destination)?;
    Ok(destination)
}

fn backup(source_path: &Path, output: &Path) -> DashboardResult<()> {
    let source = open_read_only(source_path)?;
    source.execute_batch("BEGIN")?;
    verify_database(&source)?;
    fs::create_dir(output)?;
    let destination = snapshot(&source, &output.join(DATABASE))?;
    drop(destination);
    let metadata = Metadata {
        format_version: 1,
        schema_version: SCHEMA_VERSION,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
        content_scope: CONTENT_SCOPE.into(),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join(METADATA))?;
    file.write_all(serde_json::to_string_pretty(&metadata)?.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Hold the validated source read transaction through restore, so writes cannot change its snapshot.
fn verify(input: &Path) -> DashboardResult<Connection> {
    let file = fs::File::open(input.join(METADATA))?;
    let mut bytes = Vec::new();
    file.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err(DashboardError::Validation("Backup metadata is too large.".into()));
    }
    let metadata: Metadata = serde_json::from_slice(&bytes)?;
    if metadata.format_version != 1
        || metadata.schema_version != SCHEMA_VERSION
        || metadata.created_at_ms < 0
        || metadata.content_scope != CONTENT_SCOPE
    {
        return Err(DashboardError::UnsupportedStorageVersion);
    }
    let source = open_read_only(&input.join(DATABASE))?;
    source.execute_batch("BEGIN")?;
    verify_database(&source)?;
    Ok(source)
}

fn restore(input: &Path, target: &Path) -> DashboardResult<()> {
    let source = verify(input)?;
    match fs::create_dir(target) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read_dir(target)?.next().transpose()?.is_some() {
                return Err(DashboardError::Validation(
                    "Restore requires an empty target directory.".into(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    // Publish only after session revocation and audit commit. An interrupted restore must
    // never leave a startable dashboard.db containing active snapshot sessions.
    let pending = target.join(format!(".restore-pending-{}.db", uuid::Uuid::new_v4()));
    let mut destination = snapshot(&source, &pending)?;
    let transaction = destination.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute(
        "UPDATE sessions SET revoked_at_ms=COALESCE(revoked_at_ms,?1)",
        [chrono::Utc::now().timestamp_millis()],
    )?;
    let audit = AuditContext {
        actor: Some("local-storage-tool".into()),
        environment: Arc::new(Mutex::new(None)),
        event_id: uuid::Uuid::new_v4().to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        action: AuditAction::RestoreStorage,
        resource_name: None,
    };
    audit.record_local_success(&transaction)?;
    transaction.commit()?;
    drop(destination);
    // hard_link creates the final name atomically and refuses any existing destination.
    fs::hard_link(&pending, target.join(DATABASE))?;
    fs::remove_file(pending)?;
    Ok(())
}
