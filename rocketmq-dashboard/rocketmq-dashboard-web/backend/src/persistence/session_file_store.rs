// Copyright 2023 The RocketMQ Rust Authors
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

use super::FileMutationOutcome;
use super::FilePersistence;
use super::OpenOptions;
use super::PersistenceError;
use super::Utc;
use super::rollback_jsonl_append;
use super::write_json_new_file;
use crate::model::AuditEvent;
use crate::model::NewSession;
use crate::model::SessionRecord;
use crate::model::SessionTokenHash;
use crate::persistence::session_repository::SessionCursor;
use crate::persistence::session_repository::SessionPage;
use crate::persistence::session_repository::SessionQuery;
use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileSessionRecord {
    session_id: String,
    username: String,
    created_at_ms: i64,
    expires_at_ms: i64,
    last_seen_at_ms: i64,
    revoked_at_ms: Option<i64>,
}

impl FileSessionRecord {
    fn from_new(session: NewSession) -> Self {
        Self {
            session_id: session.session_id,
            username: session.username,
            created_at_ms: session.created_at_ms,
            expires_at_ms: session.expires_at_ms,
            last_seen_at_ms: session.created_at_ms,
            revoked_at_ms: None,
        }
    }

    fn into_record(self, token_hash: SessionTokenHash) -> SessionRecord {
        SessionRecord {
            session_id: self.session_id,
            token_hash,
            username: self.username,
            created_at_ms: self.created_at_ms,
            expires_at_ms: self.expires_at_ms,
            last_seen_at_ms: self.last_seen_at_ms,
            revoked_at_ms: self.revoked_at_ms,
        }
    }
}

impl FilePersistence {
    pub(crate) async fn create_session_with_audit(
        &self,
        session: NewSession,
        audit: AuditEvent,
    ) -> Result<(), PersistenceError> {
        self.create_session_with_audit_capped(session, audit, usize::MAX, 0)
            .await
    }

    pub(crate) async fn create_session_with_audit_capped(
        &self,
        session: NewSession,
        audit: AuditEvent,
        max_active_sessions: usize,
        now_ms: i64,
    ) -> Result<(), PersistenceError> {
        let token_hash = session.token_hash;
        let username = session.username.clone();
        let record = FileSessionRecord::from_new(session);
        self.run_session_audit_mutation("dashboard-file-session-create-audit", move |root| {
            let active = active_path(root, &token_hash);
            let revoked = revoked_path(root, &token_hash);
            if active.exists() || revoked.exists() {
                return Err(PersistenceError::Conflict);
            }
            if max_active_sessions != usize::MAX
                && active_session_count(root, &username, now_ms)? >= max_active_sessions
            {
                return Err(PersistenceError::Conflict);
            }
            let prepared = prepare_session_audit_transaction(
                root,
                vec![SessionAuditStagedWrite::create(token_hash, record)],
                &audit,
            )?;
            publish_session_audit_transaction(&prepared, root, &audit)?;
            Ok(((), Some(prepared)))
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn create_session(&self, session: NewSession) -> Result<(), PersistenceError> {
        let token_hash = session.token_hash;
        let record = FileSessionRecord::from_new(session);
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-session-create", move || {
            let path = active_path(&root, &token_hash);
            write_json_new_file(&path, &record)?;
            Ok(FileMutationOutcome {
                value: (),
                finalize: Box::new(|| Ok(())),
                cleanup: Box::new(|| Ok(())),
                rollback: Box::new(move || remove_session_file(&path)),
            })
        })
        .await?;
        self.record_write();
        Ok(())
    }

    pub(crate) async fn find_session(
        &self,
        token_hash: &SessionTokenHash,
    ) -> Result<Option<SessionRecord>, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let token_hash = *token_hash;
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-session-find", move || load_session(&root, token_hash))
            .await
            .map_err(PersistenceError::Runtime)?
    }

    pub(crate) async fn touch_session(
        &self,
        token_hash: &SessionTokenHash,
        observed_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let token_hash = *token_hash;
        let mutation_blocker = self.mutation_blocker.clone();
        let changed = self
            .dispatch_file_mutation(write_guard, "dashboard-file-session-touch", move || {
                let active = active_path(&root, &token_hash);
                if !active.exists() {
                    return Ok(FileMutationOutcome {
                        value: false,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                }
                let mut record: FileSessionRecord = read_session_file(&active)?;
                if record.revoked_at_ms.is_some() || record.last_seen_at_ms >= observed_at_ms {
                    return Ok(FileMutationOutcome {
                        value: false,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                }
                record.last_seen_at_ms = observed_at_ms;
                let prepared = prepare_session_touch_transaction(&root, token_hash, &record)?;
                publish_session_touch_transaction(&prepared)?;
                mutation_blocker.wait_after_mutation();
                Ok(FileMutationOutcome {
                    value: true,
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        let cleanup_blocker = mutation_blocker.clone();
                        move || {
                            cleanup_blocker.wait_after_committed_cleanup()?;
                            prepared.cleanup()
                        }
                    }),
                    rollback: Box::new(move || prepared.rollback()),
                })
            })
            .await?;
        if changed {
            self.record_write();
        }
        Ok(changed)
    }

    pub(crate) async fn revoke_session(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
    ) -> Result<bool, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let token_hash = *token_hash;
        let changed = self
            .dispatch_file_mutation(write_guard, "dashboard-file-session-revoke", move || {
                let active = active_path(&root, &token_hash);
                if !active.exists() {
                    return Ok(FileMutationOutcome {
                        value: false,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                }
                let mut record: FileSessionRecord = read_session_file(&active)?;
                if record.revoked_at_ms.is_some() {
                    return Ok(FileMutationOutcome {
                        value: false,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                }
                record.revoked_at_ms = Some(revoked_at_ms);
                let revoked = revoked_path(&root, &token_hash);
                write_json_new_file(&revoked, &record)?;
                std::fs::remove_file(&active).map_err(PersistenceError::Io)?;
                Ok(FileMutationOutcome {
                    value: true,
                    finalize: Box::new(|| Ok(())),
                    cleanup: Box::new(|| Ok(())),
                    rollback: Box::new(move || {
                        if revoked.exists() {
                            std::fs::rename(&revoked, &active).map_err(PersistenceError::Io)?;
                        }
                        Ok(())
                    }),
                })
            })
            .await?;
        if changed {
            self.record_write();
        }
        Ok(changed)
    }

    pub(crate) async fn revoke_session_with_audit(
        &self,
        token_hash: &SessionTokenHash,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<bool, PersistenceError> {
        let token_hash = *token_hash;
        self.run_session_audit_mutation("dashboard-file-session-revoke-audit", move |root| {
            let active = active_path(root, &token_hash);
            if !active.exists() {
                return Ok((false, None));
            }
            let record: FileSessionRecord = read_session_file(&active)?;
            if record.revoked_at_ms.is_some() || revoked_path(root, &token_hash).exists() {
                return Err(PersistenceError::CorruptedData);
            }
            let prepared = prepare_session_audit_transaction(
                root,
                vec![SessionAuditStagedWrite::revoke(token_hash, record, revoked_at_ms)],
                &audit,
            )?;
            publish_session_audit_transaction(&prepared, root, &audit)?;
            Ok((true, Some(prepared)))
        })
        .await
    }

    pub(crate) async fn revoke_all_sessions(
        &self,
        username: &str,
        revoked_at_ms: i64,
    ) -> Result<u64, PersistenceError> {
        let mut revoked = 0;
        let mut cursor = None;
        loop {
            let page = self
                .list_sessions(SessionQuery {
                    username: Some(username.to_string()),
                    cursor,
                    limit: 200,
                })
                .await?;
            for record in page.records {
                if record.revoked_at_ms.is_none()
                    && record.expires_at_ms > revoked_at_ms
                    && self.revoke_session(&record.token_hash, revoked_at_ms).await?
                {
                    revoked += 1;
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(revoked)
    }

    pub(crate) async fn revoke_all_sessions_with_audit(
        &self,
        username: &str,
        revoked_at_ms: i64,
        audit: AuditEvent,
    ) -> Result<u64, PersistenceError> {
        let username = username.to_string();
        self.run_session_audit_mutation("dashboard-file-session-revoke-all-audit", move |root| {
            let directory = root.join("sessions").join("active");
            let mut writes = Vec::new();
            if directory.exists() {
                for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
                    let entry = entry.map_err(PersistenceError::Io)?;
                    let hash = parse_hash(&entry.path()).ok_or(PersistenceError::CorruptedData)?;
                    let record: FileSessionRecord = read_session_file(&entry.path())?;
                    if record.username == username
                        && record.revoked_at_ms.is_none()
                        && record.expires_at_ms > revoked_at_ms
                    {
                        if revoked_path(root, &hash).exists() {
                            return Err(PersistenceError::CorruptedData);
                        }
                        writes.push(SessionAuditStagedWrite::revoke(hash, record, revoked_at_ms));
                    }
                }
            }
            let count = writes.len() as u64;
            let prepared = prepare_session_audit_transaction(root, writes, &audit)?;
            publish_session_audit_transaction(&prepared, root, &audit)?;
            Ok((count, Some(prepared)))
        })
        .await
    }

    pub(crate) async fn list_sessions(&self, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
        let _read_guard = self.read_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-session-list", move || list_sessions(&root, query))
            .await
            .map_err(PersistenceError::Runtime)?
    }

    pub(crate) async fn delete_sessions_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let mutation_blocker = self.mutation_blocker.clone();
        let deleted = self
            .dispatch_file_mutation(write_guard, "dashboard-file-session-cleanup", move || {
                let Some(prepared) = prepare_session_cleanup_transaction(&root, cutoff_ms, limit)? else {
                    return Ok(FileMutationOutcome {
                        value: 0,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                };
                let deleted = prepared.delete_staged_sessions()?;
                mutation_blocker.wait_after_mutation();
                Ok(FileMutationOutcome {
                    value: deleted,
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.cleanup()
                    }),
                    rollback: Box::new(move || prepared.rollback()),
                })
            })
            .await?;
        if deleted > 0 {
            self.record_write();
        }
        Ok(deleted)
    }

    /// Publishes the session transition and its one audit append under one
    /// durable decision marker. The marker is retained until the committed
    /// decision is fsynced, so request cancellation cannot leave the two
    /// files with an unknowable relationship.
    async fn run_session_audit_mutation<T, F>(&self, name: &'static str, operation: F) -> Result<T, PersistenceError>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<(T, Option<PreparedSessionAuditTransaction>), PersistenceError> + Send + 'static,
    {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let value = self
            .dispatch_file_mutation(write_guard, name, move || {
                let (value, prepared) = operation(&root)?;
                let Some(prepared) = prepared else {
                    return Ok(FileMutationOutcome {
                        value,
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                };
                Ok(FileMutationOutcome {
                    value,
                    finalize: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.finalize()
                    }),
                    cleanup: Box::new({
                        let prepared = prepared.clone();
                        move || prepared.cleanup()
                    }),
                    rollback: Box::new(move || prepared.rollback()),
                })
            })
            .await?;
        self.record_write();
        Ok(value)
    }
}

fn active_session_count(root: &Path, username: &str, now_ms: i64) -> Result<usize, PersistenceError> {
    let directory = root.join("sessions").join("active");
    if !directory.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        let _hash = parse_hash(&entry.path()).ok_or(PersistenceError::CorruptedData)?;
        let record: FileSessionRecord = read_session_file(&entry.path())?;
        if record.username == username && record.revoked_at_ms.is_none() && record.expires_at_ms > now_ms {
            count += 1;
        }
    }
    Ok(count)
}

const SESSION_AUDIT_MARKER_VERSION: u32 = 1;
const SESSION_TOUCH_MARKER_VERSION: u32 = 1;
// One revoke-all decision can legitimately cover more than a single list
// page. Keep the marker bounded, but above the public 200-record page cap.
const MAX_SESSION_AUDIT_WRITES: usize = 1_000;
const SESSION_CLEANUP_MARKER_VERSION: u32 = 1;
const MAX_SESSION_CLEANUP_WRITES: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SessionAuditOperation {
    Create,
    Revoke,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionAuditWrite {
    hash_hex: String,
    operation: SessionAuditOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditAppendRollback {
    path: String,
    original_length: u64,
    existed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionAuditTransaction {
    format_version: u32,
    writes: Vec<SessionAuditWrite>,
    audit: AuditAppendRollback,
}

/// The touch marker only names the hashed session target. Its exact stage and
/// hard-link backup paths are derived from the durable transaction ID, so a
/// prepared recovery can restore the old record without trusting temporary
/// filenames left by an interrupted request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionTouchTransaction {
    format_version: u32,
    hash_hex: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SessionCleanupLocation {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCleanupWrite {
    hash_hex: String,
    location: SessionCleanupLocation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCleanupTransaction {
    format_version: u32,
    writes: Vec<SessionCleanupWrite>,
}

#[derive(Debug)]
struct SessionAuditStagedWrite {
    hash: SessionTokenHash,
    operation: SessionAuditOperation,
    record: FileSessionRecord,
}

impl SessionAuditStagedWrite {
    fn create(hash: SessionTokenHash, record: FileSessionRecord) -> Self {
        Self {
            hash,
            operation: SessionAuditOperation::Create,
            record,
        }
    }

    fn revoke(hash: SessionTokenHash, mut record: FileSessionRecord, revoked_at_ms: i64) -> Self {
        record.revoked_at_ms = Some(revoked_at_ms);
        Self {
            hash,
            operation: SessionAuditOperation::Revoke,
            record,
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedSessionAuditTransaction {
    root: PathBuf,
    transaction_id: String,
    prepared_marker: PathBuf,
    committed_marker: PathBuf,
}

#[derive(Debug, Clone)]
struct PreparedSessionCleanupTransaction {
    root: PathBuf,
    transaction_id: String,
    prepared_marker: PathBuf,
    committed_marker: PathBuf,
}

#[derive(Debug, Clone)]
struct PreparedSessionTouchTransaction {
    root: PathBuf,
    transaction_id: String,
    prepared_marker: PathBuf,
    committed_marker: PathBuf,
}

impl PreparedSessionAuditTransaction {
    fn finalize(&self) -> Result<(), PersistenceError> {
        let transaction = read_session_audit_transaction(&self.prepared_marker)?;
        write_json_new_file(&self.committed_marker, &transaction)
    }

    fn cleanup(&self) -> Result<(), PersistenceError> {
        cleanup_session_audit_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }

    fn rollback(&self) -> Result<(), PersistenceError> {
        rollback_session_audit_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }
}

impl PreparedSessionCleanupTransaction {
    fn delete_staged_sessions(&self) -> Result<u64, PersistenceError> {
        let transaction = read_session_cleanup_transaction(&self.prepared_marker)?;
        for write in &transaction.writes {
            remove_file_if_exists(&session_cleanup_target(&self.root, write)?)?;
        }
        Ok(transaction.writes.len() as u64)
    }

    fn finalize(&self) -> Result<(), PersistenceError> {
        let transaction = read_session_cleanup_transaction(&self.prepared_marker)?;
        write_json_new_file(&self.committed_marker, &transaction)
    }

    fn cleanup(&self) -> Result<(), PersistenceError> {
        cleanup_session_cleanup_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }

    fn rollback(&self) -> Result<(), PersistenceError> {
        rollback_session_cleanup_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }
}

impl PreparedSessionTouchTransaction {
    fn finalize(&self) -> Result<(), PersistenceError> {
        let transaction = read_session_touch_transaction(&self.prepared_marker)?;
        write_json_new_file(&self.committed_marker, &transaction)
    }

    fn cleanup(&self) -> Result<(), PersistenceError> {
        cleanup_session_touch_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }

    fn rollback(&self) -> Result<(), PersistenceError> {
        rollback_session_touch_transaction(
            &self.root,
            &self.transaction_id,
            &self.prepared_marker,
            &self.committed_marker,
        )
    }
}

/// Recovers a touch as an old-or-new decision. A prepared marker always
/// restores the exact hard-link backup; a committed marker keeps the newly
/// observed timestamp and removes only recovery metadata.
pub(crate) fn recover_session_touch_transactions(root: &Path) -> Result<(), PersistenceError> {
    let directory = root.join("transactions");
    if !directory.exists() {
        return Ok(());
    }
    let mut prepared = std::collections::BTreeMap::new();
    let mut committed = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if !entry.file_type().map_err(PersistenceError::Io)?.is_file() {
            continue;
        }
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(PersistenceError::CorruptedData)?;
        if let Some(id) = name.strip_suffix(".session-touch.prepared.json") {
            prepared.insert(id.to_string(), path);
        } else if let Some(id) = name.strip_suffix(".session-touch.committed.json") {
            committed.insert(id.to_string(), path);
        }
    }
    for (id, committed_marker) in committed {
        let _transaction = read_session_touch_transaction(&committed_marker)?;
        let prepared_marker = prepared
            .remove(&id)
            .unwrap_or_else(|| session_touch_marker_path(&directory, &id, "prepared"));
        cleanup_session_touch_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    for (id, prepared_marker) in prepared {
        let committed_marker = session_touch_marker_path(&directory, &id, "committed");
        rollback_session_touch_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    Ok(())
}

/// Recovers only `*.session-audit.*.json` markers. Snapshot recovery calls
/// this separately because a session target is a mutable record rather than
/// an immutable revision snapshot.
pub(crate) fn recover_session_audit_transactions(root: &Path) -> Result<(), PersistenceError> {
    let directory = root.join("transactions");
    if !directory.exists() {
        return Ok(());
    }
    let mut prepared = std::collections::BTreeMap::new();
    let mut committed = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if !entry.file_type().map_err(PersistenceError::Io)?.is_file() {
            continue;
        }
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(PersistenceError::CorruptedData)?;
        if let Some(id) = name.strip_suffix(".session-audit.prepared.json") {
            prepared.insert(id.to_string(), path);
        } else if let Some(id) = name.strip_suffix(".session-audit.committed.json") {
            committed.insert(id.to_string(), path);
        }
    }
    for (id, committed_marker) in committed {
        let _transaction = read_session_audit_transaction(&committed_marker)?;
        let prepared_marker = prepared
            .remove(&id)
            .unwrap_or_else(|| marker_path(&directory, &id, "prepared"));
        cleanup_session_audit_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    for (id, prepared_marker) in prepared {
        let committed_marker = marker_path(&directory, &id, "committed");
        rollback_session_audit_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    Ok(())
}

/// Recovers the bounded deletion batches used by session retention. A
/// prepared marker restores its hard-link backups, while a committed marker
/// only removes now-obsolete recovery metadata.
pub(crate) fn recover_session_cleanup_transactions(root: &Path) -> Result<(), PersistenceError> {
    let directory = root.join("transactions");
    if !directory.exists() {
        return Ok(());
    }
    let mut prepared = std::collections::BTreeMap::new();
    let mut committed = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if !entry.file_type().map_err(PersistenceError::Io)?.is_file() {
            continue;
        }
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(PersistenceError::CorruptedData)?;
        if let Some(id) = name.strip_suffix(".session-cleanup.prepared.json") {
            prepared.insert(id.to_string(), path);
        } else if let Some(id) = name.strip_suffix(".session-cleanup.committed.json") {
            committed.insert(id.to_string(), path);
        }
    }
    for (id, committed_marker) in committed {
        let _transaction = read_session_cleanup_transaction(&committed_marker)?;
        let prepared_marker = prepared
            .remove(&id)
            .unwrap_or_else(|| session_cleanup_marker_path(&directory, &id, "prepared"));
        cleanup_session_cleanup_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    for (id, prepared_marker) in prepared {
        let committed_marker = session_cleanup_marker_path(&directory, &id, "committed");
        rollback_session_cleanup_transaction(root, &id, &prepared_marker, &committed_marker)?;
    }
    Ok(())
}

fn prepare_session_cleanup_transaction(
    root: &Path,
    cutoff_ms: i64,
    limit: usize,
) -> Result<Option<PreparedSessionCleanupTransaction>, PersistenceError> {
    let transaction_id = Uuid::now_v7().to_string();
    let directory = root.join("transactions");
    std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
    let prepared_marker = session_cleanup_marker_path(&directory, &transaction_id, "prepared");
    let committed_marker = session_cleanup_marker_path(&directory, &transaction_id, "committed");
    let mut writes = Vec::new();
    for (location, session_directory) in [
        (SessionCleanupLocation::Active, root.join("sessions").join("active")),
        (SessionCleanupLocation::Revoked, root.join("sessions").join("revoked")),
    ] {
        if !session_directory.exists() {
            continue;
        }
        for entry in std::fs::read_dir(session_directory).map_err(PersistenceError::Io)? {
            if writes.len() >= limit {
                break;
            }
            let entry = entry.map_err(PersistenceError::Io)?;
            let hash = parse_hash(&entry.path()).ok_or(PersistenceError::CorruptedData)?;
            let record: FileSessionRecord = read_session_file(&entry.path())?;
            if record.revoked_at_ms.unwrap_or(record.expires_at_ms) < cutoff_ms {
                writes.push(SessionCleanupWrite {
                    hash_hex: hash.lower_hex(),
                    location,
                });
            }
        }
        if writes.len() >= limit {
            break;
        }
    }
    if writes.is_empty() {
        return Ok(None);
    }
    if writes.len() > MAX_SESSION_CLEANUP_WRITES {
        return Err(PersistenceError::InvalidConfig(
            "session cleanup batch exceeds the recoverable marker limit".to_string(),
        ));
    }
    let transaction = SessionCleanupTransaction {
        format_version: SESSION_CLEANUP_MARKER_VERSION,
        writes,
    };
    // Write the decision grammar before any original can disappear. Missing
    // backups are harmless while an original is still present; a missing
    // original requires its matching backup during rollback.
    write_json_new_file(&prepared_marker, &transaction)?;
    for write in &transaction.writes {
        let source = session_cleanup_target(root, write)?;
        let backup = session_cleanup_backup_path(&directory, &transaction_id, write)?;
        std::fs::hard_link(source, backup).map_err(PersistenceError::Io)?;
    }
    Ok(Some(PreparedSessionCleanupTransaction {
        root: root.to_path_buf(),
        transaction_id,
        prepared_marker,
        committed_marker,
    }))
}

fn prepare_session_audit_transaction(
    root: &Path,
    writes: Vec<SessionAuditStagedWrite>,
    audit: &AuditEvent,
) -> Result<PreparedSessionAuditTransaction, PersistenceError> {
    if writes.len() > MAX_SESSION_AUDIT_WRITES {
        return Err(PersistenceError::Conflict);
    }
    let transaction_id = Uuid::now_v7().to_string();
    let directory = root.join("transactions");
    std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
    let prepared_marker = marker_path(&directory, &transaction_id, "prepared");
    let committed_marker = marker_path(&directory, &transaction_id, "committed");

    let audit_path = audit_path(root, audit.created_at_ms)?;
    std::fs::create_dir_all(audit_path.parent().ok_or(PersistenceError::CorruptedData)?)
        .map_err(PersistenceError::Io)?;
    super::truncate_incomplete_tail(&audit_path)?;
    let existed = audit_path.exists();
    let original_length = if existed {
        std::fs::metadata(&audit_path).map_err(PersistenceError::Io)?.len()
    } else {
        0
    };
    let mut marker_writes = Vec::with_capacity(writes.len());
    for staged in writes {
        let hash_hex = staged.hash.lower_hex();
        let staged_path = staged_path(&directory, &transaction_id, &hash_hex);
        write_json_new_file(&staged_path, &staged.record)?;
        if matches!(staged.operation, SessionAuditOperation::Revoke) {
            let active = active_path(root, &staged.hash);
            let backup = backup_path(&directory, &transaction_id, &hash_hex);
            std::fs::hard_link(active, backup).map_err(PersistenceError::Io)?;
        }
        marker_writes.push(SessionAuditWrite {
            hash_hex,
            operation: staged.operation,
        });
    }
    write_json_new_file(
        &prepared_marker,
        &SessionAuditTransaction {
            format_version: SESSION_AUDIT_MARKER_VERSION,
            writes: marker_writes,
            audit: AuditAppendRollback {
                path: audit_path
                    .strip_prefix(root)
                    .map_err(|_| PersistenceError::CorruptedData)?
                    .to_string_lossy()
                    .replace('\\', "/"),
                original_length,
                existed,
            },
        },
    )?;
    // The payload is staged before the intent marker. The event itself is not
    // serialized into a marker, so its safe detail projection is not
    // duplicated outside the JSONL append.
    Ok(PreparedSessionAuditTransaction {
        root: root.to_path_buf(),
        transaction_id,
        prepared_marker,
        committed_marker,
    })
}

fn publish_session_audit_transaction(
    prepared: &PreparedSessionAuditTransaction,
    root: &Path,
    audit: &AuditEvent,
) -> Result<(), PersistenceError> {
    let transaction = read_session_audit_transaction(&prepared.prepared_marker)?;
    let directory = root.join("transactions");
    for write in &transaction.writes {
        let hash = hash_from_lower_hex(&write.hash_hex)?;
        let stage = staged_path(&directory, &prepared.transaction_id, &write.hash_hex);
        let target = match write.operation {
            SessionAuditOperation::Create => active_path(root, &hash),
            SessionAuditOperation::Revoke => revoked_path(root, &hash),
        };
        std::fs::create_dir_all(target.parent().ok_or(PersistenceError::CorruptedData)?)
            .map_err(PersistenceError::Io)?;
        std::fs::hard_link(stage, target).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                PersistenceError::Conflict
            } else {
                PersistenceError::Io(error)
            }
        })?;
    }
    let audit_path = root.join(&transaction.audit.path);
    // The marker is durable before the append can become visible. For
    // revocations we keep active records in place while JSONL is synced; the
    // prepared-marker rollback can therefore restore either side of every
    // crash point.
    write_session_audit_event(
        &audit_path,
        audit,
        transaction.audit.original_length,
        transaction.audit.existed,
    )?;
    for write in &transaction.writes {
        if matches!(write.operation, SessionAuditOperation::Revoke) {
            let hash = hash_from_lower_hex(&write.hash_hex)?;
            std::fs::remove_file(active_path(root, &hash)).map_err(PersistenceError::Io)?;
        }
    }
    Ok(())
}

fn write_session_audit_event(
    path: &Path,
    audit: &AuditEvent,
    original_length: u64,
    existed: bool,
) -> Result<(), PersistenceError> {
    if path.exists() != existed
        || (existed && std::fs::metadata(path).map_err(PersistenceError::Io)?.len() != original_length)
    {
        return Err(PersistenceError::Conflict);
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    serde_json::to_writer(&mut file, audit).map_err(PersistenceError::Serialization)?;
    file.write_all(b"\n").map_err(PersistenceError::Io)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_data().map_err(PersistenceError::Io)
}

fn rollback_session_audit_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    if committed_marker.exists() {
        return Err(PersistenceError::Conflict);
    }
    let transaction = read_session_audit_transaction(prepared_marker)?;
    let directory = root.join("transactions");
    for write in transaction.writes.iter().rev() {
        let hash = hash_from_lower_hex(&write.hash_hex)?;
        match write.operation {
            SessionAuditOperation::Create => remove_file_if_exists(&active_path(root, &hash))?,
            SessionAuditOperation::Revoke => {
                remove_file_if_exists(&revoked_path(root, &hash))?;
                let active = active_path(root, &hash);
                if !active.exists() {
                    std::fs::hard_link(backup_path(&directory, transaction_id, &write.hash_hex), active)
                        .map_err(PersistenceError::Io)?;
                }
            }
        }
    }
    rollback_jsonl_append(
        root.join(&transaction.audit.path),
        transaction.audit.original_length,
        transaction.audit.existed,
    )?;
    cleanup_session_audit_transaction(root, transaction_id, prepared_marker, committed_marker)
}

fn cleanup_session_audit_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    let transaction = if prepared_marker.exists() {
        Some(read_session_audit_transaction(prepared_marker)?)
    } else if committed_marker.exists() {
        Some(read_session_audit_transaction(committed_marker)?)
    } else {
        None
    };
    if let Some(transaction) = transaction {
        let directory = root.join("transactions");
        for write in transaction.writes {
            remove_file_if_exists(&staged_path(&directory, transaction_id, &write.hash_hex))?;
            remove_file_if_exists(&backup_path(&directory, transaction_id, &write.hash_hex))?;
        }
    }
    remove_file_if_exists(prepared_marker)?;
    remove_file_if_exists(committed_marker)
}

fn read_session_audit_transaction(path: &Path) -> Result<SessionAuditTransaction, PersistenceError> {
    let transaction: SessionAuditTransaction = serde_json::from_reader(File::open(path).map_err(PersistenceError::Io)?)
        .map_err(|_| PersistenceError::CorruptedData)?;
    if transaction.format_version != SESSION_AUDIT_MARKER_VERSION || transaction.writes.len() > MAX_SESSION_AUDIT_WRITES
    {
        return Err(PersistenceError::CorruptedData);
    }
    if transaction.audit.original_length > 0 && !transaction.audit.existed {
        return Err(PersistenceError::CorruptedData);
    }
    if !is_audit_relative_path(&transaction.audit.path) {
        return Err(PersistenceError::CorruptedData);
    }
    for write in &transaction.writes {
        let _ = hash_from_lower_hex(&write.hash_hex)?;
    }
    Ok(transaction)
}

fn marker_path(directory: &Path, transaction_id: &str, decision: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.session-audit.{decision}.json"))
}

fn staged_path(directory: &Path, transaction_id: &str, hash_hex: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.{hash_hex}.session-stage.json"))
}

fn backup_path(directory: &Path, transaction_id: &str, hash_hex: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.{hash_hex}.session-backup.json"))
}

fn session_cleanup_marker_path(directory: &Path, transaction_id: &str, decision: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.session-cleanup.{decision}.json"))
}

fn session_touch_marker_path(directory: &Path, transaction_id: &str, decision: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.session-touch.{decision}.json"))
}

fn session_touch_staged_path(directory: &Path, transaction_id: &str, hash_hex: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.{hash_hex}.session-touch-stage.json"))
}

fn session_touch_backup_path(directory: &Path, transaction_id: &str, hash_hex: &str) -> PathBuf {
    directory.join(format!("{transaction_id}.{hash_hex}.session-touch-backup.json"))
}

#[cfg(test)]
pub(super) fn stage_session_touch_for_reopen_test(
    root: &Path,
    hash: SessionTokenHash,
    observed_at_ms: i64,
    committed: bool,
) -> Result<(), PersistenceError> {
    let active = active_path(root, &hash);
    let mut record: FileSessionRecord = read_session_file(&active)?;
    record.last_seen_at_ms = observed_at_ms;
    let prepared = prepare_session_touch_transaction(root, hash, &record)?;
    publish_session_touch_transaction(&prepared)?;
    if committed {
        prepared.finalize()?;
    }
    Ok(())
}

fn prepare_session_touch_transaction(
    root: &Path,
    hash: SessionTokenHash,
    record: &FileSessionRecord,
) -> Result<PreparedSessionTouchTransaction, PersistenceError> {
    let transaction_id = Uuid::now_v7().to_string();
    let hash_hex = hash.lower_hex();
    let directory = root.join("transactions");
    std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
    let prepared_marker = session_touch_marker_path(&directory, &transaction_id, "prepared");
    let committed_marker = session_touch_marker_path(&directory, &transaction_id, "committed");
    // The durable intent exists before any sidecar. This is what lets startup
    // distinguish a pre-publication failure from an old-or-new publication.
    write_json_new_file(
        &prepared_marker,
        &SessionTouchTransaction {
            format_version: SESSION_TOUCH_MARKER_VERSION,
            hash_hex: hash_hex.clone(),
        },
    )?;
    let active = active_path(root, &hash);
    let backup = session_touch_backup_path(&directory, &transaction_id, &hash_hex);
    std::fs::hard_link(&active, &backup).map_err(PersistenceError::Io)?;
    write_json_new_file(
        &session_touch_staged_path(&directory, &transaction_id, &hash_hex),
        record,
    )?;
    Ok(PreparedSessionTouchTransaction {
        root: root.to_path_buf(),
        transaction_id,
        prepared_marker,
        committed_marker,
    })
}

fn publish_session_touch_transaction(prepared: &PreparedSessionTouchTransaction) -> Result<(), PersistenceError> {
    let transaction = read_session_touch_transaction(&prepared.prepared_marker)?;
    let hash = hash_from_lower_hex(&transaction.hash_hex)?;
    let directory = prepared.root.join("transactions");
    let active = active_path(&prepared.root, &hash);
    let backup = session_touch_backup_path(&directory, &prepared.transaction_id, &transaction.hash_hex);
    let stage = session_touch_staged_path(&directory, &prepared.transaction_id, &transaction.hash_hex);
    if !backup.exists() || !stage.exists() {
        return Err(PersistenceError::CorruptedData);
    }
    std::fs::remove_file(&active).map_err(PersistenceError::Io)?;
    std::fs::hard_link(stage, active).map_err(PersistenceError::Io)
}

fn rollback_session_touch_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    if committed_marker.exists() {
        return Err(PersistenceError::Conflict);
    }
    let transaction = read_session_touch_transaction(prepared_marker)?;
    let hash = hash_from_lower_hex(&transaction.hash_hex)?;
    let directory = root.join("transactions");
    let active = active_path(root, &hash);
    let backup = session_touch_backup_path(&directory, transaction_id, &transaction.hash_hex);
    if backup.exists() {
        remove_session_file(&active)?;
        std::fs::hard_link(&backup, &active).map_err(PersistenceError::Io)?;
    } else if !active.exists() {
        return Err(PersistenceError::CorruptedData);
    }
    cleanup_session_touch_transaction(root, transaction_id, prepared_marker, committed_marker)
}

fn cleanup_session_touch_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    let transaction = if prepared_marker.exists() {
        Some(read_session_touch_transaction(prepared_marker)?)
    } else if committed_marker.exists() {
        Some(read_session_touch_transaction(committed_marker)?)
    } else {
        None
    };
    if let Some(transaction) = transaction {
        let directory = root.join("transactions");
        remove_file_if_exists(&session_touch_staged_path(
            &directory,
            transaction_id,
            &transaction.hash_hex,
        ))?;
        remove_file_if_exists(&session_touch_backup_path(
            &directory,
            transaction_id,
            &transaction.hash_hex,
        ))?;
    }
    remove_file_if_exists(prepared_marker)?;
    remove_file_if_exists(committed_marker)
}

fn read_session_touch_transaction(path: &Path) -> Result<SessionTouchTransaction, PersistenceError> {
    let transaction: SessionTouchTransaction = serde_json::from_reader(File::open(path).map_err(PersistenceError::Io)?)
        .map_err(|_| PersistenceError::CorruptedData)?;
    if transaction.format_version != SESSION_TOUCH_MARKER_VERSION {
        return Err(PersistenceError::CorruptedData);
    }
    let _hash = hash_from_lower_hex(&transaction.hash_hex)?;
    Ok(transaction)
}

fn session_cleanup_target(root: &Path, write: &SessionCleanupWrite) -> Result<PathBuf, PersistenceError> {
    let hash = hash_from_lower_hex(&write.hash_hex)?;
    Ok(match write.location {
        SessionCleanupLocation::Active => active_path(root, &hash),
        SessionCleanupLocation::Revoked => revoked_path(root, &hash),
    })
}

fn session_cleanup_backup_path(
    directory: &Path,
    transaction_id: &str,
    write: &SessionCleanupWrite,
) -> Result<PathBuf, PersistenceError> {
    let location = match write.location {
        SessionCleanupLocation::Active => "active",
        SessionCleanupLocation::Revoked => "revoked",
    };
    let _hash = hash_from_lower_hex(&write.hash_hex)?;
    Ok(directory.join(format!(
        "{transaction_id}.{}.{}.session-cleanup-backup.json",
        write.hash_hex, location
    )))
}

fn rollback_session_cleanup_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    if committed_marker.exists() {
        return Err(PersistenceError::Conflict);
    }
    let transaction = read_session_cleanup_transaction(prepared_marker)?;
    let directory = root.join("transactions");
    for write in transaction.writes.iter().rev() {
        let target = session_cleanup_target(root, write)?;
        let backup = session_cleanup_backup_path(&directory, transaction_id, write)?;
        if !target.exists() {
            std::fs::hard_link(backup, target).map_err(PersistenceError::Io)?;
        }
    }
    cleanup_session_cleanup_transaction(root, transaction_id, prepared_marker, committed_marker)
}

fn cleanup_session_cleanup_transaction(
    root: &Path,
    transaction_id: &str,
    prepared_marker: &Path,
    committed_marker: &Path,
) -> Result<(), PersistenceError> {
    let transaction = if prepared_marker.exists() {
        Some(read_session_cleanup_transaction(prepared_marker)?)
    } else if committed_marker.exists() {
        Some(read_session_cleanup_transaction(committed_marker)?)
    } else {
        None
    };
    if let Some(transaction) = transaction {
        let directory = root.join("transactions");
        for write in &transaction.writes {
            remove_file_if_exists(&session_cleanup_backup_path(&directory, transaction_id, write)?)?;
        }
    }
    remove_file_if_exists(prepared_marker)?;
    remove_file_if_exists(committed_marker)
}

fn read_session_cleanup_transaction(path: &Path) -> Result<SessionCleanupTransaction, PersistenceError> {
    let transaction: SessionCleanupTransaction =
        serde_json::from_reader(File::open(path).map_err(PersistenceError::Io)?)
            .map_err(|_| PersistenceError::CorruptedData)?;
    if transaction.format_version != SESSION_CLEANUP_MARKER_VERSION
        || transaction.writes.is_empty()
        || transaction.writes.len() > MAX_SESSION_CLEANUP_WRITES
    {
        return Err(PersistenceError::CorruptedData);
    }
    for write in &transaction.writes {
        let _hash = hash_from_lower_hex(&write.hash_hex)?;
    }
    Ok(transaction)
}

fn audit_path(root: &Path, created_at_ms: i64) -> Result<PathBuf, PersistenceError> {
    let day = chrono::DateTime::from_timestamp_millis(created_at_ms)
        .ok_or_else(|| PersistenceError::InvalidConfig("audit timestamp is invalid".to_string()))?
        .with_timezone(&Utc)
        .format("%Y-%m-%d");
    Ok(root.join("audit").join(format!("{day}.jsonl")))
}

fn is_audit_relative_path(path: &str) -> bool {
    let path = Path::new(path);
    path.components().count() == 2
        && path.parent().is_some_and(|parent| parent == Path::new("audit"))
        && path.extension().is_some_and(|extension| extension == "jsonl")
}

fn hash_from_lower_hex(value: &str) -> Result<SessionTokenHash, PersistenceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(PersistenceError::CorruptedData);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_value(pair[0]).ok_or(PersistenceError::CorruptedData)? << 4)
            | hex_value(pair[1]).ok_or(PersistenceError::CorruptedData)?;
    }
    Ok(SessionTokenHash(bytes))
}

fn remove_file_if_exists(path: &Path) -> Result<(), PersistenceError> {
    if path.exists() {
        std::fs::remove_file(path).map_err(PersistenceError::Io)?;
    }
    Ok(())
}

fn active_path(root: &Path, hash: &SessionTokenHash) -> PathBuf {
    root.join("sessions")
        .join("active")
        .join(format!("{}.json", hash.lower_hex()))
}

fn revoked_path(root: &Path, hash: &SessionTokenHash) -> PathBuf {
    root.join("sessions")
        .join("revoked")
        .join(format!("{}.json", hash.lower_hex()))
}

fn load_session(root: &Path, token_hash: SessionTokenHash) -> Result<Option<SessionRecord>, PersistenceError> {
    let active = active_path(root, &token_hash);
    let revoked = revoked_path(root, &token_hash);
    let path = if active.exists() { active } else { revoked };
    if !path.exists() {
        return Ok(None);
    }
    read_session_file::<FileSessionRecord>(&path).map(|record| Some(record.into_record(token_hash)))
}

fn list_sessions(root: &Path, query: SessionQuery) -> Result<SessionPage, PersistenceError> {
    let mut records = Vec::new();
    for directory in [
        root.join("sessions").join("active"),
        root.join("sessions").join("revoked"),
    ] {
        if !directory.exists() {
            continue;
        }
        for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
            let entry = entry.map_err(PersistenceError::Io)?;
            let Some(hash) = parse_hash(entry.path().as_path()) else {
                return Err(PersistenceError::CorruptedData);
            };
            let record = read_session_file::<FileSessionRecord>(&entry.path())?.into_record(hash);
            if query
                .username
                .as_deref()
                .is_none_or(|username| username == record.username)
            {
                records.push(record);
            }
        }
    }
    records.sort_by(|left, right| {
        right
            .created_at_ms
            .cmp(&left.created_at_ms)
            .then_with(|| right.session_id.cmp(&left.session_id))
    });
    if let Some(cursor) = query.cursor {
        records.retain(|record| {
            record.created_at_ms < cursor.created_at_ms
                || (record.created_at_ms == cursor.created_at_ms && record.session_id < cursor.session_id)
        });
    }
    let has_more = records.len() > query.limit;
    records.truncate(query.limit);
    let next_cursor = if has_more {
        records.last().map(|last| SessionCursor {
            created_at_ms: last.created_at_ms,
            session_id: last.session_id.clone(),
        })
    } else {
        None
    };
    Ok(SessionPage { records, next_cursor })
}

fn parse_hash(path: &Path) -> Option<SessionTokenHash> {
    let name = path.file_stem()?.to_str()?;
    if path.extension().is_none_or(|extension| extension != "json")
        || name.len() != 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    let mut bytes = [0; 32];
    for (index, pair) in name.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(SessionTokenHash(bytes))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn remove_session_file(path: &Path) -> Result<(), PersistenceError> {
    if path.exists() {
        std::fs::remove_file(path).map_err(PersistenceError::Io)?;
    }
    Ok(())
}

fn read_session_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, PersistenceError> {
    serde_json::from_reader(File::open(path).map_err(PersistenceError::Io)?)
        .map_err(|_| PersistenceError::CorruptedData)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AuditAction;
    use crate::model::AuditActor;
    use crate::model::AuditOutcome;
    use crate::model::AuditResourceType;

    fn event() -> AuditEvent {
        AuditEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            request_id: uuid::Uuid::now_v7().to_string(),
            actor: AuditActor::admin("operator"),
            action: AuditAction::SessionRevokeCurrent,
            resource_type: AuditResourceType::Session,
            resource_name: Some("operator".to_string()),
            environment_id: None,
            outcome: AuditOutcome::Succeeded,
            detail: None,
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn active_record() -> FileSessionRecord {
        FileSessionRecord {
            session_id: uuid::Uuid::now_v7().to_string(),
            username: "operator".to_string(),
            created_at_ms: 1,
            expires_at_ms: 2,
            last_seen_at_ms: 1,
            revoked_at_ms: None,
        }
    }

    #[test]
    fn prepared_revoke_marker_rolls_back_session_and_audit_tail_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([3; 32]);
        write_json_new_file(&active_path(root, &hash), &active_record()).expect("seed active session");
        let audit = event();
        let prepared = prepare_session_audit_transaction(
            root,
            vec![SessionAuditStagedWrite::revoke(hash, active_record(), 3)],
            &audit,
        )
        .expect("prepare marker");
        publish_session_audit_transaction(&prepared, root, &audit).expect("publish session and audit");

        recover_session_audit_transactions(root).expect("recover prepared transaction");

        assert!(active_path(root, &hash).exists());
        assert!(!revoked_path(root, &hash).exists());
        assert!(!audit_path(root, audit.created_at_ms).expect("audit path").exists());
    }

    #[test]
    fn committed_revoke_marker_keeps_the_published_session_and_audit_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([4; 32]);
        write_json_new_file(&active_path(root, &hash), &active_record()).expect("seed active session");
        let audit = event();
        let prepared = prepare_session_audit_transaction(
            root,
            vec![SessionAuditStagedWrite::revoke(hash, active_record(), 3)],
            &audit,
        )
        .expect("prepare marker");
        publish_session_audit_transaction(&prepared, root, &audit).expect("publish session and audit");
        prepared.finalize().expect("commit decision");

        recover_session_audit_transactions(root).expect("recover committed transaction");

        assert!(!active_path(root, &hash).exists());
        assert!(revoked_path(root, &hash).exists());
        assert!(audit_path(root, audit.created_at_ms).expect("audit path").exists());
    }

    #[test]
    fn prepared_create_marker_removes_the_new_session_and_audit_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([5; 32]);
        let audit = event();
        let prepared = prepare_session_audit_transaction(
            root,
            vec![SessionAuditStagedWrite::create(hash, active_record())],
            &audit,
        )
        .expect("prepare marker");
        publish_session_audit_transaction(&prepared, root, &audit).expect("publish session and audit");

        recover_session_audit_transactions(root).expect("recover prepared create");

        assert!(!active_path(root, &hash).exists());
        assert!(!audit_path(root, audit.created_at_ms).expect("audit path").exists());
    }

    #[test]
    fn committed_revoke_all_marker_keeps_every_active_transition_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let first = SessionTokenHash([6; 32]);
        let second = SessionTokenHash([7; 32]);
        write_json_new_file(&active_path(root, &first), &active_record()).expect("seed first active session");
        write_json_new_file(&active_path(root, &second), &active_record()).expect("seed second active session");
        let audit = event();
        let prepared = prepare_session_audit_transaction(
            root,
            vec![
                SessionAuditStagedWrite::revoke(first, active_record(), 3),
                SessionAuditStagedWrite::revoke(second, active_record(), 3),
            ],
            &audit,
        )
        .expect("prepare revoke all marker");
        publish_session_audit_transaction(&prepared, root, &audit).expect("publish transitions");
        prepared.finalize().expect("commit decision");

        recover_session_audit_transactions(root).expect("recover committed revoke all");

        assert!(!active_path(root, &first).exists());
        assert!(!active_path(root, &second).exists());
        assert!(revoked_path(root, &first).exists());
        assert!(revoked_path(root, &second).exists());
    }

    #[test]
    fn prepared_session_cleanup_restores_expired_records_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([8; 32]);
        write_json_new_file(&active_path(root, &hash), &active_record()).expect("seed expired session");
        let prepared = prepare_session_cleanup_transaction(root, 3, 10)
            .expect("prepare cleanup")
            .expect("expired cleanup record");
        assert_eq!(prepared.delete_staged_sessions().expect("delete staged session"), 1);
        assert!(!active_path(root, &hash).exists());

        recover_session_cleanup_transactions(root).expect("recover prepared cleanup");

        assert!(active_path(root, &hash).exists());
        assert!(!prepared.prepared_marker.exists());
    }

    #[test]
    fn committed_session_cleanup_keeps_expired_records_deleted_after_a_crash() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([9; 32]);
        write_json_new_file(&active_path(root, &hash), &active_record()).expect("seed expired session");
        let prepared = prepare_session_cleanup_transaction(root, 3, 10)
            .expect("prepare cleanup")
            .expect("expired cleanup record");
        assert_eq!(prepared.delete_staged_sessions().expect("delete staged session"), 1);
        prepared.finalize().expect("commit cleanup");

        recover_session_cleanup_transactions(root).expect("recover committed cleanup");

        assert!(!active_path(root, &hash).exists());
        assert!(!prepared.prepared_marker.exists());
        assert!(!prepared.committed_marker.exists());
    }

    #[test]
    fn prepared_touch_marker_restores_the_exact_prior_record_without_sidecars_after_recovery() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([10; 32]);
        let before = active_record();
        write_json_new_file(&active_path(root, &hash), &before).expect("seed active session");
        let mut after = before.clone();
        after.last_seen_at_ms = 2;
        let prepared = prepare_session_touch_transaction(root, hash, &after).expect("prepare touch");
        publish_session_touch_transaction(&prepared).expect("publish touch");

        recover_session_touch_transactions(root).expect("recover prepared touch");

        let recovered: FileSessionRecord = read_session_file(&active_path(root, &hash)).expect("restored record");
        assert_eq!(recovered.session_id, before.session_id);
        assert_eq!(recovered.last_seen_at_ms, before.last_seen_at_ms);
        assert!(!prepared.prepared_marker.exists());
        assert!(!prepared.committed_marker.exists());
        let remaining = std::fs::read_dir(root.join("transactions"))
            .expect("transaction directory")
            .count();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn committed_touch_marker_keeps_the_new_record_without_sidecars_after_recovery() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let hash = SessionTokenHash([11; 32]);
        let before = active_record();
        write_json_new_file(&active_path(root, &hash), &before).expect("seed active session");
        let mut after = before;
        after.last_seen_at_ms = 2;
        let prepared = prepare_session_touch_transaction(root, hash, &after).expect("prepare touch");
        publish_session_touch_transaction(&prepared).expect("publish touch");
        prepared.finalize().expect("commit touch");

        recover_session_touch_transactions(root).expect("recover committed touch");

        let recovered: FileSessionRecord = read_session_file(&active_path(root, &hash)).expect("committed record");
        assert_eq!(recovered.last_seen_at_ms, 2);
        assert!(!prepared.prepared_marker.exists());
        assert!(!prepared.committed_marker.exists());
        let remaining = std::fs::read_dir(root.join("transactions"))
            .expect("transaction directory")
            .count();
        assert_eq!(remaining, 0);
    }
}
