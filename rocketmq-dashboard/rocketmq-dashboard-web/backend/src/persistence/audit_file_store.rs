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
use crate::model::AuditEvent;
use crate::persistence::audit_repository::AuditCursor;
use crate::persistence::audit_repository::AuditPage;
use crate::persistence::audit_repository::AuditQuery;
use serde::Deserialize;
use serde::Serialize;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

const AUDIT_REWRITE_MARKER_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditRewriteTransaction {
    format_version: u32,
    target: String,
    backup: String,
}

impl FilePersistence {
    pub(crate) async fn append_audit_event(&self, event: AuditEvent) -> Result<(), PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-audit-append", move || {
            let day = chrono::DateTime::from_timestamp_millis(event.created_at_ms)
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d")
                .to_string();
            let directory = root.join("audit");
            std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
            let path = directory.join(format!("{day}.jsonl"));
            let existed = path.exists();
            super::truncate_incomplete_tail(&path)?;
            let original_length = if existed {
                std::fs::metadata(&path).map_err(PersistenceError::Io)?.len()
            } else {
                0
            };
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(PersistenceError::Io)?;
            serde_json::to_writer(&mut file, &event).map_err(PersistenceError::Serialization)?;
            file.write_all(b"\n").map_err(PersistenceError::Io)?;
            file.flush().map_err(PersistenceError::Io)?;
            file.sync_data().map_err(PersistenceError::Io)?;
            Ok(FileMutationOutcome {
                value: (),
                finalize: Box::new(|| Ok(())),
                cleanup: Box::new(|| Ok(())),
                rollback: Box::new(move || super::rollback_jsonl_append(path, original_length, existed)),
            })
        })
        .await?;
        self.record_write();
        Ok(())
    }

    pub(crate) async fn query_audit_events(&self, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
        // Opening a JSONL audit journal repairs only a torn final append. A
        // completed middle line remains a fail-closed corruption error.
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-audit-query-recovery", move || {
            recover_audit_tails(&root)?;
            let page = query_audit_events(&root, query)?;
            Ok(FileMutationOutcome {
                value: page,
                finalize: Box::new(|| Ok(())),
                cleanup: Box::new(|| Ok(())),
                rollback: Box::new(|| Ok(())),
            })
        })
        .await
    }

    pub(crate) async fn delete_audit_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        let deleted = self
            .dispatch_file_mutation(write_guard, "dashboard-file-audit-cleanup", move || {
                let deleted = delete_audit_before(&root, cutoff_ms, limit)?;
                Ok(FileMutationOutcome {
                    value: deleted,
                    finalize: Box::new(|| Ok(())),
                    cleanup: Box::new(|| Ok(())),
                    rollback: Box::new(|| Ok(())),
                })
            })
            .await?;
        if deleted > 0 {
            self.record_write();
        }
        Ok(deleted)
    }
}

fn query_audit_events(root: &std::path::Path, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
    let directory = root.join("audit");
    if !directory.exists() {
        return Ok(AuditPage {
            events: Vec::new(),
            next_cursor: None,
        });
    }
    let mut events = Vec::with_capacity(query.limit.saturating_add(1));
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        if entry.path().extension().is_none_or(|extension| extension != "jsonl") {
            continue;
        }
        let file = std::fs::File::open(entry.path()).map_err(PersistenceError::Io)?;
        for line in BufReader::new(file).lines() {
            let line = line.map_err(PersistenceError::Io)?;
            if line.is_empty() {
                return Err(PersistenceError::CorruptedData);
            }
            let event: AuditEvent = serde_json::from_str(&line).map_err(|_| PersistenceError::CorruptedData)?;
            if matches_audit_query(&event, &query) && is_after_audit_cursor(&event, query.cursor.as_ref()) {
                insert_descending_bounded(&mut events, event, query.limit.saturating_add(1));
            }
        }
    }
    let has_more = events.len() > query.limit;
    events.truncate(query.limit);
    let next_cursor = if has_more {
        events.last().map(|event| AuditCursor {
            created_at_ms: event.created_at_ms,
            event_id: event.event_id.clone(),
        })
    } else {
        None
    };
    Ok(AuditPage { events, next_cursor })
}

fn recover_audit_tails(root: &std::path::Path) -> Result<(), PersistenceError> {
    let directory = root.join("audit");
    if !directory.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let path = entry.map_err(PersistenceError::Io)?.path();
        if path.extension().is_some_and(|extension| extension == "jsonl") {
            super::truncate_incomplete_tail(&path)?;
        }
    }
    Ok(())
}

fn is_after_audit_cursor(event: &AuditEvent, cursor: Option<&AuditCursor>) -> bool {
    cursor.is_none_or(|cursor| {
        event.created_at_ms < cursor.created_at_ms
            || (event.created_at_ms == cursor.created_at_ms && event.event_id < cursor.event_id)
    })
}

fn insert_descending_bounded(events: &mut Vec<AuditEvent>, event: AuditEvent, maximum: usize) {
    let index = events.partition_point(|current| {
        current.created_at_ms > event.created_at_ms
            || (current.created_at_ms == event.created_at_ms && current.event_id > event.event_id)
    });
    if index < maximum {
        events.insert(index, event);
        if events.len() > maximum {
            events.pop();
        }
    }
}

fn matches_audit_query(event: &AuditEvent, query: &AuditQuery) -> bool {
    event.created_at_ms >= query.start_ms
        && event.created_at_ms <= query.end_ms
        && query
            .actor
            .as_deref()
            .is_none_or(|actor| event.actor.stable_name() == actor)
        && query.action.is_none_or(|action| event.action == action)
        && query.outcome.is_none_or(|outcome| event.outcome == outcome)
        && query.environment_id.as_deref().is_none_or(|id| {
            event
                .environment_id
                .as_ref()
                .is_some_and(|environment| environment.0 == id)
        })
}

fn delete_audit_before(root: &std::path::Path, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
    let directory = root.join("audit");
    if !directory.exists() {
        return Ok(0);
    }
    let mut deleted = 0;
    let mut paths = std::fs::read_dir(directory)
        .map_err(PersistenceError::Io)?
        .map(|entry| entry.map(|entry| entry.path()).map_err(PersistenceError::Io))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    for path in paths {
        if deleted as usize >= limit {
            break;
        }
        if path.extension().is_none_or(|extension| extension != "jsonl") {
            continue;
        }
        // Stream and rewrite at most the requested number of expired records.
        // A day may straddle the retention cutoff, or contain more records
        // than one maintenance batch; skipping such a file would permanently
        // block retention behind the same oldest day.
        super::truncate_incomplete_tail(&path)?;
        let file = std::fs::File::open(&path).map_err(PersistenceError::Io)?;
        let temporary = path.with_file_name(format!(
            ".{}.cleanup.{}.tmp",
            path.file_name().and_then(|name| name.to_str()).unwrap_or("audit"),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let mut rewritten = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(PersistenceError::Io)?;
        let mut removed_from_file = 0_usize;
        {
            let mut reader = BufReader::new(file);
            loop {
                let mut line = String::new();
                let read = reader.read_line(&mut line).map_err(PersistenceError::Io)?;
                if read == 0 {
                    break;
                }
                if !line.ends_with('\n') {
                    return Err(PersistenceError::CorruptedData);
                }
                let line = line.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    let _ = std::fs::remove_file(&temporary);
                    return Err(PersistenceError::CorruptedData);
                }
                let event: AuditEvent = serde_json::from_str(line).map_err(|_| PersistenceError::CorruptedData)?;
                if event.created_at_ms < cutoff_ms && deleted as usize + removed_from_file < limit {
                    removed_from_file += 1;
                    continue;
                }
                rewritten.write_all(line.as_bytes()).map_err(PersistenceError::Io)?;
                rewritten.write_all(b"\n").map_err(PersistenceError::Io)?;
            }
            drop(reader);
        }
        if removed_from_file > 0 {
            rewritten.flush().map_err(PersistenceError::Io)?;
            rewritten.sync_all().map_err(PersistenceError::Io)?;
            drop(rewritten);
            let remaining = std::fs::metadata(&temporary).map_err(PersistenceError::Io)?.len();
            replace_jsonl_file(root, &path, &temporary, remaining == 0)?;
            deleted += removed_from_file as u64;
        } else {
            drop(rewritten);
            std::fs::remove_file(&temporary).map_err(PersistenceError::Io)?;
        }
        if deleted as usize >= limit {
            break;
        }
    }
    Ok(deleted)
}

fn replace_jsonl_file(root: &Path, path: &Path, temporary: &Path, remove_target: bool) -> Result<(), PersistenceError> {
    // Rewrites are journaled even on platforms where rename is atomic. The
    // marker plus an exact backup gives the Windows truncate/copy fallback a
    // recovery decision without relying on a hash, checksum, or custom
    // framing format.
    let transaction_id = format!(
        "{:020}-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        std::process::id()
    );
    let target = audit_relative_path(root, path)?;
    let backup = format!(
        "audit/.{}.rewrite-backup-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or(PersistenceError::CorruptedData)?,
        transaction_id
    );
    let backup_path = root.join(&backup);
    std::fs::copy(path, &backup_path).map_err(PersistenceError::Io)?;
    let backup_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&backup_path)
        .map_err(PersistenceError::Io)?;
    backup_file.sync_all().map_err(PersistenceError::Io)?;
    let transactions = root.join("transactions");
    std::fs::create_dir_all(&transactions).map_err(PersistenceError::Io)?;
    let prepared = transactions.join(format!("{transaction_id}.audit-rewrite.prepared.json"));
    let committed = transactions.join(format!("{transaction_id}.audit-rewrite.committed.json"));
    let marker = AuditRewriteTransaction {
        format_version: AUDIT_REWRITE_MARKER_VERSION,
        target,
        backup,
    };
    super::write_json_new_file(&prepared, &marker)?;

    if remove_target {
        std::fs::remove_file(path).map_err(PersistenceError::Io)?;
        std::fs::remove_file(temporary).map_err(PersistenceError::Io)?;
    } else if let Err(rename_error) = std::fs::rename(temporary, path) {
        // Windows cannot replace an existing file with rename. The prepared
        // marker owns a synced copy of the old journal, so an interruption
        // during this overwrite is restored on reopen.
        let mut source = std::fs::File::open(temporary).map_err(PersistenceError::Io)?;
        let mut target_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(PersistenceError::Io)?;
        std::io::copy(&mut source, &mut target_file).map_err(PersistenceError::Io)?;
        target_file.flush().map_err(PersistenceError::Io)?;
        target_file.sync_all().map_err(PersistenceError::Io)?;
        drop(target_file);
        drop(source);
        std::fs::remove_file(temporary).map_err(|_| PersistenceError::Io(rename_error))?;
    }
    super::write_json_new_file(&committed, &marker)?;
    cleanup_audit_rewrite_transaction(Some(&prepared), &committed, &backup_path)
}

pub(crate) fn recover_audit_rewrite_transactions(root: &Path) -> Result<(), PersistenceError> {
    let directory = root.join("transactions");
    if !directory.exists() {
        return Ok(());
    }
    let mut prepared = std::collections::BTreeMap::new();
    let mut committed = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
        let path = entry.map_err(PersistenceError::Io)?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(id) = name.strip_suffix(".audit-rewrite.prepared.json") {
            prepared.insert(id.to_string(), path);
        } else if let Some(id) = name.strip_suffix(".audit-rewrite.committed.json") {
            committed.insert(id.to_string(), path);
        }
    }
    for (id, marker_path) in committed {
        let marker = read_audit_rewrite_transaction(root, &marker_path)?;
        let prepared_path = prepared.remove(&id);
        cleanup_audit_rewrite_transaction(prepared_path.as_deref(), &marker_path, &root.join(marker.backup))?;
    }
    for marker_path in prepared.into_values() {
        let marker = read_audit_rewrite_transaction(root, &marker_path)?;
        let target = root.join(marker.target);
        let backup = root.join(marker.backup);
        if backup.exists() {
            restore_audit_rewrite_backup(&backup, &target)?;
        }
        super::remove_file_if_exists(Some(&backup))?;
        super::remove_file_if_exists(Some(&marker_path))?;
    }
    Ok(())
}

fn cleanup_audit_rewrite_transaction(
    prepared: Option<&Path>,
    committed: &Path,
    backup: &Path,
) -> Result<(), PersistenceError> {
    super::remove_file_if_exists(Some(backup))?;
    super::remove_file_if_exists(prepared)?;
    super::remove_file_if_exists(Some(committed))
}

fn restore_audit_rewrite_backup(backup: &Path, target: &Path) -> Result<(), PersistenceError> {
    let temporary = target.with_file_name(format!(
        ".{}.restore.tmp",
        target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(PersistenceError::CorruptedData)?
    ));
    std::fs::copy(backup, &temporary).map_err(PersistenceError::Io)?;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&temporary)
        .map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)?;
    drop(file);
    if target.exists() {
        let mut source = std::fs::File::open(&temporary).map_err(PersistenceError::Io)?;
        let mut target_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(target)
            .map_err(PersistenceError::Io)?;
        std::io::copy(&mut source, &mut target_file).map_err(PersistenceError::Io)?;
        target_file.sync_all().map_err(PersistenceError::Io)?;
        drop(target_file);
        drop(source);
        std::fs::remove_file(&temporary).map_err(PersistenceError::Io)?;
    } else {
        std::fs::rename(&temporary, target).map_err(PersistenceError::Io)?;
    }
    Ok(())
}

fn read_audit_rewrite_transaction(root: &Path, path: &Path) -> Result<AuditRewriteTransaction, PersistenceError> {
    let marker: AuditRewriteTransaction =
        serde_json::from_reader(std::fs::File::open(path).map_err(PersistenceError::Io)?)
            .map_err(|_| PersistenceError::CorruptedData)?;
    if marker.format_version != AUDIT_REWRITE_MARKER_VERSION
        || audit_relative_path(root, &root.join(&marker.target))? != marker.target
        || audit_backup_relative_path(root, &root.join(&marker.backup))? != marker.backup
    {
        return Err(PersistenceError::CorruptedData);
    }
    Ok(marker)
}

fn audit_relative_path(root: &Path, path: &Path) -> Result<String, PersistenceError> {
    let relative = path.strip_prefix(root).map_err(|_| PersistenceError::CorruptedData)?;
    let parent = relative.parent();
    if parent != Some(Path::new("audit"))
        || relative.extension().is_none_or(|extension| extension != "jsonl")
        || relative
            .file_name()
            .and_then(|name| name.to_str())
            .is_none_or(|name| name.contains('/') || name.contains('\\'))
    {
        return Err(PersistenceError::CorruptedData);
    }
    relative
        .to_str()
        .map(str::to_string)
        .ok_or(PersistenceError::CorruptedData)
}

fn audit_backup_relative_path(root: &Path, path: &Path) -> Result<String, PersistenceError> {
    let relative = path.strip_prefix(root).map_err(|_| PersistenceError::CorruptedData)?;
    let file_name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(PersistenceError::CorruptedData)?;
    if relative.parent() != Some(Path::new("audit"))
        || !file_name.starts_with('.')
        || !file_name.contains(".rewrite-backup-")
        || file_name.contains('/')
        || file_name.contains('\\')
    {
        return Err(PersistenceError::CorruptedData);
    }
    relative
        .to_str()
        .map(str::to_string)
        .ok_or(PersistenceError::CorruptedData)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AuditAction;
    use crate::model::AuditActor;
    use crate::model::AuditOutcome;
    use crate::model::AuditResourceType;

    fn event(created_at_ms: i64) -> AuditEvent {
        AuditEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            request_id: uuid::Uuid::now_v7().to_string(),
            actor: AuditActor::admin("operator"),
            action: AuditAction::TopicCreate,
            resource_type: AuditResourceType::Topic,
            resource_name: Some("orders".to_string()),
            environment_id: None,
            outcome: AuditOutcome::Succeeded,
            detail: None,
            created_at_ms,
        }
    }

    #[test]
    fn retention_rewrites_a_boundary_day_in_successive_bounded_batches() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let path = root.join("audit").join("2023-11-14.jsonl");
        std::fs::create_dir_all(path.parent().expect("audit parent")).expect("create audit parent");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .expect("open audit file");
        for created_at_ms in [1_i64, 2, 3, 100] {
            serde_json::to_writer(&mut file, &event(created_at_ms)).expect("write audit event");
            file.write_all(b"\n").expect("write separator");
        }
        file.sync_all().expect("sync audit file");
        drop(file);

        assert_eq!(delete_audit_before(root, 10, 2).expect("first retention"), 2);
        assert_eq!(delete_audit_before(root, 10, 2).expect("second retention"), 1);
        let page = query_audit_events(
            root,
            AuditQuery {
                start_ms: 0,
                end_ms: 200,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: None,
                limit: 10,
            },
        )
        .expect("query retained events");
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].created_at_ms, 100);
    }

    #[test]
    fn audit_open_recovers_only_an_incomplete_final_line() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let path = root.join("audit").join("2023-11-14.jsonl");
        std::fs::create_dir_all(path.parent().expect("audit parent")).expect("create audit parent");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .expect("open audit file");
        serde_json::to_writer(&mut file, &event(10)).expect("write complete event");
        file.write_all(b"\n{\"eventId\":").expect("write torn tail");
        file.sync_all().expect("sync audit file");

        recover_audit_tails(root).expect("recover torn final append");
        let page = query_audit_events(
            root,
            AuditQuery {
                start_ms: 0,
                end_ms: 100,
                actor: None,
                action: None,
                outcome: None,
                environment_id: None,
                cursor: None,
                limit: 10,
            },
        )
        .expect("query recovered journal");
        assert_eq!(page.events.len(), 1);
    }

    #[test]
    fn prepared_audit_rewrite_recovers_the_original_journal_after_interruption() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let target = root.join("audit").join("2023-11-14.jsonl");
        let backup = root.join("audit").join(".2023-11-14.jsonl.rewrite-backup-prepared");
        let transactions = root.join("transactions");
        std::fs::create_dir_all(target.parent().expect("audit parent")).expect("create audit directory");
        std::fs::create_dir_all(&transactions).expect("create transaction directory");
        std::fs::write(&target, b"rewritten\n").expect("write interrupted journal");
        std::fs::write(&backup, b"original\n").expect("write backup journal");
        let marker = AuditRewriteTransaction {
            format_version: AUDIT_REWRITE_MARKER_VERSION,
            target: "audit/2023-11-14.jsonl".to_string(),
            backup: "audit/.2023-11-14.jsonl.rewrite-backup-prepared".to_string(),
        };
        let prepared = transactions.join("prepared.audit-rewrite.prepared.json");
        super::super::write_json_new_file(&prepared, &marker).expect("write prepared marker");

        recover_audit_rewrite_transactions(root).expect("recover prepared rewrite");

        assert_eq!(std::fs::read(&target).expect("read restored journal"), b"original\n");
        assert!(!backup.exists());
        assert!(!prepared.exists());
    }

    #[test]
    fn committed_audit_rewrite_preserves_the_new_journal_and_cleans_markers() {
        let directory = tempfile::tempdir().expect("temporary storage");
        let root = directory.path();
        let target = root.join("audit").join("2023-11-14.jsonl");
        let backup = root.join("audit").join(".2023-11-14.jsonl.rewrite-backup-committed");
        let transactions = root.join("transactions");
        std::fs::create_dir_all(target.parent().expect("audit parent")).expect("create audit directory");
        std::fs::create_dir_all(&transactions).expect("create transaction directory");
        std::fs::write(&target, b"rewritten\n").expect("write committed journal");
        std::fs::write(&backup, b"original\n").expect("write backup journal");
        let marker = AuditRewriteTransaction {
            format_version: AUDIT_REWRITE_MARKER_VERSION,
            target: "audit/2023-11-14.jsonl".to_string(),
            backup: "audit/.2023-11-14.jsonl.rewrite-backup-committed".to_string(),
        };
        let prepared = transactions.join("committed.audit-rewrite.prepared.json");
        let committed = transactions.join("committed.audit-rewrite.committed.json");
        super::super::write_json_new_file(&prepared, &marker).expect("write prepared marker");
        super::super::write_json_new_file(&committed, &marker).expect("write committed marker");

        recover_audit_rewrite_transactions(root).expect("recover committed rewrite");

        assert_eq!(std::fs::read(&target).expect("read committed journal"), b"rewritten\n");
        assert!(!backup.exists());
        assert!(!prepared.exists());
        assert!(!committed.exists());
    }
}
