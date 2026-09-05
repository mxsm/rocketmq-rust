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
use super::*;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::persistence::history_repository::HistoryQuery;
use crate::persistence::history_repository::HistoryRetentionResult;
use crate::persistence::history_repository::is_valid_history_timestamp_ms;
use chrono::TimeZone;
use chrono::Utc;
use std::collections::BTreeMap;
use std::fs::OpenOptions;

impl FilePersistence {
    /// Reads every current history segment while the caller owns the File
    /// storage directory lock. This narrow operations hook is intentionally
    /// not exposed through HTTP query APIs.
    pub(crate) async fn snapshot_history_for_operations(&self) -> Result<Vec<MetricSample>, PersistenceError> {
        self.ensure_available()?;
        let _read_guard = self.read_guard().await;
        let root = self.root.clone();
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-snapshot-history", move || {
                let history_root = root.join("history").join("metric-samples");
                if !history_root.exists() {
                    return Ok(Vec::new());
                }
                let mut records = Vec::new();
                for environment in std::fs::read_dir(&history_root).map_err(PersistenceError::Io)? {
                    let environment = environment.map_err(PersistenceError::Io)?;
                    if environment.file_type().map_err(PersistenceError::Io)?.is_symlink()
                        || !environment.file_type().map_err(PersistenceError::Io)?.is_dir()
                    {
                        return Err(PersistenceError::CorruptedData);
                    }
                    for entry in std::fs::read_dir(environment.path()).map_err(PersistenceError::Io)? {
                        let entry = entry.map_err(PersistenceError::Io)?;
                        let path = entry.path();
                        let file_type = entry.file_type().map_err(PersistenceError::Io)?;
                        if file_type.is_symlink() {
                            return Err(PersistenceError::CorruptedData);
                        }
                        if file_type.is_file()
                            && path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
                        {
                            records.extend(read_history_samples_file(&path)?);
                        }
                    }
                }
                records.sort_by(|left, right| {
                    left.environment_id
                        .0
                        .cmp(&right.environment_id.0)
                        .then_with(|| left.metric.cmp(&right.metric))
                        .then_with(|| left.bucket_ms.cmp(&right.bucket_ms))
                });
                Ok(records)
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    pub(crate) async fn append_history(&self, samples: Vec<MetricSample>) -> Result<(), PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        let root = self.root.clone();
        #[cfg(test)]
        let replace_failpoint = self.history_replace_failpoint.clone();
        self.dispatch_file_mutation(write_guard, "dashboard-file-storage-append-history", move || {
            let environment_id = samples
                .first()
                .map(|sample| sample.environment_id.clone())
                .ok_or_else(|| PersistenceError::InvalidConfig("history append batch is empty".to_string()))?;
            if samples.iter().any(|sample| sample.environment_id != environment_id) {
                return Err(PersistenceError::InvalidConfig(
                    "history append batch must contain exactly one environment".to_string(),
                ));
            }
            let mut grouped = BTreeMap::<String, Vec<MetricSample>>::new();
            for sample in samples {
                grouped.entry(history_day(sample.bucket_ms)?).or_default().push(sample);
            }
            if grouped.len() != 1 {
                return Err(PersistenceError::InvalidConfig(
                    "file history append batches must fit within one UTC day".to_string(),
                ));
            }
            let directory = history_directory(&root, &environment_id);
            std::fs::create_dir_all(&directory).map_err(PersistenceError::Io)?;
            let (day, pending) = grouped.into_iter().next().ok_or(PersistenceError::CorruptedData)?;
            let path = directory.join(format!("{day}.jsonl"));
            // A rewrite reads the day exactly once, detects conflicts before
            // publication, then uses a durable marker to recover either the
            // old or the complete new JSONL generation after interruption.
            let mut records = read_history_samples_file(&path)?
                .into_iter()
                .map(|sample| Ok((history_key(&sample)?, sample)))
                .collect::<Result<BTreeMap<_, _>, PersistenceError>>()?;
            for sample in pending {
                let key = history_key(&sample)?;
                if let Some(existing) = records.get(&key) {
                    if existing.value.to_bits() != sample.value.to_bits() {
                        return Err(PersistenceError::Conflict);
                    }
                    continue;
                }
                records.insert(key, sample);
            }
            #[cfg(test)]
            replace_history_file(&path, records.into_values().collect(), &replace_failpoint)?;
            #[cfg(not(test))]
            replace_history_file(&path, records.into_values().collect())?;
            Ok(FileMutationOutcome {
                value: (),
                finalize: Box::new(|| Ok(())),
                cleanup: Box::new(|| Ok(())),
                rollback: Box::new(|| Ok(())),
            })
        })
        .await?;
        self.record_write();
        Ok(())
    }

    pub(crate) async fn read_history(&self, query: &HistoryQuery) -> Result<Vec<MetricSample>, PersistenceError> {
        self.ensure_available()?;
        let mut query = query.clone();
        query.validate_and_normalize()?;
        let _read_guard = self.read_guard().await;
        let root = self.root.clone();
        let environment_id = query.environment_id.clone();
        let start_day = history_day(query.range.start_ms)?;
        let end_day = history_day(query.range.end_ms)?;
        self.service_context
            .storage_io()
            .spawn_io("dashboard-file-storage-read-history", move || {
                let directory = history_directory(&root, &environment_id);
                if !directory.exists() {
                    return Ok(Vec::new());
                }
                let mut paths = std::fs::read_dir(directory)
                    .map_err(PersistenceError::Io)?
                    .filter_map(Result::ok)
                    .filter_map(|entry| {
                        let path = entry.path();
                        let day = path.file_stem()?.to_str()?;
                        if path.extension().and_then(|value| value.to_str()) == Some("jsonl")
                            && is_history_day(day)
                            && day >= start_day.as_str()
                            && day <= end_day.as_str()
                        {
                            Some(path)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                paths.sort();
                let mut samples = Vec::new();
                for path in paths {
                    samples.extend(read_history_samples_file(&path)?);
                }
                Ok(samples)
            })
            .await
            .map_err(PersistenceError::Runtime)?
    }

    /// File retention removes only complete UTC-day segments before the cutoff
    /// day. Therefore a cutoff within a day intentionally retains that day's
    /// samples until the following pass instead of rewriting JSONL files.
    pub(crate) async fn delete_history_before(
        &self,
        environment_id: &EnvironmentId,
        cutoff_ms: i64,
        batch_size: u32,
    ) -> Result<HistoryRetentionResult, PersistenceError> {
        let write_guard = self.write_guard().await;
        self.ensure_available()?;
        if !is_valid_history_timestamp_ms(cutoff_ms) {
            return Err(PersistenceError::InvalidConfig(
                "history retention request is invalid".to_string(),
            ));
        }
        let root = self.root.clone();
        let environment_id = environment_id.clone();
        let cutoff_day = history_day(cutoff_ms)?;
        let result = self
            .dispatch_file_mutation(write_guard, "dashboard-file-storage-retain-history", move || {
                let directory = history_directory(&root, &environment_id);
                if !directory.exists() {
                    return Ok(FileMutationOutcome {
                        value: HistoryRetentionResult {
                            deleted: 0,
                            has_more: false,
                        },
                        finalize: Box::new(|| Ok(())),
                        cleanup: Box::new(|| Ok(())),
                        rollback: Box::new(|| Ok(())),
                    });
                }
                let mut candidates = std::fs::read_dir(&directory)
                    .map_err(PersistenceError::Io)?
                    .filter_map(Result::ok)
                    .filter_map(|entry| {
                        let path = entry.path();
                        let day = path.file_stem()?.to_str()?;
                        (path.extension().and_then(|value| value.to_str()) == Some("jsonl")
                            && is_history_day(day)
                            && day < cutoff_day.as_str())
                        .then_some(path)
                    })
                    .collect::<Vec<_>>();
                candidates.sort();
                let has_more = candidates.len() > batch_size as usize;
                candidates.truncate(batch_size as usize);
                let deleted = candidates.iter().try_fold(0_u64, |total, path| {
                    Ok(total + read_history_samples_file(path)?.len() as u64)
                })?;
                let trash = directory.join(format!(".retention-{}", uuid::Uuid::now_v7()));
                if !candidates.is_empty() {
                    std::fs::create_dir_all(&trash).map_err(PersistenceError::Io)?;
                }
                let mut moved = Vec::new();
                for path in candidates {
                    let Some(name) = path.file_name() else {
                        return Err(PersistenceError::CorruptedData);
                    };
                    let staged = trash.join(name);
                    if let Err(error) = std::fs::rename(&path, &staged) {
                        for (original, staged) in moved.iter().rev() {
                            let _ = std::fs::rename(staged, original);
                        }
                        return Err(PersistenceError::Io(error));
                    }
                    moved.push((path, staged));
                }
                Ok(FileMutationOutcome {
                    value: HistoryRetentionResult { deleted, has_more },
                    finalize: Box::new(|| Ok(())),
                    cleanup: Box::new(move || {
                        if trash.exists() {
                            std::fs::remove_dir_all(trash).map_err(PersistenceError::Io)?;
                        }
                        Ok(())
                    }),
                    rollback: Box::new(move || {
                        for (original, staged) in moved.into_iter().rev() {
                            if staged.exists() {
                                std::fs::rename(staged, original).map_err(PersistenceError::Io)?;
                            }
                        }
                        Ok(())
                    }),
                })
            })
            .await?;
        if result.deleted > 0 {
            self.record_write();
        }
        Ok(result)
    }
}

fn history_directory(root: &Path, environment_id: &EnvironmentId) -> PathBuf {
    let segment = environment_id
        .0
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join("history").join("metric-samples").join(segment)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HistoryReplaceMarker {
    target: String,
    temporary: String,
    backup: String,
}

fn publish_history_replace_marker(path: &Path, value: &HistoryReplaceMarker) -> Result<(), PersistenceError> {
    let directory = path.parent().ok_or(PersistenceError::CorruptedData)?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(PersistenceError::CorruptedData)?;
    let pending = directory.join(format!("{name}.pending"));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&pending)
        .map_err(PersistenceError::Io)?;
    serde_json::to_writer(&mut file, value).map_err(PersistenceError::Serialization)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)?;
    drop(file);
    std::fs::rename(pending, path).map_err(PersistenceError::Io)
}

fn history_key(sample: &MetricSample) -> Result<(String, String, i64, String), PersistenceError> {
    Ok((
        sample.environment_id.0.clone(),
        sample.metric.clone(),
        sample.bucket_ms,
        sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?,
    ))
}

#[cfg(not(test))]
fn replace_history_file(path: &Path, samples: Vec<MetricSample>) -> Result<(), PersistenceError> {
    replace_history_file_inner(path, samples, |_| Ok(()))
}

#[cfg(test)]
fn replace_history_file(
    path: &Path,
    samples: Vec<MetricSample>,
    failpoint: &std::sync::atomic::AtomicU8,
) -> Result<(), PersistenceError> {
    replace_history_file_inner(path, samples, |stage| {
        if failpoint
            .compare_exchange(
                stage,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            return Err(PersistenceError::Io(std::io::Error::other(
                "injected history publication interruption",
            )));
        }
        Ok(())
    })
}

fn replace_history_file_inner(
    path: &Path,
    samples: Vec<MetricSample>,
    interrupt_after_stage: impl Fn(u8) -> Result<(), PersistenceError>,
) -> Result<(), PersistenceError> {
    let directory = path.parent().ok_or(PersistenceError::CorruptedData)?;
    let suffix = uuid::Uuid::now_v7().to_string();
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(PersistenceError::CorruptedData)?;
    let temporary_name = format!(".{filename}.{suffix}.tmp");
    let backup_name = format!(".{filename}.{suffix}.previous");
    let marker_name = format!(".history-replace-{suffix}.json");
    let temporary = directory.join(&temporary_name);
    let backup = directory.join(&backup_name);
    let marker = directory.join(&marker_name);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(PersistenceError::Io)?;
    for sample in samples {
        serde_json::to_writer(&mut file, &sample).map_err(PersistenceError::Serialization)?;
        file.write_all(b"\n").map_err(PersistenceError::Io)?;
    }
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)?;
    drop(file);
    let marker_value = HistoryReplaceMarker {
        target: filename.to_string(),
        temporary: temporary_name,
        backup: backup_name,
    };
    publish_history_replace_marker(&marker, &marker_value)?;
    interrupt_after_stage(1)?;
    if path.exists() {
        std::fs::rename(path, &backup).map_err(PersistenceError::Io)?;
    }
    interrupt_after_stage(2)?;
    std::fs::rename(&temporary, path).map_err(PersistenceError::Io)?;
    interrupt_after_stage(3)?;
    if backup.exists() {
        std::fs::remove_file(&backup).map_err(PersistenceError::Io)?;
    }
    std::fs::remove_file(marker).map_err(PersistenceError::Io)
}

pub(super) fn recover_history_file_operations(root: &Path) -> Result<(), PersistenceError> {
    let history_root = root.join("history").join("metric-samples");
    if !history_root.exists() {
        return Ok(());
    }
    for environment in std::fs::read_dir(history_root).map_err(PersistenceError::Io)? {
        let environment = environment.map_err(PersistenceError::Io)?;
        let directory = environment.path();
        if !directory.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&directory).map_err(PersistenceError::Io)? {
            let entry = entry.map_err(PersistenceError::Io)?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name.starts_with(".retention-") && path.is_dir() {
                std::fs::remove_dir_all(path).map_err(PersistenceError::Io)?;
                continue;
            }
            if name.starts_with(".history-replace-") && name.ends_with(".json.pending") {
                recover_unpublished_history_replace_marker(&directory, &path, name)?;
                continue;
            }
            if !name.starts_with(".history-replace-")
                || path.extension().and_then(|value| value.to_str()) != Some("json")
            {
                continue;
            }
            let marker: HistoryReplaceMarker =
                match serde_json::from_reader(std::fs::File::open(&path).map_err(PersistenceError::Io)?) {
                    Ok(marker) => marker,
                    Err(_) if !history_marker_has_sidecars(&directory, name)? => {
                        std::fs::remove_file(path).map_err(PersistenceError::Io)?;
                        continue;
                    }
                    Err(_) => return Err(PersistenceError::CorruptedData),
                };
            let target = directory.join(marker.target);
            let temporary = directory.join(marker.temporary);
            let backup = directory.join(marker.backup);
            if target.exists() {
                if temporary.exists() {
                    std::fs::remove_file(temporary).map_err(PersistenceError::Io)?;
                }
                if backup.exists() {
                    std::fs::remove_file(backup).map_err(PersistenceError::Io)?;
                }
            } else if temporary.exists() {
                std::fs::rename(temporary, &target).map_err(PersistenceError::Io)?;
                if backup.exists() {
                    std::fs::remove_file(backup).map_err(PersistenceError::Io)?;
                }
            } else if backup.exists() {
                std::fs::rename(backup, &target).map_err(PersistenceError::Io)?;
            } else {
                return Err(PersistenceError::CorruptedData);
            }
            std::fs::remove_file(path).map_err(PersistenceError::Io)?;
        }
    }
    Ok(())
}

fn recover_unpublished_history_replace_marker(
    directory: &Path,
    pending_marker: &Path,
    marker_name: &str,
) -> Result<(), PersistenceError> {
    let Some(suffix) = pending_history_replace_suffix(marker_name) else {
        // A torn marker that never had a valid UUID cannot be associated with
        // any sidecar safely. It is itself disposable, while unrelated files
        // remain untouched.
        std::fs::remove_file(pending_marker).map_err(PersistenceError::Io)?;
        return Ok(());
    };
    let mut sidecars = Vec::new();
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(target_name) = history_sidecar_target_name(name, &suffix) else {
            continue;
        };
        let target = directory.join(target_name);
        if !target.exists() {
            // A published marker is required to decide whether the temporary
            // generation or its backup is authoritative once the target moved.
            return Err(PersistenceError::CorruptedData);
        }
        sidecars.push(entry.path());
    }
    for sidecar in sidecars {
        std::fs::remove_file(sidecar).map_err(PersistenceError::Io)?;
    }
    std::fs::remove_file(pending_marker).map_err(PersistenceError::Io)
}

fn pending_history_replace_suffix(marker_name: &str) -> Option<String> {
    let suffix = marker_name
        .strip_prefix(".history-replace-")?
        .strip_suffix(".json.pending")?;
    let parsed = uuid::Uuid::parse_str(suffix).ok()?;
    (parsed.to_string() == suffix).then(|| suffix.to_string())
}

fn history_sidecar_target_name(name: &str, suffix: &str) -> Option<String> {
    let name = name.strip_prefix('.')?;
    for extension in ["tmp", "previous"] {
        let sidecar_suffix = format!(".{suffix}.{extension}");
        if let Some(target) = name.strip_suffix(&sidecar_suffix)
            && let Some(day) = target.strip_suffix(".jsonl")
            && is_history_day(day)
        {
            return Some(target.to_string());
        }
    }
    None
}

fn history_marker_has_sidecars(directory: &Path, marker_name: &str) -> Result<bool, PersistenceError> {
    let suffix = marker_name
        .strip_prefix(".history-replace-")
        .and_then(|value| value.strip_suffix(".json"))
        .ok_or(PersistenceError::CorruptedData)?;
    let temporary_suffix = format!(".{suffix}.tmp");
    let backup_suffix = format!(".{suffix}.previous");
    for entry in std::fs::read_dir(directory).map_err(PersistenceError::Io)? {
        let entry = entry.map_err(PersistenceError::Io)?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.ends_with(&temporary_suffix) || name.ends_with(&backup_suffix) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn history_day(timestamp_ms: i64) -> Result<String, PersistenceError> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|timestamp| timestamp.format("%Y-%m-%d").to_string())
        .ok_or_else(|| PersistenceError::InvalidConfig("history timestamp is invalid".to_string()))
}

fn is_history_day(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn read_history_samples_file(path: &Path) -> Result<Vec<MetricSample>, PersistenceError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = std::fs::read_to_string(path).map_err(PersistenceError::Io)?;
    let complete = if contents.ends_with('\n') {
        contents.as_str()
    } else {
        contents.rsplit_once('\n').map_or("", |(complete, _)| complete)
    };
    complete
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut sample = serde_json::from_str::<MetricSample>(line).map_err(|_| PersistenceError::CorruptedData)?;
            sample.normalize().map_err(PersistenceError::InvalidConfig)?;
            Ok(sample)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::FilePersistence;
    use super::history_directory;
    use crate::config::SqlPoolConfig;
    use crate::config::StorageConfig;
    use crate::model::EnvironmentId;
    use crate::model::MetricSample;
    use crate::model::StorageBackend;
    use crate::persistence::TimeRange;
    use crate::persistence::error::PersistenceError;
    use crate::persistence::history_repository::HistoryQuery;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_runtime::ScopeId;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn sample(environment_id: EnvironmentId, bucket_ms: i64, value: f64) -> MetricSample {
        MetricSample {
            environment_id,
            metric: "broker-count".to_string(),
            bucket_ms,
            dimensions: Vec::new(),
            value,
        }
    }

    fn query(environment_id: EnvironmentId) -> HistoryQuery {
        HistoryQuery {
            environment_id,
            metric: "broker-count".to_string(),
            range: TimeRange {
                start_ms: 0,
                end_ms: 2_000,
            },
            dimensions: Vec::new(),
            limit: 10,
            cursor: None,
        }
    }

    #[test]
    fn file_history_reopens_and_deletes_only_complete_expired_days() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let config = StorageConfig {
                backend: StorageBackend::File,
                data_path: directory.path().join("dashboard"),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            let environment_id = EnvironmentId::new();
            let sample = MetricSample {
                environment_id: environment_id.clone(),
                metric: "broker-count".to_string(),
                bucket_ms: 1_000,
                dimensions: Vec::new(),
                value: 3.0,
            };
            let store = FilePersistence::initialize(&config, owner.root_context().component("history-file"))
                .await
                .expect("file persistence");
            store
                .append_history(vec![sample.clone()])
                .await
                .expect("append history");
            let other_environment_id = EnvironmentId::new();
            let other_sample = MetricSample {
                environment_id: other_environment_id.clone(),
                metric: "broker-count".to_string(),
                bucket_ms: 1_000,
                dimensions: Vec::new(),
                value: 9.0,
            };
            store
                .append_history(vec![other_sample.clone()])
                .await
                .expect("append other environment history");
            drop(store);
            let reopened = FilePersistence::initialize(&config, owner.root_context().component("history-file-reopen"))
                .await
                .expect("reopen file persistence");
            let query = HistoryQuery {
                environment_id,
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 2_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            };
            assert_eq!(reopened.read_history(&query).await.expect("read history"), vec![sample]);
            let retention = reopened
                .delete_history_before(&query.environment_id, 86_400_000, 1)
                .await
                .expect("retention");
            assert_eq!(retention.deleted, 1);
            assert!(!retention.has_more);
            let mut invalid_range = query.clone();
            invalid_range.range.end_ms = i64::MAX;
            assert!(matches!(
                reopened.read_history(&invalid_range).await,
                Err(PersistenceError::InvalidConfig(_))
            ));
            assert!(matches!(
                reopened.delete_history_before(&query.environment_id, i64::MAX, 1).await,
                Err(PersistenceError::InvalidConfig(_))
            ));
            assert_eq!(
                reopened
                    .read_history(&HistoryQuery {
                        environment_id: other_environment_id,
                        metric: "broker-count".to_string(),
                        range: TimeRange {
                            start_ms: 0,
                            end_ms: 2_000,
                        },
                        dimensions: Vec::new(),
                        limit: 10,
                        cursor: None,
                    })
                    .await
                    .expect("read isolated environment history"),
                vec![other_sample]
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    #[ignore = "requires docker-compose.storage-test.yml"]
    fn docker_file_history_reopens_a_mounted_volume() {
        let data_path = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH")
            .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_FILE_PATH must be set by the storage test runner");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let config = StorageConfig {
                backend: StorageBackend::File,
                data_path: data_path.into(),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            let environment_id = EnvironmentId::new();
            let expired = MetricSample {
                environment_id: environment_id.clone(),
                metric: "broker-count".to_string(),
                bucket_ms: 1_000,
                dimensions: Vec::new(),
                value: 3.0,
            };
            let retained = MetricSample {
                environment_id: environment_id.clone(),
                metric: "broker-count".to_string(),
                bucket_ms: 86_401_000,
                dimensions: Vec::new(),
                value: 4.0,
            };
            let store = FilePersistence::initialize(&config, owner.root_context().component("docker-history-file"))
                .await
                .expect("file persistence");
            store
                .append_history(vec![expired.clone()])
                .await
                .expect("append expired history");
            store
                .append_history(vec![retained.clone()])
                .await
                .expect("append retained history");
            drop(store);

            let reopened =
                FilePersistence::initialize(&config, owner.root_context().component("docker-history-file-reopen"))
                    .await
                    .expect("reopen mounted file persistence");
            let query = HistoryQuery {
                environment_id,
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 172_800_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            };
            assert_eq!(
                reopened.read_history(&query).await.expect("read reopened history"),
                vec![expired, retained.clone()]
            );
            let retention = reopened
                .delete_history_before(&query.environment_id, 86_400_000, 1)
                .await
                .expect("retention");
            assert_eq!(retention.deleted, 1);
            assert!(!retention.has_more);
            assert_eq!(
                reopened.read_history(&query).await.expect("read retained history"),
                vec![retained]
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn file_history_marker_interruption_recovers_an_entire_old_or_new_batch() {
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            for (stage, expect_new) in [(1, false), (2, true), (3, true)] {
                let directory = tempfile::tempdir().expect("temporary directory");
                let config = StorageConfig {
                    backend: StorageBackend::File,
                    data_path: directory.path().join("dashboard"),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                };
                let environment_id = EnvironmentId::new();
                let old = sample(environment_id.clone(), 1_000, 1.0);
                let new = sample(environment_id.clone(), 2_000, 2.0);
                let store = FilePersistence::initialize(
                    &config,
                    owner.root_context().component(
                        ScopeId::try_new(format!("history-file-marker-{stage}"))
                            .expect("test scope has the fixed non-empty history marker prefix"),
                    ),
                )
                .await
                .expect("file persistence");
                store.append_history(vec![old.clone()]).await.expect("old batch");
                store.set_history_replace_failpoint(stage);
                assert!(store.append_history(vec![new.clone()]).await.is_err());
                let expected = if expect_new {
                    vec![old.clone(), new.clone()]
                } else {
                    vec![old.clone()]
                };
                if stage == 2 {
                    // The failed append recovers while its owned write gate is
                    // still held, so this live instance never exposes the
                    // briefly absent target between backup and publication.
                    assert_eq!(
                        store
                            .read_history(&query(environment_id.clone()))
                            .await
                            .expect("online recovery after stage two"),
                        expected
                    );
                    store
                        .append_history(vec![new.clone()])
                        .await
                        .expect("idempotent append after online recovery");
                }
                drop(store);

                let reopened = FilePersistence::initialize(
                    &config,
                    owner.root_context().component(
                        ScopeId::try_new(format!("history-file-marker-reopen-{stage}"))
                            .expect("test scope has the fixed non-empty history marker reopen prefix"),
                    ),
                )
                .await
                .expect("reopen after interrupted publication");
                assert_eq!(
                    reopened
                        .read_history(&query(environment_id))
                        .await
                        .expect("recovered batch"),
                    expected
                );
            }
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn file_history_reopen_discards_empty_or_partial_marker_with_an_intact_target() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let config = StorageConfig {
                backend: StorageBackend::File,
                data_path: directory.path().join("dashboard"),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            let environment_id = EnvironmentId::new();
            let old = sample(environment_id.clone(), 1_000, 1.0);
            let store =
                FilePersistence::initialize(&config, owner.root_context().component("history-file-torn-marker"))
                    .await
                    .expect("file persistence");
            store.append_history(vec![old.clone()]).await.expect("old sample");
            drop(store);

            let history = history_directory(&config.data_path, &environment_id);
            std::fs::write(history.join(".history-replace-empty.json"), b"").expect("empty marker");
            std::fs::write(history.join(".history-replace-partial.json"), b"{").expect("partial marker");

            let reopened = FilePersistence::initialize(
                &config,
                owner.root_context().component("history-file-torn-marker-reopen"),
            )
            .await
            .expect("intact history target survives torn marker cleanup");
            assert_eq!(
                reopened
                    .read_history(&query(environment_id))
                    .await
                    .expect("read intact target"),
                vec![old]
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn file_history_reopen_cleans_only_sidecars_for_an_unpublished_marker() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let config = StorageConfig {
                backend: StorageBackend::File,
                data_path: directory.path().join("dashboard"),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            let environment_id = EnvironmentId::new();
            let old = sample(environment_id.clone(), 1_000, 1.0);
            let store = FilePersistence::initialize(
                &config,
                owner.root_context().component("history-file-unpublished-marker"),
            )
            .await
            .expect("file persistence");
            store.append_history(vec![old.clone()]).await.expect("old sample");
            drop(store);

            let history = history_directory(&config.data_path, &environment_id);
            let suffix = uuid::Uuid::now_v7();
            let pending_marker = history.join(format!(".history-replace-{suffix}.json.pending"));
            let temporary = history.join(format!(".1970-01-01.jsonl.{suffix}.tmp"));
            let backup = history.join(format!(".1970-01-01.jsonl.{suffix}.previous"));
            let unrelated_suffix = uuid::Uuid::now_v7();
            let unrelated = history.join(format!(".1970-01-01.jsonl.{unrelated_suffix}.tmp"));
            std::fs::write(&pending_marker, b"{").expect("torn unpublished marker");
            std::fs::write(&temporary, b"new complete generation").expect("temporary sidecar");
            std::fs::write(&backup, b"defensive backup sidecar").expect("backup sidecar");
            std::fs::write(&unrelated, b"unrelated sidecar").expect("unrelated sidecar");

            let reopened = FilePersistence::initialize(
                &config,
                owner.root_context().component("history-file-unpublished-marker-reopen"),
            )
            .await
            .expect("reopen removes unpublished sidecars");
            assert_eq!(
                reopened
                    .read_history(&query(environment_id))
                    .await
                    .expect("intact target remains readable"),
                vec![old]
            );
            assert!(!pending_marker.exists());
            assert!(!temporary.exists());
            assert!(!backup.exists());
            assert!(unrelated.exists());
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }

    #[test]
    fn file_history_ignores_a_torn_tail_then_rewrites_and_cleans_retention_trash() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = RuntimeOwner::new().expect("runtime owner");
        owner.block_on(async {
            let config = StorageConfig {
                backend: StorageBackend::File,
                data_path: directory.path().join("dashboard"),
                database_url: None,
                pool: SqlPoolConfig::default(),
            };
            let environment_id = EnvironmentId::new();
            let old = sample(environment_id.clone(), 1_000, 1.0);
            let new = sample(environment_id.clone(), 2_000, 2.0);
            let store = FilePersistence::initialize(&config, owner.root_context().component("history-file-tail"))
                .await
                .expect("file persistence");
            store.append_history(vec![old.clone()]).await.expect("old sample");
            drop(store);

            let history = history_directory(&config.data_path, &environment_id);
            let mut file = OpenOptions::new()
                .append(true)
                .open(history.join("1970-01-01.jsonl"))
                .expect("history segment");
            file.write_all(br#"{\"environmentId\":\"torn"#).expect("torn tail");
            drop(file);
            let trash = history.join(".retention-crash-leftover");
            std::fs::create_dir_all(&trash).expect("retention trash");
            std::fs::write(trash.join("1970-01-01.jsonl"), b"old").expect("retention trash payload");

            let reopened =
                FilePersistence::initialize(&config, owner.root_context().component("history-file-tail-reopen"))
                    .await
                    .expect("reopen cleans retention trash");
            assert!(!trash.exists());
            reopened
                .append_history(vec![new.clone()])
                .await
                .expect("append after tail");
            drop(reopened);

            let verified =
                FilePersistence::initialize(&config, owner.root_context().component("history-file-tail-verified"))
                    .await
                    .expect("verify reopen");
            assert_eq!(
                verified
                    .read_history(&query(environment_id))
                    .await
                    .expect("rewritten history"),
                vec![old, new]
            );
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
