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

use super::BACKUP_FORMAT_VERSION;
use super::COLLECTION_FILES;
use super::MAX_BACKUP_FILE_BYTES;
use super::MAX_BACKUP_LINE_BYTES;
use super::validation;
use crate::model::ConsumerMonitorRule;
use crate::model::DashboardEnvironment;
use crate::model::Endpoint;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::model::SessionRecord;
use crate::model::StorageBackend;
use crate::persistence::error::PersistenceError;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fmt;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupManifest {
    pub format_version: u32,
    pub backend: StorageBackend,
    pub created_at_ms: i64,
    pub scope: BackupScope,
    pub counts: BackupCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupScope {
    pub collections: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupCounts {
    pub environments: u64,
    pub endpoints: u64,
    pub monitors: u64,
    pub history: u64,
    pub sessions: u64,
    pub audit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupData {
    pub manifest: BackupManifest,
    pub environments: Vec<DashboardEnvironment>,
    pub monitors: Vec<ConsumerMonitorRule>,
    pub history: Vec<MetricSample>,
    pub sessions: Vec<BackupSession>,
    pub audit: Vec<crate::model::AuditEvent>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupSession {
    pub session_id: String,
    /// A SHA-256 digest, never a plaintext login token.
    pub token_hash: String,
    pub username: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}

impl fmt::Debug for BackupSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupSession")
            .field("session_id", &self.session_id)
            .field("token_hash", &"[REDACTED]")
            .field("username", &self.username)
            .field("created_at_ms", &self.created_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("last_seen_at_ms", &self.last_seen_at_ms)
            .field("revoked_at_ms", &self.revoked_at_ms)
            .finish()
    }
}

impl BackupData {
    pub(super) fn with_backend(backend: StorageBackend) -> Self {
        Self {
            manifest: BackupManifest {
                format_version: BACKUP_FORMAT_VERSION,
                backend,
                created_at_ms: Utc::now().timestamp_millis(),
                scope: BackupScope {
                    collections: COLLECTION_FILES
                        .iter()
                        .map(|file| file.trim_end_matches(".ndjson").to_string())
                        .collect(),
                },
                counts: BackupCounts::default(),
            },
            environments: Vec::new(),
            monitors: Vec::new(),
            history: Vec::new(),
            sessions: Vec::new(),
            audit: Vec::new(),
        }
    }

    pub(super) fn refresh_counts(&mut self) -> Result<(), PersistenceError> {
        self.manifest.counts = BackupCounts {
            environments: self
                .environments
                .len()
                .try_into()
                .map_err(|_| PersistenceError::Capacity)?,
            endpoints: self
                .environments
                .iter()
                .map(|environment| environment.endpoints.len() as u64)
                .sum(),
            monitors: self.monitors.len().try_into().map_err(|_| PersistenceError::Capacity)?,
            history: self.history.len().try_into().map_err(|_| PersistenceError::Capacity)?,
            sessions: self.sessions.len().try_into().map_err(|_| PersistenceError::Capacity)?,
            audit: self.audit.len().try_into().map_err(|_| PersistenceError::Capacity)?,
        };
        Ok(())
    }
}

impl From<SessionRecord> for BackupSession {
    fn from(value: SessionRecord) -> Self {
        Self {
            session_id: value.session_id,
            token_hash: value.token_hash.lower_hex(),
            username: value.username,
            created_at_ms: value.created_at_ms,
            expires_at_ms: value.expires_at_ms,
            last_seen_at_ms: value.last_seen_at_ms,
            revoked_at_ms: value.revoked_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BackupEndpoint {
    pub(super) environment_id: EnvironmentId,
    #[serde(flatten)]
    pub(super) endpoint: Endpoint,
}

pub fn write_backup(output: &Path, data: &BackupData) -> Result<(), PersistenceError> {
    validation::verify_data(data, None)?;
    if output.exists() {
        return Err(PersistenceError::InvalidConfig(
            "backup output must not already exist".to_string(),
        ));
    }
    let parent = output
        .parent()
        .ok_or_else(|| PersistenceError::InvalidConfig("backup output parent is missing".to_string()))?;
    fs::create_dir_all(parent).map_err(PersistenceError::Io)?;
    let name = output
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PersistenceError::InvalidConfig("backup output name is invalid".to_string()))?;
    let stage = parent.join(format!(".{name}.staging-{}", uuid::Uuid::now_v7()));
    fs::create_dir(&stage).map_err(PersistenceError::Io)?;
    let result = write_backup_stage(&stage, data);
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
        return result;
    }
    fs::rename(&stage, output).map_err(PersistenceError::Io)?;
    sync_directory(parent)?;
    Ok(())
}

pub fn read_verified_backup(
    input: &Path,
    expected_backend: Option<StorageBackend>,
) -> Result<BackupData, PersistenceError> {
    validate_backup_directory(input)?;
    let manifest = read_json_file::<BackupManifest>(&input.join("manifest.json"))?;
    let mut data = BackupData {
        manifest,
        environments: read_ndjson(
            &input.join("environments.ndjson"),
            validation::validate_environment_record,
        )?,
        monitors: read_ndjson(&input.join("monitors.ndjson"), validation::validate_monitor_record)?,
        history: read_ndjson(&input.join("history.ndjson"), validation::validate_history_record)?,
        sessions: read_ndjson(&input.join("sessions.ndjson"), validation::validate_session_record)?,
        audit: read_ndjson(&input.join("audit.ndjson"), validation::validate_audit_record)?,
    };
    let endpoint_rows: Vec<BackupEndpoint> =
        read_ndjson(&input.join("endpoints.ndjson"), validation::validate_endpoint_record)?;
    validation::attach_endpoints(&mut data.environments, endpoint_rows)?;
    validation::verify_data(&data, expected_backend)?;
    Ok(data)
}

fn write_backup_stage(stage: &Path, data: &BackupData) -> Result<(), PersistenceError> {
    write_ndjson(&stage.join("environments.ndjson"), &data.environments)?;
    let endpoints = data
        .environments
        .iter()
        .flat_map(|environment| {
            environment.endpoints.iter().cloned().map(|endpoint| BackupEndpoint {
                environment_id: environment.environment_id.clone(),
                endpoint,
            })
        })
        .collect::<Vec<_>>();
    write_ndjson(&stage.join("endpoints.ndjson"), &endpoints)?;
    write_ndjson(&stage.join("monitors.ndjson"), &data.monitors)?;
    write_ndjson(&stage.join("history.ndjson"), &data.history)?;
    write_ndjson(&stage.join("sessions.ndjson"), &data.sessions)?;
    write_ndjson(&stage.join("audit.ndjson"), &data.audit)?;
    write_json_file(&stage.join("manifest.json"), &data.manifest)?;
    sync_directory(stage)
}

fn write_ndjson<T: Serialize>(path: &Path, records: &[T]) -> Result<(), PersistenceError> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    for record in records {
        serde_json::to_writer(&mut file, record).map_err(PersistenceError::Serialization)?;
        file.write_all(b"\n").map_err(PersistenceError::Io)?;
    }
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<(), PersistenceError> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    serde_json::to_writer(&mut file, value).map_err(PersistenceError::Serialization)?;
    file.write_all(b"\n").map_err(PersistenceError::Io)?;
    file.flush().map_err(PersistenceError::Io)?;
    file.sync_all().map_err(PersistenceError::Io)
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, PersistenceError> {
    let metadata = fs::symlink_metadata(path).map_err(PersistenceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_BACKUP_FILE_BYTES {
        return Err(PersistenceError::CorruptedData);
    }
    let reader = BufReader::new(fs::File::open(path).map_err(PersistenceError::Io)?);
    serde_json::from_reader(reader).map_err(|_| PersistenceError::CorruptedData)
}

fn read_ndjson<T: DeserializeOwned>(
    path: &Path,
    validate_shape: fn(&Value) -> bool,
) -> Result<Vec<T>, PersistenceError> {
    let metadata = fs::symlink_metadata(path).map_err(PersistenceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_BACKUP_FILE_BYTES {
        return Err(PersistenceError::CorruptedData);
    }
    let mut records = Vec::new();
    for line in BufReader::new(fs::File::open(path).map_err(PersistenceError::Io)?).lines() {
        let line = line.map_err(PersistenceError::Io)?;
        if line.is_empty() || line.len() > MAX_BACKUP_LINE_BYTES {
            return Err(PersistenceError::CorruptedData);
        }
        let value: Value = serde_json::from_str(&line).map_err(|_| PersistenceError::CorruptedData)?;
        if !validate_shape(&value) {
            return Err(PersistenceError::CorruptedData);
        }
        records.push(serde_json::from_value(value).map_err(|_| PersistenceError::CorruptedData)?);
    }
    Ok(records)
}

fn validate_backup_directory(input: &Path) -> Result<(), PersistenceError> {
    let metadata = fs::symlink_metadata(input).map_err(PersistenceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PersistenceError::CorruptedData);
    }
    let expected = std::iter::once("manifest.json".to_owned())
        .chain(COLLECTION_FILES.into_iter().map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    let entries = fs::read_dir(input).map_err(PersistenceError::Io)?;
    let mut seen = std::collections::BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(PersistenceError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let type_ = entry.file_type().map_err(PersistenceError::Io)?;
        if type_.is_symlink() || !type_.is_file() || !expected.contains(&name) || !seen.insert(name) {
            return Err(PersistenceError::CorruptedData);
        }
    }
    if seen != expected {
        return Err(PersistenceError::CorruptedData);
    }
    Ok(())
}

#[cfg(not(windows))]
pub(super) fn sync_directory(path: &Path) -> Result<(), PersistenceError> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(PersistenceError::Io)
}

#[cfg(windows)]
pub(super) fn sync_directory(_path: &Path) -> Result<(), PersistenceError> {
    Ok(())
}
