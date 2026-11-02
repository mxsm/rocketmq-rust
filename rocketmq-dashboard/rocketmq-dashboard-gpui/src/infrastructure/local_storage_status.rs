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

//! Process-local evidence from real local-file reads and atomic persistence attempts.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Serialize, de::DeserializeOwned};

use super::{atomic_file::write_json_atomically, config_store::ConfigStoreFailure};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum FileReadState {
    #[default]
    Unknown,
    Readable,
    Missing,
    MigrationRequired,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum FileWriteState {
    #[default]
    NotObserved,
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileIoEvidence {
    pub read: FileReadState,
    pub read_checked_at_epoch_ms: Option<u64>,
    pub schema_version: Option<u32>,
    pub file_revision: Option<u64>,
    pub last_write: FileWriteState,
    pub last_successful_write_epoch_ms: Option<u64>,
}

impl FileIoEvidence {
    pub(super) fn record_read(&mut self, state: FileReadState, schema: Option<u32>, revision: Option<u64>) {
        self.read = state;
        self.read_checked_at_epoch_ms = epoch_ms();
        self.schema_version = schema;
        self.file_revision = revision;
    }

    fn record_write(&mut self, succeeded: bool) {
        self.last_write = if succeeded {
            FileWriteState::Succeeded
        } else {
            FileWriteState::Failed
        };
        if succeeded {
            self.last_successful_write_epoch_ms = epoch_ms();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalFileStatus {
    pub path: PathBuf,
    pub writer_owned: bool,
    pub evidence: FileIoEvidence,
}

pub(super) fn write_observed<T: Serialize>(
    path: &Path,
    value: &T,
    evidence: &parking_lot::RwLock<FileIoEvidence>,
) -> Result<(), ConfigStoreFailure> {
    let result = write_json_atomically(path, value);
    evidence.write().record_write(result.is_ok());
    result
}

/// A status refresh reads and validates the current file without migrating it or probing writes.
pub(super) fn inspect_json<T: DeserializeOwned>(
    path: &Path,
    expected_schema: u32,
    evidence: &parking_lot::RwLock<FileIoEvidence>,
    validate: impl FnOnce(T) -> Result<Option<u64>, ConfigStoreFailure>,
) {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            evidence.write().record_read(
                if error.kind() == std::io::ErrorKind::NotFound {
                    FileReadState::Missing
                } else {
                    FileReadState::Failed
                },
                None,
                None,
            );
            return;
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            evidence.write().record_read(FileReadState::Failed, None, None);
            return;
        }
    };
    let schema = value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    if schema.is_none_or(|version| version < expected_schema) {
        evidence.write().record_read(FileReadState::Unknown, schema, None);
        return;
    }
    if schema != Some(expected_schema) {
        evidence.write().record_read(FileReadState::Failed, schema, None);
        return;
    }
    match serde_json::from_value::<T>(value)
        .map_err(ConfigStoreFailure::validation)
        .and_then(validate)
    {
        Ok(revision) => evidence.write().record_read(FileReadState::Readable, schema, revision),
        Err(_) => evidence.write().record_read(FileReadState::Failed, schema, None),
    }
}

fn epoch_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
}
