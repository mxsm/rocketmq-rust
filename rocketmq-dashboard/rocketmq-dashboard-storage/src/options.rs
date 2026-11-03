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

use std::{fmt, path::PathBuf};

use rocketmq_runtime::ChildServiceContext;
use serde::{Deserialize, Serialize};

use crate::{atomic_file::write_json_atomically, StorageError, StorageErrorKind};

/// The persisted choice is safe to display and contains no database credentials.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageEngine {
    #[default]
    File,
    Sqlite,
    #[serde(rename = "mysql")]
    MySql,
    Postgres,
}

impl StorageEngine {
    pub const fn is_compiled(self) -> bool {
        match self {
            Self::File => true,
            Self::Sqlite => cfg!(feature = "sqlite"),
            Self::MySql => cfg!(feature = "mysql"),
            Self::Postgres => cfg!(feature = "postgres"),
        }
    }
}

/// Persist this choice separately from the selected storage authority.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct StorageSelection {
    pub engine: StorageEngine,
}

impl StorageSelection {
    /// Reads only the nonsecret next-launch choice. Missing files have no saved choice.
    /// Invalid input is reported without modifying or formatting its contents.
    pub async fn load(path: PathBuf, context: ChildServiceContext) -> Result<Option<Self>, StorageError> {
        context
            .storage_io()
            .spawn_io("native-storage-selection-read", move || {
                let bytes = match std::fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(error) => return Err(StorageError::io("read selection", path, error)),
                };
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|error| StorageError::invalid_document(path, error))
            })
            .await
            .map_err(|_| StorageError::new(StorageErrorKind::Runtime))?
    }

    /// Atomically saves an explicit engine choice; no connection input can be serialized here.
    pub async fn save(self, path: PathBuf, context: ChildServiceContext) -> Result<(), StorageError> {
        context
            .storage_io()
            .spawn_io("native-storage-selection-write", move || {
                write_json_atomically(&path, &self)
            })
            .await
            .map_err(|_| StorageError::new(StorageErrorKind::Runtime))?
    }
}

/// An external connection input. It cannot be serialized or printed with credentials.
pub struct DatabaseConnection(String);

impl DatabaseConnection {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DatabaseConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabaseConnection([REDACTED])")
    }
}

/// File paths remain compatible with GPUI's existing config/history directory.
#[derive(Debug)]
pub enum StorageOptions {
    File {
        config_path: PathBuf,
    },
    Database {
        engine: StorageEngine,
        connection: DatabaseConnection,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_selection_engine_names_round_trip() {
        for (engine, name) in [
            (StorageEngine::File, "file"),
            (StorageEngine::Sqlite, "sqlite"),
            (StorageEngine::MySql, "mysql"),
            (StorageEngine::Postgres, "postgres"),
        ] {
            let selection = StorageSelection { engine };
            let value = serde_json::json!({ "engine": name });
            assert_eq!(serde_json::to_value(&selection).unwrap(), value);
            assert_eq!(serde_json::from_value::<StorageSelection>(value).unwrap(), selection);
        }
    }

    #[test]
    fn saved_selection_preserves_invalid_source_until_explicit_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("storage-selection.json");
        let runtime = crate::contracts_tests::runtime();
        runtime.block_on(async {
            let context = runtime.root_context().component("selection-test");
            assert_eq!(
                StorageSelection::load(path.clone(), context.clone()).await.unwrap(),
                None
            );
            let invalid = br#"{"engine":"postgres","databaseUrl":"PRIVATE-CONNECTION"}"#;
            std::fs::write(&path, invalid).unwrap();
            let error = StorageSelection::load(path.clone(), context.clone()).await.unwrap_err();
            assert_eq!(error.kind(), StorageErrorKind::InvalidDocument);
            assert!(!format!("{error:?} {error}").contains("PRIVATE-CONNECTION"));
            assert_eq!(std::fs::read(&path).unwrap(), invalid);
            StorageSelection {
                engine: StorageEngine::File,
            }
            .save(path.clone(), context.clone())
            .await
            .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&std::fs::read(&path).unwrap()).unwrap(),
                serde_json::json!({"engine":"file"})
            );
            assert_eq!(
                StorageSelection::load(path, context).await.unwrap().unwrap().engine,
                StorageEngine::File
            );
        });
    }
}
