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

use serde::{Deserialize, Serialize};

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
}
