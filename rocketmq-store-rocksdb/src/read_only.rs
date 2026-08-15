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

//! Read-only inspection of existing RocksDB state for offline compatibility checks.

use std::path::Path;
use std::path::PathBuf;

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::profile_marker::PopConsumerProfileMarker;
use crate::profile_marker::POP_CONSUMER_PROFILE_COLUMN_FAMILY;
use crate::profile_marker::POP_CONSUMER_PROFILE_MARKER_KEY;

/// Classification of the persistent POP consumer-profile format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopConsumerProfileState {
    /// No profile format has been declared by this legacy database.
    LegacyAbsent,
    /// A supported marker is present and valid.
    PresentValid(PopConsumerProfileMarker),
    /// The format was declared but its column family or marker is missing or corrupt.
    DeclaredPresentInvalid { reason: String },
}

/// Read-only handle over an existing RocksDB database.
///
/// The handle never creates a database or column family and exposes no mutating operation.
pub struct ReadOnlyRocksDb {
    db: ::rocksdb::DB,
    path: PathBuf,
    column_families: Vec<String>,
}

impl ReadOnlyRocksDb {
    /// Opens an existing database and all of its existing column families read-only.
    ///
    /// `Ok(None)` means the RocksDB `CURRENT` marker is absent. Callers must hold the
    /// owning Store's offline lock before opening a live Broker path.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Option<Self>, RocketMQError> {
        let path = path.as_ref();
        if !path.join("CURRENT").is_file() {
            return Ok(None);
        }
        let options = ::rocksdb::Options::default();
        let column_families = ::rocksdb::DB::list_cf(&options, path)
            .map_err(|error| read_error(path, format!("list column families: {error}")))?;
        let db = ::rocksdb::DB::open_cf_for_read_only(&options, path, &column_families, false)
            .map_err(|error| read_error(path, format!("open existing column families read-only: {error}")))?;
        Ok(Some(Self {
            db,
            path: path.to_path_buf(),
            column_families,
        }))
    }

    /// Returns the exact database path opened by this handle.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the column-family inventory captured before opening the handle.
    pub fn column_families(&self) -> &[String] {
        &self.column_families
    }

    /// Reads one value from an existing column family.
    pub fn get_cf(&self, column_family: &str, key: &[u8]) -> Result<Option<Bytes>, RocketMQError> {
        let handle = self
            .db
            .cf_handle(column_family)
            .ok_or_else(|| read_error(&self.path, format!("column family {column_family} is not present")))?;
        self.db
            .get_cf(&handle, key)
            .map(|value| value.map(Bytes::from))
            .map_err(|error| read_error(&self.path, format!("read {column_family}: {error}")))
    }

    /// Classifies the persistent POP consumer-profile marker without mutating the database.
    pub fn inspect_pop_consumer_profile(&self, declared: bool) -> Result<PopConsumerProfileState, RocketMQError> {
        if !self
            .column_families
            .iter()
            .any(|name| name == POP_CONSUMER_PROFILE_COLUMN_FAMILY)
        {
            return Ok(if declared {
                PopConsumerProfileState::DeclaredPresentInvalid {
                    reason: "declared POP consumer profile column family is missing".to_owned(),
                }
            } else {
                PopConsumerProfileState::LegacyAbsent
            });
        }

        let Some(bytes) = self.get_cf(POP_CONSUMER_PROFILE_COLUMN_FAMILY, POP_CONSUMER_PROFILE_MARKER_KEY)? else {
            return Ok(if declared {
                PopConsumerProfileState::DeclaredPresentInvalid {
                    reason: "declared POP consumer profile marker is missing".to_owned(),
                }
            } else {
                PopConsumerProfileState::LegacyAbsent
            });
        };
        Ok(match PopConsumerProfileMarker::decode(&bytes) {
            Ok(marker) => PopConsumerProfileState::PresentValid(marker),
            Err(error) => PopConsumerProfileState::DeclaredPresentInvalid {
                reason: error.to_string(),
            },
        })
    }
}

fn read_error(path: &Path, reason: String) -> RocketMQError {
    RocketMQError::storage_read_failed(path.display().to_string(), reason)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use crate::config::RocksDbColumnFamilyConfig;
    use crate::config::RocksDbConfig;
    use crate::profile_marker::PopConsumerProfileMarker;
    use crate::profile_marker::POP_CONSUMER_PROFILE_COLUMN_FAMILY;
    use crate::profile_marker::POP_CONSUMER_PROFILE_MARKER_KEY;
    use crate::store::KeyValueStore;
    use crate::store::RocksDbStore;

    use super::*;

    #[test]
    fn missing_database_is_legacy_absent() {
        let temp = tempfile::tempdir().expect("tempdir");
        assert!(ReadOnlyRocksDb::open_existing(temp.path().join("missing"))
            .expect("inspection")
            .is_none());
    }

    #[test]
    fn profile_marker_is_read_without_changing_database_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = RocksDbConfig {
            enabled: true,
            path: temp.path().join("db"),
            ..RocksDbConfig::default()
        };
        let mut profile = RocksDbColumnFamilyConfig::consume_queue_default();
        profile.name = POP_CONSUMER_PROFILE_COLUMN_FAMILY.to_owned();
        config.column_families.push(profile);
        let store = RocksDbStore::open(config).expect("open writable fixture");
        store
            .put_cf(
                POP_CONSUMER_PROFILE_COLUMN_FAMILY,
                POP_CONSUMER_PROFILE_MARKER_KEY,
                &PopConsumerProfileMarker::new(7).encode().expect("encode marker"),
            )
            .expect("write marker");
        store.flush().expect("flush fixture");
        store.close();
        drop(store);
        let before = snapshot(&temp.path().join("db"));

        let read_only = ReadOnlyRocksDb::open_existing(temp.path().join("db"))
            .expect("read-only open")
            .expect("database exists");
        assert_eq!(
            read_only.inspect_pop_consumer_profile(true).expect("inspect profile"),
            PopConsumerProfileState::PresentValid(PopConsumerProfileMarker::new(7))
        );
        drop(read_only);

        assert_eq!(snapshot(&temp.path().join("db")), before);
    }

    fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut output = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).expect("read fixture directory") {
                let path = entry.expect("fixture entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    output.insert(path.strip_prefix(root).unwrap().to_path_buf(), fs::read(path).unwrap());
                }
            }
        }
        output
    }
}
