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

use std::fmt;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StoreType {
    #[default]
    LocalFile,
    RocksDB,
}

impl StoreType {
    pub fn get_store_type(&self) -> &'static str {
        match self {
            StoreType::LocalFile => "LocalFile",
            StoreType::RocksDB => "RocksDB",
        }
    }

    /// Converts the Java 5.5 `storeType` aliases without weakening canonical
    /// Rust configuration deserialization.
    pub fn from_java_alias(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("default") {
            Some(Self::LocalFile)
        } else if value.eq_ignore_ascii_case("defaultRocksDB") {
            Some(Self::RocksDB)
        } else {
            None
        }
    }
}

impl Serialize for StoreType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            StoreType::LocalFile => "LocalFile",
            StoreType::RocksDB => "RocksDB",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for StoreType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StoreTypeVisitor;

        impl serde::de::Visitor<'_> for StoreTypeVisitor {
            type Value = StoreType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`LocalFile` or `RocksDB`")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "LocalFile" => Ok(StoreType::LocalFile),
                    "RocksDB" => Ok(StoreType::RocksDB),
                    _ => Err(serde::de::Error::unknown_variant(value, &["LocalFile", "RocksDB"])),
                }
            }
        }

        deserializer.deserialize_str(StoreTypeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn get_store_type_returns_correct_string() {
        assert_eq!(StoreType::LocalFile.get_store_type(), "LocalFile");
        assert_eq!(StoreType::RocksDB.get_store_type(), "RocksDB");
    }

    #[test]
    fn serialize_returns_correct_json() {
        let local_file = StoreType::LocalFile;
        let rocks_db = StoreType::RocksDB;

        assert_eq!(serde_json::to_value(local_file).unwrap(), json!("LocalFile"));
        assert_eq!(serde_json::to_value(rocks_db).unwrap(), json!("RocksDB"));
    }

    #[test]
    fn deserialize_returns_correct_enum() {
        let local_file: StoreType = serde_json::from_value(json!("LocalFile")).unwrap();
        let rocks_db: StoreType = serde_json::from_value(json!("RocksDB")).unwrap();

        assert_eq!(local_file, StoreType::LocalFile);
        assert_eq!(rocks_db, StoreType::RocksDB);
    }

    #[test]
    fn deserialize_rejects_unknown_variant() {
        let error = serde_json::from_value::<StoreType>(json!("Unknown"))
            .expect_err("unknown canonical store type should fail");
        assert!(error.to_string().contains("LocalFile"));
        assert!(error.to_string().contains("RocksDB"));
    }

    #[test]
    fn java_aliases_are_isolated_from_canonical_deserialization() {
        assert_eq!(StoreType::from_java_alias("DeFaUlT"), Some(StoreType::LocalFile));
        assert_eq!(StoreType::from_java_alias("DEFAULTROCKSDB"), Some(StoreType::RocksDB));
        assert_eq!(StoreType::from_java_alias("rocksdb"), None);
        assert!(serde_json::from_value::<StoreType>(json!("default")).is_err());
    }
}
