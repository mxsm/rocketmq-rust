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

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum BrokerRole {
    #[default]
    AsyncMaster,
    SyncMaster,
    Slave,
}

impl BrokerRole {
    pub fn get_broker_role(&self) -> &'static str {
        match self {
            BrokerRole::AsyncMaster => "ASYNC_MASTER",
            BrokerRole::SyncMaster => "SYNC_MASTER",
            BrokerRole::Slave => "SLAVE",
        }
    }
}

impl<'de> Deserialize<'de> for BrokerRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BrokerRoleVisitor;

        impl serde::de::Visitor<'_> for BrokerRoleVisitor {
            type Value = BrokerRole;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing BrokerRole")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ASYNC_MASTER" | "AsyncMaster" => Ok(BrokerRole::AsyncMaster),
                    "SYNC_MASTER" | "SyncMaster" => Ok(BrokerRole::SyncMaster),
                    "SLAVE" | "Slave" => Ok(BrokerRole::Slave),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["ASYNC_MASTER/AsyncMaster", "SYNC_MASTER/SyncMaster", "SLAVE/Slave"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(BrokerRoleVisitor)
    }
}

impl Serialize for BrokerRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.get_broker_role())
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn broker_role_async_master_serialization() {
        let role = BrokerRole::AsyncMaster;
        let serialized = serde_json::to_string(&role).unwrap();
        assert_eq!(serialized, "\"ASYNC_MASTER\"");
    }

    #[test]
    fn broker_role_sync_master_serialization() {
        let role = BrokerRole::SyncMaster;
        let serialized = serde_json::to_string(&role).unwrap();
        assert_eq!(serialized, "\"SYNC_MASTER\"");
    }

    #[test]
    fn broker_role_slave_serialization() {
        let role = BrokerRole::Slave;
        let serialized = serde_json::to_string(&role).unwrap();
        assert_eq!(serialized, "\"SLAVE\"");
    }

    #[test]
    fn broker_role_async_master_deserialization() {
        let json = "\"ASYNC_MASTER\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::AsyncMaster);

        let json = "\"AsyncMaster\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::AsyncMaster);
    }

    #[test]
    fn broker_role_sync_master_deserialization() {
        let json = "\"SYNC_MASTER\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::SyncMaster);

        let json = "\"SyncMaster\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::SyncMaster);
    }

    #[test]
    fn broker_role_slave_deserialization() {
        let json = "\"SLAVE\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::Slave);

        let json = "\"Slave\"";
        let deserialized: BrokerRole = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, BrokerRole::Slave);
    }

    #[test]
    fn broker_role_invalid_deserialization() {
        let json = "\"INVALID_ROLE\"";
        let deserialized: Result<BrokerRole, _> = serde_json::from_str(json);
        assert!(deserialized.is_err());
    }
}
