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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::mix_all::FIRST_BROKER_CONTROLLER_ID;
use serde::Deserialize;
use serde::Serialize;

/// Broker replicas info, mapping from brokerAddress to (brokerId, brokerHaAddress).
///
/// This struct manages the broker replica information including:
/// - Cluster and broker names
/// - Auto-incrementing broker ID assignment
/// - Mapping from broker ID to (IP address, register check code)
#[derive(Debug, Clone)]
pub struct BrokerReplicaInfo {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    /// Start from FIRST_BROKER_CONTROLLER_ID (1)
    next_assign_broker_id: Arc<AtomicU64>,
    /// brokerId -> (ipAddress, registerCheckCode)
    broker_id_info: DashMap<u64, (CheetahString, CheetahString)>,
}

impl BrokerReplicaInfo {
    /// Create a new BrokerReplicaInfo
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    pub fn new(cluster_name: impl Into<CheetahString>, broker_name: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            next_assign_broker_id: Arc::new(AtomicU64::new(FIRST_BROKER_CONTROLLER_ID)),
            broker_id_info: DashMap::new(),
        }
    }

    /// Remove a broker by ID
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID to remove
    pub fn remove_broker_id(&self, broker_id: u64) {
        self.broker_id_info.remove(&broker_id);
    }

    /// Get the next broker ID to be assigned
    pub fn get_next_assign_broker_id(&self) -> u64 {
        self.next_assign_broker_id.load(Ordering::SeqCst)
    }

    /// Get cluster name
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Get broker name
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    /// Add a broker
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID
    /// * `ip_address` - Broker IP address
    /// * `register_check_code` - Register check code
    pub fn add_broker(
        &self,
        broker_id: u64,
        ip_address: impl Into<CheetahString>,
        register_check_code: impl Into<CheetahString>,
    ) {
        self.broker_id_info
            .insert(broker_id, (ip_address.into(), register_check_code.into()));
        self.next_assign_broker_id.fetch_add(1, Ordering::SeqCst);
    }

    /// Check if a broker exists
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID to check
    pub fn is_broker_exist(&self, broker_id: u64) -> bool {
        self.broker_id_info.contains_key(&broker_id)
    }

    /// Get all broker IDs
    pub fn get_all_broker(&self) -> HashSet<u64> {
        self.broker_id_info.iter().map(|entry| *entry.key()).collect()
    }

    /// Get broker ID to address mapping
    pub fn get_broker_id_table(&self) -> HashMap<u64, CheetahString> {
        let mut map = HashMap::with_capacity(self.broker_id_info.len());
        for entry in self.broker_id_info.iter() {
            map.insert(*entry.key(), entry.value().0.clone());
        }
        map
    }

    /// Get broker address by ID
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// Broker address if found, None otherwise
    pub fn get_broker_address(&self, broker_id: u64) -> Option<CheetahString> {
        self.broker_id_info.get(&broker_id).map(|p| p.0.clone())
    }

    /// Get broker register check code by ID
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// Register check code if found, None otherwise
    pub fn get_broker_register_check_code(&self, broker_id: u64) -> Option<CheetahString> {
        self.broker_id_info.get(&broker_id).map(|p| p.1.clone())
    }

    /// Update broker address
    ///
    /// # Arguments
    ///
    /// * `broker_id` - Broker ID
    /// * `broker_address` - New broker address
    pub fn update_broker_address(&self, broker_id: u64, broker_address: impl Into<CheetahString>) {
        if let Some(mut pair) = self.broker_id_info.get_mut(&broker_id) {
            pair.0 = broker_address.into();
        }
    }
}

impl Serialize for BrokerReplicaInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("BrokerReplicaInfo", 4)?;
        state.serialize_field("cluster_name", self.cluster_name.as_str())?;
        state.serialize_field("broker_name", self.broker_name.as_str())?;
        state.serialize_field(
            "next_assign_broker_id",
            &self.next_assign_broker_id.load(Ordering::SeqCst),
        )?;

        // Serialize broker_id_info as map directly without intermediate allocation
        let broker_id_info: HashMap<u64, (String, String)> = self
            .broker_id_info
            .iter()
            .map(|entry| (*entry.key(), (entry.value().0.to_string(), entry.value().1.to_string())))
            .collect();
        state.serialize_field("broker_id_info", &broker_id_info)?;

        state.end()
    }
}

impl<'de> Deserialize<'de> for BrokerReplicaInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::fmt;

        use serde::de::MapAccess;
        use serde::de::Visitor;
        use serde::de::{self};

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            ClusterName,
            BrokerName,
            NextAssignBrokerId,
            BrokerIdInfo,
        }

        struct BrokerReplicaInfoVisitor;

        impl<'de> Visitor<'de> for BrokerReplicaInfoVisitor {
            type Value = BrokerReplicaInfo;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("struct BrokerReplicaInfo")
            }

            fn visit_map<V>(self, mut map: V) -> Result<BrokerReplicaInfo, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut cluster_name = None;
                let mut broker_name = None;
                let mut next_assign_broker_id = None;
                let mut broker_id_info = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::ClusterName => {
                            if cluster_name.is_some() {
                                return Err(de::Error::duplicate_field("cluster_name"));
                            }
                            cluster_name = Some(map.next_value::<String>()?);
                        }
                        Field::BrokerName => {
                            if broker_name.is_some() {
                                return Err(de::Error::duplicate_field("broker_name"));
                            }
                            broker_name = Some(map.next_value::<String>()?);
                        }
                        Field::NextAssignBrokerId => {
                            if next_assign_broker_id.is_some() {
                                return Err(de::Error::duplicate_field("next_assign_broker_id"));
                            }
                            next_assign_broker_id = Some(map.next_value::<u64>()?);
                        }
                        Field::BrokerIdInfo => {
                            if broker_id_info.is_some() {
                                return Err(de::Error::duplicate_field("broker_id_info"));
                            }
                            broker_id_info = Some(map.next_value::<HashMap<u64, (String, String)>>()?);
                        }
                    }
                }

                let cluster_name = cluster_name.ok_or_else(|| de::Error::missing_field("cluster_name"))?;
                let broker_name = broker_name.ok_or_else(|| de::Error::missing_field("broker_name"))?;
                let next_assign_broker_id =
                    next_assign_broker_id.ok_or_else(|| de::Error::missing_field("next_assign_broker_id"))?;
                let broker_id_info_map = broker_id_info.ok_or_else(|| de::Error::missing_field("broker_id_info"))?;

                let broker_id_info = DashMap::new();
                for (k, (a, c)) in broker_id_info_map {
                    broker_id_info.insert(k, (CheetahString::from_string(a), CheetahString::from_string(c)));
                }

                Ok(BrokerReplicaInfo {
                    cluster_name: CheetahString::from_string(cluster_name),
                    broker_name: CheetahString::from_string(broker_name),
                    next_assign_broker_id: Arc::new(AtomicU64::new(next_assign_broker_id)),
                    broker_id_info,
                })
            }
        }

        const FIELDS: &[&str] = &["cluster_name", "broker_name", "next_assign_broker_id", "broker_id_info"];
        deserializer.deserialize_struct("BrokerReplicaInfo", FIELDS, BrokerReplicaInfoVisitor)
    }
}
