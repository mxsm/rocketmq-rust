//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct BrokerReplicaInfo {
    cluster_name: CheetahString,
    broker_name: CheetahString,
    // Start from 1
    next_assign_broker_id: Arc<AtomicU64>,
    // brokerId -> (ipAddress, registerCheckCode)
    broker_id_info: HashMap<u64, (CheetahString, CheetahString)>,
}

impl BrokerReplicaInfo {
    pub fn new(cluster_name: impl Into<CheetahString>, broker_name: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            next_assign_broker_id: Arc::new(AtomicU64::new(1)),
            broker_id_info: HashMap::new(),
        }
    }

    pub fn remove_broker_id(&mut self, broker_id: u64) {
        self.broker_id_info.remove(&broker_id);
    }

    pub fn get_next_assign_broker_id(&self) -> u64 {
        self.next_assign_broker_id.load(Ordering::SeqCst)
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn add_broker(&mut self, broker_id: u64, ip_address: impl Into<CheetahString>, register_check_code: impl Into<CheetahString>) {
        self.broker_id_info.insert(broker_id, (ip_address.into(), register_check_code.into()));
        self.next_assign_broker_id.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_broker_exist(&self, broker_id: u64) -> bool {
        self.broker_id_info.contains_key(&broker_id)
    }

    pub fn get_all_broker(&self) -> HashSet<u64> {
        self.broker_id_info.keys().cloned().collect()
    }

    pub fn get_broker_id_table(&self) -> HashMap<u64, CheetahString> {
        let mut map = HashMap::with_capacity(self.broker_id_info.len());
        for (id, pair) in &self.broker_id_info {
            map.insert(*id, pair.0.clone());
        }
        map
    }

    pub fn get_broker_address(&self, broker_id: u64) -> Option<CheetahString> {
        self.broker_id_info.get(&broker_id).map(|p| p.0.clone())
    }

    pub fn get_broker_register_check_code(&self, broker_id: u64) -> Option<CheetahString> {
        self.broker_id_info.get(&broker_id).map(|p| p.1.clone())
    }

    pub fn update_broker_address(&mut self, broker_id: u64, broker_address: impl Into<CheetahString>) {
        if let Some(pair) = self.broker_id_info.get_mut(&broker_id) {
            pair.0 = broker_address.into();
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BrokerReplicaInfoDef {
    cluster_name: String,
    broker_name: String,
    next_assign_broker_id: u64,
    broker_id_info: HashMap<u64, (String, String)>,
}

impl Serialize for BrokerReplicaInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let helper = BrokerReplicaInfoDef {
            cluster_name: self.cluster_name.to_string(),
            broker_name: self.broker_name.to_string(),
            next_assign_broker_id: self.next_assign_broker_id.load(Ordering::SeqCst),
            broker_id_info: self
                .broker_id_info
                .iter()
                .map(|(k, v)| (*k, (v.0.to_string(), v.1.to_string())))
                .collect(),
        };
        helper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BrokerReplicaInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = BrokerReplicaInfoDef::deserialize(deserializer)?;
        Ok(BrokerReplicaInfo {
            cluster_name: CheetahString::from_string(helper.cluster_name),
            broker_name: CheetahString::from_string(helper.broker_name),
            next_assign_broker_id: Arc::new(AtomicU64::new(helper.next_assign_broker_id)),
            broker_id_info: helper
                .broker_id_info
                .into_iter()
                .map(|(k, (a, c))| (k, (CheetahString::from_string(a), CheetahString::from_string(c))))
                .collect(),
        })
    }
}
