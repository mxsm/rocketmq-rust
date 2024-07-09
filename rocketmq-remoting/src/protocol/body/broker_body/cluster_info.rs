/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterInfo {
    #[serde(rename = "brokerAddrTable")]
    broker_addr_table: HashMap<String, BrokerData>,

    #[serde(rename = "clusterAddrTable")]
    cluster_addr_table: HashMap<String, HashSet<String>>,
}

impl ClusterInfo {
    pub fn new() -> Self {
        ClusterInfo {
            broker_addr_table: HashMap::new(),
            cluster_addr_table: HashMap::new(),
        }
    }

    pub fn new_with_var(
        broker_addr_table: HashMap<String, BrokerData>,
        cluster_addr_table: HashMap<String, HashSet<String>>,
    ) -> Self {
        ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        }
    }

    pub fn get_broker_addr_table(&self) -> &HashMap<String, BrokerData> {
        &self.broker_addr_table
    }

    pub fn set_broker_addr_table(&mut self, broker_addr_table: HashMap<String, BrokerData>) {
        self.broker_addr_table = broker_addr_table;
    }

    pub fn get_cluster_addr_table(&self) -> &HashMap<String, HashSet<String>> {
        &self.cluster_addr_table
    }

    pub fn set_cluster_addr_table(&mut self, cluster_addr_table: HashMap<String, HashSet<String>>) {
        self.cluster_addr_table = cluster_addr_table;
    }

    pub fn retrieve_all_addr_by_cluster(&self, cluster: &str) -> Vec<String> {
        let mut addrs = Vec::new();
        if let Some(broker_names) = self.cluster_addr_table.get(cluster) {
            for broker_name in broker_names {
                if let Some(broker_data) = self.broker_addr_table.get(broker_name) {
                    addrs.extend(broker_data.broker_addrs().values().cloned());
                }
            }
        }
        addrs
    }

    pub fn retrieve_all_cluster_names(&self) -> Vec<String> {
        self.cluster_addr_table.keys().cloned().collect()
    }
}

impl PartialEq for ClusterInfo {
    fn eq(&self, other: &Self) -> bool {
        self.broker_addr_table == other.broker_addr_table
            && self.cluster_addr_table == other.cluster_addr_table
    }
}

impl Eq for ClusterInfo {}

impl Hash for ClusterInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.broker_addr_table.hash(state);
        self.cluster_addr_table.hash(state);
    }
}
