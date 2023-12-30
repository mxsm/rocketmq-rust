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

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::protocol::{route::route_data_view::BrokerData, RemotingSerializable};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterInfo {
    #[serde(rename = "brokerAddrTable")]
    broker_addr_table: Option<HashMap<String, BrokerData>>,

    #[serde(rename = "clusterAddrTable")]
    cluster_addr_table: Option<HashMap<String, HashSet<String>>>,
}

impl RemotingSerializable for ClusterInfo {
    type Output = ClusterInfo;

    fn decode(bytes: &[u8]) -> ClusterInfo {
        serde_json::from_slice::<Self::Output>(bytes).unwrap()
    }

    fn encode(&self, compress: bool) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }
}

impl ClusterInfo {
    pub fn new(
        broker_addr_table: Option<HashMap<String, BrokerData>>,
        cluster_addr_table: Option<HashMap<String, HashSet<String>>>,
    ) -> ClusterInfo {
        ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        }
    }
}
