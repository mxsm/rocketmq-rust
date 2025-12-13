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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterInfo {
    #[serde(rename = "brokerAddrTable")]
    pub broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,

    #[serde(rename = "clusterAddrTable")]
    pub cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
}

impl ClusterInfo {
    pub fn new(
        broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,
        cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
    ) -> ClusterInfo {
        ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        }
    }
}
