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

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBrokerToControllerRequestHeader {
    pub cluster_name: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub broker_id: Option<i64>,
    pub broker_address: Option<CheetahString>,
    pub invoke_time: u64,
}

impl Default for RegisterBrokerToControllerRequestHeader {
    fn default() -> Self {
        Self {
            cluster_name: None,
            broker_name: None,
            broker_id: None,
            broker_address: None,
            invoke_time: get_current_millis(),
        }
    }
}
