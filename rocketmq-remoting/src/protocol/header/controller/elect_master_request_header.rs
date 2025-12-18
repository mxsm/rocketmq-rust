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
pub struct ElectMasterRequestHeader {
    #[required]
    pub cluster_name: CheetahString,

    #[required]
    pub broker_name: CheetahString,

    #[required]
    pub broker_id: i64,

    #[required]
    pub designate_elect: bool,

    pub invoke_time: u64,
}

impl ElectMasterRequestHeader {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: i64,
        designate_elect: bool,
        invoke_time: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
            designate_elect,
            invoke_time,
        }
    }
}

impl Default for ElectMasterRequestHeader {
    fn default() -> Self {
        Self {
            cluster_name: CheetahString::new(),
            broker_name: CheetahString::new(),
            broker_id: 0,
            designate_elect: false,
            invoke_time: get_current_millis(),
        }
    }
}
