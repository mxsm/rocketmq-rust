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
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::protocol::{
    heartbeat::{consumer_data::ConsumerData, producer_data::ProducerData},
    RemotingSerializable,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub producer_data_set: HashSet<ProducerData>,
    pub consumer_data_set: HashSet<ConsumerData>,
    pub heartbeat_fingerprint: i32,
    #[serde(rename = "withoutSub")]
    pub is_without_sub: bool,
}

impl RemotingSerializable for HeartbeatData {
    type Output = HeartbeatData;
}
