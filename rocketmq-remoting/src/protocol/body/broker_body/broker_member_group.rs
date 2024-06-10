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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::RemotingSerializable;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMemberGroup {
    pub cluster: Option<String>,
    pub broker_name: Option<String>,
    pub broker_addrs: Option<HashMap<i64, String>>,
}

impl BrokerMemberGroup {
    pub fn new(
        cluster: Option<String>,
        broker_name: Option<String>,
        broker_addrs: Option<HashMap<i64, String>>,
    ) -> Self {
        Self {
            cluster,
            broker_name,
            broker_addrs,
        }
    }
}

impl RemotingSerializable for BrokerMemberGroup {
    type Output = BrokerMemberGroup;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerMemberGroupResponseBody {
    pub broker_member_group: Option<BrokerMemberGroup>,
}

impl RemotingSerializable for GetBrokerMemberGroupResponseBody {
    type Output = GetBrokerMemberGroupResponseBody;
}
