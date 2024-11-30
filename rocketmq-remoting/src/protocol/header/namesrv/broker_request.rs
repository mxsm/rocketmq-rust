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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnRegisterBrokerRequestHeader {
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    pub cluster_name: CheetahString,
    pub broker_id: u64,
}

impl UnRegisterBrokerRequestHeader {
    const BROKER_ADDR: &'static str = "brokerAddr";
    const BROKER_ID: &'static str = "brokerId";
    const BROKER_NAME: &'static str = "brokerName";
    const CLUSTER_NAME: &'static str = "clusterName";

    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        cluster_name: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_addr: broker_addr.into(),
            cluster_name: cluster_name.into(),
            broker_id,
        }
    }
}

impl CommandCustomHeader for UnRegisterBrokerRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([
            (
                CheetahString::from_static_str(Self::BROKER_NAME),
                self.broker_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_ADDR),
                self.broker_addr.clone(),
            ),
            (
                CheetahString::from_static_str(Self::CLUSTER_NAME),
                self.cluster_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_ID),
                CheetahString::from_string(self.broker_id.to_string()),
            ),
        ]))
    }
}

impl FromMap for UnRegisterBrokerRequestHeader {
    type Error = crate::remoting_error::RemotingError;
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(UnRegisterBrokerRequestHeader {
            broker_name: map
                .get(&CheetahString::from_static_str(Self::BROKER_NAME))
                .cloned()
                .unwrap_or_default(),
            broker_addr: map
                .get(&CheetahString::from_static_str(Self::BROKER_ADDR))
                .cloned()
                .unwrap_or_default(),
            cluster_name: map
                .get(&CheetahString::from_static_str(Self::CLUSTER_NAME))
                .cloned()
                .unwrap_or_default(),
            broker_id: map
                .get(&CheetahString::from_static_str(Self::BROKER_ID))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerHeartbeatRequestHeader {
    pub cluster_name: CheetahString,
    pub broker_addr: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: Option<i64>,
    pub epoch: Option<i32>,
    pub max_offset: Option<i64>,
    pub confirm_offset: Option<i64>,
    pub heartbeat_timeout_mills: Option<i64>,
    pub election_priority: Option<i32>,
}

impl BrokerHeartbeatRequestHeader {
    const BROKER_ADDR: &'static str = "brokerAddr";
    const BROKER_ID: &'static str = "brokerId";
    const BROKER_NAME: &'static str = "brokerName";
    const CLUSTER_NAME: &'static str = "clusterName";
    const CONFIRM_OFFSET: &'static str = "confirmOffset";
    const ELECTION_PRIORITY: &'static str = "electionPriority";
    const EPOCH: &'static str = "epoch";
    const HEARTBEAT_TIMEOUT_MILLS: &'static str = "heartbeatTimeoutMills";
    const MAX_OFFSET: &'static str = "maxOffset";

    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: Option<i64>,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        heartbeat_timeout_mills: Option<i64>,
        election_priority: Option<i32>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_addr: broker_addr.into(),
            broker_name: broker_name.into(),
            broker_id,
            epoch,
            max_offset,
            confirm_offset,
            heartbeat_timeout_mills,
            election_priority,
        }
    }
}

impl CommandCustomHeader for BrokerHeartbeatRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::from([
            (
                CheetahString::from_static_str(Self::CLUSTER_NAME),
                self.cluster_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_ADDR),
                self.broker_addr.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_NAME),
                self.broker_name.clone(),
            ),
        ]);
        if let Some(broker_id) = self.broker_id {
            map.insert(
                CheetahString::from_static_str(Self::BROKER_ID),
                CheetahString::from_string(broker_id.to_string()),
            );
        }
        if let Some(epoch) = self.epoch {
            map.insert(
                CheetahString::from_static_str(Self::EPOCH),
                CheetahString::from_string(epoch.to_string()),
            );
        }
        if let Some(max_offset) = self.max_offset {
            map.insert(
                CheetahString::from_static_str(Self::MAX_OFFSET),
                CheetahString::from_string(max_offset.to_string()),
            );
        }
        if let Some(confirm_offset) = self.confirm_offset {
            map.insert(
                CheetahString::from_static_str(Self::CONFIRM_OFFSET),
                CheetahString::from_string(confirm_offset.to_string()),
            );
        }
        if let Some(heartbeat_timeout_mills) = self.heartbeat_timeout_mills {
            map.insert(
                CheetahString::from_static_str(Self::HEARTBEAT_TIMEOUT_MILLS),
                CheetahString::from_string(heartbeat_timeout_mills.to_string()),
            );
        }
        if let Some(election_priority) = self.election_priority {
            map.insert(
                CheetahString::from_static_str(Self::ELECTION_PRIORITY),
                CheetahString::from_string(election_priority.to_string()),
            );
        }
        Some(map)
    }
}

impl FromMap for BrokerHeartbeatRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(BrokerHeartbeatRequestHeader {
            cluster_name: map
                .get(&CheetahString::from_static_str(Self::CLUSTER_NAME))
                .cloned()
                .unwrap_or_default(),
            broker_addr: map
                .get(&CheetahString::from_static_str(Self::BROKER_ADDR))
                .cloned()
                .unwrap_or_default(),
            broker_name: map
                .get(&CheetahString::from_static_str(Self::BROKER_NAME))
                .cloned()
                .unwrap_or_default(),
            broker_id: map
                .get(&CheetahString::from_static_str(Self::BROKER_ID))
                .and_then(|s| s.parse::<i64>().ok()),
            epoch: map
                .get(&CheetahString::from_static_str(Self::EPOCH))
                .and_then(|s| s.parse::<i32>().ok()),
            max_offset: map
                .get(&CheetahString::from_static_str(Self::MAX_OFFSET))
                .and_then(|s| s.parse::<i64>().ok()),
            confirm_offset: map
                .get(&CheetahString::from_static_str(Self::CONFIRM_OFFSET))
                .and_then(|s| s.parse::<i64>().ok()),
            heartbeat_timeout_mills: map
                .get(&CheetahString::from_static_str(
                    Self::HEARTBEAT_TIMEOUT_MILLS,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            election_priority: map
                .get(&CheetahString::from_static_str(Self::ELECTION_PRIORITY))
                .and_then(|s| s.parse::<i32>().ok()),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerMemberGroupRequestHeader {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
}

impl GetBrokerMemberGroupRequestHeader {
    const BROKER_NAME: &'static str = "brokerName";
    const CLUSTER_NAME: &'static str = "clusterName";

    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),

            broker_name: broker_name.into(),
        }
    }
}

impl CommandCustomHeader for GetBrokerMemberGroupRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([
            (
                CheetahString::from_static_str(Self::CLUSTER_NAME),
                self.cluster_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_NAME),
                self.broker_name.clone(),
            ),
        ]))
    }
}

impl FromMap for GetBrokerMemberGroupRequestHeader {
    type Error = crate::remoting_error::RemotingError;
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetBrokerMemberGroupRequestHeader {
            cluster_name: map
                .get(&CheetahString::from_static_str(Self::CLUSTER_NAME))
                .cloned()
                .unwrap_or_default(),

            broker_name: map
                .get(&CheetahString::from_static_str(Self::BROKER_NAME))
                .cloned()
                .unwrap_or_default(),
        })
    }
}
