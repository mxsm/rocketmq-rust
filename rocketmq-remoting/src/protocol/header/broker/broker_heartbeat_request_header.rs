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

use anyhow::Error;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BrokerHeartbeatRequestHeader {
    #[serde(rename = "clusterName")]
    pub cluster_name: CheetahString,
    #[serde(rename = "brokerAddr")]
    pub broker_addr: CheetahString,
    #[serde(rename = "brokerName")]
    pub broker_name: CheetahString,
    #[serde(rename = "brokerId")]
    pub broker_id: Option<i64>,
    pub epoch: Option<i32>,
    #[serde(rename = "maxOffset")]
    pub max_offset: Option<i64>,
    #[serde(rename = "confirmOffset")]
    pub confirm_offset: Option<i64>,
    #[serde(rename = "heartbeatTimeoutMills")]
    pub heartbeat_timeout_mills: Option<i64>,
    #[serde(rename = "electionPriority")]
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
    const HEARTBEAT_TIMEOUT_MILLIS: &'static str = "heartbeatTimeoutMills";
    const MAX_OFFSET: &'static str = "maxOffset";

    pub fn new(
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<i64>,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        heartbeat_timeout_mills: Option<i64>,
        election_priority: Option<i32>,
    ) -> Self {
        Self {
            cluster_name,
            broker_addr,
            broker_name,
            broker_id,
            epoch,
            max_offset,
            confirm_offset,
            heartbeat_timeout_mills,
            election_priority,
        }
    }
}

impl FromMap for BrokerHeartbeatRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = BrokerHeartbeatRequestHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(BrokerHeartbeatRequestHeader {
            cluster_name: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::CLUSTER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_addr: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::BROKER_ADDR,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_name: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::BROKER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_id: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::BROKER_ID,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            epoch: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::EPOCH,
                ))
                .and_then(|s| s.parse::<i32>().ok()),
            max_offset: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::MAX_OFFSET,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            confirm_offset: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::CONFIRM_OFFSET,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            heartbeat_timeout_mills: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::HEARTBEAT_TIMEOUT_MILLIS,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            election_priority: map
                .get(&CheetahString::from_static_str(
                    BrokerHeartbeatRequestHeader::ELECTION_PRIORITY,
                ))
                .and_then(|s| s.parse::<i32>().ok()),
        })
    }
}

impl CommandCustomHeader for BrokerHeartbeatRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        todo!()
    }

    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        todo!()
    }
}
