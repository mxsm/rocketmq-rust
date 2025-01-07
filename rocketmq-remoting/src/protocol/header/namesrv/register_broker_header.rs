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
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

/// Represents the header for a broker registration request.
#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodec)]
pub struct RegisterBrokerRequestHeader {
    /// The name of the broker.
    #[serde(rename = "brokerName")]
    #[required]
    pub broker_name: CheetahString,

    /// The address of the broker.
    #[serde(rename = "brokerAddr")]
    #[required]
    pub broker_addr: CheetahString,

    /// The name of the cluster to which the broker belongs.
    #[serde(rename = "clusterName")]
    #[required]
    pub cluster_name: CheetahString,

    /// The address of the highly available (HA) remoting_server associated with the broker.
    #[serde(rename = "haServerAddr")]
    #[required]
    pub ha_server_addr: CheetahString,

    /// The unique identifier for the broker.
    #[serde(rename = "brokerId")]
    #[required]
    pub broker_id: u64,

    /// The optional heartbeat timeout in milliseconds.
    #[serde(rename = "heartbeatTimeoutMillis")]
    pub heartbeat_timeout_millis: Option<i64>,

    /// The optional flag indicating whether acting as the master is enabled.
    #[serde(rename = "enableActingMaster")]
    pub enable_acting_master: Option<bool>,

    /// Indicates whether the data is compressed.
    #[required]
    pub compressed: bool,

    /// The CRC32 checksum for the message body.
    #[serde(rename = "bodyCrc32")]
    #[required]
    pub body_crc32: u32,
}

impl RegisterBrokerRequestHeader {
    /// Creates a new instance of `RegisterBrokerRequestHeader`.
    ///
    /// # Arguments
    ///
    /// * `broker_name` - The name of the broker.
    /// * `broker_addr` - The address of the broker.
    /// * `cluster_name` - The name of the cluster.
    /// * `ha_server_addr` - The address of the HA remoting_server.
    /// * `broker_id` - The unique identifier for the broker.
    /// * `heartbeat_timeout_millis` - The optional heartbeat timeout in milliseconds.
    /// * `enable_acting_master` - The optional flag indicating whether acting as the master is
    ///   enabled.
    /// * `compressed` - Indicates whether the data is compressed.
    /// * `body_crc32` - The CRC32 checksum for the message body.
    ///
    /// # Returns
    ///
    /// A new `RegisterBrokerRequestHeader` instance.
    pub fn new(
        broker_name: CheetahString,
        broker_addr: CheetahString,
        cluster_name: CheetahString,
        ha_server_addr: CheetahString,
        broker_id: u64,
        heartbeat_timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        compressed: bool,
        body_crc32: u32,
    ) -> Self {
        RegisterBrokerRequestHeader {
            broker_name,
            broker_addr,
            cluster_name,
            ha_server_addr,
            broker_id,
            heartbeat_timeout_millis,
            enable_acting_master,
            compressed,
            body_crc32,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBrokerResponseHeader {
    pub ha_server_addr: Option<CheetahString>,
    pub master_addr: Option<CheetahString>,
}

impl RegisterBrokerResponseHeader {
    const HA_SERVER_ADDR: &'static str = "haServerAddr";
    const MASTER_ADDR: &'static str = "masterAddr";

    pub fn new(ha_server_addr: Option<CheetahString>, master_addr: Option<CheetahString>) -> Self {
        RegisterBrokerResponseHeader {
            ha_server_addr,
            master_addr,
        }
    }
}

impl CommandCustomHeader for RegisterBrokerResponseHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        Ok(())
    }

    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::<CheetahString, CheetahString>::new();

        if let Some(ref ha_server_addr) = self.ha_server_addr {
            map.insert(
                CheetahString::from_static_str(RegisterBrokerResponseHeader::HA_SERVER_ADDR),
                ha_server_addr.clone(),
            );
        }
        if let Some(ref master_addr) = self.master_addr {
            map.insert(
                CheetahString::from_static_str(RegisterBrokerResponseHeader::MASTER_ADDR),
                master_addr.clone(),
            );
        }

        Some(map)
    }
}

impl FromMap for RegisterBrokerResponseHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(RegisterBrokerResponseHeader {
            ha_server_addr: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerResponseHeader::HA_SERVER_ADDR,
                ))
                .cloned(),
            master_addr: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerResponseHeader::MASTER_ADDR,
                ))
                .cloned(),
        })
    }
}
