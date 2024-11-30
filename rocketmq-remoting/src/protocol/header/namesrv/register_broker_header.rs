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

/// Represents the header for a broker registration request.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RegisterBrokerRequestHeader {
    /// The name of the broker.
    #[serde(rename = "brokerName")]
    pub broker_name: CheetahString,

    /// The address of the broker.
    #[serde(rename = "brokerAddr")]
    pub broker_addr: CheetahString,

    /// The name of the cluster to which the broker belongs.
    #[serde(rename = "clusterName")]
    pub cluster_name: CheetahString,

    /// The address of the highly available (HA) remoting_server associated with the broker.
    #[serde(rename = "haServerAddr")]
    pub ha_server_addr: CheetahString,

    /// The unique identifier for the broker.
    #[serde(rename = "brokerId")]
    pub broker_id: u64,

    /// The optional heartbeat timeout in milliseconds.
    #[serde(rename = "heartbeatTimeoutMillis")]
    pub heartbeat_timeout_millis: Option<i64>,

    /// The optional flag indicating whether acting as the master is enabled.
    #[serde(rename = "enableActingMaster")]
    pub enable_acting_master: Option<bool>,

    /// Indicates whether the data is compressed.
    pub compressed: bool,

    /// The CRC32 checksum for the message body.
    #[serde(rename = "bodyCrc32")]
    pub body_crc32: u32,
}

impl RegisterBrokerRequestHeader {
    const BODY_CRC32: &'static str = "bodyCrc32";
    const BROKER_ADDR: &'static str = "brokerAddr";
    const BROKER_ID: &'static str = "brokerId";
    const BROKER_NAME: &'static str = "brokerName";
    const CLUSTER_NAME: &'static str = "clusterName";
    const COMPRESSED: &'static str = "compressed";
    const ENABLE_ACTING_MASTER: &'static str = "enableActingMaster";
    const HA_SERVER_ADDR: &'static str = "haServerAddr";
    const HEARTBEAT_TIMEOUT_MILLIS: &'static str = "heartbeatTimeoutMillis";

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

impl FromMap for RegisterBrokerRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(RegisterBrokerRequestHeader {
            broker_name: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::BROKER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_addr: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::BROKER_ADDR,
                ))
                .cloned()
                .unwrap_or_default(),
            cluster_name: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::CLUSTER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            ha_server_addr: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::HA_SERVER_ADDR,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_id: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::BROKER_ID,
                ))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
            heartbeat_timeout_millis: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::HEARTBEAT_TIMEOUT_MILLIS,
                ))
                .and_then(|s| s.parse::<i64>().ok()),
            enable_acting_master: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::ENABLE_ACTING_MASTER,
                ))
                .and_then(|s| s.parse::<bool>().ok()),
            compressed: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::COMPRESSED,
                ))
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(false),
            body_crc32: map
                .get(&CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::BODY_CRC32,
                ))
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0),
        })
    }
}

impl CommandCustomHeader for RegisterBrokerRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        Ok(())
    }

    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();

        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::BROKER_NAME),
            self.broker_name.clone(),
        );
        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::BROKER_ADDR),
            self.broker_addr.clone(),
        );
        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::CLUSTER_NAME),
            self.cluster_name.clone(),
        );
        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::HA_SERVER_ADDR),
            self.ha_server_addr.clone(),
        );
        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::BROKER_ID),
            CheetahString::from_string(self.broker_id.to_string()),
        );

        if let Some(heartbeat_timeout) = self.heartbeat_timeout_millis {
            map.insert(
                CheetahString::from_static_str(
                    RegisterBrokerRequestHeader::HEARTBEAT_TIMEOUT_MILLIS,
                ),
                CheetahString::from_string(heartbeat_timeout.to_string()),
            );
        }

        if let Some(enable_acting_master) = self.enable_acting_master {
            map.insert(
                CheetahString::from_static_str(RegisterBrokerRequestHeader::ENABLE_ACTING_MASTER),
                CheetahString::from_string(enable_acting_master.to_string()),
            );
        }

        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::COMPRESSED),
            CheetahString::from_string(self.compressed.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(RegisterBrokerRequestHeader::BODY_CRC32),
            CheetahString::from_string(self.body_crc32.to_string()),
        );

        Some(map)
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
