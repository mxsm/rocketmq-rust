// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::heartbeat::broker_identity_info::BrokerIdentityInfo;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerCloseChannelRequest {
    pub cluster_name: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub broker_id: Option<u64>,
}

impl BrokerCloseChannelRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_broker_identity(info: &BrokerIdentityInfo) -> Self {
        Self {
            cluster_name: Some(info.cluster_name.to_owned()),
            broker_name: Some(info.broker_name.to_owned()),
            broker_id: info.broker_id,
        }
    }

    pub fn broker_identity_info(&self) -> BrokerIdentityInfo {
        BrokerIdentityInfo::new(
            self.cluster_name.clone().unwrap_or_default(),
            self.broker_name.clone().unwrap_or_default(),
            self.broker_id,
        )
    }

    pub fn set_broker_identity_info(&mut self, info: &BrokerIdentityInfo) {
        self.cluster_name = Some(info.cluster_name.to_owned());
        self.broker_name = Some(info.broker_name.to_owned());
        self.broker_id = info.broker_id;
    }
}
