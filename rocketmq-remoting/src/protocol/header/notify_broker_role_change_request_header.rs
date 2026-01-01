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

use std::fmt::Display;

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct NotifyBrokerRoleChangedRequestHeader {
    #[required]
    pub master_address: Option<CheetahString>,

    #[required]
    pub master_epoch: Option<i32>,

    #[required]
    pub sync_state_set_epoch: Option<i32>,

    #[required]
    pub master_broker_id: Option<u64>,
}

impl Display for NotifyBrokerRoleChangedRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(master_address={:?}, master_epoch={:?}, sync_state_set_epoch={:?}, master_broker_id={:?})",
            self.master_address, self.master_epoch, self.sync_state_set_epoch, self.master_broker_id
        )
    }
}
