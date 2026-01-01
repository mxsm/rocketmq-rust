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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntry;
use tracing::warn;

#[derive(Default)]
pub struct ReplicasManager {}

impl ReplicasManager {
    pub fn start(&mut self) {
        warn!("ReplicasManager started not implemented");
    }

    pub fn shutdown(&mut self) {
        warn!("ReplicasManager shutdown not implemented");
    }

    pub fn get_epoch_entries(&self) -> Vec<EpochEntry> {
        unimplemented!("")
    }

    pub async fn change_broker_role(
        &mut self,
        _new_master_broker_id: Option<u64>,
        _new_master_address: Option<CheetahString>,
        _new_master_epoch: Option<i32>,
        _sync_state_set_epoch: Option<i32>,
        _sync_state_set: Option<&HashSet<i64>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }
}
