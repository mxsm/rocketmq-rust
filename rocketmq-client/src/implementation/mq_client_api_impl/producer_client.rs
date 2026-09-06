// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

pub struct ProducerClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl ProducerClient<'_> {
    pub async fn send_heartbeat(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(i32, Option<RemotingCommand>)> {
        self.api.send_heartbeat(addr, heartbeat_data, timeout_millis).await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn producer_client(&self) -> ProducerClient<'_> {
        ProducerClient { api: self }
    }

    pub async fn send_heartbeat_async(
        &self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.send_heartbeat(addr, heartbeat_data, timeout_millis)
            .await
            .map(|(version, _)| version)
    }
}
