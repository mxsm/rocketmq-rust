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

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::controller_rollout::RolloutQuorumStatus;

use super::DefaultMQAdminExtImpl;
use crate::ClientResult;

impl DefaultMQAdminExtImpl {
    /// Uses this admin session's credentials and transport for a fresh rollout check.
    ///
    /// # Errors
    ///
    /// Fails if the session is stopped or the current leader cannot confirm the remaining quorum.
    pub async fn check_controller_rollout(
        &self,
        controller_address: CheetahString,
        target_node_id: u64,
    ) -> ClientResult<RolloutQuorumStatus> {
        self.mq_client_api()?
            .check_controller_rollout(controller_address, target_node_id, self.remoting_timeout_millis()?)
            .await
    }
}
