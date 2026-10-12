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

//! Exact-Broker, read-only Consumer client runtime capability.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Additive capability for reading what one consumer client reports about
/// itself. One already resolved Broker relays the request to the client that
/// is connected to it. Address resolution remains the responsibility of a
/// trusted adapter.
///
/// The thread dump of the client is never requested: it is large, and it shows
/// application internals that a state read has no use for.
#[allow(async_fn_in_trait)]
pub trait MQAdminConsumerRuntimeReadExt: Send {
    /// Reads the running state of one consumer client through one exact Broker.
    async fn consumer_running_info_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<ConsumerRunningInfo>;
}

impl MQAdminConsumerRuntimeReadExt for DefaultMQAdminExt {
    async fn consumer_running_info_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
    ) -> crate::ClientResult<ConsumerRunningInfo> {
        self.inner()
            .mq_client_api()?
            .get_consumer_running_info(
                &broker_addr,
                consumer_group,
                client_id,
                false,
                self.inner().remoting_timeout_millis()?,
            )
            .await
    }
}
