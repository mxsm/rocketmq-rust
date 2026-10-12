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

//! Exact-Broker, read-only Lite Topic capability.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Additive capability for reading what one already resolved Broker holds for
/// Lite Topics: its limits, its counters, and the Lite Topics and bound groups
/// it knows. Address resolution remains the responsibility of a trusted
/// adapter and addresses never cross this trait's result boundary.
#[allow(async_fn_in_trait)]
pub trait MQAdminLiteReadExt: Send {
    /// Reads the Lite Topic state of one exact Broker.
    async fn broker_lite_info_at(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<GetBrokerLiteInfoResponseBody>;
}

impl MQAdminLiteReadExt for DefaultMQAdminExt {
    async fn broker_lite_info_at(
        &self,
        broker_addr: CheetahString,
    ) -> crate::ClientResult<GetBrokerLiteInfoResponseBody> {
        self.inner()
            .mq_client_api()?
            .get_broker_lite_info(&broker_addr, self.inner().remoting_timeout_millis()?)
            .await
    }
}
