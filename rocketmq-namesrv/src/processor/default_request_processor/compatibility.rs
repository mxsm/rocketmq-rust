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

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::v1::Channel;

use super::DefaultRequestProcessor;
use crate::route::types::BrokerSession;

impl DefaultRequestProcessor {
    /// Retained V1 compatibility entry point.
    ///
    /// Production dispatch uses `RequestProcessorV2`. This facade immediately
    /// projects the legacy channel to owned, read-only broker session facts and
    /// never retains write or cancellation authority.
    #[deprecated(since = "1.0.0", note = "use the RequestProcessorV2 ingress boundary")]
    pub async fn process_request_inner(
        &self,
        channel: Channel,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_session =
            (request_code == RequestCode::RegisterBroker).then(|| BrokerSession::from_legacy_channel(&channel));
        self.process_request_inner_v2(broker_session, request_code, request)
            .await
    }
}
