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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_proxy_core::EmbeddedDispatchOutcome;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyRemotingBackend;
use rocketmq_proxy_core::ProxyServiceFuture;
use rocketmq_proxy_core::ResponsePlan;

use crate::cluster::RocketmqClusterClient;

/// Remoting operations that require direct access to the Client runtime.
pub struct ClusterRemotingBackend {
    client: Arc<RocketmqClusterClient>,
}

impl ClusterRemotingBackend {
    pub fn new(client: Arc<RocketmqClusterClient>) -> Self {
        Self { client }
    }
}

impl ProxyRemotingBackend for ClusterRemotingBackend {
    fn process(&self, request: RemotingCommand) -> ProxyServiceFuture<'_, EmbeddedDispatchOutcome> {
        Box::pin(async move {
            let opaque = request.opaque();
            let body = request
                .body()
                .ok_or_else(|| ProxyError::invalid_metadata("lock/unlock batch request body is missing"))?;
            let response = match RequestCode::from(request.code()) {
                RequestCode::LockBatchMq => {
                    let request_body = LockBatchRequestBody::decode(body.as_ref())?;
                    let response_body = self.client.lock_batch_mq(request_body).await?;
                    Ok(
                        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                            .set_body(response_body.encode()?)
                            .set_opaque(opaque),
                    )
                }
                RequestCode::UnlockBatchMq => {
                    let request_body = UnlockBatchRequestBody::decode(body.as_ref())?;
                    self.client.unlock_batch_mq(request_body).await?;
                    Ok(RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(opaque))
                }
                RequestCode::ConsumerSendMsgBack
                | RequestCode::EndTransaction
                | RequestCode::RecallMessage
                | RequestCode::PopMessage
                | RequestCode::AckMessage
                | RequestCode::ChangeMessageInvisibleTime => {
                    if let Some(broker_name) = broker_name(&request) {
                        let timeout_millis = forward_timeout_millis(&request);
                        self.client.forward_remoting(broker_name, request, timeout_millis).await
                    } else {
                        Ok(
                            RemotingCommand::create_response_command_with_code(ResponseCode::VersionNotSupported)
                                .set_remark("Request doesn't have field bname")
                                .set_opaque(opaque),
                        )
                    }
                }
                _ => return Err(ProxyError::not_implemented("cluster remoting backend request")),
            }?;
            let plan = ResponsePlan::from_command(response).map_err(|error| ProxyError::Transport {
                message: format!("cluster backend response could not become a response plan: {error}"),
            })?;
            Ok(EmbeddedDispatchOutcome::Reply(plan))
        })
    }
}

fn broker_name(request: &RemotingCommand) -> Option<String> {
    request.ext_fields().and_then(|fields| {
        fields
            .iter()
            .find(|(key, _)| key.as_str() == "bname")
            .map(|(_, value)| value.to_string())
            .filter(|value| !value.trim().is_empty())
    })
}

fn forward_timeout_millis(request: &RemotingCommand) -> u64 {
    if RequestCode::from(request.code()) == RequestCode::PopMessage {
        return request
            .decode_command_custom_header::<PopMessageRequestHeader>()
            .map(|header| {
                header
                    .poll_time
                    .saturating_add(Duration::from_secs(10).as_millis() as u64)
            })
            .unwrap_or(13_000);
    }
    if RequestCode::from(request.code()) == RequestCode::RecallMessage {
        2_000
    } else {
        3_000
    }
}
