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

use async_trait::async_trait;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyRemotingBackend;
use rocketmq_proxy_core::ProxyResult;

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

#[async_trait]
impl ProxyRemotingBackend for ClusterRemotingBackend {
    async fn process(&self, request: RemotingCommand) -> ProxyResult<RemotingCommand> {
        let opaque = request.opaque();
        let body = request
            .body()
            .ok_or_else(|| ProxyError::invalid_metadata("lock/unlock batch request body is missing"))?;
        match RequestCode::from(request.code()) {
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
            _ => Err(ProxyError::not_implemented("cluster remoting backend request")),
        }
    }
}
