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

use crate::broker_runtime::BrokerRuntimeInner;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

#[derive(Clone)]
pub struct UpdateColdDataFlowCtrGroupConfigRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> UpdateColdDataFlowCtrGroupConfigRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn update_cold_data_flow_ctr_group_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::default();

        let body = match request.get_body() {
            Some(body) => body,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("Request body is empty"),
                ));
            }
        };

        let body = mix_all::string_to_properties(&String::from_utf8_lossy(body));
        match body {
            Some(_body) => {
                // TODO get broker controller and do operations
                Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(
                    "UpdateColdDataFlowCtrGroupConfig not implemented: ColdDataCgCtrService not configured in \
                     broker_runtime",
                )))
            }
            None => Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("Request body is empty"),
            )),
        }
    }
}
