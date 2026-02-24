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

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::ha::ha_service::HAService;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct GetBrokerHaStatusHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> GetBrokerHaStatusHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn get_broker_ha_status(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_runtime_inner = self.broker_runtime_inner.as_mut();
        let response = RemotingCommand::create_response_command();

        let message_store = match broker_runtime_inner.message_store() {
            Some(store) => store,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is not available"),
                ));
            }
        };

        let ha_service = match message_store.get_ha_service() {
            Some(service) => service,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("HA service is not available"),
                ));
            }
        };

        let master_put_where = message_store.get_max_phy_offset();
        let ha_runtime_info = ha_service.get_runtime_info(master_put_where);

        match serde_json::to_vec(&ha_runtime_info) {
            Ok(body) => Ok(Some(response.set_body(body).set_code(ResponseCode::Success))),
            Err(e) => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("Failed to serialize HARuntimeInfo: {}", e)),
            )),
        }
    }
}
