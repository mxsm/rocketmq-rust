/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_error::RocketmqError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use tracing::error;
use tracing::warn;

use crate::broker_controller::BrokerController;

#[derive(Default)]
pub struct NotifyMinBrokerChangeIdProcessor {
    broker_controller: Option<BrokerController>,
}

impl NotifyMinBrokerChangeIdProcessor {
    pub fn new(broker_controller: Option<BrokerController>) -> Self {
        Self {
            broker_controller: broker_controller,
        }
    }

    pub async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let change_header =
            request.decode_command_custom_header::<NotifyMinBrokerIdChangeRequestHeader>()?;

        if let Some(broker_controller) = self.broker_controller.as_ref() {
            match broker_controller.get_current_broker_id() {
                Ok(id) => {
                    warn!(
                        "min broker id changed, prev {}, new {}",
                        id,
                        change_header
                            .min_broker_id
                            .expect("min broker id not must be present")
                    );
                }
                Err(err) => {
                    error!("failed to get min broker id group, {}", err);
                }
            }

            broker_controller.update_min_broker(
                &change_header.get_min_broker_id(),
                &change_header.get_broker_name(),
                &change_header.get_offline_broker_addr(),
                &change_header.get_ha_broker_addr(),
            );

            let mut response = RemotingCommand::default();
            response.set_code_ref(ResponseCode::Success);
            Ok(Some(response))
        } else {
            Err(RocketmqError::NoneError(String::from(
                "broker controller not initialized",
            )))
        }
    }
}
