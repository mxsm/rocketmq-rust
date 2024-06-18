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
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;

use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;

#[derive(Clone)]
pub struct PullMessageProcessor {
    consume_message_hook_vec: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    pull_message_result_handler: Arc<dyn PullMessageResultHandler>,
    broker_config: Arc<BrokerConfig>,
}

impl Default for PullMessageProcessor {
    fn default() -> Self {
        todo!()
    }
}

impl PullMessageProcessor {
    pub async fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        self.process_request_inner(request_code, ctx, request, true, true)
            .await
    }

    async fn process_request_inner(
        &mut self,
        request_code: RequestCode,
        _ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
        _broker_allow_suspend: bool,
        _broker_allow_flow_ctr_suspend: bool,
    ) -> Option<RemotingCommand> {
        let _begin_time_mills = get_current_millis();
        let mut response = RemotingCommand::create_response_command();
        response.set_opaque_mut(request.opaque());
        let _request_header =
            request.decode_command_custom_header_fast::<PullMessageRequestHeader>();
        if !PermName::is_readable(self.broker_config.broker_permission) {
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark(Some(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_config.broker_ip1
                    ))),
            );
        }
        if RequestCode::LitePullMessage == request_code
            && !self.broker_config.lite_pull_message_enable
        {
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark(Some(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_config.broker_ip1
                    ))),
            );
        }
        Some(response)
    }
}
