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

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct NotifyMinBrokerChangeIdHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> NotifyMinBrokerChangeIdHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
        }
    }

    pub async fn notify_min_broker_id_change(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> Option<RemotingCommand> {
        let change_header = request
            .decode_command_custom_header::<NotifyMinBrokerIdChangeRequestHeader>()
            .unwrap();

        let current_broker_id = self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_id;

        warn!(
            "min broker id changed, prev {}, new {}",
            current_broker_id,
            change_header
                .min_broker_id
                .expect("min broker id not must be present")
        );

        // TODO Implement update broker id method in the near future

        let mut response = RemotingCommand::default();
        response.set_code_ref(ResponseCode::Success);
        Some(response)
    }
}
