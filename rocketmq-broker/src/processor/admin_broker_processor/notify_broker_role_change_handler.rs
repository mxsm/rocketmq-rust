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
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct NotifyBrokerRoleChangeHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> NotifyBrokerRoleChangeHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn notify_broker_role_changed(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<NotifyBrokerRoleChangedRequestHeader>();

        let sync_state_set_info = SyncStateSet::decode(request.get_body().unwrap()).unwrap_or_default();

        let response = RemotingCommand::create_response_command();

        info!(
            "Receive notifyBrokerRoleChanged request, try to change brokerRole, request:{}",
            request_header.as_ref().expect("null")
        );

        if let Some(replicas_mangesr) = self.broker_runtime_inner.replicas_manager_mut() {
            if let Ok(request_header) = request_header {
                match replicas_mangesr
                    .change_broker_role(
                        request_header.master_broker_id,
                        request_header.master_address,
                        request_header.master_epoch,
                        request_header.sync_state_set_epoch,
                        sync_state_set_info.get_sync_state_set(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Failed to call method change_broker_role: {}", e);
                    }
                }
            }
        }

        Ok(Some(response.set_code(ResponseCode::Success)))
    }
}
