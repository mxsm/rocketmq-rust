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
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use super::broker_config_request_handler::BrokerConfigRequestHandler;

#[derive(Clone)]
pub struct NotifyBrokerRoleChangeHandler;

impl NotifyBrokerRoleChangeHandler {
    pub fn new() -> Self {
        Self
    }

    pub async fn notify_broker_role_changed<MS: MessageStore>(
        &self,
        broker_config_request_handler: &BrokerConfigRequestHandler<MS>,
        channel: Channel,
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

        if broker_config_request_handler
            .broker_runtime_inner()
            .replicas_manager()
            .is_none()
        {
            warn!("Ignore notifyBrokerRoleChanged because controller mode is not initialized");
            return Ok(Some(response.set_code(ResponseCode::Success)));
        }

        if let Ok(request_header) = request_header {
            let sync_state_set = sync_state_set_info.get_sync_state_set().cloned().unwrap_or_default();
            let controller_leader_address = channel.remote_address().to_string().into();

            if let Err(error) = broker_config_request_handler
                .apply_controller_role_change(
                    Some(controller_leader_address),
                    request_header.master_broker_id,
                    request_header.master_address,
                    request_header.master_epoch,
                    request_header.sync_state_set_epoch,
                    sync_state_set,
                )
                .await
            {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    error.to_string(),
                )));
            }
        }

        Ok(Some(response.set_code(ResponseCode::Success)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
    use rocketmq_remoting::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use crate::broker_runtime::BrokerRuntime;

    use super::BrokerConfigRequestHandler;
    use super::NotifyBrokerRoleChangeHandler;

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new()));
        let inner = Arc::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn uninitialized_controller_returns_success_without_retaining_runtime_owner() {
        let mut runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let inner = runtime.inner_for_test().clone();
        let broker_config_request_handler = BrokerConfigRequestHandler::new(runtime.admin_runtime_handle_for_test());
        let strong_count_before = inner.strong_count();
        let handler = NotifyBrokerRoleChangeHandler::new();

        assert_eq!(inner.strong_count(), strong_count_before);

        let channel = create_test_channel().await;
        let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let header = NotifyBrokerRoleChangedRequestHeader {
            master_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            master_epoch: Some(1),
            sync_state_set_epoch: Some(1),
            master_broker_id: Some(0),
        };
        let body = SyncStateSet::with_values(HashSet::from([0]), 1);
        let mut request = RemotingCommand::create_request_command(RequestCode::NotifyBrokerRoleChanged, header)
            .set_body(Bytes::from(
                serde_json::to_vec(&body).expect("serialize sync-state set"),
            ));

        let response = handler
            .notify_broker_role_changed(
                &broker_config_request_handler,
                channel,
                ctx,
                RequestCode::NotifyBrokerRoleChanged,
                &mut request,
            )
            .await
            .expect("uninitialized controller notification should not fail")
            .expect("notification should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(inner.strong_count(), strong_count_before);
    }
}
