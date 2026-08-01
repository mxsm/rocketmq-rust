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

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_store::BrokerAdminStore;
use rocketmq_transport::Channel;
use rocketmq_transport::ConnectionHandlerContext;
use tracing::info;
use tracing::warn;

use super::broker_config_request_handler::BrokerConfigRequestHandler;

#[derive(Clone)]
pub struct NotifyBrokerRoleChangeHandler;

impl NotifyBrokerRoleChangeHandler {
    pub fn new() -> Self {
        Self
    }

    pub async fn notify_broker_role_changed<MS: BrokerAdminStore>(
        &self,
        broker_config_request_handler: &BrokerConfigRequestHandler<MS>,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();

        let request_header = match request.decode_command_custom_header::<NotifyBrokerRoleChangedRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invalid role-change header: {error}"),
                )));
            }
        };

        let Some(body) = request.get_body() else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "notify broker role change is missing the sync-state-set body",
            )));
        };
        let sync_state_set_info = match SyncStateSet::decode(body) {
            Ok(sync_state_set) => sync_state_set,
            Err(error) => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invalid sync-state-set body: {error}"),
                )));
            }
        };

        info!(
            "Receive notifyBrokerRoleChanged request, try to change brokerRole, request:{}",
            request_header
        );

        if broker_config_request_handler
            .broker_runtime_inner()
            .replicas_manager()
            .is_none()
        {
            warn!("Ignore notifyBrokerRoleChanged because controller mode is not initialized");
            return Ok(Some(response.set_code(ResponseCode::Success)));
        }

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

        Ok(Some(response.set_code(ResponseCode::Success)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
    use rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::Channel;
    use rocketmq_transport::ChannelInner;
    use rocketmq_transport::Connection;
    use rocketmq_transport::ConnectionHandlerContextWrapper;
    use rocketmq_transport::ResponseFuture;

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
        let inner = Arc::new(ChannelInner::new(
            connection,
            response_table,
            crate::test_task_group("channel"),
        ));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn uninitialized_controller_fails_closed_without_retaining_runtime_owner() {
        let runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let admin = runtime.admin_runtime_for_test();
        let broker_config_request_handler = BrokerConfigRequestHandler::new(admin);
        let handler = NotifyBrokerRoleChangeHandler::new();

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
            .expect("uninitialized controller notification should return a response")
            .expect("notification should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    }
}
