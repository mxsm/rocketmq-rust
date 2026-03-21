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

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::list_users_request_header::ListUsersRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use std::sync::Arc;

#[derive(Clone)]
pub struct ListUsersRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> ListUsersRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn list_users(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ListUsersRequestHeader>()?;
        let mut response = RemotingCommand::create_response_command();

        match self
            .auth_admin_service
            .list_users(non_empty(request_header.filter.as_str()))
            .await
        {
            Ok(users) => {
                response.set_body_mut_ref(users.encode()?);
                Ok(Some(response.set_code(ResponseCode::Success)))
            }
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }
}

fn non_empty(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    match error {
        RocketMQError::IllegalArgument(message) => {
            response.set_code(ResponseCode::InvalidParameter).set_remark(message)
        }
        RocketMQError::BrokerPermissionDenied { operation } => {
            response.set_code(ResponseCode::NoPermission).set_remark(operation)
        }
        other => response
            .set_code(ResponseCode::SystemError)
            .set_remark(other.to_string()),
    }
}
