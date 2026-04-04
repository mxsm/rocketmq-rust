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

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::header::create_acl_request_header::CreateAclRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_store::base::message_store::MessageStore;

use crate::auth::acl_converter::AclConverter;
use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct CreateAclRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> CreateAclRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn create_acl(
        &mut self,
        _channel: Channel,
        _ctx: rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<CreateAclRequestHeader>()?;
        let response = RemotingCommand::create_response_command();

        if request_header.subject.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The subject is blank"),
            ));
        }

        let Some(body) = request.get_body() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("Request body is empty"),
            ));
        };
        let acl_info = AclInfo::decode(body)?;
        let acl = AclConverter::convert_acl_info(&acl_info, request_header.subject.as_str())?;

        if self.is_not_super_user_login(request).await? {
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("ACL can only be created by super user"),
            ));
        }

        match self.auth_admin_service.create_acl(acl).await {
            Ok(()) => Ok(Some(response.set_code(ResponseCode::Success))),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }

    async fn is_not_super_user_login(&self, request: &RemotingCommand) -> rocketmq_error::RocketMQResult<bool> {
        let Some(access_key) = request
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("AccessKey")))
        else {
            return Ok(false);
        };

        Ok(!self.auth_admin_service.is_super_user(access_key.as_str()).await?)
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
