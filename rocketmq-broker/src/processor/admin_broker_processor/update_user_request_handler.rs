// Copyright 2026 The RocketMQ Rust Authors
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
use crate::auth::user_converter::UserConverter;
use rocketmq_auth::UserType;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::update_user_request_header::UpdateUserRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use std::sync::Arc;

#[derive(Clone)]
pub struct UpdateUserRequestHandler {
    auth_admin_service: Arc<AuthAdminService>,
}

impl UpdateUserRequestHandler {
    pub fn new(auth_admin_service: Arc<AuthAdminService>) -> Self {
        Self { auth_admin_service }
    }

    pub async fn update_user(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UpdateUserRequestHeader>()?;

        let response = RemotingCommand::create_java_default_error_response_command();

        if request_header.username.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The username is blank"),
            ));
        }

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
        let mut user_info: UserInfo = match UserInfo::decode(body) {
            Ok(user_info) => user_info,
            Err(error) => {
                return Ok(Some(map_error_response(
                    response,
                    super::auth_admin_body_decode_error("decode user body", error),
                )));
            }
        };

        user_info.username = Option::from(request_header.username);
        let user = UserConverter::convert_user(&user_info);
        let is_not_super_user_login = self.is_not_super_user_login(request).await?;

        if user.user_type() == Option::from(UserType::Super) && is_not_super_user_login {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("The super user can only be update by super user"),
            ));
        }

        match self.auth_admin_service.get_user(user.username().as_str()).await {
            Ok(Some(existing_user))
                if existing_user.user_type.as_deref().and_then(UserType::get_by_name) == Some(UserType::Super)
                    && is_not_super_user_login =>
            {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::NoPermission)
                        .set_remark("The super user can only be update by super user"),
                ));
            }
            Ok(_) => {}
            Err(error) => return Ok(Some(map_error_response(response, error))),
        }

        match self.auth_admin_service.update_user(user).await {
            Ok(()) => Ok(Some(RemotingCommand::create_success_response_command())),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }

    async fn is_not_super_user_login(&self, request: &RemotingCommand) -> rocketmq_error::RocketMQResult<bool> {
        let Some(access_key) = request.ext_fields().and_then(|fields| fields.get("AccessKey")) else {
            return Ok(false);
        };

        Ok(!self.auth_admin_service.is_super_user(access_key.as_str()).await?)
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    super::map_auth_admin_error_response(response, error)
}
