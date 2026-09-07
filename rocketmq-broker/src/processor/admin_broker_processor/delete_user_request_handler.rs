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

use std::sync::Arc;

use rocketmq_auth::UserType;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::delete_user_request_header::DeleteUserRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::auth::auth_admin_service::AuthAdminService;

#[derive(Clone)]
pub struct DeleteUserRequestHandler {
    auth_admin_service: Arc<AuthAdminService>,
}

impl DeleteUserRequestHandler {
    pub fn new(auth_admin_service: Arc<AuthAdminService>) -> Self {
        Self { auth_admin_service }
    }

    pub async fn delete_user(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<DeleteUserRequestHeader>()?;
        let response = RemotingCommand::create_java_default_error_response_command();

        if request_header.username.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The username is blank"),
            ));
        }

        let target_user = self
            .auth_admin_service
            .get_user(request_header.username.as_str())
            .await?;
        if target_user
            .as_ref()
            .and_then(|user| user.user_type.as_deref())
            .and_then(UserType::get_by_name)
            == Some(UserType::Super)
            && self.is_not_super_user_login(request).await?
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("The super user can only be update by super user"),
            ));
        }

        match self
            .auth_admin_service
            .delete_user(request_header.username.as_str())
            .await
        {
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
