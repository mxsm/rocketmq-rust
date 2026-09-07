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

use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::delete_acl_request_header::DeleteAclRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::auth::auth_admin_service::AuthAdminService;

#[derive(Clone)]
pub struct DeleteAclRequestHandler {
    auth_admin_service: Arc<AuthAdminService>,
}

impl DeleteAclRequestHandler {
    pub fn new(auth_admin_service: Arc<AuthAdminService>) -> Self {
        Self { auth_admin_service }
    }

    pub async fn delete_acl(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<DeleteAclRequestHeader>()?;
        let response = RemotingCommand::create_java_default_error_response_command();

        if request_header.subject.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The subject is blank"),
            ));
        }

        match self
            .auth_admin_service
            .delete_acl(
                request_header.subject.as_str(),
                request_header
                    .policy_type
                    .as_ref()
                    .map(|policy_type| policy_type.as_str()),
                request_header.resource.as_ref().map(|resource| resource.as_str()),
            )
            .await
        {
            Ok(()) => Ok(Some(RemotingCommand::create_success_response_command())),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    super::map_auth_admin_error_response(response, error)
}
