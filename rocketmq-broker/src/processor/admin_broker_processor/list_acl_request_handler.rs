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
use rocketmq_protocol::protocol::header::list_acl_request_header::ListAclRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;

use crate::auth::auth_admin_service::AuthAdminService;

#[derive(Clone)]
pub struct ListAclRequestHandler {
    auth_admin_service: Arc<AuthAdminService>,
}

impl ListAclRequestHandler {
    pub fn new(auth_admin_service: Arc<AuthAdminService>) -> Self {
        Self { auth_admin_service }
    }

    pub async fn list_acl(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ListAclRequestHeader>()?;
        let response = RemotingCommand::create_java_default_error_response_command();

        match self
            .auth_admin_service
            .list_acls(
                non_empty(request_header.subject_filter.as_str()),
                non_empty(request_header.resource_filter.as_str()),
            )
            .await
        {
            Ok(acls) => {
                let success = RemotingCommand::create_success_response_command();
                Ok(Some(if acls.is_empty() {
                    success
                } else {
                    success.set_body(acls.encode()?)
                }))
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
    super::map_auth_admin_error_response(response, error)
}
