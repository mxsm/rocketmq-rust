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

use super::*;
#[cfg(feature = "admin-read")]
use crate::admin::AclWorkspaceReadKind;
#[cfg(feature = "admin-mutation")]
use crate::admin::AclWorkspaceWrite;

impl MQClientAPIImpl {
    #[cfg(feature = "admin-read")]
    pub(crate) async fn acl_workspace_documents(
        &self,
        address: CheetahString,
        kind: AclWorkspaceReadKind,
        timeout: u64,
    ) -> ClientResult<Vec<serde_json::Value>> {
        let request = match kind {
            AclWorkspaceReadKind::Users => self.create_request_command(
                RequestCode::AuthListUsers,
                ListUsersRequestHeader {
                    filter: CheetahString::new(),
                },
            ),
            AclWorkspaceReadKind::Policies => self.create_request_command(
                RequestCode::AuthListAcl,
                ListAclRequestHeader {
                    subject_filter: CheetahString::new(),
                    resource_filter: CheetahString::new(),
                },
            ),
        };
        let response = self.acl_workspace_exchange(address, request, timeout).await?;
        response
            .get_body()
            .map_or_else(|| Ok(Vec::new()), |body| decode_admin_json(body.as_ref()))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn acl_workspace_apply(
        &self,
        address: CheetahString,
        operation: &AclWorkspaceWrite,
        timeout: u64,
    ) -> ClientResult<()> {
        let request = match operation {
            AclWorkspaceWrite::CreateUser(user) => {
                let username = user
                    .username
                    .clone()
                    .ok_or_else(|| ClientError::illegal_argument("Username is required"))?;
                let mut header = CreateUserRequestHeader::default();
                header.set_username(username);
                self.create_request_command(RequestCode::AuthCreateUser, header)
                    .set_body(user.encode()?)
            }
            AclWorkspaceWrite::UpdateUser(user) => {
                let username = user
                    .username
                    .clone()
                    .ok_or_else(|| ClientError::illegal_argument("Username is required"))?;
                let mut header = UpdateUserRequestHeader::default();
                header.set_username(username);
                self.create_request_command(RequestCode::AuthUpdateUser, header)
                    .set_body(user.encode()?)
            }
            AclWorkspaceWrite::DeleteUser { username } => {
                let mut header = DeleteUserRequestHeader::default();
                header.set_username(username.clone());
                self.create_request_command(RequestCode::AuthDeleteUser, header)
            }
            AclWorkspaceWrite::CreatePolicy(policy) => {
                let subject = policy
                    .subject
                    .clone()
                    .ok_or_else(|| ClientError::illegal_argument("ACL subject is required"))?;
                self.create_request_command(RequestCode::AuthCreateAcl, CreateAclRequestHeader { subject })
                    .set_body(policy.encode()?)
            }
            AclWorkspaceWrite::UpdatePolicy(policy) => {
                let subject = policy
                    .subject
                    .clone()
                    .ok_or_else(|| ClientError::illegal_argument("ACL subject is required"))?;
                self.create_request_command(RequestCode::AuthUpdateAcl, UpdateAclRequestHeader { subject })
                    .set_body(policy.encode()?)
            }
            AclWorkspaceWrite::DeleteEntry {
                subject,
                policy_type,
                resource,
            } => self.create_request_command(
                RequestCode::AuthDeleteAcl,
                acl_entry_delete_header(subject.clone(), policy_type.clone(), resource.clone())?,
            ),
        };
        self.acl_workspace_exchange(address, request, timeout).await.map(|_| ())
    }

    async fn acl_workspace_exchange(
        &self,
        address: CheetahString,
        request: RemotingCommand,
        timeout: u64,
    ) -> ClientResult<RemotingCommand> {
        let response = match self
            .remoting_client
            .invoke_request(Some(&address), request, timeout)
            .await
        {
            Ok(OutboundRequestResult::Response(response)) => response,
            Ok(OutboundRequestResult::Rejected(rejection)) => {
                return Err(admin_request_error("acl_workspace", RetryInput::Rejected(rejection)))
            }
            Ok(OutboundRequestResult::Contract(contract)) => {
                return Err(admin_request_error("acl_workspace", RetryInput::Contract(contract)))
            }
            Err(error) => return Err(ClientError::from_shared(error.into_shared_error())),
        };
        acl_workspace_response(response)
    }
}

fn acl_workspace_response(response: RemotingCommand) -> ClientResult<RemotingCommand> {
    if response.code() == ResponseCode::Success as i32 {
        Ok(response)
    } else {
        // Preserve the actual response code. Arbitrary Broker remarks may contain ACL material.
        Err(ClientError::broker_operation_failed(
            "acl_workspace",
            response.code(),
            "",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_workspace_real_wire_failures_preserve_codes_without_retaining_sensitive_remarks() {
        for code in [16, 3, 1] {
            let response = RemotingCommand::create_response_command_with_code_remark(code, "private-acl-material");
            let error = acl_workspace_response(response).err().unwrap();
            assert_eq!(error.broker_response_code(), Some(code));
            assert!(error.is(&rocketmq_error::BROKER_OPERATION_FAILED));
            assert!(!format!("{error:?} {error}").contains("private-acl-material"));
            let canonical = error.into_error();
            let diagnostic = canonical.diagnostic_view().unwrap();
            assert!(diagnostic.fields().any(|field| matches!((field.name(), field.value()), ("broker_code", rocketmq_error::ViewValueRef::I64(value)) if value == i64::from(code))));
        }
        assert!(acl_workspace_response(RemotingCommand::create_response_command_with_code(0)).is_ok());
    }
}
