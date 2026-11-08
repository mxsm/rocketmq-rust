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
use crate::core::acl_workspace::AclExactWrite;
use rocketmq_client_rust::{AclWorkspaceWrite, MQAdminAclWorkspaceMutationExt as _};
use rocketmq_protocol::protocol::body::{
    acl_info::{AclInfo, PolicyEntryInfo, PolicyInfo},
    user_info::UserInfo,
};

fn wire_operation(request: &AclExactWrite) -> AclWorkspaceWrite {
    match request {
        AclExactWrite::User {
            create,
            username,
            password,
            user_type,
            status,
        } => {
            let user = UserInfo {
                username: Some(username.as_str().into()),
                password: password.as_deref().map(Into::into),
                user_type: Some(user_type.as_str().into()),
                user_status: Some(status.as_str().into()),
            };
            if *create {
                AclWorkspaceWrite::CreateUser(user)
            } else {
                AclWorkspaceWrite::UpdateUser(user)
            }
        }
        AclExactWrite::DeleteUser { username } => AclWorkspaceWrite::DeleteUser {
            username: username.as_str().into(),
        },
        AclExactWrite::Policy {
            create,
            subject,
            policy_type,
            entry,
        } => {
            let acl = AclInfo {
                subject: Some(subject.as_str().into()),
                policies: Some(vec![PolicyInfo {
                    policy_type: Some(policy_type.as_str().into()),
                    entries: Some(vec![PolicyEntryInfo {
                        resource: Some(entry.resource.as_str().into()),
                        actions: Some(entry.actions.iter().map(|value| value.as_str().into()).collect()),
                        source_ips: Some(entry.source_ips.iter().map(|value| value.as_str().into()).collect()),
                        decision: Some(entry.decision.as_str().into()),
                    }]),
                }]),
            };
            if *create {
                AclWorkspaceWrite::CreatePolicy(acl)
            } else {
                AclWorkspaceWrite::UpdatePolicy(acl)
            }
        }
        AclExactWrite::DeleteEntry {
            subject,
            policy_type,
            resource,
        } => AclWorkspaceWrite::DeleteEntry {
            subject: subject.as_str().into(),
            policy_type: policy_type.as_str().into(),
            resource: resource.as_str().into(),
        },
    }
}
impl MutationAdminSession {
    /// Applies exactly one structured operation once to one advertised Broker.
    ///
    /// # Errors
    /// Returns validation, authorization, session, protocol, or transport failures.
    pub async fn apply_acl_at(&mut self, address: &str, request: &AclExactWrite) -> AdminResult<()> {
        request.validate(address)?;
        self.inner.ensure_open()?;
        self.inner
            .inner
            .apply_acl_at(address.into(), &wire_operation(request))
            .await
            .map_err(|source| promote_to_admin_error("acl_workspace_write", source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_user_wire_keeps_omitted_password_and_create_status() {
        let edit = AclExactWrite::User {
            create: false,
            username: "alice".into(),
            password: None,
            user_type: "Normal".into(),
            status: "disable".into(),
        };
        match wire_operation(&edit) {
            AclWorkspaceWrite::UpdateUser(user) => {
                assert!(user.password.is_none());
                assert_eq!(user.user_status.as_deref(), Some("disable"));
            }
            _ => panic!("Wrong wire operation"),
        }
        let create = AclExactWrite::User {
            create: true,
            username: "alice".into(),
            password: Some("secret".into()),
            user_type: "Normal".into(),
            status: "disable".into(),
        };
        match wire_operation(&create) {
            AclWorkspaceWrite::CreateUser(user) => {
                assert_eq!(user.password.as_deref(), Some("secret"));
                assert_eq!(user.user_status.as_deref(), Some("disable"));
            }
            _ => panic!("Wrong wire operation"),
        }
    }
}
