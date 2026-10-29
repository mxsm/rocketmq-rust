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

//! Exact-target ACL workspace capability; callers own scope confirmation and secret presentation.

use super::DefaultMQAdminExt;
use cheetah_string::CheetahString;

#[derive(Clone, Copy)]
#[cfg(feature = "admin-read")]
pub enum AclWorkspaceReadKind {
    Users,
    Policies,
}

/// Transient ACL documents whose Debug output reveals only a count.
#[cfg(feature = "admin-read")]
pub struct AclWorkspaceDocuments(Vec<serde_json::Value>);
#[cfg(feature = "admin-read")]
impl AclWorkspaceDocuments {
    /// Consumes sensitive data for an explicit management projection.
    pub fn into_values(self) -> Vec<serde_json::Value> {
        self.0
    }
}
#[cfg(feature = "admin-read")]
impl std::fmt::Debug for AclWorkspaceDocuments {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AclWorkspaceDocuments")
            .field("count", &self.0.len())
            .finish()
    }
}

/// Reads retain field presence so a management client can refuse destructive edits to new schemas.
/// Values may contain backend-returned passwords; they must never be logged or persisted.
#[allow(async_fn_in_trait, reason = "Concrete SDK session capability")]
#[cfg(feature = "admin-read")]
pub trait MQAdminAclWorkspaceReadExt: Send {
    /// # Errors
    /// Returns Broker authorization, protocol, session, or transport failures.
    async fn read_acl_documents_at(
        &self,
        address: CheetahString,
        kind: AclWorkspaceReadKind,
    ) -> crate::ClientResult<AclWorkspaceDocuments>;
}
#[cfg(feature = "admin-read")]
impl MQAdminAclWorkspaceReadExt for DefaultMQAdminExt {
    async fn read_acl_documents_at(
        &self,
        address: CheetahString,
        kind: AclWorkspaceReadKind,
    ) -> crate::ClientResult<AclWorkspaceDocuments> {
        self.inner()
            .mq_client_api()?
            .acl_workspace_documents(address, kind, self.inner().remoting_timeout_millis()?)
            .await
            .map(AclWorkspaceDocuments)
    }
}

#[cfg(feature = "admin-mutation")]
/// Structured operations only. None user passwords stay absent on the wire during edits.
pub enum AclWorkspaceWrite {
    CreateUser(rocketmq_protocol::protocol::body::user_info::UserInfo),
    UpdateUser(rocketmq_protocol::protocol::body::user_info::UserInfo),
    DeleteUser {
        username: CheetahString,
    },
    CreatePolicy(rocketmq_protocol::protocol::body::acl_info::AclInfo),
    UpdatePolicy(rocketmq_protocol::protocol::body::acl_info::AclInfo),
    DeleteEntry {
        subject: CheetahString,
        policy_type: CheetahString,
        resource: CheetahString,
    },
}
#[cfg(feature = "admin-mutation")]
#[allow(async_fn_in_trait, reason = "Concrete SDK session capability")]
pub trait MQAdminAclWorkspaceMutationExt: Send {
    /// Applies one operation once to the supplied exact advertised address, without retries.
    ///
    /// # Errors
    /// Returns Broker authorization, validation, session, or transport failures.
    async fn apply_acl_at(&self, address: CheetahString, operation: &AclWorkspaceWrite) -> crate::ClientResult<()>;
}
#[cfg(feature = "admin-mutation")]
impl MQAdminAclWorkspaceMutationExt for DefaultMQAdminExt {
    async fn apply_acl_at(&self, address: CheetahString, operation: &AclWorkspaceWrite) -> crate::ClientResult<()> {
        self.inner()
            .mq_client_api()?
            .acl_workspace_apply(address, operation, self.inner().remoting_timeout_millis()?)
            .await
    }
}
