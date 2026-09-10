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

use super::{admin::ManagedAclAdmin, types::*};
use crate::error::{DashboardError, DashboardResult};
use crate::nameserver::NameServerRuntimeState;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::dashboard::{
    DashboardAclQuery, DashboardAclUser, DashboardAclUserMutationRequest, DashboardAdmin, DashboardBrokerInfo,
    TargetSelector,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct AclManager {
    runtime: Arc<NameServerRuntimeState>,
    pub(super) session: Arc<Mutex<Option<ManagedAclAdmin>>>,
}
impl AclManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            session: Arc::new(Mutex::new(None)),
        }
    }
    pub(crate) async fn shutdown(&self) {
        if let Some(mut admin) = self.session.lock().await.take() {
            admin.shutdown().await;
        }
    }
    pub(super) async fn ensure(&self, slot: &mut Option<ManagedAclAdmin>) -> DashboardResult<()> {
        if slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(self.runtime.generation()))
        {
            if let Some(mut old) = slot.take() {
                old.shutdown().await;
            }
            *slot = Some(ManagedAclAdmin::connect(&self.runtime).await?);
        }
        Ok(())
    }
    pub(crate) async fn list_users(&self, scope: AclScope) -> DashboardResult<Vec<AclUser>> {
        scope.validate()?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        validate_scope(&session.admin, &scope).await?;
        list_users(&session.admin, &scope).await
    }
    pub(crate) async fn change_user(
        &self,
        request: AclUserChange,
        operation: AclUserOperation,
    ) -> DashboardResult<AclUserResult> {
        request.to_core(operation)?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        change_user(&session.admin, request, operation).await
    }
    pub(crate) async fn delete_user(&self, request: AclUserDelete) -> DashboardResult<AclUserResult> {
        request.scope.validate()?;
        validate_username(&request.username)?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        validate_scope(&session.admin, &request.scope).await?;
        session
            .admin
            .delete_user(&request.scope.selector(), &request.username)
            .await?;
        Ok(read_user_result(
            &session.admin,
            request.scope,
            request.username,
            AclUserOperation::Delete,
        )
        .await)
    }
}

pub(super) trait AclAccess {
    async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>>;
    async fn users(&self, query: &DashboardAclQuery) -> DashboardResult<Vec<DashboardAclUser>>;
    async fn create_user(&self, request: &DashboardAclUserMutationRequest) -> DashboardResult<()>;
    async fn update_user(&self, request: &DashboardAclUserMutationRequest) -> DashboardResult<()>;
    async fn delete_user(&self, selector: &TargetSelector, username: &str) -> DashboardResult<()>;
}
impl AclAccess for AdminSession {
    async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>> {
        Ok(self.dashboard_list_brokers().await?.items)
    }
    async fn users(&self, query: &DashboardAclQuery) -> DashboardResult<Vec<DashboardAclUser>> {
        Ok(self.dashboard_list_acl_users(query).await?)
    }
    async fn create_user(&self, request: &DashboardAclUserMutationRequest) -> DashboardResult<()> {
        self.dashboard_create_acl_user(request).await?;
        Ok(())
    }
    async fn update_user(&self, request: &DashboardAclUserMutationRequest) -> DashboardResult<()> {
        self.dashboard_update_acl_user(request).await?;
        Ok(())
    }
    async fn delete_user(&self, selector: &TargetSelector, username: &str) -> DashboardResult<()> {
        self.dashboard_delete_acl_user(selector, username).await?;
        Ok(())
    }
}
pub(super) async fn validate_scope(admin: &impl AclAccess, scope: &AclScope) -> DashboardResult<()> {
    scope.validate()?;
    if !admin.brokers().await?.iter().any(|broker| {
        broker.broker_id == 0
            && broker.cluster_name == scope.cluster_name
            && broker.broker_name == scope.broker_name
            && broker.address == scope.broker_addr
    }) {
        return Err(DashboardError::Validation(
            "The selected ACL master Broker is no longer in the current cluster.".into(),
        ));
    }
    Ok(())
}
pub(super) async fn list_users(admin: &impl AclAccess, scope: &AclScope) -> DashboardResult<Vec<AclUser>> {
    let users = admin
        .users(&DashboardAclQuery {
            selector: scope.selector(),
            ..Default::default()
        })
        .await?;
    if users
        .iter()
        .any(|user| user.broker_name != scope.broker_name || user.broker_addr != scope.broker_addr)
    {
        return Err(DashboardError::Validation(
            "ACL Broker identity changed during the query.".into(),
        ));
    }
    Ok(users
        .into_iter()
        .map(|user| AclUser {
            username: user.username,
            user_type: user.user_type,
            user_status: user.user_status,
        })
        .collect())
}
pub(super) async fn change_user(
    admin: &impl AclAccess,
    request: AclUserChange,
    operation: AclUserOperation,
) -> DashboardResult<AclUserResult> {
    let core = request.to_core(operation)?;
    validate_scope(admin, &request.scope).await?;
    match operation {
        AclUserOperation::Create => admin.create_user(&core).await?,
        AclUserOperation::Update => admin.update_user(&core).await?,
        AclUserOperation::Delete => return Err(DashboardError::Internal("User change cannot represent deletion")),
    }
    Ok(read_user_result(admin, request.scope, request.username, operation).await)
}
async fn read_user_result(
    admin: &impl AclAccess,
    scope: AclScope,
    username: String,
    operation: AclUserOperation,
) -> AclUserResult {
    let (users, read_back_error) = match list_users(admin, &scope).await {
        Ok(users) => (Some(users), None),
        Err(_) => (
            None,
            Some(
                "ACL write was acknowledged, but the user list could not be read back. Refresh before another operation.",
            ),
        ),
    };
    AclUserResult {
        scope,
        username,
        operation,
        success: true,
        users,
        read_back_error,
    }
}
