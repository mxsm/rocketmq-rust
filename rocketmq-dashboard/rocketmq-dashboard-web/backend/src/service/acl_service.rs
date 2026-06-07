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
use crate::error::DashboardError;
use crate::model::AclMutationResult;
use crate::model::AclPolicyRequest;
use crate::model::AclPolicyView;
use crate::model::AclQuery;
use crate::model::AclUserUpsertRequest;
use crate::model::AclUserView;
use crate::state::AppState;

pub async fn list_users(state: &AppState, query: AclQuery) -> Result<Vec<AclUserView>, DashboardError> {
    state.admin_client.list_acl_users(query).await
}

pub async fn create_user(state: &AppState, request: AclUserUpsertRequest) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.create_acl_user(request).await
}

pub async fn update_user(
    state: &AppState,
    username: &str,
    request: AclUserUpsertRequest,
) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.update_acl_user(username, request).await
}

pub async fn delete_user(
    state: &AppState,
    username: &str,
    query: AclQuery,
) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.delete_acl_user(username, query).await
}

pub async fn list_policies(state: &AppState, query: AclQuery) -> Result<Vec<AclPolicyView>, DashboardError> {
    state.admin_client.list_acl_policies(query).await
}

pub async fn create_policy(state: &AppState, request: AclPolicyRequest) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.create_acl_policy(request).await
}

pub async fn update_policy(
    state: &AppState,
    subject: &str,
    request: AclPolicyRequest,
) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.update_acl_policy(subject, request).await
}

pub async fn delete_policy(
    state: &AppState,
    subject: &str,
    query: AclQuery,
) -> Result<AclMutationResult, DashboardError> {
    state.admin_client.delete_acl_policy(subject, query).await
}
