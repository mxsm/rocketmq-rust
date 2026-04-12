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

//! Authentication and authorization admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthTarget {
    BrokerAddr(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUserRequest {
    target: AuthTarget,
    username: CheetahString,
    password: CheetahString,
    user_type: CheetahString,
    namesrv_addr: Option<String>,
}

impl CreateUserRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        user_type: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            username: trim_required_cheetah("username", username)?,
            password: trim_required_cheetah("password", password)?,
            user_type: trim_optional_string(user_type)
                .map(CheetahString::from)
                .unwrap_or_default(),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &AuthTarget {
        &self.target
    }

    pub fn username(&self) -> &CheetahString {
        &self.username
    }

    pub fn password(&self) -> &CheetahString {
        &self.password
    }

    pub fn user_type(&self) -> &CheetahString {
        &self.user_type
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateUserRequest {
    target: AuthTarget,
    username: CheetahString,
    password: Option<CheetahString>,
    user_type: Option<CheetahString>,
    user_status: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl UpdateUserRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        username: impl Into<String>,
        password: Option<String>,
        user_type: Option<String>,
        user_status: Option<String>,
    ) -> RocketMQResult<Self> {
        let password = trim_optional_string(password).map(CheetahString::from);
        let user_type = trim_optional_string(user_type).map(CheetahString::from);
        let user_status = trim_optional_string(user_status).map(CheetahString::from);
        if password.is_none() && user_type.is_none() && user_status.is_none() {
            return Err(ToolsError::validation_error(
                "updateField",
                "at least one of password, userType, or userStatus must be provided",
            )
            .into());
        }

        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            username: trim_required_cheetah("username", username)?,
            password,
            user_type,
            user_status,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &AuthTarget {
        &self.target
    }

    pub fn username(&self) -> &CheetahString {
        &self.username
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(|value| value.as_str())
    }

    pub fn user_type(&self) -> Option<&str> {
        self.user_type.as_ref().map(|value| value.as_str())
    }

    pub fn user_status(&self) -> Option<&str> {
        self.user_status.as_ref().map(|value| value.as_str())
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteUserRequest {
    target: AuthTarget,
    username: CheetahString,
    namesrv_addr: Option<String>,
}

impl DeleteUserRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        username: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            username: trim_required_cheetah("username", username)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &AuthTarget {
        &self.target
    }

    pub fn username(&self) -> &CheetahString {
        &self.username
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetUserRequest {
    target: AuthTarget,
    username: CheetahString,
    namesrv_addr: Option<String>,
}

impl GetUserRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        username: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            username: trim_required_cheetah("username", username)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &AuthTarget {
        &self.target
    }

    pub fn username(&self) -> &CheetahString {
        &self.username
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListUsersRequest {
    target: AuthTarget,
    filter: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ListUsersRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        filter: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: target_from_options(broker_addr, cluster_name)?,
            filter: trim_optional_string(filter).map(CheetahString::from),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &AuthTarget {
        &self.target
    }

    pub fn filter(&self) -> Option<&str> {
        self.filter.as_ref().map(|filter| filter.as_str())
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyUsersRequest {
    from_broker: CheetahString,
    to_broker: CheetahString,
    usernames: Option<Vec<CheetahString>>,
    namesrv_addr: Option<String>,
}

impl CopyUsersRequest {
    pub fn try_new(
        from_broker: impl Into<String>,
        to_broker: impl Into<String>,
        usernames: Option<String>,
    ) -> RocketMQResult<Self> {
        let usernames = usernames
            .map(|usernames| {
                usernames
                    .split(',')
                    .map(str::trim)
                    .filter(|username| !username.is_empty())
                    .map(CheetahString::from)
                    .collect::<Vec<_>>()
            })
            .filter(|usernames| !usernames.is_empty());

        Ok(Self {
            from_broker: trim_required_cheetah("fromBroker", from_broker)?,
            to_broker: trim_required_cheetah("toBroker", to_broker)?,
            usernames,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn from_broker(&self) -> &CheetahString {
        &self.from_broker
    }

    pub fn to_broker(&self) -> &CheetahString {
        &self.to_broker
    }

    pub fn usernames(&self) -> Option<&[CheetahString]> {
        self.usernames.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthOperationResult {
    pub broker_addrs: Vec<CheetahString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthOperationFailure {
    pub broker_addr: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetUserResult {
    pub users: Vec<UserInfo>,
    pub failed_broker_addrs: Vec<CheetahString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUsersResult {
    pub users: Vec<UserInfo>,
    pub failed_broker_addrs: Vec<CheetahString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyUsersResult {
    pub copied_usernames: Vec<CheetahString>,
    pub skipped_usernames: Vec<CheetahString>,
    pub failures: Vec<AuthOperationFailure>,
}

pub struct AuthService;

impl AuthService {
    pub async fn create_user_by_request_with_rpc_hook(
        request: CreateUserRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<AuthOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::create_user_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn create_user_with_admin(
        admin: &DefaultMQAdminExt,
        request: &CreateUserRequest,
    ) -> RocketMQResult<AuthOperationResult> {
        let broker_addrs = resolve_master_and_slave_targets(admin, request.target()).await?;
        for broker_addr in &broker_addrs {
            admin
                .create_user(
                    broker_addr.clone(),
                    request.username().clone(),
                    request.password().clone(),
                    request.user_type().clone(),
                )
                .await?;
        }
        Ok(AuthOperationResult { broker_addrs })
    }

    pub async fn update_user_by_request_with_rpc_hook(
        request: UpdateUserRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<AuthOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_user_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_user_with_admin(
        admin: &DefaultMQAdminExt,
        request: &UpdateUserRequest,
    ) -> RocketMQResult<AuthOperationResult> {
        let broker_addrs = resolve_master_targets(admin, request.target()).await?;
        for broker_addr in &broker_addrs {
            admin
                .update_user(
                    broker_addr.clone(),
                    request.username().clone(),
                    request.password.clone().unwrap_or_default(),
                    request.user_type.clone().unwrap_or_default(),
                    request.user_status.clone().unwrap_or_default(),
                )
                .await?;
        }
        Ok(AuthOperationResult { broker_addrs })
    }

    pub async fn delete_user_by_request_with_rpc_hook(
        request: DeleteUserRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<AuthOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::delete_user_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn delete_user_with_admin(
        admin: &DefaultMQAdminExt,
        request: &DeleteUserRequest,
    ) -> RocketMQResult<AuthOperationResult> {
        let broker_addrs = resolve_master_and_slave_targets(admin, request.target()).await?;
        for broker_addr in &broker_addrs {
            admin
                .delete_user(broker_addr.clone(), request.username().clone())
                .await?;
        }
        Ok(AuthOperationResult { broker_addrs })
    }

    pub async fn get_user_by_request_with_rpc_hook(
        request: GetUserRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<GetUserResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::get_user_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn get_user_with_admin(
        admin: &DefaultMQAdminExt,
        request: &GetUserRequest,
    ) -> RocketMQResult<GetUserResult> {
        match request.target() {
            AuthTarget::BrokerAddr(broker_addr) => {
                let user = admin.get_user(broker_addr.clone(), request.username().clone()).await?;
                Ok(GetUserResult {
                    users: user.into_iter().collect(),
                    failed_broker_addrs: Vec::new(),
                })
            }
            AuthTarget::ClusterName(_) => {
                let broker_addrs = resolve_master_targets(admin, request.target()).await?;
                let results = futures::future::join_all(broker_addrs.into_iter().map(|broker_addr| async move {
                    admin
                        .get_user(broker_addr.clone(), request.username().clone())
                        .await
                        .map(|user| (broker_addr.clone(), user))
                        .map_err(|_| broker_addr)
                }))
                .await;

                let mut users = Vec::new();
                let mut failed_broker_addrs = Vec::new();
                for result in results {
                    match result {
                        Ok((_, Some(user))) => users.push(user),
                        Ok((_, None)) => {}
                        Err(broker_addr) => failed_broker_addrs.push(broker_addr),
                    }
                }
                Ok(GetUserResult {
                    users,
                    failed_broker_addrs,
                })
            }
        }
    }

    pub async fn list_users_by_request_with_rpc_hook(
        request: ListUsersRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ListUsersResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::list_users_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn list_users_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ListUsersRequest,
    ) -> RocketMQResult<ListUsersResult> {
        let filter = request.filter.clone().unwrap_or_default();
        match request.target() {
            AuthTarget::BrokerAddr(broker_addr) => {
                let users = admin.list_users(broker_addr.clone(), filter).await?;
                Ok(ListUsersResult {
                    users,
                    failed_broker_addrs: Vec::new(),
                })
            }
            AuthTarget::ClusterName(_) => {
                let broker_addrs = resolve_master_targets(admin, request.target()).await?;
                let results = futures::future::join_all(broker_addrs.into_iter().map(|broker_addr| {
                    let filter = filter.clone();
                    async move {
                        admin
                            .list_users(broker_addr.clone(), filter)
                            .await
                            .map(|users| (broker_addr.clone(), users))
                            .map_err(|_| broker_addr)
                    }
                }))
                .await;

                let mut users = Vec::new();
                let mut failed_broker_addrs = Vec::new();
                for result in results {
                    match result {
                        Ok((_, broker_users)) => users.extend(broker_users),
                        Err(broker_addr) => failed_broker_addrs.push(broker_addr),
                    }
                }
                Ok(ListUsersResult {
                    users,
                    failed_broker_addrs,
                })
            }
        }
    }

    pub async fn copy_users_by_request_with_rpc_hook(
        request: CopyUsersRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<CopyUsersResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::copy_users_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn copy_users_with_admin(
        admin: &DefaultMQAdminExt,
        request: &CopyUsersRequest,
    ) -> RocketMQResult<CopyUsersResult> {
        let mut skipped_usernames = Vec::new();
        let mut failures = Vec::new();
        let user_infos = if let Some(usernames) = request.usernames() {
            let mut user_infos = Vec::new();
            for username in usernames {
                match admin.get_user(request.from_broker().clone(), username.clone()).await {
                    Ok(Some(user_info)) => user_infos.push(user_info),
                    Ok(None) => skipped_usernames.push(username.clone()),
                    Err(error) => failures.push(AuthOperationFailure {
                        broker_addr: request.from_broker().clone(),
                        error: format!("get user {username}: {error}"),
                    }),
                }
            }
            user_infos
        } else {
            admin
                .list_users(request.from_broker().clone(), CheetahString::default())
                .await?
        };

        let mut copied_usernames = Vec::new();
        for user_info in user_infos {
            let Some(username) = user_info.username.clone() else {
                skipped_usernames.push(CheetahString::default());
                continue;
            };

            let copy_result = match admin.get_user(request.to_broker().clone(), username.clone()).await {
                Ok(Some(_)) => {
                    admin
                        .update_user_with_user_info(request.to_broker().clone(), user_info.clone())
                        .await
                }
                Ok(None) | Err(_) => {
                    admin
                        .create_user_with_user_info(request.to_broker().clone(), user_info.clone())
                        .await
                }
            };

            match copy_result {
                Ok(()) => copied_usernames.push(username),
                Err(error) => failures.push(AuthOperationFailure {
                    broker_addr: request.to_broker().clone(),
                    error: format!("copy user {username}: {error}"),
                }),
            }
        }

        Ok(CopyUsersResult {
            copied_usernames,
            skipped_usernames,
            failures,
        })
    }
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

fn target_from_options(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<AuthTarget> {
    let broker_addr = trim_optional_string(broker_addr);
    let cluster_name = trim_optional_string(cluster_name);
    match (broker_addr, cluster_name) {
        (Some(addr), None) => Ok(AuthTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?)),
        (None, Some(cluster)) => Ok(AuthTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?)),
        (None, None) => {
            Err(ToolsError::validation_error("target", "either brokerAddr or clusterName must be provided").into())
        }
        (Some(_), Some(_)) => {
            Err(ToolsError::validation_error("target", "brokerAddr and clusterName cannot be provided together").into())
        }
    }
}

fn builder_with_namesrv(namesrv_addr: Option<&str>) -> AdminBuilder {
    let builder = AdminBuilder::new();
    match namesrv_addr {
        Some(addr) => builder.namesrv_addr(addr),
        None => builder,
    }
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}

async fn resolve_master_targets(admin: &DefaultMQAdminExt, target: &AuthTarget) -> RocketMQResult<Vec<CheetahString>> {
    match target {
        AuthTarget::BrokerAddr(addr) => Ok(vec![addr.clone()]),
        AuthTarget::ClusterName(cluster_name) => {
            let cluster_info = admin.examine_broker_cluster_info().await?;
            BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())
        }
    }
}

async fn resolve_master_and_slave_targets(
    admin: &DefaultMQAdminExt,
    target: &AuthTarget,
) -> RocketMQResult<Vec<CheetahString>> {
    match target {
        AuthTarget::BrokerAddr(addr) => Ok(vec![addr.clone()]),
        AuthTarget::ClusterName(cluster_name) => {
            let cluster_info = admin.examine_broker_cluster_info().await?;
            BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name.as_str())
        }
    }
}
