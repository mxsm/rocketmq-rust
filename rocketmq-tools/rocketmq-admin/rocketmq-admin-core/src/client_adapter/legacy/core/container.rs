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

//! Broker container admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerAddBrokerRequest {
    broker_container_addr: CheetahString,
    broker_config_path: CheetahString,
    namesrv_addr: Option<String>,
}

impl ContainerAddBrokerRequest {
    pub fn try_new(
        broker_container_addr: impl Into<String>,
        broker_config_path: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            broker_container_addr: trim_required_cheetah("brokerContainerAddr", broker_container_addr)?,
            broker_config_path: trim_required_cheetah("brokerConfigPath", broker_config_path)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_container_addr(&self) -> &CheetahString {
        &self.broker_container_addr
    }

    pub fn broker_config_path(&self) -> &CheetahString {
        &self.broker_config_path
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerRemoveBrokerRequest {
    broker_container_addr: CheetahString,
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_id: u64,
    namesrv_addr: Option<String>,
}

impl ContainerRemoveBrokerRequest {
    pub fn try_new(
        broker_container_addr: impl Into<String>,
        cluster_name: impl Into<String>,
        broker_name: impl Into<String>,
        broker_id: i64,
    ) -> RocketMQResult<Self> {
        if broker_id < 0 {
            return Err(ToolsError::validation_error("brokerId", "brokerId must be greater than or equal to 0").into());
        }

        Ok(Self {
            broker_container_addr: trim_required_cheetah("brokerContainerAddr", broker_container_addr)?,
            cluster_name: trim_required_cheetah("clusterName", cluster_name)?,
            broker_name: trim_required_cheetah("brokerName", broker_name)?,
            broker_id: broker_id as u64,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_container_addr(&self) -> &CheetahString {
        &self.broker_container_addr
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
    }

    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn broker_id(&self) -> u64 {
        self.broker_id
    }

    pub fn broker_identity(&self) -> String {
        format!("{}:{}:{}", self.cluster_name, self.broker_name, self.broker_id)
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerOperationKind {
    AddBroker,
    RemoveBroker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerOperationResult {
    pub operation: ContainerOperationKind,
    pub broker_container_addr: CheetahString,
    pub target: CheetahString,
}

pub struct ContainerService;

impl ContainerService {
    pub async fn add_broker_by_request_with_rpc_hook(
        request: ContainerAddBrokerRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ContainerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::add_broker_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn add_broker_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ContainerAddBrokerRequest,
    ) -> RocketMQResult<ContainerOperationResult> {
        admin
            .add_broker_to_container(
                request.broker_container_addr.clone(),
                request.broker_config_path.clone(),
            )
            .await?;
        Ok(ContainerOperationResult {
            operation: ContainerOperationKind::AddBroker,
            broker_container_addr: request.broker_container_addr.clone(),
            target: request.broker_config_path.clone(),
        })
    }

    pub async fn remove_broker_by_request_with_rpc_hook(
        request: ContainerRemoveBrokerRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ContainerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::remove_broker_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn remove_broker_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ContainerRemoveBrokerRequest,
    ) -> RocketMQResult<ContainerOperationResult> {
        admin
            .remove_broker_from_container(
                request.broker_container_addr.clone(),
                request.cluster_name.clone(),
                request.broker_name.clone(),
                request.broker_id,
            )
            .await?;
        Ok(ContainerOperationResult {
            operation: ContainerOperationKind::RemoveBroker,
            broker_container_addr: request.broker_container_addr.clone(),
            target: CheetahString::from(request.broker_identity()),
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
