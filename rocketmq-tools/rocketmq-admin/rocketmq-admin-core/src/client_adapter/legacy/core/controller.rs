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

//! Controller admin service models and operations.

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerConfigQueryRequest {
    controller_servers: Vec<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ControllerConfigQueryRequest {
    pub fn try_new(controller_address: impl Into<String>) -> RocketMQResult<Self> {
        let controller_servers = split_controller_addresses(controller_address);
        if controller_servers.is_empty() {
            return Err(ToolsError::validation_error(
                "controllerAddress",
                "controllerAddress must contain at least one address",
            )
            .into());
        }
        Ok(Self {
            controller_servers,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn controller_servers(&self) -> &[CheetahString] {
        &self.controller_servers
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerConfigQueryResult {
    pub controller_configs: HashMap<CheetahString, HashMap<CheetahString, CheetahString>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerConfigUpdateRequest {
    controller_servers: Vec<CheetahString>,
    properties: HashMap<CheetahString, CheetahString>,
    namesrv_addr: Option<String>,
}

impl ControllerConfigUpdateRequest {
    pub fn try_new(
        controller_address: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<Self> {
        let controller_servers = split_controller_addresses(controller_address);
        if controller_servers.is_empty() {
            return Err(ToolsError::validation_error(
                "controllerAddress",
                "controllerAddress must contain at least one address",
            )
            .into());
        }

        let key = trim_required_cheetah("key", key)?;
        let value = trim_required_cheetah("value", value)?;
        let mut properties = HashMap::new();
        properties.insert(key, value);

        Ok(Self {
            controller_servers,
            properties,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn controller_servers(&self) -> &[CheetahString] {
        &self.controller_servers
    }

    pub fn properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.properties
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerMetadataQueryRequest {
    controller_addr: CheetahString,
    namesrv_addr: Option<String>,
}

impl ControllerMetadataQueryRequest {
    pub fn try_new(controller_addr: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            controller_addr: trim_required_cheetah("controllerAddress", controller_addr)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn controller_addr(&self) -> &CheetahString {
        &self.controller_addr
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerMetadataQueryResult {
    pub meta_data: GetMetaDataResponseHeader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerElectMasterRequest {
    controller_addr: CheetahString,
    cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_id: u64,
    namesrv_addr: Option<String>,
}

impl ControllerElectMasterRequest {
    pub fn try_new(
        controller_addr: impl Into<String>,
        cluster_name: impl Into<String>,
        broker_name: impl Into<String>,
        broker_id: i64,
    ) -> RocketMQResult<Self> {
        if broker_id < 0 {
            return Err(ToolsError::validation_error("brokerId", "brokerId must be greater than or equal to 0").into());
        }

        Ok(Self {
            controller_addr: trim_required_cheetah("controllerAddress", controller_addr)?,
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

    pub fn controller_addr(&self) -> &CheetahString {
        &self.controller_addr
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

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerElectMasterResult {
    pub response_header: ElectMasterResponseHeader,
    pub broker_member_group: BrokerMemberGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerMetadataCleanRequest {
    controller_addr: CheetahString,
    broker_name: CheetahString,
    broker_controller_ids_to_clean: Option<CheetahString>,
    cluster_name: Option<CheetahString>,
    clean_living_broker: bool,
    namesrv_addr: Option<String>,
}

impl ControllerMetadataCleanRequest {
    pub fn try_new(
        controller_addr: impl Into<String>,
        broker_name: impl Into<String>,
        broker_controller_ids_to_clean: Option<String>,
        cluster_name: Option<String>,
        clean_living_broker: bool,
    ) -> RocketMQResult<Self> {
        let controller_addr = trim_required_cheetah("controllerAddress", controller_addr)?;
        let broker_name = trim_required_cheetah("brokerName", broker_name)?;
        let cluster_name = trim_optional_cheetah(cluster_name);
        if !clean_living_broker && cluster_name.is_none() {
            return Err(ToolsError::validation_error(
                "clusterName",
                "clusterName must not be empty when cleanLivingBroker is false",
            )
            .into());
        }

        let broker_controller_ids_to_clean =
            normalize_broker_controller_ids(broker_controller_ids_to_clean.as_deref())?;

        Ok(Self {
            controller_addr,
            broker_name,
            broker_controller_ids_to_clean,
            cluster_name,
            clean_living_broker,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn controller_addr(&self) -> &CheetahString {
        &self.controller_addr
    }

    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn broker_controller_ids_to_clean(&self) -> Option<&CheetahString> {
        self.broker_controller_ids_to_clean.as_ref()
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn clean_living_broker(&self) -> bool {
        self.clean_living_broker
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

pub struct ControllerService;

impl ControllerService {
    pub async fn query_controller_config_by_request_with_rpc_hook(
        request: ControllerConfigQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ControllerConfigQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_controller_config_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_controller_config_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ControllerConfigQueryRequest,
    ) -> RocketMQResult<ControllerConfigQueryResult> {
        let controller_configs = admin.get_controller_config(request.controller_servers.clone()).await?;
        Ok(ControllerConfigQueryResult { controller_configs })
    }

    pub async fn update_controller_config_by_request_with_rpc_hook(
        request: ControllerConfigUpdateRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_controller_config_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_controller_config_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ControllerConfigUpdateRequest,
    ) -> RocketMQResult<()> {
        admin
            .update_controller_config(request.properties.clone(), request.controller_servers.clone())
            .await
    }

    pub async fn query_controller_metadata_by_request_with_rpc_hook(
        request: ControllerMetadataQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ControllerMetadataQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_controller_metadata_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_controller_metadata_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ControllerMetadataQueryRequest,
    ) -> RocketMQResult<ControllerMetadataQueryResult> {
        let meta_data = admin.get_controller_meta_data(request.controller_addr.clone()).await?;
        Ok(ControllerMetadataQueryResult { meta_data })
    }

    pub async fn elect_master_by_request_with_rpc_hook(
        request: ControllerElectMasterRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ControllerElectMasterResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::elect_master_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn elect_master_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ControllerElectMasterRequest,
    ) -> RocketMQResult<ControllerElectMasterResult> {
        let (response_header, broker_member_group) = admin
            .elect_master(
                request.controller_addr.clone(),
                request.cluster_name.clone(),
                request.broker_name.clone(),
                Some(request.broker_id),
            )
            .await?;
        Ok(ControllerElectMasterResult {
            response_header,
            broker_member_group,
        })
    }

    pub async fn clean_controller_metadata_by_request_with_rpc_hook(
        request: ControllerMetadataCleanRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::clean_controller_metadata_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn clean_controller_metadata_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ControllerMetadataCleanRequest,
    ) -> RocketMQResult<()> {
        admin
            .clean_controller_broker_data(
                request.controller_addr.clone(),
                request.cluster_name.clone().unwrap_or_default(),
                request.broker_name.clone(),
                request.broker_controller_ids_to_clean.clone(),
                request.clean_living_broker,
            )
            .await
    }
}

fn split_controller_addresses(controller_address: impl Into<String>) -> Vec<CheetahString> {
    controller_address
        .into()
        .split(';')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(CheetahString::from)
        .collect()
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_optional_cheetah(value: Option<String>) -> Option<CheetahString> {
    trim_optional_string(value).map(CheetahString::from)
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

fn normalize_broker_controller_ids(value: Option<&str>) -> RocketMQResult<Option<CheetahString>> {
    let Some(value) = value else {
        return Ok(None);
    };

    let mut ids = Vec::new();
    for id in value.split(';').map(str::trim).filter(|id| !id.is_empty()) {
        id.parse::<i64>().map_err(|_| {
            ToolsError::validation_error(
                "brokerControllerIdsToClean",
                format!("brokerControllerIdsToClean contains invalid id: {id}"),
            )
        })?;
        ids.push(id);
    }

    if ids.is_empty() {
        Ok(None)
    } else {
        Ok(Some(CheetahString::from(ids.join(";"))))
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
