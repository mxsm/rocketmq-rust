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
