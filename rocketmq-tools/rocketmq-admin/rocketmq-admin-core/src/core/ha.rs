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

//! HA admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HaStatusTarget {
    BrokerAddr(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaStatusQueryRequest {
    target: HaStatusTarget,
    namesrv_addr: Option<String>,
}

impl HaStatusQueryRequest {
    pub fn try_new(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        Ok(Self {
            target: ha_status_target_from_options(broker_addr, cluster_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &HaStatusTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaStatusEntry {
    pub broker_addr: CheetahString,
    pub runtime_info: HARuntimeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaStatusQueryResult {
    pub entries: Vec<HaStatusEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStateSetTarget {
    BrokerName(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStateSetQueryRequest {
    controller_address: CheetahString,
    target: SyncStateSetTarget,
    namesrv_addr: Option<String>,
}

impl SyncStateSetQueryRequest {
    pub fn try_new(
        controller_address: impl Into<String>,
        broker_name: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            controller_address: trim_required_controller_address(controller_address)?,
            target: sync_state_set_target_from_options(broker_name, cluster_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn controller_address(&self) -> &CheetahString {
        &self.controller_address
    }

    pub fn target(&self) -> &SyncStateSetTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Serialize, Deserialize)]
pub struct SyncStateSetQueryResult {
    pub broker_replicas_info: Option<BrokerReplicasInfo>,
}

pub struct HaService;

impl HaService {
    pub async fn query_ha_status_by_request_with_rpc_hook(
        request: HaStatusQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<HaStatusQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_ha_status_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_ha_status_with_admin(
        admin: &DefaultMQAdminExt,
        request: &HaStatusQueryRequest,
    ) -> RocketMQResult<HaStatusQueryResult> {
        let broker_addrs = match request.target() {
            HaStatusTarget::BrokerAddr(broker_addr) => vec![broker_addr.clone()],
            HaStatusTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("HaService: Failed to get cluster info: {error}"))
                })?;
                BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())?
            }
        };

        let mut entries = Vec::with_capacity(broker_addrs.len());
        for broker_addr in broker_addrs {
            let runtime_info = admin.get_broker_ha_status(broker_addr.clone()).await.map_err(|error| {
                RocketMQError::Internal(format!(
                    "HaService: Failed to get broker HA status from {broker_addr}: {error}"
                ))
            })?;
            entries.push(HaStatusEntry {
                broker_addr,
                runtime_info,
            });
        }

        Ok(HaStatusQueryResult { entries })
    }

    pub async fn query_sync_state_set_by_request_with_rpc_hook(
        request: SyncStateSetQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<SyncStateSetQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_sync_state_set_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_sync_state_set_with_admin(
        admin: &DefaultMQAdminExt,
        request: &SyncStateSetQueryRequest,
    ) -> RocketMQResult<SyncStateSetQueryResult> {
        let brokers = match request.target() {
            SyncStateSetTarget::BrokerName(broker_name) => vec![broker_name.clone()],
            SyncStateSetTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("HaService: Failed to get cluster info: {error}"))
                })?;
                BrokerAddressResolver::fetch_broker_name_by_cluster_name(&cluster_info, cluster_name.as_str())?
                    .into_iter()
                    .map(CheetahString::from_string)
                    .collect()
            }
        };

        if brokers.is_empty() {
            return Ok(SyncStateSetQueryResult {
                broker_replicas_info: None,
            });
        }

        let broker_replicas_info = admin
            .get_in_sync_state_data(request.controller_address.clone(), brokers)
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!("HaService: Failed to get in sync state data: {error}"))
            })?;
        Ok(SyncStateSetQueryResult {
            broker_replicas_info: Some(broker_replicas_info),
        })
    }
}

fn ha_status_target_from_options(
    broker_addr: Option<String>,
    cluster_name: Option<String>,
) -> RocketMQResult<HaStatusTarget> {
    match (trim_optional_string(broker_addr), trim_optional_string(cluster_name)) {
        (Some(broker_addr), None) => Ok(HaStatusTarget::BrokerAddr(CheetahString::from(broker_addr))),
        (None, Some(cluster_name)) => Ok(HaStatusTarget::ClusterName(CheetahString::from(cluster_name))),
        (None, None) => {
            Err(ToolsError::validation_error("target", "either brokerAddr or clusterName must be specified").into())
        }
        (Some(_), Some(_)) => {
            Err(ToolsError::validation_error("target", "brokerAddr and clusterName cannot both be specified").into())
        }
    }
}

fn sync_state_set_target_from_options(
    broker_name: Option<String>,
    cluster_name: Option<String>,
) -> RocketMQResult<SyncStateSetTarget> {
    match (trim_optional_string(broker_name), trim_optional_string(cluster_name)) {
        (Some(broker_name), None) => Ok(SyncStateSetTarget::BrokerName(CheetahString::from(broker_name))),
        (None, Some(cluster_name)) => Ok(SyncStateSetTarget::ClusterName(CheetahString::from(cluster_name))),
        (None, None) => {
            Err(ToolsError::validation_error("target", "either brokerName or clusterName must be specified").into())
        }
        (Some(_), Some(_)) => {
            Err(ToolsError::validation_error("target", "brokerName and clusterName cannot both be specified").into())
        }
    }
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_controller_address(value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let controller_address = value.trim().split(';').next().unwrap_or("").trim();
    if controller_address.is_empty() {
        return Err(ToolsError::validation_error("controllerAddress", "controllerAddress must not be empty").into());
    }
    Ok(CheetahString::from(controller_address))
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
