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

//! Closed, address-free infrastructure observation contracts.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub const MAX_HA_BROKER_TARGETS: usize = 64;
pub const MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER: usize = 64;
pub const MAX_HA_CONNECTIONS_PER_BROKER: usize = 64;
pub const MAX_SYNC_BROKERS: usize = 64;
pub const MAX_SYNC_REPLICAS_PER_BROKER: usize = 64;
pub const MAX_CONTROLLER_TARGETS: usize = 32;
pub const MAX_CONTROLLER_PEERS: usize = 32;
pub const MAX_NAMESERVER_TARGETS: usize = 16;
pub const MAX_INFRASTRUCTURE_QUERY_ROWS: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryHaStatusRequest {
    pub cluster: String,
    pub broker_names: Vec<String>,
    pub include_sync_state: bool,
    pub controller_names: Vec<String>,
}

impl QueryHaStatusRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        broker_names: impl IntoIterator<Item = String>,
        include_sync_state: bool,
        controller_names: impl IntoIterator<Item = String>,
    ) -> AdminResult<Self> {
        let broker_names = normalize_broker_selectors(broker_names)?;
        let controller_names = normalize_controller_selectors(controller_names)?;
        if !include_sync_state && !controller_names.is_empty() {
            return Err(AdminError::invalid_argument(
                "controller_names",
                "controller_names require include_sync_state=true",
            ));
        }
        Ok(Self {
            cluster: logical_identifier("cluster", required("cluster", cluster)?)?,
            broker_names,
            include_sync_state,
            controller_names,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryControllerMetadataRequest {
    pub cluster: String,
    pub controller_names: Vec<String>,
}

impl QueryControllerMetadataRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        controller_names: impl IntoIterator<Item = String>,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster: logical_identifier("cluster", required("cluster", cluster)?)?,
            controller_names: normalize_controller_selectors(controller_names)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryNameserverConfigSummaryRequest {
    pub cluster: String,
}

impl QueryNameserverConfigSummaryRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: logical_identifier("cluster", required("cluster", cluster)?)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalBrokerInstance {
    pub broker_name: String,
    pub broker_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaConnectionObservation {
    pub replica: LogicalBrokerInstance,
    pub slave_ack_offset: u64,
    pub diff: i64,
    pub in_sync: bool,
    pub transferred_bytes_per_second: u64,
    pub transfer_from_where: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerHaObservation {
    pub broker_name: String,
    pub broker_id: u64,
    pub master_commit_log_max_offset: u64,
    pub in_sync_slave_count: u32,
    pub pending_group_transfer_request_count: u64,
    pub pending_group_transfer_oldest_wait_millis: u64,
    pub group_transfer_ack_notify_count: u64,
    pub connections: Vec<HaConnectionObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerSyncStateObservation {
    pub broker_name: String,
    pub master_broker_id: u64,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub in_sync_replicas: Vec<LogicalBrokerInstance>,
    pub not_in_sync_replicas: Vec<LogicalBrokerInstance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerSyncStateObservation {
    pub controller_name: String,
    pub brokers: Vec<BrokerSyncStateObservation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryHaStatusResult {
    pub brokers: Vec<BrokerHaObservation>,
    pub controller_sync_states: Vec<ControllerSyncStateObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerMetadataObservation {
    pub controller_name: String,
    pub group: Option<String>,
    pub leader_id: Option<String>,
    pub is_leader: Option<bool>,
    pub peer_count: Option<usize>,
    pub last_log_index: Option<u64>,
    pub committed_log_index: Option<u64>,
    pub applied_log_index: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryControllerMetadataResult {
    pub controllers: Vec<ControllerMetadataObservation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameserverConfigValues {
    pub cluster_test: Option<bool>,
    pub order_message_enable: Option<bool>,
    pub return_order_topic_config_to_broker: Option<bool>,
    pub client_request_thread_pool_nums: Option<i32>,
    pub client_request_thread_pool_queue_capacity: Option<i32>,
    pub scan_not_active_broker_interval_ms: Option<u64>,
    pub unregister_broker_queue_capacity: Option<i32>,
    pub support_acting_master: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameserverConfigObservation {
    pub nameserver_name: String,
    pub values: NameserverConfigValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameserverConfigDifferenceField {
    ClusterTest,
    OrderMessageEnable,
    ReturnOrderTopicConfigToBroker,
    ClientRequestThreadPoolNums,
    ClientRequestThreadPoolQueueCapacity,
    ScanNotActiveBrokerIntervalMs,
    UnregisterBrokerQueueCapacity,
    SupportActingMaster,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryNameserverConfigSummaryResult {
    pub nameservers: Vec<NameserverConfigObservation>,
    pub inconsistent_fields: Vec<NameserverConfigDifferenceField>,
}

pub trait InfrastructureObservationQueryAdmin: Send {
    fn query_ha_status<'a>(
        &'a mut self,
        request: &'a QueryHaStatusRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryHaStatusResult>>;

    fn query_controller_metadata<'a>(
        &'a mut self,
        request: &'a QueryControllerMetadataRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryControllerMetadataResult>>;

    fn query_nameserver_config_summary<'a>(
        &'a mut self,
        request: &'a QueryNameserverConfigSummaryRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryNameserverConfigSummaryResult>>;
}

fn normalize_broker_selectors(selectors: impl IntoIterator<Item = String>) -> AdminResult<Vec<String>> {
    let selectors = selectors.into_iter().collect::<Vec<_>>();
    if selectors.len() > MAX_HA_BROKER_TARGETS {
        return Err(AdminError::invalid_argument(
            "broker_names",
            format!("must contain at most {MAX_HA_BROKER_TARGETS} logical Brokers"),
        ));
    }
    let mut normalized = selectors
        .into_iter()
        .map(|selector| logical_identifier("broker_names", selector))
        .collect::<AdminResult<Vec<_>>>()?;
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn normalize_controller_selectors(selectors: impl IntoIterator<Item = String>) -> AdminResult<Vec<String>> {
    let selectors = selectors.into_iter().collect::<Vec<_>>();
    if selectors.len() > MAX_CONTROLLER_TARGETS {
        return Err(AdminError::invalid_argument(
            "controller_names",
            format!("must contain at most {MAX_CONTROLLER_TARGETS} logical Controllers"),
        ));
    }
    let normalized = selectors
        .into_iter()
        .map(|selector| logical_identifier("controller_names", selector))
        .collect::<AdminResult<Vec<_>>>()?;
    let unique = normalized.iter().collect::<BTreeSet<_>>();
    if unique.len() != normalized.len() {
        return Err(AdminError::invalid_argument(
            "controller_names",
            "duplicate logical Controller aliases are not allowed",
        ));
    }
    let mut normalized = normalized;
    normalized.sort();
    Ok(normalized)
}

fn logical_identifier(field: &'static str, value: impl Into<String>) -> AdminResult<String> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty()
        || value.len() > 100
        || value.parse::<std::net::IpAddr>().is_ok()
        || value.parse::<std::net::SocketAddr>().is_ok()
        || value.contains([':', '/', '\\', '@', '=', '&', '?'])
        || value.chars().any(char::is_control)
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        Err(AdminError::invalid_argument(
            field,
            "must be a logical identifier of at most 100 bytes",
        ))
    } else {
        Ok(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_contracts_are_bounded_and_relationship_checked() {
        let request = QueryHaStatusRequest::try_new(
            " cluster-a ",
            ["broker-b".to_string(), "broker-a".to_string(), "broker-b".to_string()],
            true,
            ["controller-b".to_string(), "controller-a".to_string()],
        )
        .unwrap();
        assert_eq!(request.broker_names, ["broker-a", "broker-b"]);
        assert_eq!(request.controller_names, ["controller-a", "controller-b"]);
        assert!(QueryHaStatusRequest::try_new("cluster-a", Vec::new(), false, ["controller-a".to_string()]).is_err());
        assert!(QueryControllerMetadataRequest::try_new(
            "cluster-a",
            ["controller-a".to_string(), "controller-a".to_string()]
        )
        .is_err());
        assert!(QueryControllerMetadataRequest::try_new(
            "cluster-a",
            (0..=MAX_CONTROLLER_TARGETS).map(|index| format!("controller-{index}"))
        )
        .is_err());
        assert!(QueryHaStatusRequest::try_new(
            "cluster-a",
            (0..=MAX_HA_BROKER_TARGETS).map(|index| format!("broker-{index}")),
            false,
            Vec::new()
        )
        .is_err());
    }

    #[test]
    fn nameserver_difference_fields_have_fixed_wire_order() {
        let fields = [
            NameserverConfigDifferenceField::ClusterTest,
            NameserverConfigDifferenceField::OrderMessageEnable,
            NameserverConfigDifferenceField::ReturnOrderTopicConfigToBroker,
            NameserverConfigDifferenceField::ClientRequestThreadPoolNums,
            NameserverConfigDifferenceField::ClientRequestThreadPoolQueueCapacity,
            NameserverConfigDifferenceField::ScanNotActiveBrokerIntervalMs,
            NameserverConfigDifferenceField::UnregisterBrokerQueueCapacity,
            NameserverConfigDifferenceField::SupportActingMaster,
        ];
        assert_eq!(
            serde_json::to_value(fields).unwrap(),
            serde_json::json!([
                "cluster_test",
                "order_message_enable",
                "return_order_topic_config_to_broker",
                "client_request_thread_pool_nums",
                "client_request_thread_pool_queue_capacity",
                "scan_not_active_broker_interval_ms",
                "unregister_broker_queue_capacity",
                "support_acting_master"
            ])
        );
    }
}
