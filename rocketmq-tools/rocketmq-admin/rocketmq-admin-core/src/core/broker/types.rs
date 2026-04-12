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

//! Broker-related request and result models.

use std::collections::BTreeMap;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use regex::Regex;
use rocketmq_common::common::message::MessageConst;
use serde::Deserialize;
use serde::Serialize;

use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

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

pub(super) fn validate_config_key(key: &str) -> RocketMQResult<()> {
    if key.is_empty() {
        return Err(ToolsError::validation_error("key", "config key must not be empty").into());
    }
    if key.contains('=') {
        return Err(
            ToolsError::validation_error("key", format!("invalid config key '{key}', '=' is not allowed")).into(),
        );
    }
    if key.chars().any(char::is_whitespace) {
        return Err(ToolsError::validation_error(
            "key",
            format!("invalid config key '{key}', whitespace is not allowed"),
        )
        .into());
    }
    Ok(())
}

pub(super) fn validate_update_value(key: &str, new_value: &str, old_value: Option<&str>) -> RocketMQResult<()> {
    if new_value.trim().is_empty() {
        return Err(
            ToolsError::validation_error("value", format!("config value for key '{key}' must not be empty")).into(),
        );
    }
    if new_value.contains('\n') || new_value.contains('\r') {
        return Err(ToolsError::validation_error(
            "value",
            format!("config value for key '{key}' must not contain line breaks"),
        )
        .into());
    }

    if let Some(old_value) = old_value {
        if parse_bool(old_value).is_some() && parse_bool(new_value).is_none() {
            return Err(ToolsError::validation_error(
                "value",
                format!("config key '{key}' expects boolean value, old='{old_value}', new='{new_value}'"),
            )
            .into());
        }
        if old_value.parse::<i64>().is_ok() && new_value.parse::<i64>().is_err() {
            return Err(ToolsError::validation_error(
                "value",
                format!("config key '{key}' expects integer, old='{old_value}', new='{new_value}'"),
            )
            .into());
        }
        if old_value.parse::<f64>().is_ok() && new_value.parse::<f64>().is_err() {
            return Err(ToolsError::validation_error(
                "value",
                format!("config key '{key}' expects numeric value, old='{old_value}', new='{new_value}'"),
            )
            .into());
        }
    }

    Ok(())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerTarget {
    BrokerAddr(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerOptionalTarget {
    broker_addr: Option<CheetahString>,
    cluster_name: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl BrokerOptionalTarget {
    pub fn new(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr)
            .map(|addr| trim_required_cheetah("brokerAddr", addr))
            .transpose()?;
        let cluster_name = trim_optional_string(cluster_name)
            .map(|cluster| trim_required_cheetah("clusterName", cluster))
            .transpose()?;

        Ok(Self {
            broker_addr,
            cluster_name,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerBooleanOperationResult {
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigQueryRequest {
    target: BrokerTarget,
    key_pattern: Option<String>,
    namesrv_addr: Option<String>,
}

impl BrokerConfigQueryRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        key_pattern: Option<String>,
    ) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let key_pattern = trim_optional_string(key_pattern);

        if let Some(pattern) = &key_pattern {
            Regex::new(pattern).map_err(|error| {
                ToolsError::validation_error("keyPattern", format!("invalid key regex pattern '{pattern}': {error}"))
            })?;
        }

        let target = match (broker_addr, cluster_name) {
            (Some(addr), None) => BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?),
            (None, Some(cluster)) => BrokerTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        Ok(Self {
            target,
            key_pattern,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn key_pattern(&self) -> Option<&str> {
        self.key_pattern.as_deref()
    }

    pub fn key_pattern_regex(&self) -> RocketMQResult<Option<Regex>> {
        self.key_pattern()
            .map(|pattern| {
                Regex::new(pattern).map_err(|error| {
                    ToolsError::validation_error(
                        "keyPattern",
                        format!("invalid key regex pattern '{pattern}': {error}"),
                    )
                    .into()
                })
            })
            .transpose()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerConfigSectionTarget {
    Broker(CheetahString),
    Master(CheetahString),
    Slave {
        master_addr: CheetahString,
        slave_addr: CheetahString,
    },
    NoMaster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigEntry {
    pub key: CheetahString,
    pub value: CheetahString,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigSection {
    pub target: BrokerConfigSectionTarget,
    pub entries: Vec<BrokerConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigQueryResult {
    pub sections: Vec<BrokerConfigSection>,
    pub key_pattern: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigUpdateRequest {
    target: BrokerTarget,
    update_entries: BTreeMap<CheetahString, CheetahString>,
    namesrv_addr: Option<String>,
    rollback_enabled: bool,
}

impl BrokerConfigUpdateRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        entries: BTreeMap<String, String>,
    ) -> RocketMQResult<Self> {
        if entries.is_empty() {
            return Err(ToolsError::validation_error("entries", "at least one config entry must be provided").into());
        }

        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let target = match (broker_addr, cluster_name) {
            (Some(addr), None) => BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?),
            (None, Some(cluster)) => BrokerTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        let mut update_entries = BTreeMap::new();
        for (key, value) in entries {
            let key = key.trim();
            let value = value.trim();
            validate_config_key(key)?;
            validate_update_value(key, value, None)?;
            update_entries.insert(CheetahString::from(key), CheetahString::from(value));
        }

        Ok(Self {
            target,
            update_entries,
            namesrv_addr: None,
            rollback_enabled: true,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn with_rollback_enabled(mut self, rollback_enabled: bool) -> Self {
        self.rollback_enabled = rollback_enabled;
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn update_entries(&self) -> &BTreeMap<CheetahString, CheetahString> {
        &self.update_entries
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn rollback_enabled(&self) -> bool {
        self.rollback_enabled
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigChange {
    pub key: CheetahString,
    pub old_value: Option<CheetahString>,
    pub new_value: CheetahString,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigUpdatePlan {
    pub broker_addr: CheetahString,
    pub changes: Vec<BrokerConfigChange>,
}

impl BrokerConfigUpdatePlan {
    pub fn update_properties(&self) -> HashMap<CheetahString, CheetahString> {
        self.changes
            .iter()
            .map(|change| (change.key.clone(), change.new_value.clone()))
            .collect()
    }

    pub fn rollback_properties(&self) -> HashMap<CheetahString, CheetahString> {
        self.changes
            .iter()
            .filter_map(|change| {
                change
                    .old_value
                    .as_ref()
                    .map(|old_value| (change.key.clone(), old_value.clone()))
            })
            .collect()
    }

    pub fn non_rollbackable_keys(&self) -> Vec<CheetahString> {
        self.changes
            .iter()
            .filter(|change| change.old_value.is_none())
            .map(|change| change.key.clone())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigUpdatePlanResult {
    pub plans: Vec<BrokerConfigUpdatePlan>,
}

impl BrokerConfigUpdatePlanResult {
    pub fn changed_broker_count(&self) -> usize {
        self.plans.iter().filter(|plan| !plan.changes.is_empty()).count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedBrokerConfigUpdate {
    pub broker_addr: CheetahString,
    pub rollback_properties: HashMap<CheetahString, CheetahString>,
    pub non_rollbackable_keys: Vec<CheetahString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigUpdateApplyResult {
    pub applied_updates: Vec<AppliedBrokerConfigUpdate>,
    pub skipped_brokers: Vec<CheetahString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRuntimeStatsQueryRequest {
    target: BrokerTarget,
    namesrv_addr: Option<String>,
}

impl BrokerRuntimeStatsQueryRequest {
    pub fn try_new(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let target = match (broker_addr, cluster_name) {
            (Some(addr), None) => BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?),
            (None, Some(cluster)) => BrokerTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        Ok(Self {
            target,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRuntimeStatsEntry {
    pub key: CheetahString,
    pub value: CheetahString,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRuntimeStatsSection {
    pub broker_addr: CheetahString,
    pub entries: Vec<BrokerRuntimeStatsEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRuntimeStatsFailure {
    pub broker_addr: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRuntimeStatsResult {
    pub sections: Vec<BrokerRuntimeStatsSection>,
    pub failures: Vec<BrokerRuntimeStatsFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConsumeStatsQueryRequest {
    broker_addr: CheetahString,
    timeout_millis: u64,
    diff_level: i64,
    is_order: bool,
    namesrv_addr: Option<String>,
}

impl BrokerConsumeStatsQueryRequest {
    pub fn try_new(
        broker_addr: impl Into<String>,
        timeout_millis: u64,
        diff_level: i64,
        is_order: bool,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            broker_addr: trim_required_cheetah("brokerAddr", broker_addr)?,
            timeout_millis,
            diff_level,
            is_order,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub fn diff_level(&self) -> i64 {
        self.diff_level
    }

    pub fn is_order(&self) -> bool {
        self.is_order
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConsumeStatsRow {
    pub topic: CheetahString,
    pub group: CheetahString,
    pub broker_name: CheetahString,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff: i64,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConsumeStatsResult {
    pub broker_addr: Option<CheetahString>,
    pub total_diff: i64,
    pub total_inflight_diff: i64,
    pub rows: Vec<BrokerConsumeStatsRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetMasterFlushOffsetRequest {
    broker_addr: CheetahString,
    master_flush_offset: u64,
    namesrv_addr: Option<String>,
}

impl ResetMasterFlushOffsetRequest {
    pub fn try_new(broker_addr: Option<String>, offset: Option<i64>) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr)
            .ok_or_else(|| ToolsError::validation_error("brokerAddr", "brokerAddr must not be empty"))?;
        let offset = offset.ok_or_else(|| ToolsError::validation_error("offset", "offset is required"))?;
        if offset < 0 {
            return Err(ToolsError::validation_error("offset", "offset must not be negative").into());
        }

        Ok(Self {
            broker_addr: trim_required_cheetah("brokerAddr", broker_addr)?,
            master_flush_offset: offset as u64,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    pub fn master_flush_offset(&self) -> u64 {
        self.master_flush_offset
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchTimerEngineRequest {
    target: BrokerTarget,
    engine_type: CheetahString,
    engine_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl SwitchTimerEngineRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        engine_type: impl Into<String>,
    ) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let target = match (broker_addr, cluster_name) {
            (Some(addr), None) => BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?),
            (None, Some(cluster)) => BrokerTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        let engine_type = engine_type.into();
        let engine_type = engine_type.trim();
        let engine_name = match engine_type {
            MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE => "ROCKSDB_TIMELINE",
            MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL => "FILE_TIME_WHEEL",
            _ => {
                return Err(ToolsError::validation_error("engineType", "engineType must be R or F").into());
            }
        };

        Ok(Self {
            target,
            engine_type: CheetahString::from(engine_type),
            engine_name: CheetahString::from(engine_name),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn engine_type(&self) -> &CheetahString {
        &self.engine_type
    }

    pub fn engine_name(&self) -> &CheetahString {
        &self.engine_name
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerOperationFailure {
    pub broker_addr: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerOperationResult {
    pub broker_addrs: Vec<CheetahString>,
    pub failures: Vec<BrokerOperationFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdDataFlowCtrGroupConfigUpdateRequest {
    target: BrokerTarget,
    consumer_group: CheetahString,
    threshold: CheetahString,
    namesrv_addr: Option<String>,
}

impl ColdDataFlowCtrGroupConfigUpdateRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        consumer_group: impl Into<String>,
        threshold: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: broker_target_from_options(broker_addr, cluster_name)?,
            consumer_group: trim_required_cheetah("consumerGroup", consumer_group)?,
            threshold: trim_required_cheetah("threshold", threshold)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn threshold(&self) -> &CheetahString {
        &self.threshold
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdDataFlowCtrGroupConfigRemoveRequest {
    target: BrokerTarget,
    consumer_group: CheetahString,
    namesrv_addr: Option<String>,
}

impl ColdDataFlowCtrGroupConfigRemoveRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        consumer_group: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            target: broker_target_from_options(broker_addr, cluster_name)?,
            consumer_group: trim_required_cheetah("consumerGroup", consumer_group)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdDataFlowCtrInfoQueryRequest {
    target: BrokerTarget,
    namesrv_addr: Option<String>,
}

impl ColdDataFlowCtrInfoQueryRequest {
    pub fn try_new(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        Ok(Self {
            target: broker_target_from_options(broker_addr, cluster_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdDataFlowCtrInfoSection {
    pub target: BrokerConfigSectionTarget,
    pub broker_addr: CheetahString,
    pub raw_info: CheetahString,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdDataFlowCtrInfoQueryResult {
    pub sections: Vec<ColdDataFlowCtrInfoSection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerEpochQueryTarget {
    BrokerName(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEpochQueryRequest {
    target: BrokerEpochQueryTarget,
    namesrv_addr: Option<String>,
}

impl BrokerEpochQueryRequest {
    pub fn try_new(broker_name: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        let broker_name = trim_optional_string(broker_name);
        let cluster_name = trim_optional_string(cluster_name);
        let target = match (broker_name, cluster_name) {
            (Some(broker), None) => BrokerEpochQueryTarget::BrokerName(trim_required_cheetah("brokerName", broker)?),
            (None, Some(cluster)) => {
                BrokerEpochQueryTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?)
            }
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerName or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerName and clusterName cannot be provided together",
                )
                .into());
            }
        };

        Ok(Self {
            target,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerEpochQueryTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEpochSection {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    pub broker_id: u64,
    pub epochs: Vec<BrokerEpochEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEpochQueryResult {
    pub sections: Vec<BrokerEpochSection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanExpiredConsumeQueueRequest {
    broker_addr: Option<CheetahString>,
    cluster_name: Option<CheetahString>,
    topic: Option<CheetahString>,
    dry_run: bool,
    namesrv_addr: Option<String>,
}

impl CleanExpiredConsumeQueueRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        topic: Option<String>,
        dry_run: bool,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            broker_addr: trim_optional_string(broker_addr)
                .map(|addr| trim_required_cheetah("brokerAddr", addr))
                .transpose()?,
            cluster_name: trim_optional_string(cluster_name)
                .map(|cluster| trim_required_cheetah("clusterName", cluster))
                .transpose()?,
            topic: trim_optional_string(topic)
                .map(|topic| trim_required_cheetah("topic", topic))
                .transpose()?,
            dry_run,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn topic(&self) -> Option<&CheetahString> {
        self.topic.as_ref()
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn is_global_mode(&self) -> bool {
        self.broker_addr.is_none() && self.cluster_name.is_none() && self.topic.is_none()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanExpiredConsumeQueueTargetResult {
    pub broker_addr: CheetahString,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanExpiredConsumeQueueReport {
    pub targets: Vec<CheetahString>,
    pub target_results: Vec<CleanExpiredConsumeQueueTargetResult>,
    pub dry_run: bool,
    pub is_global_mode: bool,
    pub scanned_brokers: usize,
    pub cleanup_invocations: usize,
    pub cleanup_successes: usize,
    pub cleanup_false_results: usize,
    pub failures: Vec<BrokerOperationFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitLogReadAheadMode {
    Normal,
    Random,
}

impl CommitLogReadAheadMode {
    pub fn try_from_config_value(value: &str) -> RocketMQResult<Self> {
        match value.trim() {
            "0" => Ok(Self::Normal),
            "1" => Ok(Self::Random),
            other => Err(ToolsError::validation_error(
                "commitLogReadAheadMode",
                format!("invalid commitLogReadAheadMode '{other}', expected 0 or 1"),
            )
            .into()),
        }
    }

    pub fn config_value(self) -> &'static str {
        match self {
            Self::Normal => "0",
            Self::Random => "1",
        }
    }

    pub fn read_ahead_enabled(self) -> bool {
        matches!(self, Self::Normal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitLogReadAheadRequest {
    target: BrokerTarget,
    mode: Option<CommitLogReadAheadMode>,
    read_ahead_size: Option<u64>,
    read_ahead_size_key: Option<CheetahString>,
    show_only: bool,
    namesrv_addr: Option<String>,
}

impl CommitLogReadAheadRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        mode: Option<String>,
        enable: bool,
        disable: bool,
        read_ahead_size: Option<String>,
        read_ahead_size_key: Option<String>,
        show_only: bool,
    ) -> RocketMQResult<Self> {
        let mode = if enable {
            Some(CommitLogReadAheadMode::Normal)
        } else if disable {
            Some(CommitLogReadAheadMode::Random)
        } else {
            mode.as_deref()
                .map(CommitLogReadAheadMode::try_from_config_value)
                .transpose()?
        };
        let read_ahead_size = match read_ahead_size
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(value) => {
                let parsed = value.parse::<u64>().map_err(|error| {
                    ToolsError::validation_error("readAheadSize", format!("invalid readAheadSize '{value}': {error}"))
                })?;
                if parsed == 0 {
                    return Err(
                        ToolsError::validation_error("readAheadSize", "readAheadSize must be greater than 0").into(),
                    );
                }
                Some(parsed)
            }
            None => None,
        };

        if show_only && (mode.is_some() || read_ahead_size.is_some()) {
            return Err(
                ToolsError::validation_error("showOnly", "--showOnly cannot be used with update options").into(),
            );
        }

        Ok(Self {
            target: broker_target_from_options(broker_addr, cluster_name)?,
            mode,
            read_ahead_size,
            read_ahead_size_key: trim_optional_string(read_ahead_size_key)
                .map(|key| trim_required_cheetah("readAheadSizeKey", key))
                .transpose()?,
            show_only,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn mode(&self) -> Option<CommitLogReadAheadMode> {
        self.mode
    }

    pub fn read_ahead_size(&self) -> Option<u64> {
        self.read_ahead_size
    }

    pub fn read_ahead_size_key(&self) -> Option<&CheetahString> {
        self.read_ahead_size_key.as_ref()
    }

    pub fn show_only(&self) -> bool {
        self.show_only
    }

    pub fn has_updates(&self) -> bool {
        self.mode.is_some() || self.read_ahead_size.is_some()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitLogReadAheadSection {
    pub target: BrokerConfigSectionTarget,
    pub broker_addr: CheetahString,
    pub current_config: HashMap<CheetahString, CheetahString>,
    pub updated_config: Option<HashMap<CheetahString, CheetahString>>,
    pub size_key_for_update: Option<CheetahString>,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitLogReadAheadResult {
    pub sections: Vec<CommitLogReadAheadSection>,
    pub failures: Vec<BrokerOperationFailure>,
}

fn broker_target_from_options(
    broker_addr: Option<String>,
    cluster_name: Option<String>,
) -> RocketMQResult<BrokerTarget> {
    let broker_addr = trim_optional_string(broker_addr);
    let cluster_name = trim_optional_string(cluster_name);
    match (broker_addr, cluster_name) {
        (Some(addr), None) => Ok(BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?)),
        (None, Some(cluster)) => Ok(BrokerTarget::ClusterName(trim_required_cheetah(
            "clusterName",
            cluster,
        )?)),
        (None, None) => {
            Err(ToolsError::validation_error("target", "either brokerAddr or clusterName must be provided").into())
        }
        (Some(_), Some(_)) => {
            Err(ToolsError::validation_error("target", "brokerAddr and clusterName cannot be provided together").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn broker_config_query_request_requires_one_target() {
        let broker_request =
            BrokerConfigQueryRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None, Some(" ^flush.* ".into()))
                .unwrap();
        assert_eq!(
            broker_request.target(),
            &BrokerTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
        assert_eq!(broker_request.key_pattern(), Some("^flush.*"));

        let cluster_request = BrokerConfigQueryRequest::try_new(None, Some(" DefaultCluster ".into()), None).unwrap();
        assert_eq!(
            cluster_request.target(),
            &BrokerTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );

        assert!(BrokerConfigQueryRequest::try_new(None, None, None).is_err());
        assert!(
            BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), Some("DefaultCluster".into()), None)
                .is_err()
        );
    }

    #[test]
    fn broker_config_query_request_rejects_invalid_key_pattern() {
        let result = BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), None, Some("[".into()));
        assert!(result.is_err());
    }

    #[test]
    fn broker_config_query_request_keeps_namesrv_optional() {
        let request = BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), None, None)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn broker_config_update_request_validates_entries_and_target() {
        let mut entries = BTreeMap::new();
        entries.insert(" flushDiskType ".to_string(), " ASYNC_FLUSH ".to_string());

        let request = BrokerConfigUpdateRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None, entries)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()))
            .with_rollback_enabled(false);

        assert_eq!(
            request.target(),
            &BrokerTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(!request.rollback_enabled());
        assert_eq!(
            request
                .update_entries()
                .get(&CheetahString::from("flushDiskType"))
                .unwrap(),
            "ASYNC_FLUSH"
        );

        assert!(BrokerConfigUpdateRequest::try_new(Some("127.0.0.1:10911".into()), None, BTreeMap::new()).is_err());

        let mut invalid_entries = BTreeMap::new();
        invalid_entries.insert("bad key".to_string(), "value".to_string());
        assert!(BrokerConfigUpdateRequest::try_new(Some("127.0.0.1:10911".into()), None, invalid_entries).is_err());
    }

    #[test]
    fn broker_runtime_stats_query_request_validates_target() {
        let broker_request = BrokerRuntimeStatsQueryRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));
        assert_eq!(
            broker_request.target(),
            &BrokerTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
        assert_eq!(broker_request.namesrv_addr(), Some("127.0.0.1:9876"));

        let cluster_request = BrokerRuntimeStatsQueryRequest::try_new(None, Some(" DefaultCluster ".into())).unwrap();
        assert_eq!(
            cluster_request.target(),
            &BrokerTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );

        assert!(BrokerRuntimeStatsQueryRequest::try_new(None, None).is_err());
        assert!(
            BrokerRuntimeStatsQueryRequest::try_new(Some("127.0.0.1:10911".into()), Some("DefaultCluster".into()))
                .is_err()
        );
    }

    #[test]
    fn broker_consume_stats_query_request_trims_broker_and_keeps_filters() {
        let request = BrokerConsumeStatsQueryRequest::try_new(" 127.0.0.1:10911 ", 3_000, 42, true)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.timeout_millis(), 3_000);
        assert_eq!(request.diff_level(), 42);
        assert!(request.is_order());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));

        assert!(BrokerConsumeStatsQueryRequest::try_new(" ", 3_000, 0, false).is_err());
    }

    #[test]
    fn broker_optional_target_trims_optional_fields() {
        let request = BrokerOptionalTarget::new(
            Some(" 127.0.0.1:10911 ".to_string()),
            Some(" DefaultCluster ".to_string()),
        )
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.broker_addr().unwrap().as_str(), "127.0.0.1:10911");
        assert_eq!(request.cluster_name().unwrap().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn reset_master_flush_offset_request_validates_input() {
        let request = ResetMasterFlushOffsetRequest::try_new(Some(" 127.0.0.1:10911 ".into()), Some(1024))
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.master_flush_offset(), 1024);
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(ResetMasterFlushOffsetRequest::try_new(None, Some(1024)).is_err());
        assert!(ResetMasterFlushOffsetRequest::try_new(Some("127.0.0.1:10911".into()), None).is_err());
        assert!(ResetMasterFlushOffsetRequest::try_new(Some("127.0.0.1:10911".into()), Some(-1)).is_err());
    }

    #[test]
    fn switch_timer_engine_request_validates_target_and_engine() {
        let request = SwitchTimerEngineRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None, " R ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
        ));
        assert_eq!(request.engine_type().as_str(), "R");
        assert_eq!(request.engine_name().as_str(), "ROCKSDB_TIMELINE");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));

        assert!(SwitchTimerEngineRequest::try_new(None, Some("DefaultCluster".into()), "F").is_ok());
        assert!(SwitchTimerEngineRequest::try_new(None, Some("DefaultCluster".into()), "bad").is_err());
        assert!(SwitchTimerEngineRequest::try_new(None, None, "R").is_err());
    }

    #[test]
    fn cold_data_flow_ctr_update_request_validates_target_and_fields() {
        let request = ColdDataFlowCtrGroupConfigUpdateRequest::try_new(
            Some(" 127.0.0.1:10911 ".into()),
            None,
            " GroupA ",
            " 1024 ",
        )
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
        ));
        assert_eq!(request.consumer_group().as_str(), "GroupA");
        assert_eq!(request.threshold().as_str(), "1024");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(ColdDataFlowCtrGroupConfigUpdateRequest::try_new(None, None, "GroupA", "1024").is_err());
        assert!(
            ColdDataFlowCtrGroupConfigUpdateRequest::try_new(None, Some("DefaultCluster".into()), " ", "1024").is_err()
        );
    }

    #[test]
    fn cold_data_flow_ctr_remove_request_validates_target_and_group() {
        let request =
            ColdDataFlowCtrGroupConfigRemoveRequest::try_new(None, Some(" DefaultCluster ".into()), " GroupA ")
                .unwrap()
                .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
        ));
        assert_eq!(request.consumer_group().as_str(), "GroupA");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(ColdDataFlowCtrGroupConfigRemoveRequest::try_new(
            Some("127.0.0.1:10911".into()),
            Some("DefaultCluster".into()),
            "GroupA",
        )
        .is_err());
    }

    #[test]
    fn cold_data_flow_ctr_info_query_request_validates_target() {
        let request = ColdDataFlowCtrInfoQueryRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
        ));
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(ColdDataFlowCtrInfoQueryRequest::try_new(None, None).is_err());
    }

    #[test]
    fn broker_epoch_query_request_validates_target() {
        let request = BrokerEpochQueryRequest::try_new(Some(" broker-a ".into()), None)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert!(matches!(
            request.target(),
            BrokerEpochQueryTarget::BrokerName(broker) if broker.as_str() == "broker-a"
        ));
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(BrokerEpochQueryRequest::try_new(None, Some(" DefaultCluster ".into())).is_ok());
        assert!(BrokerEpochQueryRequest::try_new(None, None).is_err());
        assert!(BrokerEpochQueryRequest::try_new(Some("broker-a".into()), Some("DefaultCluster".into())).is_err());
    }

    #[test]
    fn clean_expired_consume_queue_request_allows_global_and_topic_scope() {
        let request = CleanExpiredConsumeQueueRequest::try_new(None, None, Some(" TestTopic ".into()), true)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.topic().unwrap().as_str(), "TestTopic");
        assert!(request.dry_run());
        assert!(!request.is_global_mode());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));

        let global = CleanExpiredConsumeQueueRequest::try_new(None, None, None, false).unwrap();
        assert!(global.is_global_mode());
    }

    #[test]
    fn commit_log_read_ahead_request_validates_mode_and_updates() {
        let request = CommitLogReadAheadRequest::try_new(
            Some(" 127.0.0.1:10911 ".into()),
            None,
            Some(" 1 ".into()),
            false,
            false,
            Some(" 4096 ".into()),
            Some(" commitLogReadAheadSize ".into()),
            false,
        )
        .unwrap();

        assert_eq!(request.mode(), Some(CommitLogReadAheadMode::Random));
        assert_eq!(request.read_ahead_size(), Some(4096));
        assert_eq!(
            request.read_ahead_size_key().unwrap().as_str(),
            "commitLogReadAheadSize"
        );
        assert!(request.has_updates());
        assert!(CommitLogReadAheadRequest::try_new(
            Some("127.0.0.1:10911".into()),
            None,
            Some("2".into()),
            false,
            false,
            None,
            None,
            false,
        )
        .is_err());
        assert!(CommitLogReadAheadRequest::try_new(
            Some("127.0.0.1:10911".into()),
            None,
            None,
            true,
            false,
            None,
            None,
            true,
        )
        .is_err());
    }
}
