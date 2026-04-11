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
}
