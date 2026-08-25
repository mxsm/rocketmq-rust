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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_auth::AuthConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::mix_all::ROCKETMQ_HOME_ENV;
use rocketmq_model::common::mix_all::ROCKETMQ_HOME_PROPERTY;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerDecodeLimits;
use serde::Deserialize;
use serde_json::Value;

pub const REMOVED_ROUTE_MANAGER_CONFIG_KEY: &str = "useRouteInfoManagerV2";
pub const DEFAULT_NAMESRV_LISTEN_PORT: u32 = 9876;
const REMOVED_ROUTE_MANAGER_CONFIG_FIELD: &str = "use_route_info_manager_v2";

const MAX_THREAD_COUNT: i32 = 4096;
const MAX_QUEUE_CAPACITY: i32 = 10_000_000;
const MAX_SCAN_INTERVAL_MILLIS: u64 = 3_600_000;
const MAX_WAIT_SECONDS: i32 = 3600;
const MAX_ROUTE_FRESHNESS_SAMPLE_INTERVAL: u64 = 1_000_000;
const MAX_ROUTE_RESPONSE_CACHE_BYTES: u64 = 1_073_741_824;
const MAX_ROUTE_RESPONSE_CACHE_ENTRIES: u64 = 1_000_000;
const MAX_ROUTE_RESPONSE_CACHE_SHARDS: u64 = 256;
const MAX_WORKLOAD_ADMISSION_TIMEOUT_MILLIS: u64 = 60_000;
const MAX_UNREGISTER_BATCH_SIZE: u64 = 1024;
const MAX_UNREGISTER_BATCH_TIME_MILLIS: u64 = 50;
const MIN_EXPIRY_SAFETY_SCAN_INTERVAL_MILLIS: u64 = 30_000;
const MAX_EXPIRY_SAFETY_SCAN_INTERVAL_MILLIS: u64 = 3_600_000;
const MAX_MIN_BROKER_NOTIFY_CONCURRENCY: u64 = 128;
const MAX_KV_MUTATION_QUEUE_CAPACITY: u64 = 1_000_000;
const MAX_KV_MUTATION_BATCH_SIZE: u64 = 1024;
const MAX_CLUSTER_TEST_ROUTE_CACHE_TTL_MILLIS: u64 = 60_000;
const MAX_CLUSTER_TEST_ROUTE_CACHE_ENTRIES: u64 = 1_000_000;
const MAX_CLUSTER_TEST_ROUTE_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ExpiryIndexMode {
    #[default]
    Off,
    Shadow,
    Active,
}

impl ExpiryIndexMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Active => "active",
        }
    }
}

impl std::str::FromStr for ExpiryIndexMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "active" => Ok(Self::Active),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigMutability {
    Live,
    RestartRequired,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamesrvConfigKey {
    RocketmqHome,
    KvConfigPath,
    ConfigStorePath,
    ProductEnvName,
    ClusterTest,
    OrderMessageEnable,
    RouteFreshnessSampleInterval,
    NamesrvTypedZoneRouteEnable,
    NamesrvTypedZoneRouteShadow,
    NamesrvRouteResponseCacheEnable,
    NamesrvRouteResponseCacheMaxBytes,
    NamesrvRouteResponseCacheMaxEntries,
    NamesrvRouteResponseCacheMaxSingleResponseBytes,
    NamesrvRouteResponseCacheShards,
    NamesrvWorkloadAdmissionEnable,
    NamesrvWorkloadAdmissionObserveOnly,
    NamesrvWorkloadAdmissionTimeoutMillis,
    EnableRegistrationDelta,
    ClusterTestRouteCachePositiveTtlMillis,
    ClusterTestRouteCacheNegativeTtlMillis,
    ClusterTestRouteCacheMaxEntries,
    ClusterTestRouteCacheMaxBytes,
    KvMutationQueueCapacity,
    KvMutationBatchSize,
    UnregisterBrokerBatchSize,
    UnregisterBrokerBatchTimeMillis,
    ExpiryIndexMode,
    ExpirySafetyScanInterval,
    MinBrokerNotifyConcurrency,
    ReturnOrderTopicConfigToBroker,
    ClientRequestThreadPoolNums,
    DefaultThreadPoolNums,
    ClientRequestThreadPoolQueueCapacity,
    DefaultThreadPoolQueueCapacity,
    ScanNotActiveBrokerInterval,
    UnregisterBrokerQueueCapacity,
    SupportActingMaster,
    EnableAllTopicList,
    EnableTopicList,
    NotifyMinBrokerIdChanged,
    EnableControllerInNamesrv,
    NeedWaitForService,
    WaitSecondsForService,
    DeleteTopicWithBrokerRegistration,
    AllowInsecurePublicListener,
    ConfigBlackList,
}

impl NamesrvConfigKey {
    #[cfg(test)]
    const ALL: [Self; 46] = [
        Self::RocketmqHome,
        Self::KvConfigPath,
        Self::ConfigStorePath,
        Self::ProductEnvName,
        Self::ClusterTest,
        Self::OrderMessageEnable,
        Self::RouteFreshnessSampleInterval,
        Self::NamesrvTypedZoneRouteEnable,
        Self::NamesrvTypedZoneRouteShadow,
        Self::NamesrvRouteResponseCacheEnable,
        Self::NamesrvRouteResponseCacheMaxBytes,
        Self::NamesrvRouteResponseCacheMaxEntries,
        Self::NamesrvRouteResponseCacheMaxSingleResponseBytes,
        Self::NamesrvRouteResponseCacheShards,
        Self::NamesrvWorkloadAdmissionEnable,
        Self::NamesrvWorkloadAdmissionObserveOnly,
        Self::NamesrvWorkloadAdmissionTimeoutMillis,
        Self::EnableRegistrationDelta,
        Self::ClusterTestRouteCachePositiveTtlMillis,
        Self::ClusterTestRouteCacheNegativeTtlMillis,
        Self::ClusterTestRouteCacheMaxEntries,
        Self::ClusterTestRouteCacheMaxBytes,
        Self::KvMutationQueueCapacity,
        Self::KvMutationBatchSize,
        Self::UnregisterBrokerBatchSize,
        Self::UnregisterBrokerBatchTimeMillis,
        Self::ExpiryIndexMode,
        Self::ExpirySafetyScanInterval,
        Self::MinBrokerNotifyConcurrency,
        Self::ReturnOrderTopicConfigToBroker,
        Self::ClientRequestThreadPoolNums,
        Self::DefaultThreadPoolNums,
        Self::ClientRequestThreadPoolQueueCapacity,
        Self::DefaultThreadPoolQueueCapacity,
        Self::ScanNotActiveBrokerInterval,
        Self::UnregisterBrokerQueueCapacity,
        Self::SupportActingMaster,
        Self::EnableAllTopicList,
        Self::EnableTopicList,
        Self::NotifyMinBrokerIdChanged,
        Self::EnableControllerInNamesrv,
        Self::NeedWaitForService,
        Self::WaitSecondsForService,
        Self::DeleteTopicWithBrokerRegistration,
        Self::AllowInsecurePublicListener,
        Self::ConfigBlackList,
    ];

    pub(crate) fn from_java_name(key: &str) -> Option<Self> {
        Some(match key {
            "rocketmqHome" => Self::RocketmqHome,
            "kvConfigPath" => Self::KvConfigPath,
            "configStorePath" => Self::ConfigStorePath,
            "productEnvName" => Self::ProductEnvName,
            "clusterTest" => Self::ClusterTest,
            "orderMessageEnable" => Self::OrderMessageEnable,
            "routeFreshnessSampleInterval" => Self::RouteFreshnessSampleInterval,
            "namesrvTypedZoneRouteEnable" => Self::NamesrvTypedZoneRouteEnable,
            "namesrvTypedZoneRouteShadow" => Self::NamesrvTypedZoneRouteShadow,
            "namesrvRouteResponseCacheEnable" => Self::NamesrvRouteResponseCacheEnable,
            "namesrvRouteResponseCacheMaxBytes" => Self::NamesrvRouteResponseCacheMaxBytes,
            "namesrvRouteResponseCacheMaxEntries" => Self::NamesrvRouteResponseCacheMaxEntries,
            "namesrvRouteResponseCacheMaxSingleResponseBytes" => Self::NamesrvRouteResponseCacheMaxSingleResponseBytes,
            "namesrvRouteResponseCacheShards" => Self::NamesrvRouteResponseCacheShards,
            "namesrvWorkloadAdmissionEnable" => Self::NamesrvWorkloadAdmissionEnable,
            "namesrvWorkloadAdmissionObserveOnly" => Self::NamesrvWorkloadAdmissionObserveOnly,
            "namesrvWorkloadAdmissionTimeoutMillis" => Self::NamesrvWorkloadAdmissionTimeoutMillis,
            "enableRegistrationDelta" => Self::EnableRegistrationDelta,
            "clusterTestRouteCachePositiveTtlMillis" => Self::ClusterTestRouteCachePositiveTtlMillis,
            "clusterTestRouteCacheNegativeTtlMillis" => Self::ClusterTestRouteCacheNegativeTtlMillis,
            "clusterTestRouteCacheMaxEntries" => Self::ClusterTestRouteCacheMaxEntries,
            "clusterTestRouteCacheMaxBytes" => Self::ClusterTestRouteCacheMaxBytes,
            "kvMutationQueueCapacity" => Self::KvMutationQueueCapacity,
            "kvMutationBatchSize" => Self::KvMutationBatchSize,
            "unRegisterBrokerBatchSize" => Self::UnregisterBrokerBatchSize,
            "unRegisterBrokerBatchTimeMillis" => Self::UnregisterBrokerBatchTimeMillis,
            "expiryIndexMode" => Self::ExpiryIndexMode,
            "expirySafetyScanInterval" => Self::ExpirySafetyScanInterval,
            "minBrokerNotifyConcurrency" => Self::MinBrokerNotifyConcurrency,
            "returnOrderTopicConfigToBroker" => Self::ReturnOrderTopicConfigToBroker,
            "clientRequestThreadPoolNums" => Self::ClientRequestThreadPoolNums,
            "defaultThreadPoolNums" => Self::DefaultThreadPoolNums,
            "clientRequestThreadPoolQueueCapacity" => Self::ClientRequestThreadPoolQueueCapacity,
            "defaultThreadPoolQueueCapacity" => Self::DefaultThreadPoolQueueCapacity,
            "scanNotActiveBrokerInterval" => Self::ScanNotActiveBrokerInterval,
            "unRegisterBrokerQueueCapacity" => Self::UnregisterBrokerQueueCapacity,
            "supportActingMaster" => Self::SupportActingMaster,
            "enableAllTopicList" => Self::EnableAllTopicList,
            "enableTopicList" => Self::EnableTopicList,
            "notifyMinBrokerIdChanged" => Self::NotifyMinBrokerIdChanged,
            "enableControllerInNamesrv" => Self::EnableControllerInNamesrv,
            "needWaitForService" => Self::NeedWaitForService,
            "waitSecondsForService" => Self::WaitSecondsForService,
            "deleteTopicWithBrokerRegistration" => Self::DeleteTopicWithBrokerRegistration,
            "allowInsecurePublicListener" => Self::AllowInsecurePublicListener,
            "configBlackList" => Self::ConfigBlackList,
            _ => return None,
        })
    }

    pub(crate) fn mutability(self) -> ConfigMutability {
        match self {
            Self::OrderMessageEnable
            | Self::RouteFreshnessSampleInterval
            | Self::NamesrvTypedZoneRouteEnable
            | Self::NamesrvTypedZoneRouteShadow
            | Self::NamesrvRouteResponseCacheEnable
            | Self::NamesrvWorkloadAdmissionEnable
            | Self::NamesrvWorkloadAdmissionObserveOnly
            | Self::ReturnOrderTopicConfigToBroker
            | Self::SupportActingMaster
            | Self::EnableAllTopicList
            | Self::EnableTopicList
            | Self::NotifyMinBrokerIdChanged
            | Self::DeleteTopicWithBrokerRegistration => ConfigMutability::Live,
            Self::ProductEnvName
            | Self::ClusterTest
            | Self::ClientRequestThreadPoolNums
            | Self::DefaultThreadPoolNums
            | Self::ClientRequestThreadPoolQueueCapacity
            | Self::DefaultThreadPoolQueueCapacity
            | Self::ScanNotActiveBrokerInterval
            | Self::UnregisterBrokerQueueCapacity
            | Self::EnableControllerInNamesrv
            | Self::NeedWaitForService
            | Self::WaitSecondsForService => ConfigMutability::RestartRequired,
            Self::NamesrvRouteResponseCacheMaxBytes
            | Self::NamesrvRouteResponseCacheMaxEntries
            | Self::NamesrvRouteResponseCacheMaxSingleResponseBytes
            | Self::NamesrvRouteResponseCacheShards => ConfigMutability::RestartRequired,
            Self::NamesrvWorkloadAdmissionTimeoutMillis
            | Self::EnableRegistrationDelta
            | Self::ClusterTestRouteCachePositiveTtlMillis
            | Self::ClusterTestRouteCacheNegativeTtlMillis
            | Self::ClusterTestRouteCacheMaxEntries
            | Self::ClusterTestRouteCacheMaxBytes
            | Self::KvMutationQueueCapacity
            | Self::KvMutationBatchSize
            | Self::UnregisterBrokerBatchSize
            | Self::UnregisterBrokerBatchTimeMillis
            | Self::ExpiryIndexMode
            | Self::ExpirySafetyScanInterval
            | Self::MinBrokerNotifyConcurrency => ConfigMutability::RestartRequired,
            Self::RocketmqHome
            | Self::KvConfigPath
            | Self::ConfigStorePath
            | Self::AllowInsecurePublicListener
            | Self::ConfigBlackList => ConfigMutability::Unsupported,
        }
    }
}

pub(crate) fn validate_namesrv_property(key: NamesrvConfigKey, value: &str) -> RocketMQResult<()> {
    match key {
        NamesrvConfigKey::ClientRequestThreadPoolNums => {
            parse_bounded_i32(key, value, 1, MAX_THREAD_COUNT)?;
        }
        NamesrvConfigKey::DefaultThreadPoolNums => {
            parse_bounded_i32(key, value, 2, MAX_THREAD_COUNT)?;
        }
        NamesrvConfigKey::ClientRequestThreadPoolQueueCapacity | NamesrvConfigKey::UnregisterBrokerQueueCapacity => {
            parse_bounded_i32(key, value, 1, MAX_QUEUE_CAPACITY)?;
        }
        NamesrvConfigKey::DefaultThreadPoolQueueCapacity => {
            parse_bounded_i32(key, value, 2, MAX_QUEUE_CAPACITY)?;
        }
        NamesrvConfigKey::ScanNotActiveBrokerInterval => {
            let value = value
                .parse::<u64>()
                .map_err(|_| invalid_value(key.java_name(), "expected a non-negative integer"))?;
            if !(1..=MAX_SCAN_INTERVAL_MILLIS).contains(&value) {
                return Err(invalid_value(
                    key.java_name(),
                    &format!("must be between 1 and {MAX_SCAN_INTERVAL_MILLIS}"),
                ));
            }
        }
        NamesrvConfigKey::WaitSecondsForService => {
            parse_bounded_i32(key, value, 0, MAX_WAIT_SECONDS)?;
        }
        NamesrvConfigKey::RouteFreshnessSampleInterval => {
            let value = value
                .parse::<u64>()
                .map_err(|_| invalid_value(key.java_name(), "expected a positive integer"))?;
            if !(1..=MAX_ROUTE_FRESHNESS_SAMPLE_INTERVAL).contains(&value) {
                return Err(invalid_value(
                    key.java_name(),
                    &format!("must be between 1 and {MAX_ROUTE_FRESHNESS_SAMPLE_INTERVAL}"),
                ));
            }
        }
        NamesrvConfigKey::NamesrvRouteResponseCacheMaxBytes => {
            parse_bounded_u64(key, value, 1, MAX_ROUTE_RESPONSE_CACHE_BYTES)?;
        }
        NamesrvConfigKey::NamesrvRouteResponseCacheMaxEntries => {
            parse_bounded_u64(key, value, 1, MAX_ROUTE_RESPONSE_CACHE_ENTRIES)?;
        }
        NamesrvConfigKey::NamesrvRouteResponseCacheMaxSingleResponseBytes => {
            parse_bounded_u64(key, value, 1, MAX_ROUTE_RESPONSE_CACHE_BYTES)?;
        }
        NamesrvConfigKey::NamesrvRouteResponseCacheShards => {
            parse_bounded_u64(key, value, 1, MAX_ROUTE_RESPONSE_CACHE_SHARDS)?;
        }
        NamesrvConfigKey::NamesrvWorkloadAdmissionTimeoutMillis => {
            parse_bounded_u64(key, value, 1, MAX_WORKLOAD_ADMISSION_TIMEOUT_MILLIS)?;
        }
        NamesrvConfigKey::UnregisterBrokerBatchSize => {
            parse_bounded_u64(key, value, 1, MAX_UNREGISTER_BATCH_SIZE)?;
        }
        NamesrvConfigKey::KvMutationQueueCapacity => {
            parse_bounded_u64(key, value, 1, MAX_KV_MUTATION_QUEUE_CAPACITY)?;
        }
        NamesrvConfigKey::KvMutationBatchSize => {
            parse_bounded_u64(key, value, 1, MAX_KV_MUTATION_BATCH_SIZE)?;
        }
        NamesrvConfigKey::ClusterTestRouteCachePositiveTtlMillis
        | NamesrvConfigKey::ClusterTestRouteCacheNegativeTtlMillis => {
            parse_bounded_u64(key, value, 1, MAX_CLUSTER_TEST_ROUTE_CACHE_TTL_MILLIS)?;
        }
        NamesrvConfigKey::ClusterTestRouteCacheMaxEntries => {
            parse_bounded_u64(key, value, 1, MAX_CLUSTER_TEST_ROUTE_CACHE_ENTRIES)?;
        }
        NamesrvConfigKey::ClusterTestRouteCacheMaxBytes => {
            parse_bounded_u64(key, value, 1, MAX_CLUSTER_TEST_ROUTE_CACHE_BYTES)?;
        }
        NamesrvConfigKey::UnregisterBrokerBatchTimeMillis => {
            parse_bounded_u64(key, value, 1, MAX_UNREGISTER_BATCH_TIME_MILLIS)?;
        }
        NamesrvConfigKey::ExpirySafetyScanInterval => {
            parse_bounded_u64(
                key,
                value,
                MIN_EXPIRY_SAFETY_SCAN_INTERVAL_MILLIS,
                MAX_EXPIRY_SAFETY_SCAN_INTERVAL_MILLIS,
            )?;
        }
        NamesrvConfigKey::MinBrokerNotifyConcurrency => {
            parse_bounded_u64(key, value, 1, MAX_MIN_BROKER_NOTIFY_CONCURRENCY)?;
        }
        NamesrvConfigKey::ExpiryIndexMode => {
            value
                .parse::<ExpiryIndexMode>()
                .map_err(|_| invalid_value(key.java_name(), "expected one of off, shadow, active"))?;
        }
        NamesrvConfigKey::ClusterTest
        | NamesrvConfigKey::OrderMessageEnable
        | NamesrvConfigKey::NamesrvTypedZoneRouteEnable
        | NamesrvConfigKey::NamesrvTypedZoneRouteShadow
        | NamesrvConfigKey::NamesrvRouteResponseCacheEnable
        | NamesrvConfigKey::NamesrvWorkloadAdmissionEnable
        | NamesrvConfigKey::NamesrvWorkloadAdmissionObserveOnly
        | NamesrvConfigKey::EnableRegistrationDelta
        | NamesrvConfigKey::ReturnOrderTopicConfigToBroker
        | NamesrvConfigKey::SupportActingMaster
        | NamesrvConfigKey::EnableAllTopicList
        | NamesrvConfigKey::EnableTopicList
        | NamesrvConfigKey::NotifyMinBrokerIdChanged
        | NamesrvConfigKey::EnableControllerInNamesrv
        | NamesrvConfigKey::NeedWaitForService
        | NamesrvConfigKey::DeleteTopicWithBrokerRegistration
        | NamesrvConfigKey::AllowInsecurePublicListener => {
            value
                .parse::<bool>()
                .map_err(|_| invalid_value(key.java_name(), "expected a boolean"))?;
        }
        NamesrvConfigKey::ProductEnvName
        | NamesrvConfigKey::RocketmqHome
        | NamesrvConfigKey::KvConfigPath
        | NamesrvConfigKey::ConfigStorePath
        | NamesrvConfigKey::ConfigBlackList => {
            if value.trim().is_empty() {
                return Err(invalid_value(key.java_name(), "must not be empty"));
            }
        }
    }
    Ok(())
}

impl NamesrvConfigKey {
    fn java_name(self) -> &'static str {
        match self {
            Self::RocketmqHome => "rocketmqHome",
            Self::KvConfigPath => "kvConfigPath",
            Self::ConfigStorePath => "configStorePath",
            Self::ProductEnvName => "productEnvName",
            Self::ClusterTest => "clusterTest",
            Self::OrderMessageEnable => "orderMessageEnable",
            Self::RouteFreshnessSampleInterval => "routeFreshnessSampleInterval",
            Self::NamesrvTypedZoneRouteEnable => "namesrvTypedZoneRouteEnable",
            Self::NamesrvTypedZoneRouteShadow => "namesrvTypedZoneRouteShadow",
            Self::NamesrvRouteResponseCacheEnable => "namesrvRouteResponseCacheEnable",
            Self::NamesrvRouteResponseCacheMaxBytes => "namesrvRouteResponseCacheMaxBytes",
            Self::NamesrvRouteResponseCacheMaxEntries => "namesrvRouteResponseCacheMaxEntries",
            Self::NamesrvRouteResponseCacheMaxSingleResponseBytes => "namesrvRouteResponseCacheMaxSingleResponseBytes",
            Self::NamesrvRouteResponseCacheShards => "namesrvRouteResponseCacheShards",
            Self::NamesrvWorkloadAdmissionEnable => "namesrvWorkloadAdmissionEnable",
            Self::NamesrvWorkloadAdmissionObserveOnly => "namesrvWorkloadAdmissionObserveOnly",
            Self::NamesrvWorkloadAdmissionTimeoutMillis => "namesrvWorkloadAdmissionTimeoutMillis",
            Self::EnableRegistrationDelta => "enableRegistrationDelta",
            Self::ClusterTestRouteCachePositiveTtlMillis => "clusterTestRouteCachePositiveTtlMillis",
            Self::ClusterTestRouteCacheNegativeTtlMillis => "clusterTestRouteCacheNegativeTtlMillis",
            Self::ClusterTestRouteCacheMaxEntries => "clusterTestRouteCacheMaxEntries",
            Self::ClusterTestRouteCacheMaxBytes => "clusterTestRouteCacheMaxBytes",
            Self::KvMutationQueueCapacity => "kvMutationQueueCapacity",
            Self::KvMutationBatchSize => "kvMutationBatchSize",
            Self::UnregisterBrokerBatchSize => "unRegisterBrokerBatchSize",
            Self::UnregisterBrokerBatchTimeMillis => "unRegisterBrokerBatchTimeMillis",
            Self::ExpiryIndexMode => "expiryIndexMode",
            Self::ExpirySafetyScanInterval => "expirySafetyScanInterval",
            Self::MinBrokerNotifyConcurrency => "minBrokerNotifyConcurrency",
            Self::ReturnOrderTopicConfigToBroker => "returnOrderTopicConfigToBroker",
            Self::ClientRequestThreadPoolNums => "clientRequestThreadPoolNums",
            Self::DefaultThreadPoolNums => "defaultThreadPoolNums",
            Self::ClientRequestThreadPoolQueueCapacity => "clientRequestThreadPoolQueueCapacity",
            Self::DefaultThreadPoolQueueCapacity => "defaultThreadPoolQueueCapacity",
            Self::ScanNotActiveBrokerInterval => "scanNotActiveBrokerInterval",
            Self::UnregisterBrokerQueueCapacity => "unRegisterBrokerQueueCapacity",
            Self::SupportActingMaster => "supportActingMaster",
            Self::EnableAllTopicList => "enableAllTopicList",
            Self::EnableTopicList => "enableTopicList",
            Self::NotifyMinBrokerIdChanged => "notifyMinBrokerIdChanged",
            Self::EnableControllerInNamesrv => "enableControllerInNamesrv",
            Self::NeedWaitForService => "needWaitForService",
            Self::WaitSecondsForService => "waitSecondsForService",
            Self::DeleteTopicWithBrokerRegistration => "deleteTopicWithBrokerRegistration",
            Self::AllowInsecurePublicListener => "allowInsecurePublicListener",
            Self::ConfigBlackList => "configBlackList",
        }
    }
}

fn parse_bounded_i32(key: NamesrvConfigKey, value: &str, minimum: i32, maximum: i32) -> RocketMQResult<i32> {
    let value = value
        .parse::<i32>()
        .map_err(|_| invalid_value(key.java_name(), "expected an integer"))?;
    if !(minimum..=maximum).contains(&value) {
        return Err(invalid_value(
            key.java_name(),
            &format!("must be between {minimum} and {maximum}"),
        ));
    }
    Ok(value)
}

fn parse_bounded_u64(key: NamesrvConfigKey, value: &str, minimum: u64, maximum: u64) -> RocketMQResult<u64> {
    let value = value
        .parse::<u64>()
        .map_err(|_| invalid_value(key.java_name(), "expected a positive integer"))?;
    if !(minimum..=maximum).contains(&value) {
        return Err(invalid_value(
            key.java_name(),
            &format!("must be between {minimum} and {maximum}"),
        ));
    }
    Ok(value)
}

/// Default value functions for serde deserialization
mod defaults {
    use super::*;

    pub fn rocketmq_home() -> String {
        std::env::var(ROCKETMQ_HOME_PROPERTY).unwrap_or_else(|_| std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_default())
    }

    pub fn kv_config_path() -> String {
        let mut kv_config_path = dirs::home_dir().unwrap_or_default();
        kv_config_path.push("rocketmq-namesrv");
        kv_config_path.push("kvConfig.json");
        kv_config_path.to_str().unwrap_or_default().to_string()
    }

    pub fn config_store_path() -> String {
        let mut kv_config_path = dirs::home_dir().unwrap_or_default();
        kv_config_path.push("rocketmq-namesrv");
        kv_config_path.push("rocketmq-namesrv.properties");
        kv_config_path.to_str().unwrap_or_default().to_string()
    }

    pub fn product_env_name() -> String {
        "center".to_string()
    }

    pub fn return_order_topic_config_to_broker() -> bool {
        true
    }

    pub fn client_request_thread_pool_nums() -> i32 {
        8
    }

    pub fn default_thread_pool_nums() -> i32 {
        16
    }

    pub fn client_request_thread_pool_queue_capacity() -> i32 {
        50000
    }

    pub fn default_thread_pool_queue_capacity() -> i32 {
        10000
    }

    pub fn scan_not_active_broker_interval() -> u64 {
        5 * 1000
    }

    pub fn unregister_broker_queue_capacity() -> i32 {
        3000
    }

    pub fn enable_all_topic_list() -> bool {
        true
    }

    pub fn enable_topic_list() -> bool {
        true
    }

    pub fn wait_seconds_for_service() -> i32 {
        45
    }

    pub fn route_freshness_sample_interval() -> u64 {
        1000
    }

    pub fn namesrv_route_response_cache_max_bytes() -> u64 {
        67_108_864
    }

    pub fn namesrv_route_response_cache_max_entries() -> u64 {
        10_000
    }

    pub fn namesrv_route_response_cache_max_single_response_bytes() -> u64 {
        1_048_576
    }

    pub fn namesrv_route_response_cache_shards() -> usize {
        16
    }

    pub fn namesrv_workload_admission_timeout_millis() -> u64 {
        100
    }

    pub fn default_true() -> bool {
        true
    }

    pub fn unregister_broker_batch_size() -> usize {
        100
    }

    pub fn unregister_broker_batch_time_millis() -> u64 {
        2
    }

    pub fn expiry_safety_scan_interval() -> u64 {
        300_000
    }

    pub fn min_broker_notify_concurrency() -> usize {
        8
    }

    pub fn kv_mutation_queue_capacity() -> usize {
        1024
    }

    pub fn kv_mutation_batch_size() -> usize {
        100
    }

    pub fn cluster_test_route_cache_positive_ttl_millis() -> u64 {
        1_000
    }

    pub fn cluster_test_route_cache_negative_ttl_millis() -> u64 {
        250
    }

    pub fn cluster_test_route_cache_max_entries() -> u64 {
        1_000
    }

    pub fn cluster_test_route_cache_max_bytes() -> u64 {
        16 * 1024 * 1024
    }

    pub fn config_black_list() -> String {
        "configBlackList;configStorePath;kvConfigPath".to_string()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamesrvConfig {
    #[serde(alias = "rocketmqHome", default = "defaults::rocketmq_home")]
    pub rocketmq_home: String,

    #[serde(default)]
    pub observability: rocketmq_observability::ObservabilityOverrides,

    #[serde(alias = "kvConfigPath", default = "defaults::kv_config_path")]
    pub kv_config_path: String,

    #[serde(alias = "configStorePath", default = "defaults::config_store_path")]
    pub config_store_path: String,

    #[serde(alias = "productEnvName", default = "defaults::product_env_name")]
    pub product_env_name: String,

    #[serde(alias = "clusterTest", default)]
    pub cluster_test: bool,

    #[serde(alias = "orderMessageEnable", default)]
    pub order_message_enable: bool,

    #[serde(
        alias = "routeFreshnessSampleInterval",
        default = "defaults::route_freshness_sample_interval"
    )]
    pub route_freshness_sample_interval: u64,

    #[serde(alias = "namesrvTypedZoneRouteEnable", default)]
    pub namesrv_typed_zone_route_enable: bool,

    #[serde(alias = "namesrvTypedZoneRouteShadow", default)]
    pub namesrv_typed_zone_route_shadow: bool,

    #[serde(alias = "namesrvRouteResponseCacheEnable", default)]
    pub namesrv_route_response_cache_enable: bool,

    #[serde(
        alias = "namesrvRouteResponseCacheMaxBytes",
        default = "defaults::namesrv_route_response_cache_max_bytes"
    )]
    pub namesrv_route_response_cache_max_bytes: u64,

    #[serde(
        alias = "namesrvRouteResponseCacheMaxEntries",
        default = "defaults::namesrv_route_response_cache_max_entries"
    )]
    pub namesrv_route_response_cache_max_entries: u64,

    #[serde(
        alias = "namesrvRouteResponseCacheMaxSingleResponseBytes",
        default = "defaults::namesrv_route_response_cache_max_single_response_bytes"
    )]
    pub namesrv_route_response_cache_max_single_response_bytes: u64,

    #[serde(
        alias = "namesrvRouteResponseCacheShards",
        default = "defaults::namesrv_route_response_cache_shards"
    )]
    pub namesrv_route_response_cache_shards: usize,

    /// Enables semantic NameServer admission using the Java-compatible pool knobs.
    #[serde(alias = "namesrvWorkloadAdmissionEnable", default = "defaults::default_true")]
    pub namesrv_workload_admission_enable: bool,

    /// Measures saturation but does not reject while admission is being rolled out.
    #[serde(alias = "namesrvWorkloadAdmissionObserveOnly", default = "defaults::default_true")]
    pub namesrv_workload_admission_observe_only: bool,

    #[serde(
        alias = "namesrvWorkloadAdmissionTimeoutMillis",
        default = "defaults::namesrv_workload_admission_timeout_millis"
    )]
    pub namesrv_workload_admission_timeout_millis: u64,

    #[serde(alias = "enableRegistrationDelta", default)]
    pub enable_registration_delta: bool,

    #[serde(
        alias = "clusterTestRouteCachePositiveTtlMillis",
        default = "defaults::cluster_test_route_cache_positive_ttl_millis"
    )]
    pub cluster_test_route_cache_positive_ttl_millis: u64,

    #[serde(
        alias = "clusterTestRouteCacheNegativeTtlMillis",
        default = "defaults::cluster_test_route_cache_negative_ttl_millis"
    )]
    pub cluster_test_route_cache_negative_ttl_millis: u64,

    #[serde(
        alias = "clusterTestRouteCacheMaxEntries",
        default = "defaults::cluster_test_route_cache_max_entries"
    )]
    pub cluster_test_route_cache_max_entries: u64,

    #[serde(
        alias = "clusterTestRouteCacheMaxBytes",
        default = "defaults::cluster_test_route_cache_max_bytes"
    )]
    pub cluster_test_route_cache_max_bytes: u64,

    #[serde(alias = "kvMutationQueueCapacity", default = "defaults::kv_mutation_queue_capacity")]
    pub kv_mutation_queue_capacity: usize,

    #[serde(alias = "kvMutationBatchSize", default = "defaults::kv_mutation_batch_size")]
    pub kv_mutation_batch_size: usize,

    #[serde(
        alias = "unRegisterBrokerBatchSize",
        default = "defaults::unregister_broker_batch_size"
    )]
    pub unregister_broker_batch_size: usize,

    #[serde(
        alias = "unRegisterBrokerBatchTimeMillis",
        default = "defaults::unregister_broker_batch_time_millis"
    )]
    pub unregister_broker_batch_time_millis: u64,

    #[serde(alias = "expiryIndexMode", default)]
    pub expiry_index_mode: ExpiryIndexMode,

    #[serde(
        alias = "expirySafetyScanInterval",
        default = "defaults::expiry_safety_scan_interval"
    )]
    pub expiry_safety_scan_interval: u64,

    #[serde(
        alias = "minBrokerNotifyConcurrency",
        default = "defaults::min_broker_notify_concurrency"
    )]
    pub min_broker_notify_concurrency: usize,

    #[serde(
        alias = "returnOrderTopicConfigToBroker",
        default = "defaults::return_order_topic_config_to_broker"
    )]
    pub return_order_topic_config_to_broker: bool,

    #[serde(
        alias = "clientRequestThreadPoolNums",
        default = "defaults::client_request_thread_pool_nums"
    )]
    pub client_request_thread_pool_nums: i32,

    #[serde(alias = "defaultThreadPoolNums", default = "defaults::default_thread_pool_nums")]
    pub default_thread_pool_nums: i32,

    #[serde(
        alias = "clientRequestThreadPoolQueueCapacity",
        default = "defaults::client_request_thread_pool_queue_capacity"
    )]
    pub client_request_thread_pool_queue_capacity: i32,

    #[serde(
        alias = "defaultThreadPoolQueueCapacity",
        default = "defaults::default_thread_pool_queue_capacity"
    )]
    pub default_thread_pool_queue_capacity: i32,

    #[serde(
        alias = "scanNotActiveBrokerInterval",
        default = "defaults::scan_not_active_broker_interval"
    )]
    pub scan_not_active_broker_interval: u64,

    #[serde(
        alias = "unRegisterBrokerQueueCapacity",
        default = "defaults::unregister_broker_queue_capacity"
    )]
    pub unregister_broker_queue_capacity: i32,

    #[serde(alias = "supportActingMaster", default)]
    pub support_acting_master: bool,

    #[serde(alias = "enableAllTopicList", default = "defaults::enable_all_topic_list")]
    pub enable_all_topic_list: bool,

    #[serde(alias = "enableTopicList", default = "defaults::enable_topic_list")]
    pub enable_topic_list: bool,

    #[serde(alias = "notifyMinBrokerIdChanged", default)]
    pub notify_min_broker_id_changed: bool,

    #[serde(alias = "enableControllerInNamesrv", default)]
    pub enable_controller_in_namesrv: bool,

    #[serde(alias = "needWaitForService", default)]
    pub need_wait_for_service: bool,

    #[serde(alias = "waitSecondsForService", default = "defaults::wait_seconds_for_service")]
    pub wait_seconds_for_service: i32,

    #[serde(alias = "deleteTopicWithBrokerRegistration", default)]
    pub delete_topic_with_broker_registration: bool,

    /// Migration-only escape hatch for an unauthenticated non-loopback listener.
    /// Secure deployments must leave this disabled and select the secure profile.
    #[serde(alias = "allowInsecurePublicListener", default)]
    pub allow_insecure_public_listener: bool,

    /// Shared RocketMQ authentication and authorization configuration.
    #[serde(flatten, default)]
    pub auth_config: AuthConfig,

    #[serde(alias = "configBlackList", default = "defaults::config_black_list")]
    pub config_black_list: String,
}

impl Default for NamesrvConfig {
    fn default() -> Self {
        NamesrvConfig {
            rocketmq_home: defaults::rocketmq_home(),
            observability: rocketmq_observability::ObservabilityOverrides::default(),
            kv_config_path: defaults::kv_config_path(),
            config_store_path: defaults::config_store_path(),
            product_env_name: "center".to_string(),
            cluster_test: false,
            order_message_enable: false,
            route_freshness_sample_interval: defaults::route_freshness_sample_interval(),
            namesrv_typed_zone_route_enable: false,
            namesrv_typed_zone_route_shadow: false,
            namesrv_route_response_cache_enable: false,
            namesrv_route_response_cache_max_bytes: defaults::namesrv_route_response_cache_max_bytes(),
            namesrv_route_response_cache_max_entries: defaults::namesrv_route_response_cache_max_entries(),
            namesrv_route_response_cache_max_single_response_bytes:
                defaults::namesrv_route_response_cache_max_single_response_bytes(),
            namesrv_route_response_cache_shards: defaults::namesrv_route_response_cache_shards(),
            namesrv_workload_admission_enable: true,
            namesrv_workload_admission_observe_only: true,
            namesrv_workload_admission_timeout_millis: defaults::namesrv_workload_admission_timeout_millis(),
            enable_registration_delta: false,
            cluster_test_route_cache_positive_ttl_millis: defaults::cluster_test_route_cache_positive_ttl_millis(),
            cluster_test_route_cache_negative_ttl_millis: defaults::cluster_test_route_cache_negative_ttl_millis(),
            cluster_test_route_cache_max_entries: defaults::cluster_test_route_cache_max_entries(),
            cluster_test_route_cache_max_bytes: defaults::cluster_test_route_cache_max_bytes(),
            kv_mutation_queue_capacity: defaults::kv_mutation_queue_capacity(),
            kv_mutation_batch_size: defaults::kv_mutation_batch_size(),
            unregister_broker_batch_size: defaults::unregister_broker_batch_size(),
            unregister_broker_batch_time_millis: defaults::unregister_broker_batch_time_millis(),
            expiry_index_mode: ExpiryIndexMode::Off,
            expiry_safety_scan_interval: defaults::expiry_safety_scan_interval(),
            min_broker_notify_concurrency: defaults::min_broker_notify_concurrency(),
            return_order_topic_config_to_broker: true,
            client_request_thread_pool_nums: 8,
            default_thread_pool_nums: 16,
            client_request_thread_pool_queue_capacity: 50000,
            default_thread_pool_queue_capacity: 10000,
            scan_not_active_broker_interval: 5 * 1000,
            unregister_broker_queue_capacity: 3000,
            support_acting_master: false,
            enable_all_topic_list: true,
            enable_topic_list: true,
            notify_min_broker_id_changed: false,
            enable_controller_in_namesrv: false,
            need_wait_for_service: false,
            wait_seconds_for_service: 45,
            delete_topic_with_broker_registration: false,
            allow_insecure_public_listener: false,
            auth_config: AuthConfig::default(),
            config_black_list: "configBlackList;configStorePath;kvConfigPath".to_string(),
        }
    }
}

impl NamesrvConfig {
    pub fn new() -> NamesrvConfig {
        Self::default()
    }

    /// Returns a JSON string representation of the NamesrvConfig.
    /// Compatible with Java version
    pub fn get_all_configs_format_string(&self) -> Result<String, String> {
        let mut json_map = HashMap::new();
        json_map.insert("rocketmqHome".to_string(), Value::String(self.rocketmq_home.clone()));
        json_map.insert("kvConfigPath".to_string(), Value::String(self.kv_config_path.clone()));
        json_map.insert(
            "configStorePath".to_string(),
            Value::String(self.config_store_path.clone()),
        );
        json_map.insert(
            "productEnvName".to_string(),
            Value::String(self.product_env_name.clone()),
        );
        json_map.insert("clusterTest".to_string(), Value::String(self.cluster_test.to_string()));
        json_map.insert(
            "orderMessageEnable".to_string(),
            Value::String(self.order_message_enable.to_string()),
        );
        json_map.insert(
            "routeFreshnessSampleInterval".to_string(),
            Value::String(self.route_freshness_sample_interval.to_string()),
        );
        json_map.insert(
            "namesrvTypedZoneRouteEnable".to_string(),
            Value::String(self.namesrv_typed_zone_route_enable.to_string()),
        );
        json_map.insert(
            "namesrvTypedZoneRouteShadow".to_string(),
            Value::String(self.namesrv_typed_zone_route_shadow.to_string()),
        );
        json_map.insert(
            "namesrvRouteResponseCacheEnable".to_string(),
            Value::String(self.namesrv_route_response_cache_enable.to_string()),
        );
        json_map.insert(
            "namesrvRouteResponseCacheMaxBytes".to_string(),
            Value::String(self.namesrv_route_response_cache_max_bytes.to_string()),
        );
        json_map.insert(
            "namesrvRouteResponseCacheMaxEntries".to_string(),
            Value::String(self.namesrv_route_response_cache_max_entries.to_string()),
        );
        json_map.insert(
            "namesrvRouteResponseCacheMaxSingleResponseBytes".to_string(),
            Value::String(self.namesrv_route_response_cache_max_single_response_bytes.to_string()),
        );
        json_map.insert(
            "namesrvRouteResponseCacheShards".to_string(),
            Value::String(self.namesrv_route_response_cache_shards.to_string()),
        );
        json_map.insert(
            "namesrvWorkloadAdmissionEnable".to_string(),
            Value::String(self.namesrv_workload_admission_enable.to_string()),
        );
        json_map.insert(
            "namesrvWorkloadAdmissionObserveOnly".to_string(),
            Value::String(self.namesrv_workload_admission_observe_only.to_string()),
        );
        json_map.insert(
            "namesrvWorkloadAdmissionTimeoutMillis".to_string(),
            Value::String(self.namesrv_workload_admission_timeout_millis.to_string()),
        );
        json_map.insert(
            "enableRegistrationDelta".to_string(),
            Value::String(self.enable_registration_delta.to_string()),
        );
        json_map.insert(
            "clusterTestRouteCachePositiveTtlMillis".to_string(),
            Value::String(self.cluster_test_route_cache_positive_ttl_millis.to_string()),
        );
        json_map.insert(
            "clusterTestRouteCacheNegativeTtlMillis".to_string(),
            Value::String(self.cluster_test_route_cache_negative_ttl_millis.to_string()),
        );
        json_map.insert(
            "clusterTestRouteCacheMaxEntries".to_string(),
            Value::String(self.cluster_test_route_cache_max_entries.to_string()),
        );
        json_map.insert(
            "clusterTestRouteCacheMaxBytes".to_string(),
            Value::String(self.cluster_test_route_cache_max_bytes.to_string()),
        );
        json_map.insert(
            "kvMutationQueueCapacity".to_string(),
            Value::String(self.kv_mutation_queue_capacity.to_string()),
        );
        json_map.insert(
            "kvMutationBatchSize".to_string(),
            Value::String(self.kv_mutation_batch_size.to_string()),
        );
        json_map.insert(
            "unRegisterBrokerBatchSize".to_string(),
            Value::String(self.unregister_broker_batch_size.to_string()),
        );
        json_map.insert(
            "unRegisterBrokerBatchTimeMillis".to_string(),
            Value::String(self.unregister_broker_batch_time_millis.to_string()),
        );
        json_map.insert(
            "expiryIndexMode".to_string(),
            Value::String(self.expiry_index_mode.as_str().to_string()),
        );
        json_map.insert(
            "expirySafetyScanInterval".to_string(),
            Value::String(self.expiry_safety_scan_interval.to_string()),
        );
        json_map.insert(
            "minBrokerNotifyConcurrency".to_string(),
            Value::String(self.min_broker_notify_concurrency.to_string()),
        );
        json_map.insert(
            "returnOrderTopicConfigToBroker".to_string(),
            Value::String(self.return_order_topic_config_to_broker.to_string()),
        );
        json_map.insert(
            "clientRequestThreadPoolNums".to_string(),
            Value::String(self.client_request_thread_pool_nums.to_string()),
        );
        json_map.insert(
            "defaultThreadPoolNums".to_string(),
            Value::String(self.default_thread_pool_nums.to_string()),
        );
        json_map.insert(
            "clientRequestThreadPoolQueueCapacity".to_string(),
            Value::String(self.client_request_thread_pool_queue_capacity.to_string()),
        );
        json_map.insert(
            "defaultThreadPoolQueueCapacity".to_string(),
            Value::String(self.default_thread_pool_queue_capacity.to_string()),
        );
        json_map.insert(
            "scanNotActiveBrokerInterval".to_string(),
            Value::String(self.scan_not_active_broker_interval.to_string()),
        );
        json_map.insert(
            "unRegisterBrokerQueueCapacity".to_string(),
            Value::String(self.unregister_broker_queue_capacity.to_string()),
        );
        json_map.insert(
            "supportActingMaster".to_string(),
            Value::String(self.support_acting_master.to_string()),
        );
        json_map.insert(
            "enableAllTopicList".to_string(),
            Value::String(self.enable_all_topic_list.to_string()),
        );
        json_map.insert(
            "enableTopicList".to_string(),
            Value::String(self.enable_topic_list.to_string()),
        );
        json_map.insert(
            "notifyMinBrokerIdChanged".to_string(),
            Value::String(self.notify_min_broker_id_changed.to_string()),
        );
        json_map.insert(
            "enableControllerInNamesrv".to_string(),
            Value::String(self.enable_controller_in_namesrv.to_string()),
        );
        json_map.insert(
            "needWaitForService".to_string(),
            Value::String(self.need_wait_for_service.to_string()),
        );
        json_map.insert(
            "waitSecondsForService".to_string(),
            Value::String(self.wait_seconds_for_service.to_string()),
        );
        json_map.insert(
            "deleteTopicWithBrokerRegistration".to_string(),
            Value::String(self.delete_topic_with_broker_registration.to_string()),
        );
        json_map.insert(
            "allowInsecurePublicListener".to_string(),
            Value::String(self.allow_insecure_public_listener.to_string()),
        );
        json_map.insert(
            "authenticationEnabled".to_string(),
            Value::String(self.auth_config.authentication_enabled.to_string()),
        );
        json_map.insert(
            "authorizationEnabled".to_string(),
            Value::String(self.auth_config.authorization_enabled.to_string()),
        );
        json_map.insert(
            "configBlackList".to_string(),
            Value::String(self.config_black_list.clone()),
        );

        // Convert the HashMap to a JSON value
        match SerdeJsonUtils::serialize_json(&json_map) {
            Ok(json) => Ok(json),
            Err(err) => Err(format!("Failed to serialize NamesrvConfig: {err}")),
        }
    }

    /// Splits the `config_black_list` into a `Vec<CheetahString>` for easier usage.
    pub fn get_config_blacklist(&self) -> Vec<CheetahString> {
        self.config_black_list
            .split(';')
            .map(|s| CheetahString::from(s.trim()))
            .collect()
    }

    pub fn update(&mut self, properties: HashMap<CheetahString, CheetahString>) -> RocketMQResult<()> {
        let mut candidate = self.clone();
        candidate.apply_updates(properties)?;
        candidate.validate_domains()?;
        *self = candidate;
        Ok(())
    }

    /// Applies only NameServer-owned keys from a composite persisted runtime
    /// snapshot. Listener and transport keys are returned to their owning
    /// configuration components by the startup loader.
    pub fn update_known_properties(
        &mut self,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> RocketMQResult<()> {
        let namesrv_properties = properties
            .iter()
            .filter(|(key, _)| NamesrvConfigKey::from_java_name(key.as_str()).is_some())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.update(namesrv_properties)
    }

    #[must_use]
    pub fn is_known_property(key: &str) -> bool {
        NamesrvConfigKey::from_java_name(key).is_some()
    }

    fn apply_updates(&mut self, properties: HashMap<CheetahString, CheetahString>) -> RocketMQResult<()> {
        for (key, value) in properties {
            reject_removed_route_manager_key(key.as_str())?;
            let config_key = NamesrvConfigKey::from_java_name(key.as_str()).ok_or_else(|| {
                RocketMQError::nameserver_config_invalid(format!("unknown configuration key '{key}'"))
            })?;
            validate_namesrv_property(config_key, value.as_str())?;
            match config_key {
                NamesrvConfigKey::RocketmqHome => self.rocketmq_home = value.to_string(),
                NamesrvConfigKey::KvConfigPath => self.kv_config_path = value.to_string(),
                NamesrvConfigKey::ConfigStorePath => self.config_store_path = value.to_string(),
                NamesrvConfigKey::ProductEnvName => self.product_env_name = value.to_string(),
                NamesrvConfigKey::ClusterTest => {
                    self.cluster_test = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::OrderMessageEnable => {
                    self.order_message_enable = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::RouteFreshnessSampleInterval => {
                    self.route_freshness_sample_interval =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::NamesrvTypedZoneRouteEnable => {
                    self.namesrv_typed_zone_route_enable =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NamesrvTypedZoneRouteShadow => {
                    self.namesrv_typed_zone_route_shadow =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NamesrvRouteResponseCacheEnable => {
                    self.namesrv_route_response_cache_enable =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxBytes => {
                    self.namesrv_route_response_cache_max_bytes =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxEntries => {
                    self.namesrv_route_response_cache_max_entries =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxSingleResponseBytes => {
                    self.namesrv_route_response_cache_max_single_response_bytes =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::NamesrvRouteResponseCacheShards => {
                    self.namesrv_route_response_cache_shards =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::NamesrvWorkloadAdmissionEnable => {
                    self.namesrv_workload_admission_enable =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NamesrvWorkloadAdmissionObserveOnly => {
                    self.namesrv_workload_admission_observe_only =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NamesrvWorkloadAdmissionTimeoutMillis => {
                    self.namesrv_workload_admission_timeout_millis =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::EnableRegistrationDelta => {
                    self.enable_registration_delta =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::ClusterTestRouteCachePositiveTtlMillis => {
                    self.cluster_test_route_cache_positive_ttl_millis =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ClusterTestRouteCacheNegativeTtlMillis => {
                    self.cluster_test_route_cache_negative_ttl_millis =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ClusterTestRouteCacheMaxEntries => {
                    self.cluster_test_route_cache_max_entries =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ClusterTestRouteCacheMaxBytes => {
                    self.cluster_test_route_cache_max_bytes =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::KvMutationQueueCapacity => {
                    self.kv_mutation_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::KvMutationBatchSize => {
                    self.kv_mutation_batch_size =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::UnregisterBrokerBatchSize => {
                    self.unregister_broker_batch_size =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::UnregisterBrokerBatchTimeMillis => {
                    self.unregister_broker_batch_time_millis =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ExpiryIndexMode => {
                    self.expiry_index_mode = value
                        .parse()
                        .map_err(|_| invalid_value(&key, "expected one of off, shadow, active"))?
                }
                NamesrvConfigKey::ExpirySafetyScanInterval => {
                    self.expiry_safety_scan_interval =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::MinBrokerNotifyConcurrency => {
                    self.min_broker_notify_concurrency =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ReturnOrderTopicConfigToBroker => {
                    self.return_order_topic_config_to_broker =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::ClientRequestThreadPoolNums => {
                    self.client_request_thread_pool_nums =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::DefaultThreadPoolNums => {
                    self.default_thread_pool_nums =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ClientRequestThreadPoolQueueCapacity => {
                    self.client_request_thread_pool_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::DefaultThreadPoolQueueCapacity => {
                    self.default_thread_pool_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::ScanNotActiveBrokerInterval => {
                    self.scan_not_active_broker_interval =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::UnregisterBrokerQueueCapacity => {
                    self.unregister_broker_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::SupportActingMaster => {
                    self.support_acting_master = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::EnableAllTopicList => {
                    self.enable_all_topic_list = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::EnableTopicList => {
                    self.enable_topic_list = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NotifyMinBrokerIdChanged => {
                    self.notify_min_broker_id_changed =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::EnableControllerInNamesrv => {
                    self.enable_controller_in_namesrv =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::NeedWaitForService => {
                    self.need_wait_for_service = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::WaitSecondsForService => {
                    self.wait_seconds_for_service =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                NamesrvConfigKey::DeleteTopicWithBrokerRegistration => {
                    self.delete_topic_with_broker_registration =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::AllowInsecurePublicListener => {
                    self.allow_insecure_public_listener =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                NamesrvConfigKey::ConfigBlackList => {
                    self.config_black_list = value.to_string();
                }
            }
        }

        Ok(())
    }

    pub(crate) fn validate_domains(&self) -> RocketMQResult<()> {
        for (key, value) in [
            (
                NamesrvConfigKey::ClientRequestThreadPoolNums,
                self.client_request_thread_pool_nums.to_string(),
            ),
            (
                NamesrvConfigKey::DefaultThreadPoolNums,
                self.default_thread_pool_nums.to_string(),
            ),
            (
                NamesrvConfigKey::ClientRequestThreadPoolQueueCapacity,
                self.client_request_thread_pool_queue_capacity.to_string(),
            ),
            (
                NamesrvConfigKey::DefaultThreadPoolQueueCapacity,
                self.default_thread_pool_queue_capacity.to_string(),
            ),
            (
                NamesrvConfigKey::ScanNotActiveBrokerInterval,
                self.scan_not_active_broker_interval.to_string(),
            ),
            (
                NamesrvConfigKey::UnregisterBrokerQueueCapacity,
                self.unregister_broker_queue_capacity.to_string(),
            ),
            (
                NamesrvConfigKey::WaitSecondsForService,
                self.wait_seconds_for_service.to_string(),
            ),
            (
                NamesrvConfigKey::RouteFreshnessSampleInterval,
                self.route_freshness_sample_interval.to_string(),
            ),
            (
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxBytes,
                self.namesrv_route_response_cache_max_bytes.to_string(),
            ),
            (
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxEntries,
                self.namesrv_route_response_cache_max_entries.to_string(),
            ),
            (
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxSingleResponseBytes,
                self.namesrv_route_response_cache_max_single_response_bytes.to_string(),
            ),
            (
                NamesrvConfigKey::NamesrvRouteResponseCacheShards,
                self.namesrv_route_response_cache_shards.to_string(),
            ),
            (
                NamesrvConfigKey::NamesrvWorkloadAdmissionTimeoutMillis,
                self.namesrv_workload_admission_timeout_millis.to_string(),
            ),
            (
                NamesrvConfigKey::ClusterTestRouteCachePositiveTtlMillis,
                self.cluster_test_route_cache_positive_ttl_millis.to_string(),
            ),
            (
                NamesrvConfigKey::ClusterTestRouteCacheNegativeTtlMillis,
                self.cluster_test_route_cache_negative_ttl_millis.to_string(),
            ),
            (
                NamesrvConfigKey::ClusterTestRouteCacheMaxEntries,
                self.cluster_test_route_cache_max_entries.to_string(),
            ),
            (
                NamesrvConfigKey::ClusterTestRouteCacheMaxBytes,
                self.cluster_test_route_cache_max_bytes.to_string(),
            ),
            (
                NamesrvConfigKey::KvMutationQueueCapacity,
                self.kv_mutation_queue_capacity.to_string(),
            ),
            (
                NamesrvConfigKey::KvMutationBatchSize,
                self.kv_mutation_batch_size.to_string(),
            ),
            (
                NamesrvConfigKey::UnregisterBrokerBatchSize,
                self.unregister_broker_batch_size.to_string(),
            ),
            (
                NamesrvConfigKey::UnregisterBrokerBatchTimeMillis,
                self.unregister_broker_batch_time_millis.to_string(),
            ),
            (
                NamesrvConfigKey::ExpiryIndexMode,
                self.expiry_index_mode.as_str().to_string(),
            ),
            (
                NamesrvConfigKey::ExpirySafetyScanInterval,
                self.expiry_safety_scan_interval.to_string(),
            ),
            (
                NamesrvConfigKey::MinBrokerNotifyConcurrency,
                self.min_broker_notify_concurrency.to_string(),
            ),
        ] {
            validate_namesrv_property(key, &value)?;
        }
        if self.namesrv_route_response_cache_max_single_response_bytes > self.namesrv_route_response_cache_max_bytes {
            return Err(invalid_value(
                NamesrvConfigKey::NamesrvRouteResponseCacheMaxSingleResponseBytes.java_name(),
                "must not exceed namesrvRouteResponseCacheMaxBytes",
            ));
        }
        Ok(())
    }

    pub(crate) fn unregister_broker_queue_capacity(&self) -> RocketMQResult<usize> {
        validate_namesrv_property(
            NamesrvConfigKey::UnregisterBrokerQueueCapacity,
            &self.unregister_broker_queue_capacity.to_string(),
        )?;
        usize::try_from(self.unregister_broker_queue_capacity).map_err(|_| {
            invalid_value(
                NamesrvConfigKey::UnregisterBrokerQueueCapacity.java_name(),
                "must fit the platform channel capacity",
            )
        })
    }

    /// Builds the bounded registration decoder policy owned by NameServer.
    ///
    /// These compatibility limits deliberately remain startup constants in P0;
    /// chunked registration and its generation protocol are introduced in P2.
    pub(crate) fn register_broker_decode_limits(&self) -> RegisterBrokerDecodeLimits {
        RegisterBrokerDecodeLimits::default()
    }
}

fn invalid_value(key: &str, reason: &str) -> RocketMQError {
    RocketMQError::nameserver_config_invalid(format!("invalid value for '{key}': {reason}"))
}

pub fn reject_removed_route_manager_key(key: &str) -> RocketMQResult<()> {
    let key = key.trim();
    if key == REMOVED_ROUTE_MANAGER_CONFIG_KEY || key == REMOVED_ROUTE_MANAGER_CONFIG_FIELD {
        return Err(RocketMQError::nameserver_config_invalid(format!(
            "'{key}' was removed; NameServer now always uses the canonical route manager"
        )));
    }
    Ok(())
}

pub fn reject_removed_transport_client_key(key: &str) -> RocketMQResult<()> {
    const REMOVED_KEYS: &[&str] = &[
        "clientWorkerThreads",
        "clientCallbackExecutorThreads",
        "clientOnewaySemaphoreValue",
        "clientAsyncSemaphoreValue",
        "clientChannelMaxIdleTimeSeconds",
        "clientSocketSndBufSize",
        "clientSocketRcvBufSize",
        "clientPooledByteBufAllocatorEnable",
        "clientCloseSocketIfTimeout",
        "useTls",
        "socksProxyConfig",
        "writeBufferHighWaterMark",
        "writeBufferLowWaterMark",
        "disableCallbackExecutor",
        "disableNettyWorkerGroup",
        "maxReconnectIntervalTimeSeconds",
        "enableReconnectForGoAway",
        "enableTransparentRetry",
    ];
    let key = key.trim();
    if REMOVED_KEYS.contains(&key) {
        return Err(RocketMQError::nameserver_config_invalid(format!(
            "'{key}' was removed because the Tokio transport never implemented its advertised Netty-style behavior"
        )));
    }
    Ok(())
}

pub fn validate_namesrv_config_source(source: &str) -> RocketMQResult<()> {
    for line in source.lines() {
        let candidate = line
            .split_once('#')
            .map_or(line, |(value, _)| value)
            .split_once('=')
            .or_else(|| line.split_once(':'))
            .map(|(key, _)| key.trim().trim_matches('"').trim_matches('\''));
        if let Some(key) = candidate {
            reject_removed_route_manager_key(key)?;
            reject_removed_transport_client_key(key)?;
        }
    }
    Ok(())
}

#[must_use]
pub fn is_tls_config_key(key: &str) -> bool {
    matches!(
        key,
        "tls.enable"
            | "tls.test.mode.enable"
            | "tls.config.file"
            | "tls.server.mode"
            | "tls.server.need.client.auth"
            | "tls.server.keyPath"
            | "tls.server.keyPassword"
            | "tls.server.certPath"
            | "tls.server.authClient"
            | "tls.server.trustCertPath"
            | "tls.client.keyPath"
            | "tls.client.keyPassword"
            | "tls.client.certPath"
            | "tls.client.authServer"
            | "tls.client.trustCertPath"
            | "tls.ciphers"
            | "tls.protocols"
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use rocketmq_model::common::mix_all::ROCKETMQ_HOME_ENV;
    use rocketmq_model::common::mix_all::ROCKETMQ_HOME_PROPERTY;

    #[test]
    fn test_namesrv_config() {
        let config = NamesrvConfig::new();

        assert_eq!(
            config.rocketmq_home,
            std::env::var(ROCKETMQ_HOME_PROPERTY)
                .unwrap_or_else(|_| std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_default())
        );
        assert_eq!(
            config.kv_config_path,
            format!(
                "{}{}rocketmq-namesrv{}kvConfig.json",
                dirs::home_dir().unwrap().to_str().unwrap(),
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
        );
        assert_eq!(
            config.config_store_path,
            format!(
                "{}{}rocketmq-namesrv{}rocketmq-namesrv.properties",
                dirs::home_dir().unwrap().to_str().unwrap(),
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
        );
        assert_eq!(config.product_env_name, "center");
        assert!(!config.cluster_test);
        assert!(!config.order_message_enable);
        assert_eq!(config.route_freshness_sample_interval, 1000);
        assert!(!config.namesrv_typed_zone_route_enable);
        assert!(!config.namesrv_typed_zone_route_shadow);
        assert!(!config.namesrv_route_response_cache_enable);
        assert_eq!(config.namesrv_route_response_cache_max_bytes, 67_108_864);
        assert_eq!(config.namesrv_route_response_cache_max_entries, 10_000);
        assert_eq!(config.namesrv_route_response_cache_max_single_response_bytes, 1_048_576);
        assert_eq!(config.namesrv_route_response_cache_shards, 16);
        assert!(config.namesrv_workload_admission_enable);
        assert!(config.namesrv_workload_admission_observe_only);
        assert_eq!(config.namesrv_workload_admission_timeout_millis, 100);
        assert!(!config.enable_registration_delta);
        assert_eq!(config.cluster_test_route_cache_positive_ttl_millis, 1_000);
        assert_eq!(config.cluster_test_route_cache_negative_ttl_millis, 250);
        assert_eq!(config.cluster_test_route_cache_max_entries, 1_000);
        assert_eq!(config.cluster_test_route_cache_max_bytes, 16 * 1024 * 1024);
        assert_eq!(config.kv_mutation_queue_capacity, 1024);
        assert_eq!(config.kv_mutation_batch_size, 100);
        assert_eq!(config.unregister_broker_batch_size, 100);
        assert_eq!(config.unregister_broker_batch_time_millis, 2);
        assert_eq!(config.expiry_index_mode, ExpiryIndexMode::Off);
        assert_eq!(config.expiry_safety_scan_interval, 300_000);
        assert_eq!(config.min_broker_notify_concurrency, 8);
        assert!(config.return_order_topic_config_to_broker);
        assert_eq!(config.client_request_thread_pool_nums, 8);
        assert_eq!(config.default_thread_pool_nums, 16);
        assert_eq!(config.client_request_thread_pool_queue_capacity, 50000);
        assert_eq!(config.default_thread_pool_queue_capacity, 10000);
        assert_eq!(config.scan_not_active_broker_interval, 5 * 1000);
        assert_eq!(config.unregister_broker_queue_capacity, 3000);
        assert!(!config.support_acting_master);
        assert!(config.enable_all_topic_list);
        assert!(config.enable_topic_list);
        assert!(!config.notify_min_broker_id_changed);
        assert!(!config.enable_controller_in_namesrv);
        assert!(!config.need_wait_for_service);
        assert_eq!(config.wait_seconds_for_service, 45);
        assert!(!config.delete_topic_with_broker_registration);
        assert!(!config.allow_insecure_public_listener);
        assert!(!config.auth_config.authentication_enabled);
        assert!(!config.auth_config.authorization_enabled);
        assert_eq!(
            config.config_black_list,
            "configBlackList;configStorePath;kvConfigPath".to_string()
        );
    }

    #[test]
    fn test_namesrv_config_update() {
        let mut config = NamesrvConfig::new();

        let mut properties = HashMap::new();
        properties.insert(CheetahString::from("rocketmqHome"), CheetahString::from("/new/path"));
        properties.insert(
            CheetahString::from("kvConfigPath"),
            CheetahString::from("/new/kvConfigPath"),
        );
        properties.insert(
            CheetahString::from("configStorePath"),
            CheetahString::from("/new/configStorePath"),
        );
        properties.insert(CheetahString::from("productEnvName"), CheetahString::from("new_env"));
        properties.insert(CheetahString::from("clusterTest"), CheetahString::from("true"));
        properties.insert(CheetahString::from("orderMessageEnable"), CheetahString::from("true"));
        properties.insert(
            CheetahString::from("routeFreshnessSampleInterval"),
            CheetahString::from("250"),
        );
        properties.insert(
            CheetahString::from("namesrvWorkloadAdmissionEnable"),
            CheetahString::from("false"),
        );
        properties.insert(
            CheetahString::from("namesrvWorkloadAdmissionObserveOnly"),
            CheetahString::from("false"),
        );
        properties.insert(
            CheetahString::from("namesrvWorkloadAdmissionTimeoutMillis"),
            CheetahString::from("500"),
        );
        properties.insert(
            CheetahString::from("kvMutationQueueCapacity"),
            CheetahString::from("2048"),
        );
        properties.insert(CheetahString::from("kvMutationBatchSize"), CheetahString::from("64"));
        properties.insert(
            CheetahString::from("clientRequestThreadPoolNums"),
            CheetahString::from("10"),
        );
        properties.insert(CheetahString::from("defaultThreadPoolNums"), CheetahString::from("20"));
        properties.insert(
            CheetahString::from("clientRequestThreadPoolQueueCapacity"),
            CheetahString::from("10000"),
        );
        properties.insert(
            CheetahString::from("defaultThreadPoolQueueCapacity"),
            CheetahString::from("20000"),
        );
        properties.insert(
            CheetahString::from("scanNotActiveBrokerInterval"),
            CheetahString::from("15000"),
        );
        properties.insert(
            CheetahString::from("unRegisterBrokerQueueCapacity"),
            CheetahString::from("4000"),
        );
        properties.insert(CheetahString::from("supportActingMaster"), CheetahString::from("true"));
        properties.insert(CheetahString::from("enableAllTopicList"), CheetahString::from("false"));
        properties.insert(CheetahString::from("enableTopicList"), CheetahString::from("false"));
        properties.insert(
            CheetahString::from("notifyMinBrokerIdChanged"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("enableControllerInNamesrv"),
            CheetahString::from("true"),
        );
        properties.insert(CheetahString::from("needWaitForService"), CheetahString::from("true"));
        properties.insert(CheetahString::from("waitSecondsForService"), CheetahString::from("30"));
        properties.insert(
            CheetahString::from("deleteTopicWithBrokerRegistration"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("allowInsecurePublicListener"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("configBlackList"),
            CheetahString::from("newBlackList"),
        );

        let result = config.update(properties);
        assert!(result.is_ok());

        assert_eq!(config.rocketmq_home, "/new/path");
        assert_eq!(config.kv_config_path, "/new/kvConfigPath");
        assert_eq!(config.config_store_path, "/new/configStorePath");
        assert_eq!(config.product_env_name, "new_env");
        assert!(config.cluster_test);
        assert!(config.order_message_enable);
        assert_eq!(config.route_freshness_sample_interval, 250);
        assert!(!config.namesrv_workload_admission_enable);
        assert!(!config.namesrv_workload_admission_observe_only);
        assert_eq!(config.namesrv_workload_admission_timeout_millis, 500);
        assert_eq!(config.kv_mutation_queue_capacity, 2048);
        assert_eq!(config.kv_mutation_batch_size, 64);
        assert_eq!(config.client_request_thread_pool_nums, 10);
        assert_eq!(config.default_thread_pool_nums, 20);
        assert_eq!(config.client_request_thread_pool_queue_capacity, 10000);
        assert_eq!(config.default_thread_pool_queue_capacity, 20000);
        assert_eq!(config.scan_not_active_broker_interval, 15000);
        assert_eq!(config.unregister_broker_queue_capacity, 4000);
        assert!(config.support_acting_master);
        assert!(!config.enable_all_topic_list);
        assert!(!config.enable_topic_list);
        assert!(config.notify_min_broker_id_changed);
        assert!(config.enable_controller_in_namesrv);
        assert!(config.need_wait_for_service);
        assert_eq!(config.wait_seconds_for_service, 30);
        assert!(config.delete_topic_with_broker_registration);
        assert!(config.allow_insecure_public_listener);
        assert_eq!(config.config_black_list, "newBlackList");
    }

    #[test]
    fn test_get_all_configs_format_string() {
        let config = NamesrvConfig::new();

        let json_output = config.get_all_configs_format_string().unwrap();

        assert!(!json_output.is_empty(), "JSON output should not be empty");

        let parsed: serde_json::Value = serde_json::from_str(&json_output).expect("Output should be valid JSON");

        assert_eq!(parsed["rocketmqHome"], config.rocketmq_home);
        assert_eq!(parsed["kvConfigPath"], config.kv_config_path);
        assert_eq!(parsed["configStorePath"], config.config_store_path);
        assert_eq!(parsed["productEnvName"], config.product_env_name);
        assert_eq!(parsed["clusterTest"].as_str().unwrap(), config.cluster_test.to_string());
        assert_eq!(
            parsed["orderMessageEnable"].as_str().unwrap(),
            config.order_message_enable.to_string()
        );
        assert_eq!(
            parsed["routeFreshnessSampleInterval"].as_str().unwrap(),
            config.route_freshness_sample_interval.to_string()
        );
        assert_eq!(
            parsed["kvMutationQueueCapacity"].as_str().unwrap(),
            config.kv_mutation_queue_capacity.to_string()
        );
        assert_eq!(
            parsed["kvMutationBatchSize"].as_str().unwrap(),
            config.kv_mutation_batch_size.to_string()
        );
        assert_eq!(
            parsed["returnOrderTopicConfigToBroker"].as_str().unwrap(),
            config.return_order_topic_config_to_broker.to_string()
        );
        assert_eq!(
            parsed["clientRequestThreadPoolNums"].as_str().unwrap(),
            config.client_request_thread_pool_nums.to_string()
        );
        assert_eq!(
            parsed["defaultThreadPoolNums"].as_str().unwrap(),
            config.default_thread_pool_nums.to_string()
        );
        assert_eq!(
            parsed["clientRequestThreadPoolQueueCapacity"].as_str().unwrap(),
            config.client_request_thread_pool_queue_capacity.to_string()
        );
        assert_eq!(
            parsed["defaultThreadPoolQueueCapacity"].as_str().unwrap(),
            config.default_thread_pool_queue_capacity.to_string()
        );
        assert_eq!(
            parsed["scanNotActiveBrokerInterval"].as_str().unwrap(),
            config.scan_not_active_broker_interval.to_string()
        );
        assert_eq!(
            parsed["unRegisterBrokerQueueCapacity"].as_str().unwrap(),
            config.unregister_broker_queue_capacity.to_string()
        );
        assert_eq!(
            parsed["supportActingMaster"].as_str().unwrap(),
            config.support_acting_master.to_string()
        );
        assert_eq!(
            parsed["enableAllTopicList"].as_str().unwrap(),
            config.enable_all_topic_list.to_string()
        );
        assert_eq!(
            parsed["enableTopicList"].as_str().unwrap(),
            config.enable_topic_list.to_string()
        );
        assert_eq!(
            parsed["notifyMinBrokerIdChanged"].as_str().unwrap(),
            config.notify_min_broker_id_changed.to_string()
        );
        assert_eq!(
            parsed["enableControllerInNamesrv"].as_str().unwrap(),
            config.enable_controller_in_namesrv.to_string()
        );
        assert_eq!(
            parsed["needWaitForService"].as_str().unwrap(),
            config.need_wait_for_service.to_string()
        );
        assert_eq!(
            parsed["waitSecondsForService"].as_str().unwrap(),
            config.wait_seconds_for_service.to_string()
        );
        assert_eq!(
            parsed["deleteTopicWithBrokerRegistration"].as_str().unwrap(),
            config.delete_topic_with_broker_registration.to_string()
        );
        assert_eq!(
            parsed["allowInsecurePublicListener"].as_str().unwrap(),
            config.allow_insecure_public_listener.to_string()
        );
        assert_eq!(
            parsed["authenticationEnabled"].as_str().unwrap(),
            config.auth_config.authentication_enabled.to_string()
        );
        assert_eq!(
            parsed["authorizationEnabled"].as_str().unwrap(),
            config.auth_config.authorization_enabled.to_string()
        );
        assert_eq!(parsed["configBlackList"], config.config_black_list);
    }

    #[test]
    fn namesrv_config_schema_keys_are_unique_round_trip_and_exported() {
        let config = NamesrvConfig::new();
        let exported: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            &config
                .get_all_configs_format_string()
                .expect("default NameServer configuration should serialize"),
        )
        .expect("configuration export should be a JSON object");
        let mut names = HashSet::new();

        for key in NamesrvConfigKey::ALL {
            let java_name = key.java_name();
            assert!(names.insert(java_name), "duplicate NameServer schema key: {java_name}");
            assert_eq!(NamesrvConfigKey::from_java_name(java_name), Some(key));
            assert!(
                exported.contains_key(java_name),
                "schema key is not exported: {java_name}"
            );
            let _ = key.mutability();
        }
    }

    #[test]
    fn removed_route_manager_switch_is_a_typed_config_error() {
        let mut config = NamesrvConfig::default();
        let error = config
            .update(HashMap::from([(
                CheetahString::from_static_str(REMOVED_ROUTE_MANAGER_CONFIG_KEY),
                CheetahString::from_static_str("false"),
            )]))
            .expect_err("removed switch must fail");

        assert!(matches!(
            error,
            RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
    }

    #[test]
    fn source_validation_ignores_comments_and_values() {
        validate_namesrv_config_source(
            r#"
# useRouteInfoManagerV2 = false
productEnvName = "useRouteInfoManagerV2"
"#,
        )
        .expect("comments and values must not be interpreted as keys");
    }

    #[test]
    fn removed_transport_client_field_is_a_typed_config_error() {
        let error = validate_namesrv_config_source("clientWorkerThreads = 4")
            .expect_err("removed transport client field must fail");

        assert!(matches!(
            error,
            RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
        assert!(error.to_string().contains("clientWorkerThreads"));
    }

    #[test]
    fn rejects_zero_unregister_queue_capacity() {
        let mut config = NamesrvConfig::default();
        let error = config
            .update(HashMap::from([(
                CheetahString::from_static_str("unRegisterBrokerQueueCapacity"),
                CheetahString::from_static_str("0"),
            )]))
            .expect_err("zero-capacity channel must be rejected before construction");

        assert!(matches!(
            error,
            RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
    }

    #[test]
    fn rejects_invalid_write_recovery_rollout_values() {
        for (key, value) in [
            (NamesrvConfigKey::UnregisterBrokerBatchSize, "0"),
            (NamesrvConfigKey::UnregisterBrokerBatchSize, "1025"),
            (NamesrvConfigKey::UnregisterBrokerBatchTimeMillis, "0"),
            (NamesrvConfigKey::UnregisterBrokerBatchTimeMillis, "51"),
            (NamesrvConfigKey::ExpiryIndexMode, "enabled"),
            (NamesrvConfigKey::ExpirySafetyScanInterval, "29999"),
            (NamesrvConfigKey::MinBrokerNotifyConcurrency, "129"),
            (NamesrvConfigKey::KvMutationQueueCapacity, "0"),
            (NamesrvConfigKey::KvMutationQueueCapacity, "1000001"),
            (NamesrvConfigKey::KvMutationBatchSize, "0"),
            (NamesrvConfigKey::KvMutationBatchSize, "1025"),
            (NamesrvConfigKey::ClusterTestRouteCachePositiveTtlMillis, "0"),
            (NamesrvConfigKey::ClusterTestRouteCacheNegativeTtlMillis, "60001"),
            (NamesrvConfigKey::ClusterTestRouteCacheMaxEntries, "1000001"),
            (NamesrvConfigKey::ClusterTestRouteCacheMaxBytes, "1073741825"),
        ] {
            assert!(validate_namesrv_property(key, value).is_err(), "{key:?}={value}");
            assert_eq!(key.mutability(), ConfigMutability::RestartRequired);
        }
    }
}
