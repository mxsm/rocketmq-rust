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
use std::env;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::mix_all::ROCKETMQ_HOME_ENV;
use rocketmq_model::common::mix_all::ROCKETMQ_HOME_PROPERTY;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use serde::Deserialize;
use serde_json::Value;

pub const REMOVED_ROUTE_MANAGER_CONFIG_KEY: &str = "useRouteInfoManagerV2";
const REMOVED_ROUTE_MANAGER_CONFIG_FIELD: &str = "use_route_info_manager_v2";

/// Default value functions for serde deserialization
mod defaults {
    use super::*;

    pub fn rocketmq_home() -> String {
        env::var(ROCKETMQ_HOME_PROPERTY).unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default())
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

    pub fn config_black_list() -> String {
        "configBlackList;configStorePath;kvConfigPath".to_string()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamesrvConfig {
    #[serde(alias = "rocketmqHome", default = "defaults::rocketmq_home")]
    pub rocketmq_home: String,

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

    #[serde(alias = "configBlackList", default = "defaults::config_black_list")]
    pub config_black_list: String,
}

impl Default for NamesrvConfig {
    fn default() -> Self {
        NamesrvConfig {
            rocketmq_home: defaults::rocketmq_home(),
            kv_config_path: defaults::kv_config_path(),
            config_store_path: defaults::config_store_path(),
            product_env_name: "center".to_string(),
            cluster_test: false,
            order_message_enable: false,
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
        for (key, value) in properties {
            reject_removed_route_manager_key(key.as_str())?;
            match key.as_str() {
                "rocketmqHome" => self.rocketmq_home = value.to_string(),
                "kvConfigPath" => self.kv_config_path = value.to_string(),
                "configStorePath" => self.config_store_path = value.to_string(),
                "productEnvName" => self.product_env_name = value.to_string(),
                "clusterTest" => {
                    self.cluster_test = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "orderMessageEnable" => {
                    self.order_message_enable = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "returnOrderTopicConfigToBroker" => {
                    self.return_order_topic_config_to_broker =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "clientRequestThreadPoolNums" => {
                    self.client_request_thread_pool_nums =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "defaultThreadPoolNums" => {
                    self.default_thread_pool_nums =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "clientRequestThreadPoolQueueCapacity" => {
                    self.client_request_thread_pool_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "defaultThreadPoolQueueCapacity" => {
                    self.default_thread_pool_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "scanNotActiveBrokerInterval" => {
                    self.scan_not_active_broker_interval =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "unRegisterBrokerQueueCapacity" => {
                    self.unregister_broker_queue_capacity =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "supportActingMaster" => {
                    self.support_acting_master = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "enableAllTopicList" => {
                    self.enable_all_topic_list = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "enableTopicList" => {
                    self.enable_topic_list = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "notifyMinBrokerIdChanged" => {
                    self.notify_min_broker_id_changed =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "enableControllerInNamesrv" => {
                    self.enable_controller_in_namesrv =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "needWaitForService" => {
                    self.need_wait_for_service = value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "waitSecondsForService" => {
                    self.wait_seconds_for_service =
                        value.parse().map_err(|_| invalid_value(&key, "expected an integer"))?
                }
                "deleteTopicWithBrokerRegistration" => {
                    self.delete_topic_with_broker_registration =
                        value.parse().map_err(|_| invalid_value(&key, "expected a boolean"))?
                }
                "configBlackList" => {
                    self.config_black_list = value.to_string();
                }
                _ => {
                    return Err(RocketMQError::nameserver_config_invalid(format!(
                        "unknown configuration key '{key}'"
                    )));
                }
            }
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;
    use rocketmq_model::common::mix_all::ROCKETMQ_HOME_ENV;
    use rocketmq_model::common::mix_all::ROCKETMQ_HOME_PROPERTY;

    #[test]
    fn test_namesrv_config() {
        let config = NamesrvConfig::new();

        assert_eq!(
            config.rocketmq_home,
            env::var(ROCKETMQ_HOME_PROPERTY).unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default())
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
        assert_eq!(parsed["configBlackList"], config.config_black_list);
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
}
