/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::env;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::common::mix_all::ROCKETMQ_HOME_ENV;
use crate::common::mix_all::ROCKETMQ_HOME_PROPERTY;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamesrvConfig {
    #[serde(alias = "rocketmqHome")]
    pub rocketmq_home: String,

    #[serde(alias = "kvConfigPath")]
    pub kv_config_path: String,

    #[serde(alias = "configStorePath")]
    pub config_store_path: String,

    #[serde(alias = "productEnvName")]
    pub product_env_name: String,

    #[serde(alias = "clusterTest")]
    pub cluster_test: bool,

    #[serde(alias = "orderMessageEnable")]
    pub order_message_enable: bool,

    #[serde(alias = "returnOrderTopicConfigToBroker")]
    pub return_order_topic_config_to_broker: bool,

    #[serde(alias = "clientRequestThreadPoolNums")]
    pub client_request_thread_pool_nums: i32,

    #[serde(alias = "defaultThreadPoolNums")]
    pub default_thread_pool_nums: i32,

    #[serde(alias = "clientRequestThreadPoolQueueCapacity")]
    pub client_request_thread_pool_queue_capacity: i32,

    #[serde(alias = "defaultThreadPoolQueueCapacity")]
    pub default_thread_pool_queue_capacity: i32,

    #[serde(alias = "scanNotActiveBrokerInterval")]
    pub scan_not_active_broker_interval: u64,

    #[serde(alias = "unRegisterBrokerQueueCapacity")]
    pub unregister_broker_queue_capacity: i32,

    #[serde(alias = "supportActingMaster")]
    pub support_acting_master: bool,

    #[serde(alias = "enableAllTopicList")]
    pub enable_all_topic_list: bool,

    #[serde(alias = "enableTopicList")]
    pub enable_topic_list: bool,

    #[serde(alias = "notifyMinBrokerIdChanged")]
    pub notify_min_broker_id_changed: bool,

    #[serde(alias = "enableControllerInNamesrv")]
    pub enable_controller_in_namesrv: bool,

    #[serde(alias = "needWaitForService")]
    pub need_wait_for_service: bool,

    #[serde(alias = "waitSecondsForService")]
    pub wait_seconds_for_service: i32,

    #[serde(alias = "deleteTopicWithBrokerRegistration")]
    pub delete_topic_with_broker_registration: bool,

    #[serde(alias = "configBlackList")]
    pub config_black_list: String,
}

impl Default for NamesrvConfig {
    fn default() -> Self {
        let rocketmq_home = env::var(ROCKETMQ_HOME_PROPERTY)
            .unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default());
        let kv_config_path = format!(
            "{}{}{}{}{}",
            dirs::home_dir().unwrap().to_str().unwrap(),
            std::path::MAIN_SEPARATOR,
            "rocketmq-namesrv",
            std::path::MAIN_SEPARATOR,
            "kvConfig.json"
        );

        let config_store_path = format!(
            "{}{}{}{}{}",
            dirs::home_dir().unwrap().to_str().unwrap(),
            std::path::MAIN_SEPARATOR,
            "rocketmq-namesrv",
            std::path::MAIN_SEPARATOR,
            "rocketmq-namesrv.properties"
        );

        NamesrvConfig {
            rocketmq_home,
            kv_config_path,
            config_store_path,
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

    pub fn get_all_configs_format_string(&self) -> Result<String, String> {
        let mut json_map = HashMap::new();

        json_map.insert(
            "rocketmqHome".to_string(),
            Value::String(self.rocketmq_home.clone()),
        );
        json_map.insert(
            "kvConfigPath".to_string(),
            Value::String(self.kv_config_path.clone()),
        );
        json_map.insert(
            "configStorePath".to_string(),
            Value::String(self.config_store_path.clone()),
        );
        json_map.insert(
            "productEnvName".to_string(),
            Value::String(self.product_env_name.clone()),
        );
        json_map.insert("clusterTest".to_string(), Value::Bool(self.cluster_test));
        json_map.insert(
            "orderMessageEnable".to_string(),
            Value::Bool(self.order_message_enable),
        );
        json_map.insert(
            "returnOrderTopicConfigToBroker".to_string(),
            Value::Bool(self.return_order_topic_config_to_broker),
        );
        json_map.insert(
            "clientRequestThreadPoolNums".to_string(),
            Value::Number(self.client_request_thread_pool_nums.into()),
        );
        json_map.insert(
            "defaultThreadPoolNums".to_string(),
            Value::Number(self.default_thread_pool_nums.into()),
        );
        json_map.insert(
            "clientRequestThreadPoolQueueCapacity".to_string(),
            Value::Number(self.client_request_thread_pool_queue_capacity.into()),
        );
        json_map.insert(
            "defaultThreadPoolQueueCapacity".to_string(),
            Value::Number(self.default_thread_pool_queue_capacity.into()),
        );
        json_map.insert(
            "scanNotActiveBrokerInterval".to_string(),
            Value::Number(self.scan_not_active_broker_interval.into()),
        );
        json_map.insert(
            "unRegisterBrokerQueueCapacity".to_string(),
            Value::Number(self.unregister_broker_queue_capacity.into()),
        );
        json_map.insert(
            "supportActingMaster".to_string(),
            Value::Bool(self.support_acting_master),
        );
        json_map.insert(
            "enableAllTopicList".to_string(),
            Value::Bool(self.enable_all_topic_list),
        );
        json_map.insert(
            "enableTopicList".to_string(),
            Value::Bool(self.enable_topic_list),
        );
        json_map.insert(
            "notifyMinBrokerIdChanged".to_string(),
            Value::Bool(self.notify_min_broker_id_changed),
        );
        json_map.insert(
            "enableControllerInNamesrv".to_string(),
            Value::Bool(self.enable_controller_in_namesrv),
        );
        json_map.insert(
            "needWaitForService".to_string(),
            Value::Bool(self.need_wait_for_service),
        );
        json_map.insert(
            "waitSecondsForService".to_string(),
            Value::Number(self.wait_seconds_for_service.into()),
        );
        json_map.insert(
            "deleteTopicWithBrokerRegistration".to_string(),
            Value::Bool(self.delete_topic_with_broker_registration),
        );
        json_map.insert(
            "configBlackList".to_string(),
            Value::String(self.config_black_list.clone()),
        );

        // Convert the HashMap to a JSON value
        match serde_json::to_string_pretty(&json_map) {
            Ok(json) => Ok(json),
            Err(err) => {
                return Err(format!("Failed to serialize NamesrvConfig: {}", err));
            }
        }
    }

    /// Splits the `config_black_list` into a `Vec<CheetahString>` for easier usage.
    pub fn get_config_blacklist(&self) -> Vec<CheetahString> {
        self.config_black_list
            .split(';')
            .map(|s| CheetahString::from(s.trim()))
            .collect()
    }

    pub fn update(
        &mut self,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> Result<(), String> {
        for (key, value) in properties {
            match key.as_str() {
                "rocketmqHome" => self.rocketmq_home = value.to_string(),
                "kvConfigPath" => self.kv_config_path = value.to_string(),
                "configStorePath" => self.config_store_path = value.to_string(),
                "productEnvName" => self.product_env_name = value.to_string(),
                "clusterTest" => {
                    self.cluster_test = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "orderMessageEnable" => {
                    self.order_message_enable = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "clientRequestThreadPoolNums" => {
                    self.client_request_thread_pool_nums = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "defaultThreadPoolNums" => {
                    self.default_thread_pool_nums = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "clientRequestThreadPoolQueueCapacity" => {
                    self.client_request_thread_pool_queue_capacity = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "defaultThreadPoolQueueCapacity" => {
                    self.default_thread_pool_queue_capacity = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "scanNotActiveBrokerInterval" => {
                    self.scan_not_active_broker_interval = value
                        .parse()
                        .map_err(|_| format!("Invalid value for key '{}'", key))?
                }
                "unRegisterBrokerQueueCapacity" => {
                    self.unregister_broker_queue_capacity = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "supportActingMaster" => {
                    self.support_acting_master = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "enableAllTopicList" => {
                    self.enable_all_topic_list = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "enableTopicList" => {
                    self.enable_topic_list = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "notifyMinBrokerIdChanged" => {
                    self.notify_min_broker_id_changed = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "enableControllerInNamesrv" => {
                    self.enable_controller_in_namesrv = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "needWaitForService" => {
                    self.need_wait_for_service = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "waitSecondsForService" => {
                    self.wait_seconds_for_service = value
                        .parse()
                        .map_err(|_| format!("Invalid integer value for key '{}'", key))?
                }
                "deleteTopicWithBrokerRegistration" => {
                    self.delete_topic_with_broker_registration = value
                        .parse()
                        .map_err(|_| format!("Invalid boolean value for key '{}'", key))?
                }
                "configBlackList" => {
                    self.config_black_list = value
                        .parse()
                        .map_err(|_| format!("Invalid string value for key '{}'", key))?
                }
                _ => {
                    return Err(format!("Unknown configuration key: '{}'", key));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;
    use crate::common::mix_all::ROCKETMQ_HOME_ENV;
    use crate::common::mix_all::ROCKETMQ_HOME_PROPERTY;

    #[test]
    fn test_namesrv_config() {
        let config = NamesrvConfig::new();

        assert_eq!(
            config.rocketmq_home,
            env::var(ROCKETMQ_HOME_PROPERTY)
                .unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default())
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
        assert_eq!(config.cluster_test, false);
        assert_eq!(config.order_message_enable, false);
        assert_eq!(config.return_order_topic_config_to_broker, true);
        assert_eq!(config.client_request_thread_pool_nums, 8);
        assert_eq!(config.default_thread_pool_nums, 16);
        assert_eq!(config.client_request_thread_pool_queue_capacity, 50000);
        assert_eq!(config.default_thread_pool_queue_capacity, 10000);
        assert_eq!(config.scan_not_active_broker_interval, 5 * 1000);
        assert_eq!(config.unregister_broker_queue_capacity, 3000);
        assert_eq!(config.support_acting_master, false);
        assert_eq!(config.enable_all_topic_list, true);
        assert_eq!(config.enable_topic_list, true);
        assert_eq!(config.notify_min_broker_id_changed, false);
        assert_eq!(config.enable_controller_in_namesrv, false);
        assert_eq!(config.need_wait_for_service, false);
        assert_eq!(config.wait_seconds_for_service, 45);
        assert_eq!(config.delete_topic_with_broker_registration, false);
        assert_eq!(
            config.config_black_list,
            "configBlackList;configStorePath;kvConfigPath".to_string()
        );
    }

    #[test]
    fn test_namesrv_config_update() {
        let mut config = NamesrvConfig::new();

        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from("rocketmqHome"),
            CheetahString::from("/new/path"),
        );
        properties.insert(
            CheetahString::from("kvConfigPath"),
            CheetahString::from("/new/kvConfigPath"),
        );
        properties.insert(
            CheetahString::from("configStorePath"),
            CheetahString::from("/new/configStorePath"),
        );
        properties.insert(
            CheetahString::from("productEnvName"),
            CheetahString::from("new_env"),
        );
        properties.insert(
            CheetahString::from("clusterTest"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("orderMessageEnable"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("clientRequestThreadPoolNums"),
            CheetahString::from("10"),
        );
        properties.insert(
            CheetahString::from("defaultThreadPoolNums"),
            CheetahString::from("20"),
        );
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
        properties.insert(
            CheetahString::from("supportActingMaster"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("enableAllTopicList"),
            CheetahString::from("false"),
        );
        properties.insert(
            CheetahString::from("enableTopicList"),
            CheetahString::from("false"),
        );
        properties.insert(
            CheetahString::from("notifyMinBrokerIdChanged"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("enableControllerInNamesrv"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("needWaitForService"),
            CheetahString::from("true"),
        );
        properties.insert(
            CheetahString::from("waitSecondsForService"),
            CheetahString::from("30"),
        );
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
        assert_eq!(config.cluster_test, true);
        assert_eq!(config.order_message_enable, true);
        assert_eq!(config.client_request_thread_pool_nums, 10);
        assert_eq!(config.default_thread_pool_nums, 20);
        assert_eq!(config.client_request_thread_pool_queue_capacity, 10000);
        assert_eq!(config.default_thread_pool_queue_capacity, 20000);
        assert_eq!(config.scan_not_active_broker_interval, 15000);
        assert_eq!(config.unregister_broker_queue_capacity, 4000);
        assert_eq!(config.support_acting_master, true);
        assert_eq!(config.enable_all_topic_list, false);
        assert_eq!(config.enable_topic_list, false);
        assert_eq!(config.notify_min_broker_id_changed, true);
        assert_eq!(config.enable_controller_in_namesrv, true);
        assert_eq!(config.need_wait_for_service, true);
        assert_eq!(config.wait_seconds_for_service, 30);
        assert_eq!(config.delete_topic_with_broker_registration, true);
        assert_eq!(config.config_black_list, "newBlackList");
    }

    #[test]
    fn test_get_all_configs_format_string() {
        let config = NamesrvConfig::new();

        let json_output = config.get_all_configs_format_string().unwrap();

        assert!(!json_output.is_empty(), "JSON output should not be empty");

        let parsed: serde_json::Value =
            serde_json::from_str(&json_output).expect("Output should be valid JSON");

        assert_eq!(parsed["rocketmqHome"], config.rocketmq_home);
        assert_eq!(parsed["kvConfigPath"], config.kv_config_path);
        assert_eq!(parsed["configStorePath"], config.config_store_path);
        assert_eq!(parsed["productEnvName"], config.product_env_name);
        assert_eq!(parsed["clusterTest"], config.cluster_test);
        assert_eq!(parsed["orderMessageEnable"], config.order_message_enable);
        assert_eq!(
            parsed["returnOrderTopicConfigToBroker"],
            config.return_order_topic_config_to_broker
        );
        assert_eq!(
            parsed["clientRequestThreadPoolNums"],
            config.client_request_thread_pool_nums
        );
        assert_eq!(
            parsed["defaultThreadPoolNums"],
            config.default_thread_pool_nums
        );
        assert_eq!(
            parsed["clientRequestThreadPoolQueueCapacity"],
            config.client_request_thread_pool_queue_capacity
        );
        assert_eq!(
            parsed["defaultThreadPoolQueueCapacity"],
            config.default_thread_pool_queue_capacity
        );
        assert_eq!(
            parsed["scanNotActiveBrokerInterval"],
            config.scan_not_active_broker_interval
        );
        assert_eq!(
            parsed["unRegisterBrokerQueueCapacity"],
            config.unregister_broker_queue_capacity
        );
        assert_eq!(parsed["supportActingMaster"], config.support_acting_master);
        assert_eq!(parsed["enableAllTopicList"], config.enable_all_topic_list);
        assert_eq!(parsed["enableTopicList"], config.enable_topic_list);
        assert_eq!(
            parsed["notifyMinBrokerIdChanged"],
            config.notify_min_broker_id_changed
        );
        assert_eq!(
            parsed["enableControllerInNamesrv"],
            config.enable_controller_in_namesrv
        );
        assert_eq!(parsed["needWaitForService"], config.need_wait_for_service);
        assert_eq!(
            parsed["waitSecondsForService"],
            config.wait_seconds_for_service
        );
        assert_eq!(
            parsed["deleteTopicWithBrokerRegistration"],
            config.delete_topic_with_broker_registration
        );
        assert_eq!(parsed["configBlackList"], config.config_black_list);
    }
}
