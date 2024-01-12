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

use std::env;

use serde::Deserialize;

use crate::common::mix_all::{ROCKETMQ_HOME_ENV, ROCKETMQ_HOME_PROPERTY};

#[derive(Debug, Clone, Deserialize, Default)]
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
    pub scan_not_active_broker_interval: i64,

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

impl NamesrvConfig {
    pub fn new() -> NamesrvConfig {
        let rocketmq_home = env::var(ROCKETMQ_HOME_PROPERTY)
            .unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default());
        let kv_config_path = format!(
            "{}{}{}{}{}",
            env::var("user.home").unwrap_or_default(),
            std::path::MAIN_SEPARATOR,
            "rocketmq-namesrv",
            std::path::MAIN_SEPARATOR,
            "kvConfig.json"
        );

        let config_store_path = format!(
            "{}{}{}{}{}",
            env::var("user.home").unwrap_or_default(),
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

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;
    use crate::common::mix_all::{ROCKETMQ_HOME_ENV, ROCKETMQ_HOME_PROPERTY};

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
                env::var("user.home").unwrap_or_default(),
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
        );
        assert_eq!(
            config.config_store_path,
            format!(
                "{}{}rocketmq-namesrv{}rocketmq-namesrv.properties",
                env::var("user.home").unwrap_or_default(),
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
}
