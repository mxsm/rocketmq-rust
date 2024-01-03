use std::{env, path::Path};

use crate::common::mix_all::{ROCKETMQ_HOME_ENV, ROCKETMQ_HOME_PROPERTY};

pub struct NamesrvConfig {
    rocketmq_home: String,
    kv_config_path: String,
    config_store_path: String,
    product_env_name: String,
    cluster_test: bool,
    order_message_enable: bool,
    return_order_topic_config_to_broker: bool,
    client_request_thread_pool_nums: i32,
    default_thread_pool_nums: i32,
    client_request_thread_pool_queue_capacity: i32,
    default_thread_pool_queue_capacity: i32,
    scan_not_active_broker_interval: i64,
    unregister_broker_queue_capacity: i32,
    support_acting_master: bool,
    enable_all_topic_list: bool,
    enable_topic_list: bool,
    notify_min_broker_id_changed: bool,
    enable_controller_in_namesrv: bool,
    need_wait_for_service: bool,
    wait_seconds_for_service: i32,
    delete_topic_with_broker_registration: bool,
    config_black_list: String,
}

impl NamesrvConfig {
    fn new() -> NamesrvConfig {
        let rocketmq_home = env::var(ROCKETMQ_HOME_PROPERTY)
            .unwrap_or_else(|_| env::var(ROCKETMQ_HOME_ENV).unwrap_or_default());
        let kv_config_path = format!(
            "{}{}{}{}{}",
            env::var("user.home").unwrap_or_default(),
            std::path::MAIN_SEPARATOR,
            "namesrv",
            std::path::MAIN_SEPARATOR,
            "kvConfig.json"
        );

        let config_store_path = format!(
            "{}{}{}{}{}",
            env::var("user.home").unwrap_or_default(),
            std::path::MAIN_SEPARATOR,
            "namesrv",
            std::path::MAIN_SEPARATOR,
            "namesrv.properties"
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
    use std::{env, path::Path};

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
                "{}{}namesrv{}kvConfig.json",
                env::var("user.home").unwrap_or_default(),
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
        );
        assert_eq!(
            config.config_store_path,
            format!(
                "{}{}namesrv{}namesrv.properties",
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
