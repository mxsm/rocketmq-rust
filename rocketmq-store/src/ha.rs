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

pub(crate) mod auto_switch;
pub(crate) mod default_ha_client;
mod default_ha_connection;
pub(crate) mod default_ha_service;
pub(crate) mod flow_monitor;
pub(crate) mod general_ha_client;
pub(crate) mod general_ha_connection;
pub(crate) mod general_ha_service;
mod group_transfer_service;
pub(crate) mod ha_client;
pub(crate) mod ha_connection;
pub mod ha_connection_state;
pub mod ha_connection_state_notification_request;
mod ha_connection_state_notification_service;
pub mod ha_service;
pub mod transfer_engine;
pub mod transfer_metrics;
pub(crate) mod wait_notify_object;

pub use rocketmq_store_local::ha::error::HAConnectionError;

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::Path;
    use std::sync::Arc;

    use crate::config::message_store_config::MessageStoreConfig;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;

    pub(crate) fn new_test_message_store(root: &Path, enable_controller_mode: bool) -> LocalFileMessageStore {
        std::fs::create_dir_all(root).expect("create temp root dir");
        let broker_config = BrokerConfig {
            duplication_enable: true,
            enable_controller_mode,
            ..BrokerConfig::default()
        };
        let message_store_config = MessageStoreConfig {
            duplication_enable: true,
            enable_controller_mode,
            ha_max_time_slave_not_catchup: 1000,
            ha_listen_port: 0,
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        };
        let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
        let mut store = LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(broker_config),
            topic_table,
            None,
            false,
        );
        store
            .wire_owned_root_dependencies()
            .expect("wire owned HA test message store");
        store
    }
}
