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

mod ack_frontier;
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
pub(crate) mod write_lease;

#[derive(Debug, thiserror::Error)]
pub(crate) enum HAServiceFailure {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Store(#[from] rocketmq_store_api::StoreError),
    #[error(transparent)]
    Runtime(#[from] rocketmq_runtime::RuntimeError),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Service error: {0}")]
    Service(String),
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

impl From<HAServiceFailure> for rocketmq_store_api::StoreError {
    fn from(error: HAServiceFailure) -> Self {
        match error {
            HAServiceFailure::Store(error) => error,
            error => rocketmq_store_api::StoreError::new(
                &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                rocketmq_store_api::StoreOperation::Start,
            )
            .in_component(rocketmq_store_api::StoreComponent::HighAvailability)
            .with_source(error),
        }
    }
}

#[cfg(test)]
mod failure_mapping_tests {
    use std::error::Error;

    use rocketmq_runtime::RuntimeError;
    use rocketmq_store_api::StoreComponent;
    use rocketmq_store_api::StoreError;
    use rocketmq_store_api::StoreOperation;

    use super::HAServiceFailure;

    #[test]
    fn connection_start_failure_maps_once_with_typed_source() {
        let error: StoreError = HAServiceFailure::Io(std::io::Error::other("connection start failed")).into();

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
        assert_eq!(error.operation(), StoreOperation::Start);
        assert_eq!(error.component(), StoreComponent::HighAvailability);
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<HAServiceFailure>())
            .expect("HA service source remains typed");
        assert!(matches!(source, HAServiceFailure::Io(_)));
        assert!(source
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }

    #[test]
    fn nested_store_error_passes_through_unchanged() {
        let nested = StoreError::new(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Replicate)
            .in_component(StoreComponent::CommitLog)
            .with_source(std::io::Error::other("replication read failed"));

        let error: StoreError = HAServiceFailure::Store(nested).into();

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_READ_FAILED);
        assert_eq!(error.operation(), StoreOperation::Replicate);
        assert_eq!(error.component(), StoreComponent::CommitLog);
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }

    #[test]
    fn runtime_failure_maps_once_with_its_typed_cause() {
        let error: StoreError = HAServiceFailure::Runtime(RuntimeError::NoCurrentRuntime).into();

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
        assert_eq!(error.operation(), StoreOperation::Start);
        assert_eq!(error.component(), StoreComponent::HighAvailability);
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<HAServiceFailure>())
            .expect("HA service source remains typed");
        assert!(matches!(source, HAServiceFailure::Runtime(_)));
        assert!(source
            .source()
            .and_then(|source| source.downcast_ref::<RuntimeError>())
            .is_some());
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::Path;
    use std::sync::Arc;

    use crate::config::message_store_config::MessageStoreConfig;
    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;

    pub(crate) fn new_test_message_store(root: &Path, enable_controller_mode: bool) -> LocalFileMessageStore {
        std::fs::create_dir_all(root).expect("create temp root dir");
        let broker_config = StoreRuntimeConfig {
            duplication_enable: true,
            enable_controller_mode,
            ..StoreRuntimeConfig::default()
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
            rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
                .expect("valid test policy"),
            Arc::new(broker_config),
            topic_table,
            None,
            false,
            crate::runtime::test_service_context("ha-message-store-test"),
        );
        store
            .wire_owned_root_dependencies()
            .expect("wire owned HA test message store");
        store
    }
}
