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

use std::error::Error as StdError;
use std::fmt;

pub(crate) enum HAError {
    Io(std::io::Error),
    StartIo(std::io::Error),
    Store(rocketmq_store_api::StoreError),
    Runtime(rocketmq_runtime::RuntimeError),
    InvalidWire,
    Operation {
        operation: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
    Budget(Box<dyn StdError + Send + Sync>),
    InvalidState(&'static str),
}

impl fmt::Debug for HAError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Io(_) => "HAError::Io",
            Self::StartIo(_) => "HAError::StartIo",
            Self::Store(_) => "HAError::Store",
            Self::Runtime(_) => "HAError::Runtime",
            Self::InvalidWire => "HAError::InvalidWire",
            Self::Operation { .. } => "HAError::Operation",
            Self::Budget(_) => "HAError::Budget",
            Self::InvalidState(_) => "HAError::InvalidState",
        })
    }
}

impl fmt::Display for HAError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Io(_) => "HA I/O operation failed",
            Self::StartIo(_) => "HA startup I/O failed",
            Self::Store(_) => "HA Store operation failed",
            Self::Runtime(_) => "HA runtime operation failed",
            Self::InvalidWire => "invalid HA wire frame",
            Self::Operation { .. } => "HA operation failed",
            Self::Budget(_) => "HA resource budget is exhausted",
            Self::InvalidState(_) => "invalid HA state",
        })
    }
}

impl StdError for HAError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(source) | Self::StartIo(source) => Some(source),
            Self::Store(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Operation { source, .. } | Self::Budget(source) => Some(source.as_ref()),
            Self::InvalidWire | Self::InvalidState(_) => None,
        }
    }
}

impl From<std::io::Error> for HAError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

impl From<rocketmq_store_api::StoreError> for HAError {
    fn from(source: rocketmq_store_api::StoreError) -> Self {
        Self::Store(source)
    }
}

impl HAError {
    pub(crate) fn operation(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Operation {
            operation,
            source: Box::new(source),
        }
    }

    pub(crate) fn invalid_state(detail: &'static str) -> Self {
        Self::InvalidState(detail)
    }

    pub(crate) fn budget(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Budget(Box::new(source))
    }
}

impl From<HAError> for rocketmq_store_api::StoreError {
    fn from(error: HAError) -> Self {
        match error {
            HAError::Store(error) => error,
            error @ HAError::Io(_) => rocketmq_store_api::StoreError::new(
                &rocketmq_error::STORAGE_IO_FAILED,
                rocketmq_store_api::StoreOperation::Replicate,
            )
            .in_component(rocketmq_store_api::StoreComponent::HighAvailability)
            .with_source(error),
            error @ HAError::InvalidWire => rocketmq_store_api::StoreError::new(
                &rocketmq_error::STORAGE_STATE_CORRUPTED,
                rocketmq_store_api::StoreOperation::Replicate,
            )
            .in_component(rocketmq_store_api::StoreComponent::HighAvailability)
            .with_source(error),
            error @ HAError::Runtime(_) => rocketmq_store_api::StoreError::new(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Replicate,
            )
            .in_component(rocketmq_store_api::StoreComponent::HighAvailability)
            .with_source(error),
            error @ HAError::Budget(_) => rocketmq_store_api::StoreError::new(
                &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
                rocketmq_store_api::StoreOperation::Start,
            )
            .in_component(rocketmq_store_api::StoreComponent::HighAvailability)
            .with_source(error),
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

    use rocketmq_error::ViewValueRef;
    use rocketmq_runtime::RuntimeError;
    use rocketmq_store_api::StoreComponent;
    use rocketmq_store_api::StoreError;
    use rocketmq_store_api::StoreOperation;

    use super::HAError;

    #[test]
    fn connection_start_failure_maps_once_with_typed_source() {
        let error: StoreError = HAError::Io(std::io::Error::other("connection start failed")).into();

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        assert_eq!(error.operation(), StoreOperation::Replicate);
        assert_eq!(error.component(), StoreComponent::HighAvailability);
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<HAError>())
            .expect("HA service source remains typed");
        assert!(matches!(source, HAError::Io(_)));
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

        let error: StoreError = HAError::Store(nested).into();

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
        let error: StoreError = HAError::Runtime(RuntimeError::context_unavailable(
            rocketmq_runtime::RuntimeOperation::HaRuntime,
        ))
        .into();

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_INTERNAL_FAILURE);
        assert_eq!(error.operation(), StoreOperation::Replicate);
        assert_eq!(error.component(), StoreComponent::HighAvailability);
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<HAError>())
            .expect("HA service source remains typed");
        assert!(matches!(source, HAError::Runtime(_)));
        assert!(source
            .source()
            .and_then(|source| source.downcast_ref::<RuntimeError>())
            .is_some());
    }

    #[test]
    fn startup_wire_budget_and_boxed_operation_follow_the_owner_table() {
        let startup: StoreError = HAError::StartIo(std::io::Error::other("bind failed")).into();
        assert_eq!(startup.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
        assert_eq!(startup.operation(), StoreOperation::Start);
        assert_eq!(startup.component(), StoreComponent::HighAvailability);
        let startup_source = startup
            .source()
            .and_then(|source| source.downcast_ref::<HAError>())
            .expect("startup failure remains typed");
        assert!(matches!(startup_source, HAError::StartIo(_)));
        assert!(startup_source
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());

        let wire: StoreError = HAError::InvalidWire.into();
        assert_eq!(wire.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
        assert_eq!(wire.operation(), StoreOperation::Replicate);
        assert_eq!(wire.component(), StoreComponent::HighAvailability);
        assert!(matches!(
            wire.source().and_then(|source| source.downcast_ref::<HAError>()),
            Some(HAError::InvalidWire)
        ));

        let budget: StoreError = HAError::budget(std::io::Error::other("budget exhausted")).into();
        assert_eq!(budget.descriptor(), &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED);
        assert_eq!(budget.operation(), StoreOperation::Start);
        assert_eq!(budget.component(), StoreComponent::HighAvailability);
        let budget_source = budget
            .source()
            .and_then(|source| source.downcast_ref::<HAError>())
            .expect("budget failure remains typed");
        assert!(matches!(budget_source, HAError::Budget(_)));
        assert!(budget_source
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());

        let operation: StoreError =
            HAError::operation("start sensitive operation", std::io::Error::other("operation failed")).into();
        assert_eq!(operation.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
        assert_eq!(operation.operation(), StoreOperation::Start);
        assert_eq!(operation.component(), StoreComponent::HighAvailability);
        let operation_source = operation
            .source()
            .and_then(|source| source.downcast_ref::<HAError>())
            .expect("boxed operation failure remains typed");
        assert!(matches!(operation_source, HAError::Operation { .. }));
        assert!(operation_source
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }

    #[test]
    fn ha_leaf_display_and_debug_redact_every_typed_source_and_detail() {
        const SENTINEL: &str = "sensitive-ha-source-9967";
        let nested_store = StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, StoreOperation::Replicate)
            .in_component(StoreComponent::CommitLog)
            .with_source(std::io::Error::other(SENTINEL));
        let errors = [
            HAError::Io(std::io::Error::other(SENTINEL)),
            HAError::StartIo(std::io::Error::other(SENTINEL)),
            HAError::Store(nested_store),
            HAError::Runtime(RuntimeError::configuration(
                rocketmq_runtime::RuntimeOperation::HaRuntime,
            )),
            HAError::operation(SENTINEL, std::io::Error::other(SENTINEL)),
            HAError::budget(std::io::Error::other(SENTINEL)),
            HAError::invalid_state(SENTINEL),
        ];

        for error in errors {
            assert!(!error.to_string().contains(SENTINEL));
            assert!(!format!("{error:?}").contains(SENTINEL));
        }

        let source = HAError::Io(std::io::Error::other(SENTINEL));
        assert_eq!(
            source
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .map(ToString::to_string)
                .as_deref(),
            Some(SENTINEL)
        );

        let mapped: StoreError = HAError::Io(std::io::Error::other(SENTINEL)).into();
        assert!(mapped
            .public_view()
            .expect("valid HA public view")
            .fields()
            .next()
            .is_none());
        let diagnostic_fields = mapped
            .diagnostic_view()
            .expect("valid HA diagnostic view")
            .fields()
            .map(|field| (field.name(), field.value()))
            .collect::<Vec<_>>();
        assert!(diagnostic_fields
            .iter()
            .any(|(name, value)| *name == "source_present" && *value == ViewValueRef::Redacted));
        assert!(!format!("{diagnostic_fields:?}").contains(SENTINEL));
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
        )
        .expect("create HA test message Store")
        .expect("test Timer Store configuration is valid");
        store
            .wire_owned_root_dependencies()
            .expect("wire owned HA test message store");
        store
    }
}
