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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_store::base::message_store::MessageStore;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::broker::broker_runtime_config_state::BrokerRuntimeConfigState;
use crate::failover::escape_bridge_capability::EscapeBridgeStoreCapability;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::slave::slave_synchronize::SlaveSynchronize;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_coordinator::TopicRegistrationAction;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerRegistrationStatus {
    NotConfigured,
    Unchanged,
    Registered { name_server_count: usize },
    OnewaySubmitted,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum BrokerRegistrationError {
    #[error("broker registration was requested after shutdown")]
    ShuttingDown,
    #[error("broker registration coordination failed: {0}")]
    Coordination(String),
    #[error("broker registration completion channel was dropped")]
    CompletionDropped,
    #[error("broker registration snapshot contains a topic without a name")]
    MissingTopicName,
    #[error("no NameServer accepted broker registration for configured address `{configured_address}`")]
    NoSuccessfulNameServer { configured_address: CheetahString },
}

/// Explicit capability carrier for NameServer registration.
///
/// Registration observes live configuration and topic metadata, but it does not retain the
/// complete broker composition root. Store and slave updates are routed through independently
/// synchronized capabilities after a registration result is received.
pub(crate) struct BrokerRegistrationRuntime<MS: MessageStore> {
    config: BrokerRuntimeConfigState,
    store: EscapeBridgeStoreCapability<MS>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_config_coordinator: Arc<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    broker_outer_api: BrokerOuterAPI,
    slave_synchronize: Option<Arc<SlaveSynchronize<MS>>>,
    update_master_haserver_addr_periodically: bool,
    shutdown: Arc<AtomicBool>,
}

impl<MS: MessageStore> Clone for BrokerRegistrationRuntime<MS> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            store: self.store.clone(),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            topic_config_coordinator: Arc::clone(&self.topic_config_coordinator),
            topic_queue_mapping_manager: Arc::clone(&self.topic_queue_mapping_manager),
            broker_outer_api: self.broker_outer_api.clone(),
            slave_synchronize: self.slave_synchronize.clone(),
            update_master_haserver_addr_periodically: self.update_master_haserver_addr_periodically,
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

impl<MS: MessageStore> BrokerRegistrationRuntime<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the broker composition root enumerates the complete registration boundary"
    )]
    pub(crate) fn new(
        config: BrokerRuntimeConfigState,
        store: EscapeBridgeStoreCapability<MS>,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_config_coordinator: Arc<TopicConfigCoordinator>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        broker_outer_api: BrokerOuterAPI,
        slave_synchronize: Option<Arc<SlaveSynchronize<MS>>>,
        update_master_haserver_addr_periodically: bool,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            store,
            topic_config_manager,
            topic_config_coordinator,
            topic_queue_mapping_manager,
            broker_outer_api,
            slave_synchronize,
            update_master_haserver_addr_periodically,
            shutdown,
        }
    }

    pub(crate) async fn register_broker_all(
        &self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) -> Result<BrokerRegistrationStatus, BrokerRegistrationError> {
        if self.shutdown.load(Ordering::Acquire) {
            info!("Skip broker registration after shutdown");
            return Err(BrokerRegistrationError::ShuttingDown);
        }

        let runtime = self.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let registration: TopicRegistrationAction = Box::new(move || {
            Box::pin(async move {
                let result = runtime
                    .register_full_snapshot(check_order_config, oneway, force_register)
                    .await;
                let coordination_result = result.as_ref().map(|_| ()).map_err(|error| {
                    rocketmq_error::RocketMQError::network_connection_failed("broker-registration", error.to_string())
                });
                let _ = result_tx.send(result);
                coordination_result
            })
        });
        let coordination_result = self
            .topic_config_coordinator
            .persist_and_register_wait(registration)
            .await;
        let registration_result = match result_rx.await {
            Ok(result) => result,
            Err(_) => {
                return match coordination_result {
                    Ok(()) => Err(BrokerRegistrationError::CompletionDropped),
                    Err(error) => Err(BrokerRegistrationError::Coordination(error.to_string())),
                };
            }
        };
        match registration_result {
            Ok(status) => {
                coordination_result.map_err(|error| BrokerRegistrationError::Coordination(error.to_string()))?;
                Ok(status)
            }
            Err(error) => Err(error),
        }
    }

    async fn register_full_snapshot(
        &self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) -> Result<BrokerRegistrationStatus, BrokerRegistrationError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BrokerRegistrationError::ShuttingDown);
        }

        let broker_config = self.config.broker_snapshot();
        let (raw_topic_config_table, split_data_version, final_data_version) =
            self.topic_config_manager.full_registration_snapshot(
                broker_config.enable_split_registration,
                broker_config.split_registration_size,
            );
        let mut topic_config_table = raw_topic_config_table
            .into_values()
            .map(|topic_config| {
                let topic_config = self.topic_config_for_registration(&topic_config);
                let topic_name = topic_config
                    .topic_name
                    .clone()
                    .ok_or(BrokerRegistrationError::MissingTopicName)?;
                Ok((topic_name, topic_config))
            })
            .collect::<Result<HashMap<_, _>, BrokerRegistrationError>>()?;

        if let Some(split_data_version) = split_data_version {
            let wrapper = self
                .topic_config_manager
                .build_serialize_wrapper(topic_config_table.clone(), split_data_version);
            self.register_wrapper(wrapper, check_order_config, oneway).await?;
            topic_config_table.clear();
        }

        let topic_queue_mapping_info_map = self
            .topic_queue_mapping_manager
            .snapshot_topic_queue_mapping_table()
            .into_iter()
            .map(|(topic, detail)| (topic, TopicQueueMappingDetail::clone_as_mapping_info(&detail)))
            .collect();
        let wrapper = self.topic_config_manager.build_serialize_wrapper_with_topic_queue_map(
            topic_config_table,
            topic_queue_mapping_info_map,
            final_data_version,
        );
        let should_register = broker_config.enable_split_registration
            || force_register
            || need_register(
                &self
                    .broker_outer_api
                    .need_register(
                        broker_config.broker_identity.broker_cluster_name.clone(),
                        self.broker_addr(&broker_config),
                        broker_config.broker_identity.broker_name.clone(),
                        broker_config.broker_identity.broker_id,
                        &wrapper,
                        broker_config.register_broker_timeout_mills as u64,
                        broker_config.is_in_broker_container,
                    )
                    .await,
            );
        if should_register {
            self.register_wrapper(wrapper, check_order_config, oneway).await
        } else {
            Ok(BrokerRegistrationStatus::Unchanged)
        }
    }

    pub(crate) async fn register_increment_broker_data(
        &self,
        topic_config_list: Vec<Arc<TopicConfig>>,
        data_version: DataVersion,
    ) {
        let topic_names = topic_config_list
            .iter()
            .filter_map(|topic_config| topic_config.topic_name.clone())
            .collect::<Vec<_>>();
        let (topic_config_list, current_data_version) =
            self.topic_config_manager.topic_registration_snapshot(&topic_names);
        if current_data_version != data_version {
            debug!(
                requested = ?data_version,
                current = ?current_data_version,
                "resample incremental topic registration after a newer metadata commit"
            );
        }

        let mut wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: current_data_version,
                topic_config_table: Default::default(),
            },
            ..Default::default()
        };
        wrapper.topic_config_serialize_wrapper.topic_config_table = topic_config_list
            .iter()
            .map(|topic_config| {
                let topic_config = self.topic_config_for_registration(topic_config);
                (topic_config.topic_name.clone().unwrap_or_default(), topic_config)
            })
            .collect();
        wrapper.topic_queue_mapping_info_map = topic_config_list
            .into_iter()
            .filter_map(|topic_config| {
                let topic_name = topic_config.topic_name.as_ref()?;
                self.topic_queue_mapping_manager
                    .get_topic_queue_mapping(topic_name.as_str())
                    .map(|detail| {
                        (
                            topic_name.clone(),
                            TopicQueueMappingDetail::clone_as_mapping_info(detail.as_ref()),
                        )
                    })
            })
            .collect();
        if let Err(error) = self.register_wrapper(wrapper, true, false).await {
            warn!(%error, "failed to register incremental broker data");
        }
    }

    pub(crate) async fn register_single_topic_all(&self, topic_config: Arc<TopicConfig>) {
        let Some(topic_name) = topic_config.topic_name.clone() else {
            warn!("Skip single-topic registration for config without topic name");
            return;
        };
        let (mut current_configs, _) = self
            .topic_config_manager
            .topic_registration_snapshot(std::slice::from_ref(&topic_name));
        let Some(current) = current_configs.pop() else {
            info!(topic = %topic_name, "Skip stale single-topic registration after topic removal");
            return;
        };
        let broker_config = self.config.broker_snapshot();
        self.broker_outer_api
            .register_single_topic_all(
                broker_config.broker_identity.broker_name.clone(),
                self.topic_config_for_registration(current.as_ref()),
                3_000,
            )
            .await;
    }

    pub(crate) fn topic_config_for_registration(&self, topic_config: &TopicConfig) -> TopicConfig {
        let mut topic_config = topic_config.clone();
        let broker_config = self.config.broker_snapshot();
        if !PermName::is_writeable(broker_config.broker_permission)
            || !PermName::is_readable(broker_config.broker_permission)
        {
            topic_config.perm &= broker_config.broker_permission;
        }
        topic_config
    }

    async fn register_wrapper(
        &self,
        wrapper: TopicConfigAndMappingSerializeWrapper,
        check_order_config: bool,
        oneway: bool,
    ) -> Result<BrokerRegistrationStatus, BrokerRegistrationError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BrokerRegistrationError::ShuttingDown);
        }
        let broker_config = self.config.broker_snapshot();
        let broker_addr = self.broker_addr(&broker_config);
        let results = self
            .broker_outer_api
            .register_broker_all(
                broker_config.broker_identity.broker_cluster_name.clone(),
                broker_addr.clone(),
                broker_config.broker_identity.broker_name.clone(),
                broker_config.broker_identity.broker_id,
                broker_addr,
                wrapper,
                vec![],
                oneway,
                broker_config.register_broker_timeout_mills as u64,
                broker_config.enable_slave_acting_master,
                broker_config.compressed_register,
                broker_config
                    .enable_slave_acting_master
                    .then_some(broker_config.broker_not_active_timeout_millis),
                Default::default(),
            )
            .await;
        if oneway {
            return Ok(BrokerRegistrationStatus::OnewaySubmitted);
        }
        if results.is_empty() {
            return match broker_config.namesrv_addr.clone() {
                Some(configured_address) => Err(BrokerRegistrationError::NoSuccessfulNameServer { configured_address }),
                None => Ok(BrokerRegistrationStatus::NotConfigured),
            };
        }
        let name_server_count = results.len();
        self.handle_register_broker_result(results, check_order_config).await;
        Ok(BrokerRegistrationStatus::Registered { name_server_count })
    }

    async fn handle_register_broker_result(
        &self,
        register_broker_result: Vec<RegisterBrokerResult>,
        check_order_config: bool,
    ) {
        let Some(result) = register_broker_result.into_iter().next() else {
            return;
        };
        if self.update_master_haserver_addr_periodically {
            let _ = self.store.update_master_address(&result.master_addr).await;
        }
        if let Some(slave_synchronize) = self.slave_synchronize.as_ref() {
            slave_synchronize.set_master_addr(Some(&result.master_addr));
        }
        if check_order_config {
            let state_machine_version = self.store.state_machine_version().unwrap_or_default();
            self.topic_config_manager
                .update_order_topic_config(&result.kv_table, state_machine_version);
        }
    }

    fn broker_addr(&self, broker_config: &BrokerConfig) -> CheetahString {
        CheetahString::from_string(format!(
            "{}:{}",
            broker_config.broker_ip1, broker_config.broker_server_config.listen_port
        ))
    }
}

fn need_register(change_list: &[bool]) -> bool {
    change_list.iter().any(|changed| *changed)
}

#[cfg(test)]
mod tests {
    #[test]
    fn registration_boundary_does_not_retain_the_broker_root() {
        let source = include_str!("broker_registration_runtime.rs");

        assert!(!source.contains(&["Arc", "Mut"].concat()));
        assert!(!source.contains(&["BrokerRuntime", "Inner"].concat()));
    }

    #[test]
    fn need_register_reflects_any_namesrv_change() {
        assert!(super::need_register(&[false, true, false]));
        assert!(!super::need_register(&[false, false, false]));
    }
}
