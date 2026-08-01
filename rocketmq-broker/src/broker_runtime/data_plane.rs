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

use super::*;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
pub(super) struct BrokerDataPlane {
    #[cfg(feature = "local_file_store")]
    pub(super) escape_bridge_owner: Arc<EscapeBridge<BrokerMessageStore>>,
}

impl BrokerDataPlane {
    pub(super) fn new(
        #[cfg(feature = "local_file_store")] escape_bridge_owner: Arc<EscapeBridge<BrokerMessageStore>>,
    ) -> Self {
        Self {
            #[cfg(feature = "local_file_store")]
            escape_bridge_owner,
        }
    }
}

impl BrokerRuntime {
    pub(super) fn detach_message_store_provider(&self) {
        self.composition.data_plane.escape_bridge_owner.detach_message_store();
    }

    pub(super) fn bind_message_store_provider(&self) {
        if let Some(owner) = self.composition.state.message_store.as_ref() {
            self.composition
                .data_plane
                .escape_bridge_owner
                .bind_message_store(owner);
        }
    }

    pub(super) async fn load_message_store(&mut self) -> bool {
        self.detach_message_store_provider();
        let loaded = match self.composition.state.message_store_exclusive_mut() {
            Some(message_store) => BrokerStorePort::load(message_store).await,
            None => false,
        };
        self.bind_message_store_provider();
        loaded
    }

    pub(super) async fn start_message_store(&mut self) -> Result<(), StoreError> {
        self.detach_message_store_provider();
        let result = match self.composition.state.message_store_mut() {
            Some(message_store) => BrokerStorePort::start(message_store).await,
            None => Err(StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Start)),
        };
        self.bind_message_store_provider();
        result
    }

    pub(super) async fn initialize_message_store(&mut self) -> bool {
        let mut flag = true;
        let broker_config = self.composition.state.broker_config_arc();
        let store_runtime_config = Arc::new(broker_config.store_runtime_config());
        let message_store_config = self.composition.state.message_store_config_arc();
        let Some(service_context) = self.composition.state.service_context.as_ref() else {
            error!("Message store requires an injected broker service context");
            return false;
        };
        let factory_config = StoreFactoryConfig::new(
            Arc::clone(&message_store_config),
            store_runtime_config,
            self.composition.state.topic_config_manager().topic_config_table(),
            self.composition.state.broker_stats_manager.clone(),
            false,
            self.composition.state.store_telemetry.clone(),
        );
        let opened = match StoreFactory::open(factory_config, service_context.component("broker.store")) {
            Ok(opened) => opened,
            Err(error) => {
                error!(backend = ?message_store_config.store_type, %error, "Initialize message store failed");
                return false;
            }
        };
        info!(backend = ?opened.backend(), "Use configured message store");
        let (message_store, timer_message_store) = opened.into_parts();
        self.composition.state.timer_message_store = timer_message_store;
        let message_store = Arc::new(message_store);
        self.composition.state.broker_stats = Some(Arc::new(BrokerStats::from_manager(
            self.composition.state.broker_stats_manager.clone(),
        )));
        let put_message_preflight = BrokerReadStore::put_message_preflight(message_store.as_ref());
        self.composition.state.message_store = Some(message_store);
        let page_cache_busy_timeout_millis = message_store_config.os_page_cache_busy_timeout_mills;
        self.composition
            .state
            .broker_fast_failure
            .set_page_cache_busy_checker(move || {
                put_message_preflight.is_os_page_cache_busy(page_cache_busy_timeout_millis)
            });
        if let Some(message_store) = self.composition.state.message_store_mut() {
            match BrokerStorePort::init(message_store).await {
                Ok(_) => {
                    info!("Initialize message store success");
                }
                Err(e) => {
                    warn!("Initialize message store failed, error: {:?}", e);
                    flag = false;
                }
            }
        }
        let consumer_offset_manager = self.composition.state.consumer_offset_manager_handle();
        if let Some(slave_synchronize) = self.composition.state.slave_synchronize() {
            slave_synchronize.bind_consumer_offset_manager(&consumer_offset_manager);
        }
        let Some(consumer_filter_manager) = self.composition.state.consumer_filter_manager.clone() else {
            warn!("ConsumerFilterManager is unavailable during message store initialization");
            return false;
        };
        let filter: Arc<dyn CommitLogDispatcher> = Arc::new(CommitLogDispatcherCalcBitMap::new(
            broker_config,
            consumer_filter_manager,
        ));
        if let Some(message_store) = self.composition.state.message_store_exclusive_mut() {
            BrokerStorePort::add_first_dispatcher(message_store, filter);
        } else {
            error!("Message store is not exclusively owned while installing the commit-log dispatcher");
            flag = false;
        }
        self.bind_message_store_provider();
        flag
    }

    #[allow(clippy::unnecessary_unwrap)]
    pub(super) async fn recover_initialize_service(&mut self) -> bool {
        let mut result: bool = true;

        if self.composition.state.broker_config().enable_controller_mode {
            self.composition.state.initialize_controller_mode();
        }
        if self.composition.state.message_store.is_some() {
            self.register_message_store_hook();
            // load message store
            result &= self.load_message_store().await;
            if !result {
                warn!("Load message store failed");
                return false;
            }
        }

        //scheduleMessageService load after messageStore load success
        if let Some(schedule_message_service) = &self.composition.state.schedule_message_service {
            info!("Load schedule message service");
            result &= match schedule_message_service.load_async().await {
                Ok(loaded) => loaded,
                Err(error) => {
                    warn!(?error, "Load schedule message service failed");
                    false
                }
            };
            if !result {
                warn!("Load schedule message service failed");
                return false;
            }
        } else {
            warn!("Schedule message service is None");
            return false;
        }
        if result {
            self.initialize_resources();
            self.initialize_scheduled_tasks().await;
            result &= self.initial_transaction().await;
            result &= self.initial_acl().await;
            if result {
                result &= self.initial_rpc_hooks();
            }
        }
        result
    }

    #[inline(always)]
    pub fn register_message_store_hook(&mut self) {
        let config = self.composition.state.message_store_config_arc();
        let topic_config_table = self.composition.state.topic_config_manager().topic_config_table();
        let timer_message_store = self.composition.state.timer_message_store().cloned();
        let schedule_message_service = self.composition.state.schedule_message_service().clone();
        let put_message_preflight = self
            .composition
            .state
            .message_store()
            .map(BrokerReadStore::put_message_preflight);
        self.detach_message_store_provider();
        if let Some(message_store) = self.composition.state.message_store_mut() {
            if let Some(put_message_preflight) = put_message_preflight {
                BrokerStorePort::set_put_message_hook(
                    message_store,
                    Box::new(CheckBeforePutMessageHook::new(put_message_preflight, config.clone())),
                );
            }
            BrokerStorePort::set_put_message_hook(
                message_store,
                Box::new(BatchCheckBeforePutMessageHook::new(topic_config_table)),
            );
            BrokerStorePort::set_put_message_hook(
                message_store,
                Box::new(ScheduleMessageHook::new(
                    config,
                    timer_message_store,
                    schedule_message_service,
                )),
            )
        }
        self.bind_message_store_provider();
    }

    pub(super) fn initialize_resources(&mut self) {
        self.initialize_observability();

        if self.composition.state.topic_queue_mapping_clean_service.is_none() {
            self.composition.state.topic_queue_mapping_clean_service =
                Some(self.composition.state.build_topic_queue_mapping_clean_service());
        }
    }

    pub(super) async fn initial_transaction(&mut self) -> bool {
        cfg_if::cfg_if! {
            if #[cfg(feature = "local_file_store")] {
                let message_store = TransactionMessageStore::new(&self.composition.data_plane.escape_bridge_owner);
                let topic_registration = self
                    .composition.state
                    .build_transaction_topic_registration(message_store.clone());
                let bridge = TransactionalMessageBridge::new(TransactionalMessageBridgeContext {
                    store_host: self.composition.state.store_host(),
                    broker_name: self.composition.state.broker_config().broker_name().clone(),
                    consumer_offset_manager: self.composition.state.consumer_offset_manager_handle(),
                    message_store,
                    topic_registration,
                    escape_bridge: Arc::downgrade(&self.composition.data_plane.escape_bridge_owner),
                });
                let service = match DefaultTransactionalMessageService::try_new_with_resource_budget(
                    bridge,
                    self.composition.state.broker_config_arc(),
                    self.composition.state.message_store_config().file_reserved_time as i64,
                    self.composition.state.resource_budget(),
                ) {
                    Ok(service) => Arc::new(service),
                    Err(error) => {
                        error!("Failed to initialize transactional message resource budget: {error}");
                        return false;
                    }
                };
                let weak_service = Arc::downgrade(&service);
                if let Err(error) = service.set_transactional_op_batch_service_start(weak_service).await {
                    error!("Failed to start transactional op batch service: {error}");
                }
                self.composition.state.transactional_message_service = Some(service);
            }
        }
        let broker_name = self.composition.state.broker_config().broker_name().clone();
        let task_group = self.composition.state.service_context.as_ref().map(|service_context| {
            service_context
                .component(format!("rocketmq-broker.transaction-check.{broker_name}"))
                .task_group()
                .clone()
        });
        let listener = DefaultTransactionalMessageCheckListener::new(
            broker_name,
            self.composition.state.producer_manager().channel_registry(),
            Arc::new(Broker2Client),
            task_group,
        );
        self.composition.state.transactional_message_check_listener = Some(listener.clone());
        self.composition.state.transactional_message_check_service = self
            .composition
            .state
            .transactional_message_service
            .as_ref()
            .map(|service| {
                Arc::new(TransactionalMessageCheckService::new(
                    self.composition.state.broker_config_arc(),
                    service.clone(),
                    listener,
                ))
            });
        true
    }
}
