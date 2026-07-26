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
#[derive(Default)]
pub(super) struct BrokerControlPlane {
    pub(super) broadcast_offset_scan_started: bool,
}

impl BrokerControlPlane {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

impl BrokerRuntime {
    pub(super) fn initialize_observability(&mut self) {
        let broker_config = self.composition.state.broker_config();
        let bootstrap_config = build_broker_telemetry_bootstrap_config(&broker_config);
        let config = &bootstrap_config.observability;
        if !config.enabled {
            return;
        }

        if self.composition.state.observability_guard.is_none() {
            match rocketmq_observability::logging::install_global(&bootstrap_config) {
                Ok(guard) => self.composition.state.observability_guard = Some(guard),
                Err(error) => {
                    warn!("Failed to initialize broker observability: {error}");
                    return;
                }
            }
        }

        #[cfg(feature = "otel-metrics")]
        if !self.composition.state.observability_metrics_initialized {
            self.composition.state.observability_metrics_initialized = true;
            if let Some(provider) = self
                .composition
                .state
                .observability_guard
                .as_ref()
                .and_then(|guard| guard.telemetry_guard().meter_provider())
            {
                let label_config = crate::metrics::broker_metrics_manager::BrokerMetricsLabelConfig::new(
                    config.metrics.cardinality_limit,
                    config.metrics.topic_label_enabled,
                    config.metrics.consumer_group_label_enabled,
                );
                let sampling_config = crate::metrics::broker_metrics_manager::BrokerMetricsSamplingConfig::new(
                    config.metrics.sample_ratio,
                );
                let attributes_supplier =
                    Arc::new(crate::metrics::broker_metrics_manager::BrokerAttributesSupplier::new(
                        broker_config.broker_identity.broker_cluster_name.to_string(),
                        broker_config.broker_identity.get_canonical_name(),
                    ));
                let broker_permission = i64::from(broker_config.broker_permission);
                let topic_config_manager = self.composition.state.topic_config_manager_handle();
                let subscription_group_manager = self.composition.state.subscription_group_manager().clone();
                let producer_manager = self.composition.state.producer_manager.clone_shared_state();
                let consumer_manager = self.composition.state.consumer_manager.clone_shared_state();
                crate::metrics::broker_metrics_manager::BrokerMetricsManager::init_global_with_observables_and_configs(
                    provider,
                    attributes_supplier,
                    label_config,
                    sampling_config,
                    None::<fn() -> Vec<(String, i64)>>,
                    move || broker_permission,
                    move || i64::try_from(topic_config_manager.topic_config_table().len()).unwrap_or(i64::MAX),
                    move || i64::try_from(subscription_group_manager.group_count()).unwrap_or(i64::MAX),
                    move || {
                        producer_manager
                            .connection_count_by_client_attrs()
                            .into_iter()
                            .map(|(language, version, count)| {
                                (
                                    crate::metrics::broker_metrics_manager::ProducerConnectionAttributes::new(
                                        language.to_string(),
                                        version,
                                    ),
                                    count,
                                )
                            })
                            .collect()
                    },
                    move || {
                        consumer_manager
                            .connection_count_by_client_attrs()
                            .into_iter()
                            .map(|(group, language, version, consume_type, count)| {
                                (
                                    crate::metrics::broker_metrics_manager::ConsumerConnectionAttributes::new(
                                        group.to_string(),
                                        language.to_string(),
                                        version,
                                        consume_type.to_string(),
                                    ),
                                    count,
                                )
                            })
                            .collect()
                    },
                );
                let pop_offset_processor = self.composition.state.pop_message_processor.clone();
                let pop_checkpoint_processor = self.composition.state.pop_message_processor.clone();
                let ack_message_processor = self.composition.state.ack_message_processor.clone();
                crate::metrics::pop_metrics_manager::PopMetricsManager::init_global_with_observables(
                    provider,
                    Arc::new(crate::metrics::pop_metrics_manager::BrokerAttributesSupplier::new(
                        broker_config.broker_identity.broker_cluster_name.to_string(),
                        broker_config.broker_identity.broker_name.to_string(),
                        i64::try_from(broker_config.broker_identity.broker_id).unwrap_or(i64::MAX),
                    )),
                    move || {
                        pop_offset_processor
                            .as_ref()
                            .map(|processor| {
                                i64::try_from(processor.pop_buffer_merge_service().offset_buffer_size_snapshot())
                                    .unwrap_or(i64::MAX)
                            })
                            .unwrap_or(0)
                    },
                    move || {
                        pop_checkpoint_processor
                            .as_ref()
                            .map(|processor| {
                                i64::try_from(processor.pop_buffer_merge_service().checkpoint_buffer_size())
                                    .unwrap_or(i64::MAX)
                            })
                            .unwrap_or(0)
                    },
                    move || {
                        ack_message_processor
                            .as_ref()
                            .map(|processor| processor.pop_revive_metrics())
                            .unwrap_or_default()
                    },
                );

                let store_meter = rocketmq_observability::meter(provider, "rocketmq-store");
                let store_observable = self.composition.state.escape_bridge().store_capability();
                let _ = rocketmq_observability::metrics::store::init_global_with_observables(&store_meter, move || {
                    store_observable
                        .with_store(|message_store| {
                            let max_phy_offset = message_store.get_max_phy_offset();
                            let min_phy_offset = message_store.get_min_phy_offset();
                            let earliest_message_time = message_store.get_earliest_message_time_store();
                            rocketmq_observability::metrics::store::StoreObservableValues {
                                storage_size_bytes: (max_phy_offset - min_phy_offset).max(0),
                                flush_behind_bytes: (max_phy_offset - message_store.get_flushed_where()).max(0),
                                dispatch_behind_bytes: message_store.dispatch_behind_bytes().max(0),
                                message_reserve_time_millis: if earliest_message_time > 0 {
                                    current_millis() as i64 - earliest_message_time
                                } else {
                                    0
                                },
                            }
                        })
                        .unwrap_or_default()
                });

                let timer_observable = self.composition.state.timer_message_store().cloned();
                let _ = rocketmq_observability::metrics::timer::init_global_with_observables(&store_meter, move || {
                    let Some(timer_message_store) = timer_observable.as_ref() else {
                        return rocketmq_observability::metrics::timer::TimerObservableValues::default();
                    };
                    let (timing_messages, message_snapshot) = timer_message_store.runtime_backlog_metrics();
                    rocketmq_observability::metrics::timer::TimerObservableValues {
                        enqueue_lag: timer_message_store.get_enqueue_behind_messages(),
                        enqueue_latency_millis: timer_message_store.get_enqueue_behind_millis(),
                        dequeue_lag: timer_message_store.get_all_congest_num(),
                        dequeue_latency_millis: timer_message_store.get_dequeue_behind_millis(),
                        timing_messages: timing_messages.into_iter().collect(),
                        message_snapshot: message_snapshot.into_iter().collect(),
                    }
                });

                #[cfg(feature = "rocksdb_store")]
                {
                    let rocksdb_observable = self.composition.state.escape_bridge().store_capability();
                    let _ = rocketmq_observability::metrics::rocksdb::init_global_with_observables(
                        &store_meter,
                        move || {
                            rocksdb_observable
                                .with_store(|message_store| {
                                    let Some(metrics) = message_store.rocksdb_ticker_metrics() else {
                                        return Default::default();
                                    };
                                    rocketmq_observability::metrics::rocksdb::RocksDbObservableValues {
                                        bytes_written: metrics.bytes_written,
                                        bytes_read: metrics.bytes_read,
                                        times_written_self: metrics.times_written_self,
                                        times_written_other: metrics.times_written_other,
                                        block_cache_hit: metrics.block_cache_hit,
                                        block_cache_miss: metrics.block_cache_miss,
                                        times_compressed: metrics.times_compressed,
                                        read_amplification_bytes: metrics.read_amplification_bytes,
                                        times_read: metrics.times_read,
                                    }
                                })
                                .unwrap_or_default()
                        },
                    );
                }

                #[cfg(feature = "tieredstore")]
                {
                    let tiered_store_observable = self.composition.state.escape_bridge().store_capability();
                    let _ = rocketmq_observability::metrics::tiered_store::init_global_with_observables(
                        &store_meter,
                        move || {
                            tiered_store_observable
                                .with_store(|message_store| {
                                    message_store
                                        .tiered_store_metrics()
                                        .map(|metrics| metrics.observable_values())
                                        .unwrap_or_default()
                                })
                                .unwrap_or_default()
                        },
                    );
                }

                let remoting_meter = rocketmq_observability::meter(provider, "rocketmq-transport");
                let _ = rocketmq_observability::metrics::remoting::init_global(&remoting_meter);
            }
        }

        let Some(guard) = self.composition.state.observability_guard.as_ref() else {
            return;
        };
        let subscriber_install_status = guard.subscriber_install_status();
        info!(
            metrics_exporter = ?config.metrics.exporter,
            trace_exporter = ?config.traces.exporter,
            log_exporter = ?config.logs.exporter,
            subscriber_installed = subscriber_install_status.installed,
            file_log_enabled = bootstrap_config.logging.file.enabled,
            "initialized broker observability"
        );
    }

    #[allow(clippy::incompatible_msrv)]
    pub(super) async fn initialize_scheduled_tasks(&mut self) {
        let initial_delay = compute_next_morning_time_millis() - current_millis();
        let period = Duration::from_secs(24 * 60 * 60);
        let broker_stats_shutdown = Arc::clone(&self.composition.state.shutdown);
        let broker_stats = self.composition.state.broker_stats.clone();
        Self::log_scheduled_task_start(
            "daily_broker_stats_record",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_millis(initial_delay),
                period,
                move |ctx| {
                    let broker_stats_shutdown = Arc::clone(&broker_stats_shutdown);
                    let broker_stats = broker_stats.clone();
                    async move {
                        if ctx.is_cancelled() || broker_stats_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        if let Some(broker_stats) = broker_stats.as_ref() {
                            broker_stats.record();
                        } else {
                            warn!("BrokerStats is not initialized");
                        }
                        Ok(())
                    }
                },
            ),
        );

        let consumer_offset_shutdown = Arc::clone(&self.composition.state.shutdown);
        let consumer_offset_manager = self.composition.state.consumer_offset_manager_handle();
        let flush_consumer_offset_interval = self.composition.state.broker_config().flush_consumer_offset_interval;
        let metadata_io = self
            .composition
            .state
            .metadata_io
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .cloned();
        let metadata_blocking = self
            .composition
            .state
            .service_context
            .as_ref()
            .map(|context| context.metadata_io().clone());

        Self::log_scheduled_task_start(
            "flush_consumer_offset",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_secs(10),
                Duration::from_millis(flush_consumer_offset_interval),
                move |ctx| {
                    let consumer_offset_shutdown = Arc::clone(&consumer_offset_shutdown);
                    let consumer_offset_manager = consumer_offset_manager.clone();
                    let metadata_io = metadata_io.clone();
                    let metadata_blocking = metadata_blocking.clone();
                    async move {
                        if ctx.is_cancelled() || consumer_offset_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let result = match metadata_blocking {
                            Some(blocking) => {
                                persist_config_manager(
                                    consumer_offset_manager,
                                    "broker.consumer-offset",
                                    metadata_io,
                                    blocking,
                                    MetadataDeadline::after(Duration::from_secs(5)),
                                )
                                .await
                            }
                            None => consumer_offset_manager.persist(),
                        };
                        if let Err(error) = result {
                            warn!(%error, "Failed to persist consumer offsets");
                        }
                        Ok(())
                    }
                },
            ),
        );

        let persistence_shutdown = Arc::clone(&self.composition.state.shutdown);
        let consumer_filter_manager = self.composition.state.consumer_filter_manager.clone().map(Arc::new);
        let consumer_order_info_manager = self.composition.state.consumer_order_info_manager.clone();
        let metadata_io = self
            .composition
            .state
            .metadata_io
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .cloned();
        let metadata_blocking = self
            .composition
            .state
            .service_context
            .as_ref()
            .map(|context| context.metadata_io().clone());
        Self::log_scheduled_task_start(
            "persist_consumer_filter_and_order_info",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_secs(10),
                Duration::from_secs(10),
                move |ctx| {
                    let persistence_shutdown = Arc::clone(&persistence_shutdown);
                    let consumer_filter_manager = consumer_filter_manager.clone();
                    let consumer_order_info_manager = consumer_order_info_manager.clone();
                    let metadata_io = metadata_io.clone();
                    let metadata_blocking = metadata_blocking.clone();
                    async move {
                        if ctx.is_cancelled() || persistence_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        if let Some(consumer_filter_manager) = consumer_filter_manager.as_ref() {
                            let result = match metadata_blocking.clone() {
                                Some(blocking) => {
                                    persist_config_manager(
                                        consumer_filter_manager.clone(),
                                        "broker.consumer-filter",
                                        metadata_io.clone(),
                                        blocking,
                                        MetadataDeadline::after(Duration::from_secs(5)),
                                    )
                                    .await
                                }
                                None => consumer_filter_manager.persist(),
                            };
                            if let Err(error) = result {
                                warn!(%error, "Failed to persist consumer filters");
                            }
                        } else {
                            warn!("ConsumerFilterManager is not initialized");
                        }
                        if let Some(consumer_order_info_manager) = consumer_order_info_manager.as_ref() {
                            let result = match metadata_blocking {
                                Some(blocking) => {
                                    persist_config_manager(
                                        consumer_order_info_manager.clone(),
                                        "broker.consumer-order-info",
                                        metadata_io,
                                        blocking,
                                        MetadataDeadline::after(Duration::from_secs(5)),
                                    )
                                    .await
                                }
                                None => consumer_order_info_manager.persist(),
                            };
                            if let Err(error) = result {
                                warn!(%error, "Failed to persist consumer order info");
                            }
                        } else {
                            warn!("ConsumerOrderInfoManager is not initialized");
                        }

                        Ok(())
                    }
                },
            ),
        );

        let protect_broker_shutdown = Arc::clone(&self.composition.state.shutdown);
        Self::log_scheduled_task_start(
            "protect_broker",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_mins(3),
                Duration::from_mins(3),
                move |ctx| {
                    let protect_broker_shutdown = Arc::clone(&protect_broker_shutdown);
                    async move {
                        if ctx.is_cancelled() || protect_broker_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        Ok(())
                    }
                },
            ),
        );

        let dispatch_report_shutdown = Arc::clone(&self.composition.state.shutdown);
        let dispatch_report_store = self.composition.state.escape_bridge().store_capability();
        Self::log_scheduled_task_start(
            "report_dispatch_behind_bytes",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_secs(10),
                Duration::from_secs(60),
                move |ctx| {
                    let dispatch_report_shutdown = Arc::clone(&dispatch_report_shutdown);
                    let dispatch_report_store = dispatch_report_store.clone();
                    async move {
                        if ctx.is_cancelled() || dispatch_report_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        if let Err(_unavailable) = dispatch_report_store.with_store(|message_store| {
                            let behind = message_store.dispatch_behind_bytes();
                            info!("Dispatch task fall behind commit log {behind}bytes");
                        }) {
                            warn!("MessageStore is not initialized");
                        }
                        Ok(())
                    }
                },
            ),
        );

        let broker_config = self.composition.state.broker_config();
        let message_store_config = self.composition.state.message_store_config();
        if !message_store_config.enable_dledger_commit_log
            && !message_store_config.duplication_enable
            && !message_store_config.enable_controller_mode
        {
            if BrokerRole::Slave == broker_config.broker_role {
                info!("Broker is Slave, start replicas manager");
                let ha_master_address = message_store_config.ha_master_address.as_ref();
                if let Some(ha_master_address) = ha_master_address {
                    if ha_master_address.len() > 6 {
                        if let Some(message_store) = self.composition.state.message_store() {
                            message_store.update_ha_master_address(ha_master_address.as_str()).await;
                            self.composition.state.update_master_haserver_addr_periodically = false;
                        } else {
                            warn!("MessageStore is unavailable before replica synchronization");
                            self.composition.state.update_master_haserver_addr_periodically = true;
                        }
                    } else {
                        self.composition.state.update_master_haserver_addr_periodically = true;
                    }
                } else {
                    self.composition.state.update_master_haserver_addr_periodically = true;
                }
                let slave_sync_shutdown = Arc::clone(&self.composition.state.shutdown);
                let slave_synchronize = self.composition.state.slave_synchronize.clone();
                let config_state = self.composition.state.config_state.clone();
                let last_sync_time_ms = Arc::new(AtomicU64::new(current_millis()));
                Self::log_scheduled_task_start(
                    "slave_synchronize",
                    self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                        Duration::from_secs(10),
                        Duration::from_secs(3),
                        move |ctx| {
                            let slave_sync_shutdown = Arc::clone(&slave_sync_shutdown);
                            let slave_synchronize = slave_synchronize.clone();
                            let config_state = config_state.clone();
                            let last_sync_time_ms = Arc::clone(&last_sync_time_ms);
                            async move {
                                if ctx.is_cancelled() || slave_sync_shutdown.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                                if current_millis() - last_sync_time_ms.load(Ordering::Relaxed) > 10_000 {
                                    if let Some(slave_synchronize) = slave_synchronize.as_ref() {
                                        slave_synchronize.sync_all().await;
                                    }
                                    last_sync_time_ms.store(current_millis(), Ordering::Relaxed);
                                }
                                if config_state.store_snapshot().timer_wheel_enable {
                                    if let Some(slave_synchronize) = slave_synchronize.as_ref() {
                                        slave_synchronize.sync_timer_check_point().await
                                    }
                                }
                                Ok(())
                            }
                        },
                    ),
                );
            } else {
                let master_diff_shutdown = Arc::clone(&self.composition.state.shutdown);
                let master_diff_store = self.composition.data_plane.escape_bridge_owner.store_capability();
                Self::log_scheduled_task_start(
                    "print_master_and_slave_diff",
                    self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                        Duration::from_secs(10),
                        Duration::from_secs(60),
                        move |ctx| {
                            let master_diff_shutdown = Arc::clone(&master_diff_shutdown);
                            let master_diff_store = master_diff_store.clone();
                            async move {
                                if ctx.is_cancelled() || master_diff_shutdown.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                                match (
                                    master_diff_store.append_progress(),
                                    master_diff_store.master_flushed_offset(),
                                ) {
                                    (Ok((max_phy_offset, flushed_offset)), Ok(master_flushed_offset)) => {
                                        info!(
                                            max_phy_offset,
                                            flushed_offset,
                                            master_flushed_offset,
                                            replication_diff = max_phy_offset.saturating_sub(master_flushed_offset),
                                            "Broker replication progress"
                                        );
                                    }
                                    (append_progress, master_progress) => {
                                        warn!(
                                            ?append_progress,
                                            ?master_progress,
                                            "Unable to read broker replication progress"
                                        );
                                    }
                                }
                                Ok(())
                            }
                        },
                    ),
                );
            }
        }

        if broker_config.enable_controller_mode {
            self.composition.state.update_master_haserver_addr_periodically = true;
        }

        if let Some(ref namesrv_address) = broker_config.namesrv_addr.clone() {
            self.update_namesrv_addr().await;
            info!("Set user specified name remoting_server address: {}", namesrv_address);
            let namesrv_shutdown = Arc::clone(&self.composition.state.shutdown);
            let namesrv_config = self.composition.state.config_state.clone();
            let broker_outer_api = self.composition.state.broker_outer_api.clone();
            Self::log_scheduled_task_start(
                "update_namesrv_addr",
                self.lifecycle
                    .scheduled_task_manager
                    .add_fixed_rate_no_overlap_task_async(
                        Duration::from_secs(10),
                        Duration::from_secs(60),
                        move |ctx| {
                            let namesrv_shutdown = Arc::clone(&namesrv_shutdown);
                            let namesrv_config = namesrv_config.clone();
                            let broker_outer_api = broker_outer_api.clone();
                            async move {
                                if ctx.is_cancelled() || namesrv_shutdown.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                                let broker_config = namesrv_config.broker_snapshot();
                                if broker_config.fetch_name_srv_addr_by_dns_lookup {
                                    if let Some(namesrv_addr) = &broker_config.namesrv_addr {
                                        broker_outer_api
                                            .update_name_server_address_list_by_dns_lookup(namesrv_addr.clone())
                                            .await;
                                    }
                                } else if let Some(namesrv_addr) = &broker_config.namesrv_addr {
                                    broker_outer_api
                                        .update_name_server_address_list(namesrv_addr.clone())
                                        .await;
                                }
                                Ok(())
                            }
                        },
                    ),
            );
        }
    }

    pub(super) fn log_scheduled_task_start(task_name: &str, task_id: rocketmq_runtime::RuntimeResult<u64>) {
        if let Err(error) = task_id {
            error!("Failed to start scheduled task {task_name}: {error}");
        }
    }

    pub(super) async fn initial_acl(&mut self) -> bool {
        let broker_config = self.composition.state.broker_config();
        if !broker_config.authentication_enabled && !broker_config.authorization_enabled {
            self.composition.state.auth_runtime = None;
            let Some(service_context) = self.composition.state.service_context.as_ref() else {
                error!("Initialize auth admin service failed because ChildServiceContext is unavailable");
                return false;
            };
            return match AuthAdminService::new(
                build_auth_config(&broker_config),
                service_context.child("broker.auth-admin"),
            )
            .await
            {
                Ok(service) => {
                    self.composition.state.auth_admin_service = Some(Arc::new(service));
                    true
                }
                Err(error) => {
                    error!("Initialize auth admin service failed: {error}");
                    false
                }
            };
        }

        let auth_config = build_auth_config(&self.composition.state.broker_config());
        let auth_context = match self.composition.state.service_context.as_ref() {
            Some(service_context) => service_context.child("broker.auth"),
            None => {
                error!("Initialize auth runtime failed because ChildServiceContext is unavailable");
                return false;
            }
        };
        let auth_runtime_builder = match self.composition.state.metadata_io.as_ref() {
            Some(Ok(metadata_io)) => {
                AuthRuntimeBuilder::new(auth_config, auth_context).with_metadata_io_actor(metadata_io.clone())
            }
            Some(Err(error)) => {
                error!(%error, "Initialize auth runtime failed because metadata I/O actor is unavailable");
                return false;
            }
            None => AuthRuntimeBuilder::new(auth_config, auth_context),
        };
        match auth_runtime_builder.build().await {
            Ok(auth_runtime) => {
                let auth_runtime = Arc::new(auth_runtime);
                if let Some(metrics_manager) =
                    crate::metrics::broker_metrics_manager::BrokerMetricsManager::try_global()
                {
                    let auth_runtime_for_metrics = auth_runtime.clone();
                    metrics_manager
                        .register_auth_observable_gauge(move || Some(auth_runtime_for_metrics.metrics_snapshot()));
                }
                self.composition.state.auth_admin_service =
                    Some(Arc::new(AuthAdminService::with_provider_registry_and_config(
                        auth_runtime.provider_registry().clone(),
                        auth_runtime.config().clone(),
                    )));
                self.composition.state.auth_runtime = Some(auth_runtime);
                true
            }
            Err(error) => {
                error!("Initialize auth runtime failed: {error}");
                false
            }
        }
    }

    pub(super) fn initial_rpc_hooks(&mut self) -> bool {
        let auth_config = build_auth_config(&self.composition.state.broker_config());
        match AclClientRpcHook::from_auth_config(&auth_config) {
            Ok(Some(rpc_hook)) => {
                self.composition
                    .state
                    .broker_outer_api
                    .register_rpc_hook(rpc_hook.into_rpc_hook());
                true
            }
            Ok(None) => true,
            Err(error) => {
                error!("Initialize broker ACL RPC hook failed: {error}");
                false
            }
        }
    }

    pub(crate) fn schedule_send_heartbeat(&mut self) {
        let broker_heartbeat_interval = self.composition.state.broker_config().broker_heartbeat_interval;
        let controller_runtime = self.composition.state.build_controller_runtime();
        Self::log_scheduled_task_start(
            "send_heartbeat",
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_no_overlap_task_async(
                    Duration::from_millis(1000),
                    Duration::from_millis(broker_heartbeat_interval),
                    move |ctx| {
                        let controller_runtime = controller_runtime.clone();
                        async move {
                            if ctx.is_cancelled() {
                                return Ok(());
                            }
                            controller_runtime.run_heartbeat_cycle().await;
                            Ok(())
                        }
                    },
                ),
        );
    }

    pub(crate) fn schedule_sync_controller_metadata(&mut self) {
        let period = self.composition.state.broker_config().sync_controller_metadata_period;
        let controller_runtime = self.composition.state.build_controller_runtime();
        Self::log_scheduled_task_start(
            "sync_controller_metadata",
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_no_overlap_task_async(
                    Duration::from_millis(1000),
                    Duration::from_millis(period),
                    move |ctx| {
                        let controller_runtime = controller_runtime.clone();
                        async move {
                            if ctx.is_cancelled() {
                                return Ok(());
                            }
                            controller_runtime.refresh_controller_leader().await;
                            Ok(())
                        }
                    },
                ),
        );
    }

    pub(crate) fn schedule_sync_controller_replica_info(&mut self) {
        let period = self.composition.state.broker_config().sync_broker_metadata_period;
        let controller_runtime = self.composition.state.build_controller_runtime();
        Self::log_scheduled_task_start(
            "sync_controller_replica_info",
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_no_overlap_task_async(
                    Duration::from_millis(3000),
                    Duration::from_millis(period),
                    move |ctx| {
                        let controller_runtime = controller_runtime.clone();
                        async move {
                            if ctx.is_cancelled() {
                                return Ok(());
                            }
                            controller_runtime.sync_controller_replica_info().await;
                            Ok(())
                        }
                    },
                ),
        );
    }

    pub(crate) async fn start_service_without_condition(&mut self) -> Result<(), BrokerStartupError> {
        info!(
            "{} start service",
            self.composition
                .state
                .broker_config()
                .broker_identity
                .get_canonical_name()
        );
        let broker_config = self.composition.state.broker_config();
        let is_master = broker_config.broker_identity.broker_id == mix_all::MASTER_ID;
        self.composition.state.change_special_service_status(is_master).await;
        self.register_broker_all(true, false, broker_config.force_register)
            .await
            .map_err(|error| BrokerStartupError::component_start("broker_registration", error))?;
        self.composition.state.online_role_state.set_isolated(false);
        Ok(())
    }
}
