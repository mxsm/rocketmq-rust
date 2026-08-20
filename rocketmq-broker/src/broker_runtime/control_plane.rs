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
#[cfg(feature = "otel-metrics")]
use crate::metrics::consumer_lag_snapshot::ConsumerLagSnapshotService;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerReplicationStore;
#[derive(Default)]
pub(super) struct BrokerControlPlane {
    pub(super) broadcast_offset_scan_started: bool,
    pub(super) consumer_lag_observability_initialized: bool,
}

impl BrokerControlPlane {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "otel-metrics")]
pub(super) fn consumer_lag_runtime_settings(policy: rocketmq_observability::MetricsRuntimePolicy) -> (Duration, usize) {
    (
        Duration::from_millis(policy.export_interval_millis.max(1)),
        policy.cardinality_limit,
    )
}

impl BrokerRuntime {
    pub(super) fn initialize_observability(&mut self) {
        if !self.composition.state.telemetry_handle.metrics_enabled() {
            return;
        }

        #[cfg(feature = "otel-metrics")]
        if !self.composition.state.observability_metrics_initialized {
            let broker_config = self.composition.state.broker_config();
            self.composition.state.observability_metrics_initialized = true;
            let broker_metrics_manager = self.composition.state.broker_metrics_manager.clone();
            if let Some(metrics_manager) = broker_metrics_manager {
                let broker_permission = i64::from(broker_config.broker_permission);
                let topic_config_manager = self.composition.state.topic_config_manager_handle();
                let subscription_group_manager = self.composition.state.subscription_group_manager().clone();
                let producer_manager = self.composition.state.producer_manager.clone_shared_state();
                let consumer_manager = self.composition.state.consumer_manager.clone_shared_state();
                let broker_fast_failure = self.composition.state.broker_fast_failure.clone();
                metrics_manager.register_observables(
                    Some(move || broker_fast_failure.pending_count_snapshot()),
                    move || broker_permission,
                    move || i64::try_from(topic_config_manager.topic_count()).unwrap_or(i64::MAX),
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
            }

            if let Some(metrics_manager) = self.composition.state.pop_metrics_manager.clone() {
                let pop_offset_processor = self.composition.state.pop_message_processor.clone();
                let pop_checkpoint_processor = self.composition.state.pop_message_processor.clone();
                let ack_message_processor = self.composition.state.ack_message_processor.clone();
                metrics_manager.register_observables(
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
            }

            let store_observable = self.composition.state.escape_bridge().store_capability();
            self.composition
                .state
                .store_telemetry
                .store()
                .register_observables(move || {
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
            self.composition
                .state
                .store_telemetry
                .timer()
                .register_observables(move || {
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
                self.composition
                    .state
                    .store_telemetry
                    .rocksdb()
                    .register_observables(move || {
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
                    });
            }

            #[cfg(feature = "tieredstore")]
            {
                let tiered_store_observable = self.composition.state.escape_bridge().store_capability();
                self.composition
                    .state
                    .store_telemetry
                    .tiered_store()
                    .register_observables(move || {
                        tiered_store_observable
                            .with_store(|message_store| {
                                message_store
                                    .tiered_store_metrics()
                                    .map(|metrics| metrics.observable_values())
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default()
                    });
            }
        }
    }

    pub(super) fn initialize_consumer_lag_observability(&mut self) {
        #[cfg(feature = "otel-metrics")]
        {
            if self.composition.control_plane.consumer_lag_observability_initialized {
                return;
            }
            let Some(metrics_manager) = self.composition.state.broker_metrics_manager.clone() else {
                return;
            };
            let Some(pop_processor) = self.composition.state.pop_message_processor.clone() else {
                warn!("Consumer lag observability requires an initialized POP processor");
                return;
            };
            let broker_config = self.composition.state.broker_config();
            let (refresh_interval, cardinality_limit) =
                consumer_lag_runtime_settings(self.composition.state.telemetry_handle.metrics_runtime_policy());
            let consumer_lag_snapshot = Arc::new(ConsumerLagSnapshotService::new(
                self.composition.state.consumer_offset_manager_handle(),
                self.composition.state.consumer_manager.clone_shared_state(),
                pop_processor,
                broker_config.enable_notify_before_pop_calculate_lag,
                cardinality_limit,
            ));
            let refresh_snapshot = Arc::clone(&consumer_lag_snapshot);
            let schedule_result = self
                .lifecycle
                .scheduled_task_manager
                .add_fixed_rate_no_overlap_task_async(Duration::ZERO, refresh_interval, move |ctx| {
                    let refresh_snapshot = Arc::clone(&refresh_snapshot);
                    async move {
                        if !ctx.is_cancelled() {
                            refresh_snapshot.refresh().await;
                        }
                        Ok(())
                    }
                });
            if let Err(error) = schedule_result {
                error!(%error, "Failed to start consumer lag snapshot refresh");
                return;
            }

            metrics_manager.register_consumer_lag_observable_gauge(move || {
                consumer_lag_snapshot
                    .current()
                    .iter()
                    .map(|observation| {
                        crate::metrics::broker_metrics_manager::ConsumerLagAttributes::new(
                            observation.topic.clone(),
                            observation.consumer_group.clone(),
                            observation.lag_messages,
                        )
                    })
                    .collect()
            });
            self.composition.control_plane.consumer_lag_observability_initialized = true;
        }
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
        let protect_broker_stats = self.composition.state.broker_stats_manager.clone();
        let protect_subscription_groups = self.composition.state.subscription_group_manager().clone();
        let protect_broker_config = self.composition.state.broker_config();
        let protect_slow_consumers = protect_broker_config.disable_consume_if_consumer_read_slowly;
        let consumer_fallbehind_threshold = protect_broker_config.consumer_fallbehind_threshold;
        Self::log_scheduled_task_start(
            "protect_broker",
            self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_mins(3),
                Duration::from_mins(3),
                move |ctx| {
                    let protect_broker_shutdown = Arc::clone(&protect_broker_shutdown);
                    let protect_broker_stats = protect_broker_stats.clone();
                    let protect_subscription_groups = protect_subscription_groups.clone();
                    async move {
                        if ctx.is_cancelled() || protect_broker_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        if protect_slow_consumers {
                            if let Some(fall_size_set) = protect_broker_stats
                                .as_ref()
                                .and_then(|stats| stats.get_moment_stats_item_set_fall_size())
                            {
                                let mut slow_groups = std::collections::HashMap::new();
                                for entry in fall_size_set.get_stats_item_table().iter() {
                                    let lag = entry.value().get_value().load(Ordering::Relaxed);
                                    if lag <= consumer_fallbehind_threshold {
                                        continue;
                                    }
                                    let Some(group) = entry.key().split('@').nth(2) else {
                                        continue;
                                    };
                                    slow_groups
                                        .entry(CheetahString::from(group))
                                        .and_modify(|current: &mut i64| *current = (*current).max(lag))
                                        .or_insert(lag);
                                }
                                for (group, lag) in slow_groups {
                                    protect_subscription_groups.disable_consume_for_lag(
                                        &group,
                                        lag,
                                        consumer_fallbehind_threshold,
                                    );
                                }
                            }
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
                            warn!("BrokerStorePort is not initialized");
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
                            warn!("BrokerStorePort is unavailable before replica synchronization");
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
        let auth_config = build_auth_config(&broker_config);
        let maintenance_authorizer = match auth_config.maintenance_policy_reference() {
            Ok(Some(reference)) => match reference.load_from(auth_config.auth_config_path.as_str()) {
                Ok(policy) => Some(Arc::new(MaintenanceAuthorizer::new(policy))),
                Err(error) => {
                    error!(%error, "Initialize maintenance authorization failed");
                    return false;
                }
            },
            Ok(None) => None,
            Err(error) => {
                error!(%error, "Validate maintenance authorization reference failed");
                return false;
            }
        };
        self.composition.request_pipeline.maintenance_authorizer = maintenance_authorizer;
        if !broker_config.authentication_enabled && !broker_config.authorization_enabled {
            self.composition.request_pipeline.auth_runtime = None;
            let Some(service_context) = self.composition.state.service_context.as_ref() else {
                error!("Initialize auth admin service failed because ChildServiceContext is unavailable");
                return false;
            };
            return match AuthAdminService::new(auth_config, service_context.component("broker.auth-admin")).await {
                Ok(service) => {
                    self.composition.request_pipeline.auth_admin_service = Some(Arc::new(service));
                    true
                }
                Err(error) => {
                    error!("Initialize auth admin service failed: {error}");
                    false
                }
            };
        }

        let auth_context = match self.composition.state.service_context.as_ref() {
            Some(service_context) => service_context.component("broker.auth"),
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
                if let Some(metrics_manager) = self.composition.state.broker_metrics_manager.as_ref() {
                    let auth_runtime_for_metrics = auth_runtime.clone();
                    metrics_manager
                        .register_auth_observable_gauge(move || Some(auth_runtime_for_metrics.metrics_snapshot()));
                }
                self.composition.request_pipeline.auth_admin_service =
                    Some(Arc::new(AuthAdminService::with_provider_registry_and_config(
                        auth_runtime.provider_registry().clone(),
                        auth_runtime.config().clone(),
                    )));
                self.composition.request_pipeline.auth_runtime = Some(auth_runtime);
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

fn observed_replication_lag_bytes(max_phy_offset: i64, confirm_offset: i64) -> Option<u64> {
    if max_phy_offset < 0 || confirm_offset < 0 || confirm_offset > max_phy_offset {
        return None;
    }
    u64::try_from(max_phy_offset - confirm_offset).ok()
}

#[cfg(test)]
mod tests {
    use super::observed_replication_lag_bytes;

    #[test]
    fn replication_lag_is_observed_only_for_valid_store_offsets() {
        assert_eq!(observed_replication_lag_bytes(128, 96), Some(32));
        assert_eq!(observed_replication_lag_bytes(128, 128), Some(0));
        assert_eq!(observed_replication_lag_bytes(-1, 0), None);
        assert_eq!(observed_replication_lag_bytes(128, -1), None);
        assert_eq!(observed_replication_lag_bytes(96, 128), None);
    }
}
