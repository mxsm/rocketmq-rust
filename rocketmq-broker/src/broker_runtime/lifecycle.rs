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

use super::shutdown_report::record_message_store_shutdown_outcome;
use super::*;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
pub(super) struct BrokerLifecycle {
    pub(super) shutdown_hook: Option<BrokerShutdownHook>,
    pub(super) scheduled_task_manager: BrokerScheduledTasks,
    pub(super) remoting_server_task_group: Option<TaskGroup>,
    pub(super) remoting_server_report_receivers: Vec<BrokerRemotingServerReportReceiver>,
    pub(super) request_processor_task_group: Option<TaskGroup>,
    pub(super) startup_journal: StartupJournal,
}

impl BrokerLifecycle {
    pub(super) fn new(scheduled_task_manager: BrokerScheduledTasks) -> Self {
        Self {
            shutdown_hook: None,
            scheduled_task_manager,
            remoting_server_task_group: None,
            remoting_server_report_receivers: Vec::new(),
            request_processor_task_group: None,
            startup_journal: StartupJournal::default(),
        }
    }
}

impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        // Abort all scheduled tasks spawned on the current tokio runtime so that their
        // ticker loops cannot spin at full speed and block the runtime from completing
        // shutdown when the broker is dropped (e.g. during test panic unwind).
        self.lifecycle.scheduled_task_manager.abort_all();
    }
}

impl BrokerRuntime {
    pub async fn shutdown(&mut self) {
        self.composition.state.shutdown.store(true, Ordering::SeqCst);

        self.shutdown_basic_service().await;
    }

    pub(super) async fn unregister_broker(&mut self) {
        let broker_config = self.composition.state.broker_config();
        self.composition
            .state
            .broker_outer_api
            .unregister_broker_all(
                &broker_config.broker_identity.broker_cluster_name,
                &broker_config.broker_identity.broker_name,
                self.composition.state.get_broker_addr(),
                broker_config.broker_identity.broker_id,
            )
            .await;
    }

    pub(crate) async fn shutdown_basic_service(&mut self) {
        let _ = self.shutdown_basic_service_with_report().await;
    }

    pub(crate) async fn shutdown_basic_service_with_report(&mut self) -> BrokerBasicServiceShutdownReport {
        self.shutdown_basic_service_until(ShutdownDeadline::after(BROKER_BASIC_SERVICE_SHUTDOWN_TIMEOUT))
            .await
    }

    pub(crate) async fn shutdown_basic_service_until(
        &mut self,
        deadline: ShutdownDeadline,
    ) -> BrokerBasicServiceShutdownReport {
        let progress = BrokerShutdownProgress::new();
        match await_shutdown_deadline(deadline, self.shutdown_basic_service_inner(deadline, progress.clone())).await {
            Ok(report) => report,
            Err(elapsed) => {
                warn!(
                    elapsed_ms = elapsed.as_millis(),
                    "Broker shutdown exhausted its absolute deadline"
                );
                BrokerBasicServiceShutdownReport {
                    message_store: progress
                        .message_store_report()
                        .unwrap_or_else(|| BrokerShutdownComponentReport::skipped("message_store")),
                    deadline: BrokerShutdownComponentReport::timed_out("shutdown_deadline", elapsed),
                    unfinished_components: progress.unfinished(),
                    ..Default::default()
                }
            }
        }
    }

    pub(super) async fn shutdown_basic_service_inner(
        &mut self,
        deadline: ShutdownDeadline,
        progress: BrokerShutdownProgress,
    ) -> BrokerBasicServiceShutdownReport {
        self.composition.state.shutdown.store(true, Ordering::SeqCst);
        let mut shutdown_report = BrokerBasicServiceShutdownReport {
            remoting: self.shutdown_remoting_servers(deadline).await,
            ..Default::default()
        };
        progress.complete("remoting");
        shutdown_report.request_processor = self.shutdown_request_processor_tasks(deadline).await;
        progress.complete("request_processor");

        // Authentication and ACL watchers no longer serve useful work after
        // remoting admission and request processors have stopped. Release
        // them before store-dependent drains so a later unhealthy phase
        // cannot leak security background work.
        let started = Instant::now();
        if let Some(auth_runtime) = self.composition.request_pipeline.auth_runtime.take() {
            if let Err(error) = auth_runtime.shutdown().await {
                warn!("Failed to shutdown auth runtime: {error}");
                shutdown_report.auth =
                    BrokerShutdownComponentReport::unhealthy("auth", started.elapsed(), error.to_string());
            } else {
                shutdown_report.auth = BrokerShutdownComponentReport::completed("auth", started.elapsed());
            }
        } else {
            shutdown_report.auth = BrokerShutdownComponentReport::skipped("auth");
        }
        progress.complete("auth");

        // Pre-online synchronization depends on metadata providers and Store/HA. Stop it before
        // detaching any of those providers so shutdown cannot race another online transition.
        if let Some(broker_pre_online_service) = self.composition.state.broker_pre_online_service.take() {
            if let Err(error) = broker_pre_online_service.shutdown().await {
                warn!(?error, "Failed to shutdown BrokerPreOnlineService cleanly");
            }
        }

        if let Some(slave_synchronize) = self.composition.state.slave_synchronize() {
            slave_synchronize.release_runtime_capabilities();
        }

        // Scheduled delivery must drain and persist its final coherent offset snapshot while the
        // message store is still available. Detach the service only after that owned shutdown
        // completes so in-flight delivery cannot observe a missing runtime slot.
        if let Some(schedule_message_service) = self.composition.state.schedule_message_service.as_ref().cloned() {
            if let Err(error) = schedule_message_service.shutdown().await {
                warn!(?error, "Failed to shutdown ScheduleMessageService cleanly");
            }
            self.composition.state.schedule_message_service.take();
        }

        // Transaction checking can create the checked-too-many-times topic and read/write the
        // message store. Stop admission, drain its owned check tasks, then stop the op batch
        // service before closing topic registration or the store. Taking every runtime slot also
        // breaks the service -> bridge -> runtime ownership cycle.
        let transaction_started = Instant::now();
        let mut transaction_services_present = false;
        if let Some(transactional_message_check_service) =
            self.composition.state.transactional_message_check_service.take()
        {
            transaction_services_present = true;
            if await_shutdown_deadline(deadline, transactional_message_check_service.shutdown())
                .await
                .is_err()
            {
                shutdown_report.transaction_services =
                    BrokerShutdownComponentReport::timed_out("transaction_services", transaction_started.elapsed());
                return shutdown_report;
            }
        }
        if let Some(transactional_message_check_listener) =
            self.composition.state.transactional_message_check_listener.take()
        {
            transaction_services_present = true;
            if let Some(listener_report) = transactional_message_check_listener
                .shutdown(deadline.remaining())
                .await
            {
                if !listener_report.is_healthy() {
                    shutdown_report.transaction_services = BrokerShutdownComponentReport::from_shutdown_report(
                        "transaction_services",
                        Some(&listener_report),
                        transaction_started.elapsed(),
                    );
                    return shutdown_report;
                }
            }
        }
        if let Some(transactional_message_service) = self.composition.state.transactional_message_service.take() {
            transaction_services_present = true;
            if await_shutdown_deadline(deadline, transactional_message_service.shutdown())
                .await
                .is_err()
            {
                shutdown_report.transaction_services =
                    BrokerShutdownComponentReport::timed_out("transaction_services", transaction_started.elapsed());
                return shutdown_report;
            }
        }
        shutdown_report.transaction_services = if transaction_services_present {
            BrokerShutdownComponentReport::completed("transaction_services", transaction_started.elapsed())
        } else {
            BrokerShutdownComponentReport::skipped("transaction_services")
        };
        progress.complete("transaction_services");

        if let Some(topic_config_coordinator) = self.composition.state.topic_config_coordinator.as_ref().cloned() {
            let mut topic_config_report = topic_config_coordinator.shutdown_until(deadline).await;
            if topic_config_report.can_unregister() {
                self.unregister_broker().await;
                topic_config_report.unregister_succeeded = true;
            }
            if topic_config_report.can_detach() {
                self.composition.state.topic_config_coordinator.take();
                self.composition.state.topic_config_manager.take();
                progress.complete("topic_config");
            } else {
                warn!(
                    ?topic_config_report,
                    "Topic config coordinator did not reach a detachable state"
                );
            }
            shutdown_report.topic_config = Some(topic_config_report);
            if !shutdown_report
                .topic_config
                .as_ref()
                .is_some_and(TopicConfigCoordinatorShutdownReport::is_healthy)
            {
                return shutdown_report;
            }
        } else {
            progress.complete("topic_config");
        }

        // Broker-owned scheduled tasks can hold request-scoped Store leases while executing.
        // Cancel and join them before waiting for exclusive Store ownership; otherwise a task
        // admitted immediately before shutdown can retain the store until the absolute deadline
        // and prevent a same-path Broker restart from acquiring the lock file.
        let started = Instant::now();
        let scheduled_report = self
            .shutdown_scheduled_tasks_with_timeout(deadline.remaining().min(SCHEDULED_TASK_SHUTDOWN_TIMEOUT))
            .await;
        shutdown_report.scheduled_tasks = if scheduled_report.is_healthy() {
            BrokerShutdownComponentReport::completed("scheduled_tasks", started.elapsed())
        } else {
            BrokerShutdownComponentReport::unhealthy(
                "scheduled_tasks",
                started.elapsed(),
                format!(
                    "task_count={}, completed={}, aborted={}, panicked={}, timed_out={}",
                    scheduled_report.task_count,
                    scheduled_report.completed,
                    scheduled_report.aborted,
                    scheduled_report.panicked,
                    scheduled_report.timed_out
                ),
            )
        };
        progress.complete("scheduled_tasks");

        // Shutdown uses one absolute deadline and this fixed phase order:
        // reject/drain requests -> drain store-backed delivery and scheduled Store leases ->
        // flush/replicate the store -> stop remaining background work -> telemetry.
        // Store durability therefore cannot be starved by a slow background component.
        self.detach_message_store_provider();
        while self
            .composition
            .state
            .message_store
            .as_ref()
            .is_some_and(|owner| Arc::strong_count(owner) > 1)
            && !deadline.is_expired()
        {
            tokio::task::yield_now().await;
        }
        let started = Instant::now();
        let store_owner_is_shared = self
            .composition
            .state
            .message_store
            .as_ref()
            .is_some_and(|owner| Arc::strong_count(owner) > 1);
        let message_store_outcome = if self.composition.state.message_store.is_none() {
            MessageStoreShutdownOutcome::Absent
        } else if store_owner_is_shared {
            MessageStoreShutdownOutcome::TimedOut
        } else if let Some(message_store) = self.composition.state.message_store_mut() {
            match await_shutdown_deadline(deadline, BrokerStorePort::shutdown_gracefully(message_store)).await {
                Ok(Ok(report)) => MessageStoreShutdownOutcome::Completed(report),
                Ok(Err(error)) => MessageStoreShutdownOutcome::Failed(error),
                Err(_elapsed) => MessageStoreShutdownOutcome::TimedOut,
            }
        } else {
            MessageStoreShutdownOutcome::TimedOut
        };
        let message_store_shutdown_completed =
            matches!(&message_store_outcome, MessageStoreShutdownOutcome::Completed(_));
        record_message_store_shutdown_outcome(
            &mut shutdown_report,
            &progress,
            message_store_outcome,
            started.elapsed(),
        );
        if message_store_shutdown_completed {
            // A completed shutdown is the ownership boundary for the Store root lease. Retain the
            // owner after failures or timeouts so a later shutdown attempt can finish durably.
            self.composition.state.message_store.take();
        }

        if let Some(hook) = self.lifecycle.shutdown_hook.clone() {
            let hook_result = if let Some(service_context) = self.composition.state.service_context.as_ref() {
                run_shutdown_blocking_operation(service_context, deadline, "broker.shutdown-hook", move || {
                    hook.before_shutdown();
                })
                .await
            } else {
                Err(BrokerBlockingShutdownError::MissingServiceContext)
            };
            if let Err(error) = hook_result {
                warn!(error = %error.detail(), "Broker shutdown hook did not complete cleanly");
            }
        }

        if let Some(broker_stats_manager) = self.composition.state.broker_stats_manager.as_ref() {
            broker_stats_manager.shutdown().await;
        }

        let started = Instant::now();
        let mut pull_request_hold_present = false;
        if let Some(pull_request_hold_service) = self.composition.state.pull_request_hold_service.as_ref() {
            pull_request_hold_present = true;
            pull_request_hold_service.shutdown().await;
        }
        shutdown_report.pull_request_hold = if pull_request_hold_present {
            BrokerShutdownComponentReport::completed("pull_request_hold", started.elapsed())
        } else {
            BrokerShutdownComponentReport::skipped("pull_request_hold")
        };
        progress.complete("pull_request_hold");

        let pop_started = Instant::now();
        let mut pop_services_present = false;
        if let Some(pop_message_processor) = self.composition.state.pop_message_processor.as_ref() {
            pop_services_present = true;
            pop_message_processor.shutdown().await;
        }

        if let Some(pop_lite_message_processor) = self.composition.state.pop_lite_message_processor.as_ref() {
            pop_services_present = true;
            pop_lite_message_processor.shutdown().await;
        }

        if let Some(ack_message_processor) = self.composition.state.ack_message_processor.as_ref() {
            pop_services_present = true;
            ack_message_processor.shutdown().await;
        }

        if let Some(notification_processor) = self.composition.state.notification_processor.as_ref() {
            pop_services_present = true;
            notification_processor.shutdown().await;
        }
        shutdown_report.pop_services = if pop_services_present {
            BrokerShutdownComponentReport::completed("pop_services", pop_started.elapsed())
        } else {
            BrokerShutdownComponentReport::skipped("pop_services")
        };
        progress.complete("pop_services");
        self.composition
            .request_pipeline
            .consumer_ids_change_listener
            .shutdown();
        if let Some(topic_queue_mapping_clean_service) =
            self.composition.state.topic_queue_mapping_clean_service.as_ref()
        {
            topic_queue_mapping_clean_service.shutdown().await;
        }

        self.composition.state.broadcast_offset_manager.shutdown();
        self.composition.control_plane.broadcast_offset_scan_started = false;

        self.composition
            .state
            .controller_state
            .with_replicas_mut(ReplicasManager::shutdown);

        let started = Instant::now();
        let fast_failure_report = self.composition.state.broker_fast_failure.shutdown_with_report().await;
        shutdown_report.fast_failure = BrokerShutdownComponentReport::from_shutdown_report(
            "fast_failure",
            fast_failure_report.as_ref(),
            started.elapsed(),
        );
        progress.complete("fast_failure");

        if let Some(consumer_filter_manager) = self.composition.state.consumer_filter_manager.take() {
            let result = if let Some(service_context) = self.composition.state.service_context.as_ref() {
                persist_config_manager(
                    Arc::new(consumer_filter_manager),
                    "broker.consumer-filter",
                    self.composition
                        .state
                        .metadata_io
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                        .cloned(),
                    service_context.metadata_io().clone(),
                    MetadataDeadline::after(deadline.remaining()),
                )
                .await
            } else {
                Err(rocketmq_error::RocketMQError::not_initialized(
                    "broker consumer-filter persistence requires ChildServiceContext",
                ))
            };
            if let Err(error) = result {
                warn!(%error, "Failed to persist consumer filters during shutdown");
            }
        }
        if let Some(consumer_order_info_manager) = self.composition.state.consumer_order_info_manager.take() {
            let result = if let Some(service_context) = self.composition.state.service_context.as_ref() {
                persist_config_manager(
                    consumer_order_info_manager,
                    "broker.consumer-order-info",
                    self.composition
                        .state
                        .metadata_io
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                        .cloned(),
                    service_context.metadata_io().clone(),
                    MetadataDeadline::after(deadline.remaining()),
                )
                .await
            } else {
                Err(rocketmq_error::RocketMQError::not_initialized(
                    "broker consumer-order persistence requires ChildServiceContext",
                ))
            };
            if let Err(error) = result {
                warn!(%error, "Failed to persist consumer order info during shutdown");
            }
        }

        self.composition.data_plane.escape_bridge_owner.shutdown();
        let started = Instant::now();
        let mut topic_route_present = false;
        if let Some(topic_route_info_manager) = self.composition.state.topic_route_info_manager.as_mut() {
            topic_route_present = true;
            topic_route_info_manager.shutdown().await;
        }
        shutdown_report.topic_route = if topic_route_present {
            BrokerShutdownComponentReport::completed("topic_route", started.elapsed())
        } else {
            BrokerShutdownComponentReport::skipped("topic_route")
        };
        progress.complete("topic_route");

        let started = Instant::now();
        if let Some(subscription_group_manager) = self.composition.state.subscription_group_manager.take() {
            let result = if let Some(service_context) = self.composition.state.service_context.as_ref() {
                let manager = Arc::new(subscription_group_manager);
                let result = persist_config_manager(
                    manager.clone(),
                    "broker.subscription-group",
                    self.composition
                        .state
                        .metadata_io
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                        .cloned(),
                    service_context.metadata_io().clone(),
                    MetadataDeadline::after(deadline.remaining()),
                )
                .await;
                if let Ok(mut manager) = Arc::try_unwrap(manager) {
                    manager.stop();
                }
                result
            } else {
                Err(rocketmq_error::RocketMQError::not_initialized(
                    "broker subscription-group persistence requires ChildServiceContext",
                ))
            };
            shutdown_report.subscription_group = match result {
                Ok(()) => {
                    progress.complete("subscription_group");
                    BrokerShutdownComponentReport::completed("subscription_group", started.elapsed())
                }
                Err(_error) if deadline.is_expired() => {
                    BrokerShutdownComponentReport::timed_out("subscription_group", started.elapsed())
                }
                Err(error) => {
                    BrokerShutdownComponentReport::unhealthy("subscription_group", started.elapsed(), error.to_string())
                }
            };
        } else {
            shutdown_report.subscription_group = BrokerShutdownComponentReport::skipped("subscription_group");
            progress.complete("subscription_group");
        }

        let started = Instant::now();
        let broker_config = self.composition.state.broker_config_arc();
        let message_store_config = self.composition.state.message_store_config_arc();
        let consumer_offset_manager = std::mem::replace(
            &mut self.composition.state.consumer_offset_manager,
            Arc::new(ConsumerOffsetManager::new(broker_config, message_store_config)),
        );
        let result = if let Some(service_context) = self.composition.state.service_context.as_ref() {
            let result = persist_config_manager(
                consumer_offset_manager.clone(),
                "broker.consumer-offset",
                self.composition
                    .state
                    .metadata_io
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .cloned(),
                service_context.metadata_io().clone(),
                MetadataDeadline::after(deadline.remaining()),
            )
            .await;
            match Arc::try_unwrap(consumer_offset_manager) {
                Ok(mut manager) => {
                    manager.stop();
                }
                Err(manager) => {
                    warn!(
                        strong_count = Arc::strong_count(&manager),
                        "Consumer offset manager still has live capability owners during shutdown"
                    );
                }
            }
            result
        } else {
            Err(rocketmq_error::RocketMQError::not_initialized(
                "broker consumer-offset persistence requires ChildServiceContext",
            ))
        };
        shutdown_report.consumer_offset = match result {
            Ok(()) => {
                progress.complete("consumer_offset");
                BrokerShutdownComponentReport::completed("consumer_offset", started.elapsed())
            }
            Err(_error) if deadline.is_expired() => {
                BrokerShutdownComponentReport::timed_out("consumer_offset", started.elapsed())
            }
            Err(error) => {
                BrokerShutdownComponentReport::unhealthy("consumer_offset", started.elapsed(), error.to_string())
            }
        };

        #[cfg(feature = "rocksdb_store")]
        if shutdown_report.subscription_group.healthy && shutdown_report.consumer_offset.healthy {
            if let Some(rocksdb_config_managers) = self.composition.metadata.rocksdb_config_managers.take() {
                if let Some(service_context) = self.composition.state.service_context.as_ref() {
                    if let Err(error) = run_shutdown_blocking_operation(
                        service_context,
                        deadline,
                        "broker.config-rocksdb.close",
                        move || rocksdb_config_managers.close_all(),
                    )
                    .await
                    {
                        warn!(error = %error.detail(), "Failed to close broker config RocksDB owners");
                    }
                } else {
                    // Compatibility builders share no injected BlockingExecutor. Their config
                    // stores are nevertheless closed by the aggregate owner, never by a leaf.
                    rocksdb_config_managers.close_all();
                }
            }
        }

        let metadata_flush_error = if let Some(service_context) = self.composition.state.service_context.as_ref() {
            persist_config_manager(
                self.composition.state.topic_queue_mapping_manager_handle(),
                "broker.topic-queue-mapping",
                self.composition
                    .state
                    .metadata_io
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .cloned(),
                service_context.metadata_io().clone(),
                MetadataDeadline::after(deadline.remaining()),
            )
            .await
            .err()
        } else {
            None
        };

        let started = Instant::now();
        shutdown_report.metadata_io = match self.composition.state.metadata_io.take() {
            Some(Ok(metadata_io)) => {
                let report = metadata_io
                    .shutdown_until(MetadataDeadline::after(deadline.remaining()))
                    .await;
                if report.timed_out {
                    BrokerShutdownComponentReport::timed_out("metadata_io", started.elapsed())
                } else if report.pending_operations == 0 && report.pending_bytes == 0 {
                    BrokerShutdownComponentReport::completed("metadata_io", started.elapsed())
                } else {
                    BrokerShutdownComponentReport::unhealthy(
                        "metadata_io",
                        started.elapsed(),
                        format!(
                            "pending_operations={}, pending_bytes={}, unfinished_resources={}",
                            report.pending_operations,
                            report.pending_bytes,
                            report.unfinished.len()
                        ),
                    )
                }
            }
            Some(Err(error)) => {
                BrokerShutdownComponentReport::unhealthy("metadata_io", started.elapsed(), error.to_string())
            }
            None => BrokerShutdownComponentReport::skipped("metadata_io"),
        };
        if let Some(error) = metadata_flush_error {
            shutdown_report.metadata_io = BrokerShutdownComponentReport::unhealthy(
                "metadata_io",
                started.elapsed(),
                format!("final topic queue mapping persistence failed: {error}"),
            );
        }
        if shutdown_report.metadata_io.healthy {
            progress.complete("metadata_io");
        }

        let started = Instant::now();
        let broker_outer_api_report = self
            .composition
            .state
            .broker_outer_api
            .shutdown_with_report(deadline.remaining().min(BROKER_OUTER_API_SHUTDOWN_TIMEOUT))
            .await;
        shutdown_report.broker_outer_api = if broker_outer_api_report.is_healthy() {
            BrokerShutdownComponentReport::completed("broker_outer_api", started.elapsed())
        } else {
            BrokerShutdownComponentReport::unhealthy(
                "broker_outer_api",
                started.elapsed(),
                format!("{broker_outer_api_report:?}"),
            )
        };
        progress.complete("broker_outer_api");

        let started = Instant::now();
        shutdown_report.client_housekeeping =
            if let Some(client_housekeeping_service) = self.composition.state.client_housekeeping_service.take() {
                let client_housekeeping_report = client_housekeeping_service.shutdown_with_report().await;
                BrokerShutdownComponentReport::from_shutdown_report(
                    "client_housekeeping",
                    client_housekeeping_report.as_ref(),
                    started.elapsed(),
                )
            } else {
                BrokerShutdownComponentReport::skipped("client_housekeeping")
            };
        progress.complete("client_housekeeping");

        let started = Instant::now();
        shutdown_report.service_tasks = if let Some(service_context) = self.composition.state.service_context.as_ref() {
            let report = service_context.task_group().shutdown_until(deadline).await;
            BrokerShutdownComponentReport::from_shutdown_report("service_tasks", Some(&report), started.elapsed())
        } else {
            BrokerShutdownComponentReport::unhealthy(
                "service_tasks",
                started.elapsed(),
                BrokerBlockingShutdownError::MissingServiceContext.detail(),
            )
        };
        progress.complete("service_tasks");

        let started = Instant::now();
        if let Some(guard) = self.composition.state.observability_guard.take() {
            shutdown_report.observability =
                if let Some(service_context) = self.composition.state.service_context.as_ref() {
                    let telemetry_report = guard
                        .shutdown_with_service_context(service_context, deadline.remaining())
                        .await;
                    if !telemetry_report.is_healthy() {
                        warn!(
                            report = %telemetry_report.to_json(),
                            "Failed to shutdown observability runtime cleanly"
                        );
                    }
                    BrokerShutdownComponentReport::from_telemetry_shutdown_report(&telemetry_report, started.elapsed())
                } else {
                    BrokerShutdownComponentReport::unhealthy(
                        "observability",
                        started.elapsed(),
                        BrokerBlockingShutdownError::MissingServiceContext.detail(),
                    )
                };
        } else {
            shutdown_report.observability = BrokerShutdownComponentReport::skipped("observability");
        }
        progress.complete("observability");

        shutdown_report.unfinished_components = progress.unfinished();
        shutdown_report
    }

    pub(crate) async fn shutdown_scheduled_tasks_with_timeout(
        &self,
        timeout: Duration,
    ) -> rocketmq_runtime::schedule::simple_scheduler::ScheduledShutdownReport {
        let report = self.lifecycle.scheduled_task_manager.shutdown_all(timeout).await;
        if !report.is_healthy() {
            warn!(
                task_count = report.task_count,
                completed = report.completed,
                aborted = report.aborted,
                panicked = report.panicked,
                timed_out = report.timed_out,
                elapsed_ms = report.elapsed.as_millis(),
                "Broker scheduled task shutdown report is unhealthy"
            );
        }
        report
    }

    pub(crate) async fn shutdown_remoting_servers(
        &mut self,
        deadline: ShutdownDeadline,
    ) -> Option<BrokerRemotingServerShutdownReport> {
        let task_group = self.lifecycle.remoting_server_task_group.take()?;

        let report = task_group.shutdown_until(deadline).await;
        let server_reports = Self::collect_remoting_server_reports(
            std::mem::take(&mut self.lifecycle.remoting_server_report_receivers),
            deadline,
        )
        .await;
        let shutdown_report = BrokerRemotingServerShutdownReport {
            task_group: report,
            server_reports,
        };
        if !shutdown_report.is_healthy() {
            warn!(
                task_group = %shutdown_report.task_group.to_json(),
                "Broker remoting server shutdown report is unhealthy"
            );
        }
        Some(shutdown_report)
    }

    pub(super) async fn collect_remoting_server_reports(
        receivers: Vec<BrokerRemotingServerReportReceiver>,
        deadline: ShutdownDeadline,
    ) -> Vec<BrokerRemotingServerReport> {
        let mut reports = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            let timeout = deadline.remaining().min(Duration::from_secs(1));
            let report = match tokio::time::timeout(timeout, receiver.receiver).await {
                Ok(Ok(report)) => report,
                Ok(Err(_closed)) => {
                    warn!(
                        server = receiver.name,
                        "Broker remoting server report channel closed before report was sent"
                    );
                    None
                }
                Err(_elapsed) => {
                    warn!(
                        server = receiver.name,
                        "Timed out waiting for broker remoting server shutdown report"
                    );
                    None
                }
            };
            reports.push(BrokerRemotingServerReport {
                name: receiver.name,
                report,
            });
        }
        reports
    }

    #[doc(hidden)]
    pub(crate) fn install_remoting_server_report_probe(&mut self) -> bool {
        let Some(task_group) = self.broker_task_group_or_current(
            "rocketmq-broker.remoting-server.probe",
            "failed to install broker remoting server report probe outside Tokio runtime",
        ) else {
            return false;
        };
        let shutdown_token = task_group.cancellation_token();
        let (report_tx, report_rx) = oneshot::channel();
        if let Err(error) = task_group.spawn_service("broker.remoting-server.probe", async move {
            shutdown_token.cancelled().await;
            let _ = report_tx.send(Some(ShutdownReport::new(
                "rocketmq.remoting.server.probe",
                Duration::ZERO,
            )));
        }) {
            warn!(?error, "failed to spawn broker remoting server report probe");
            return false;
        }

        self.lifecycle.remoting_server_task_group = Some(task_group);
        self.lifecycle
            .remoting_server_report_receivers
            .push(BrokerRemotingServerReportReceiver {
                name: "broker.remoting-server.probe",
                receiver: report_rx,
            });
        true
    }

    #[doc(hidden)]
    pub(crate) fn install_request_processor_task_probe(&mut self) -> bool {
        if self.lifecycle.request_processor_task_group.is_some() {
            return false;
        }

        let Some(task_group) = self.broker_task_group_or_current(
            "rocketmq-broker.request-processor.probe",
            "failed to install broker request processor task probe outside Tokio runtime",
        ) else {
            return false;
        };
        let shutdown_token = task_group.cancellation_token();
        if let Err(error) = task_group.spawn_service("broker.request-processor.probe", async move {
            shutdown_token.cancelled().await;
        }) {
            warn!(?error, "failed to spawn broker request processor task probe");
            return false;
        }

        self.lifecycle.request_processor_task_group = Some(task_group);
        true
    }

    pub(super) fn broker_task_group_or_current(
        &self,
        name: &'static str,
        no_runtime_warning: &'static str,
    ) -> Option<TaskGroup> {
        self.composition
            .state
            .broker_task_group_or_current(name, no_runtime_warning)
    }

    pub(super) async fn shutdown_request_processor_tasks(
        &mut self,
        deadline: ShutdownDeadline,
    ) -> Option<ShutdownReport> {
        let task_group = self.lifecycle.request_processor_task_group.take()?;

        let report = task_group.shutdown_until(deadline).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "Broker request processor shutdown report is unhealthy"
            );
        }
        Some(report)
    }

    pub(crate) async fn initialize(&mut self) -> Result<(), BrokerStartupError> {
        if let Some(detail) = self.composition.metadata.configuration_error.clone() {
            return Err(BrokerStartupError::Initialization {
                component: "broker_configuration",
                detail,
            });
        }
        self.initialize_metadata().await?;
        self.lifecycle.startup_journal.complete(BrokerComponent::Metadata);
        info!("====== initialize metadata Success========");
        if !self.initialize_message_store().await {
            return Err(BrokerStartupError::Initialization {
                component: "message_store",
                detail: "message store initialization returned an unsuccessful status".to_owned(),
            });
        }
        self.lifecycle
            .startup_journal
            .complete(BrokerComponent::BrokerStorePort);
        if !self.recover_initialize_service().await {
            return Err(BrokerStartupError::Initialization {
                component: "broker_services",
                detail: "service recovery or security initialization returned an unsuccessful status".to_owned(),
            });
        }
        self.lifecycle.startup_journal.complete(BrokerComponent::Security);
        Ok(())
    }

    pub async fn start(&mut self) -> Result<BrokerReadiness, BrokerStartupError> {
        match self.start_inner().await {
            Ok(readiness) => Ok(readiness),
            Err(cause) => Err(self.rollback_startup(cause).await),
        }
    }

    pub(super) async fn start_inner(&mut self) -> Result<BrokerReadiness, BrokerStartupError> {
        let broker_config = self.composition.state.broker_config();
        let message_store_config = self.composition.state.message_store_config();
        if message_store_config.cold_data_flow_control_enable {
            return Err(BrokerStartupError::UnsupportedCapability {
                capability: "cold_data_flow_control",
                reason: "the legacy cold-data hold queue had no bounded wakeup or shutdown contract",
            });
        }
        self.composition.state.should_start_time.store(
            (current_millis() as i64 + message_store_config.disappear_time_after_start) as u64,
            Ordering::Release,
        );
        if broker_config.enable_controller_mode {
            self.composition.state.online_role_state.set_isolated(true);
        }
        if message_store_config.total_replicas > 1 && broker_config.enable_slave_acting_master {
            self.composition.state.online_role_state.set_isolated(true);
        }

        self.composition.state.broker_outer_api.start().await;
        self.lifecycle.startup_journal.complete(BrokerComponent::BrokerOuterApi);
        if broker_config.namesrv_addr.is_some() {
            self.update_namesrv_addr().await;
        }
        let (normal_listener, fast_listener) = self.start_basic_service().await?;

        if broker_config.enable_controller_mode {
            self.composition
                .state
                .build_controller_runtime()
                .bootstrap_controller_mode()
                .await;
        }

        let live_broker_config = self.composition.state.broker_config();
        let live_message_store_config = self.composition.state.message_store_config();
        let mut registration_ready = false;
        if !self.composition.state.online_role_state.is_isolated()
            && !live_message_store_config.enable_dledger_commit_log
            && !live_broker_config.duplication_enable
        {
            let is_master = live_broker_config.broker_identity.broker_id == mix_all::MASTER_ID;
            self.composition.state.change_special_service_status(is_master).await;
            self.register_broker_all(true, false, true)
                .await
                .map_err(|error| BrokerStartupError::component_start("broker_registration", error))?;
            registration_ready = true;
        }
        if registration_ready {
            self.lifecycle.startup_journal.complete(BrokerComponent::Registration);
        }

        //start register broker to name server scheduled task
        let registration_runtime = self.composition.state.build_registration_runtime();
        let registration_config = self.composition.state.config_state.clone();
        let registration_shutdown = Arc::clone(&self.composition.state.shutdown);
        let registration_should_start_time = Arc::clone(&self.composition.state.should_start_time);
        let registration_role_state = Arc::clone(&self.composition.state.online_role_state);
        let period = Duration::from_millis(
            10000.max(60000.min(self.composition.state.broker_config().register_name_server_period)),
        );
        let initial_delay = Duration::from_secs(10);
        Self::log_scheduled_task_start(
            "register_broker_to_namesrv",
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_task_async(initial_delay, period, move |_ctx| {
                    let registration_runtime = registration_runtime.clone();
                    let registration_config = registration_config.clone();
                    let registration_shutdown = Arc::clone(&registration_shutdown);
                    let registration_should_start_time = Arc::clone(&registration_should_start_time);
                    let registration_role_state = Arc::clone(&registration_role_state);
                    async move {
                        if registration_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let start_time = registration_should_start_time.load(Ordering::Relaxed);
                        if current_millis() < start_time {
                            info!("Register to namesrv after {}", start_time);
                            return Ok(());
                        }
                        if registration_role_state.is_isolated() {
                            info!("Skip register for broker is isolated");
                            return Ok(());
                        }
                        let force_register = registration_config.broker_snapshot().force_register;
                        if let Err(error) = registration_runtime
                            .register_broker_all(true, false, force_register)
                            .await
                        {
                            warn!(%error, "Scheduled broker registration failed");
                        }
                        Ok(())
                    }
                }),
        );

        if broker_config.enable_slave_acting_master {
            self.schedule_send_heartbeat();
            let sync_broker_member_group_period = broker_config.sync_broker_member_group_period;
            let controller_runtime = self.composition.state.build_controller_runtime();
            Self::log_scheduled_task_start(
                "sync_broker_member_group",
                self.lifecycle.scheduled_task_manager.add_fixed_rate_task_async(
                    Duration::from_millis(1000),
                    Duration::from_millis(sync_broker_member_group_period),
                    move |ctx| {
                        let controller_runtime = controller_runtime.clone();
                        async move {
                            if ctx.is_cancelled() {
                                return Ok(());
                            }
                            controller_runtime.sync_broker_member_group().await;
                            Ok(())
                        }
                    },
                ),
            );
        }

        if broker_config.enable_controller_mode {
            self.schedule_send_heartbeat();
            self.schedule_sync_controller_metadata();
            self.schedule_sync_controller_replica_info();
        }

        if broker_config.skip_pre_online && !broker_config.enable_controller_mode {
            self.start_service_without_condition().await?;
            registration_ready = true;
            self.lifecycle.startup_journal.complete(BrokerComponent::Registration);
        }

        let metadata_shutdown = Arc::clone(&self.composition.state.shutdown);
        let broker_outer_api = self.composition.state.broker_outer_api.clone();
        let period = Duration::from_secs(5);
        let initial_delay = Duration::from_secs(10);
        Self::log_scheduled_task_start(
            "refresh_broker_metadata",
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_task_async(initial_delay, period, move |_ctx| {
                    let metadata_shutdown = Arc::clone(&metadata_shutdown);
                    let broker_outer_api = broker_outer_api.clone();
                    async move {
                        if metadata_shutdown.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        broker_outer_api.refresh_metadata();
                        Ok(())
                    }
                }),
        );
        let live_broker_config = self.composition.state.broker_config();
        // Controller-mode brokers start fenced and acquire write authority only after the
        // Controller assigns a role and grants a lease. Process readiness therefore depends on
        // the recovered store being eligible for promotion, not on already holding that lease.
        let store_writable = self
            .composition
            .state
            .message_store()
            .is_some_and(|store| BrokerReadStore::put_message_preflight(store).is_store_ready_for_promotion());
        let processors_started = self.composition.state.pop_message_processor.is_some()
            && self.composition.state.pop_lite_message_processor.is_some()
            && self.composition.state.ack_message_processor.is_some()
            && self.composition.state.notification_processor.is_some()
            && self.composition.state.query_assignment_processor.is_some()
            && self.composition.request_pipeline.proxy_request_processor.is_some()
            && self.composition.request_pipeline.processor_wiring_complete;
        let security_ready = (!live_broker_config.authentication_enabled && !live_broker_config.authorization_enabled)
            || self.composition.request_pipeline.auth_runtime.is_some();
        let readiness = BrokerReadiness::new(
            store_writable,
            normal_listener,
            fast_listener,
            processors_started,
            security_ready,
            registration_ready,
        )
        .validate()?;
        info!(
            "RocketMQ Broker({}) started successfully",
            live_broker_config.broker_identity.broker_name
        );
        Ok(readiness)
    }

    pub(crate) async fn rollback_startup(&mut self, cause: BrokerStartupError) -> BrokerStartupError {
        let rollback_order = self
            .lifecycle
            .startup_journal
            .rollback_order()
            .into_iter()
            .map(BrokerComponent::name)
            .collect::<Vec<_>>();
        warn!(?rollback_order, %cause, "Rolling back broker startup");
        let report = self
            .shutdown_basic_service_until(ShutdownDeadline::after(BROKER_BASIC_SERVICE_SHUTDOWN_TIMEOUT))
            .await;
        BrokerStartupError::RolledBack {
            cause: Box::new(cause),
            unhealthy_components: report.unhealthy_component_names(),
        }
    }
}
