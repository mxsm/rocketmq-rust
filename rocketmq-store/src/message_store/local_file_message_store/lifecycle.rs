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

mod flush_consume_queue;

pub(super) use flush_consume_queue::FlushConsumeQueueService;

fn lifecycle_invalid(operation: StoreOperation, detail: impl Into<String>) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation).with_detail(detail)
}

impl LocalFileMessageStore {
    pub(super) fn add_schedule_task(&mut self) {
        if self.scheduled_task_group.is_some() {
            return;
        }

        if let Err(error) = self.ensure_root_dependencies_wired("starting scheduled tasks") {
            error!("scheduled store tasks not started: {error}");
            return;
        }
        let self_check_commit_log = self.commit_log.read_handle();
        let self_check_consume_queue_store = self.consume_queue_store.clone();
        let Some(store_checkpoint_arc) = self.store_checkpoint.clone() else {
            error!("scheduled store tasks not started: store checkpoint is not initialized");
            return;
        };

        self.scheduled_task_shutdown = CancellationToken::new();
        let task_group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.local-file.scheduled");
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());

        #[cfg(feature = "extended_timeline")]
        if let Some(engine) = self.extended_timeline_engine.as_ref() {
            let engine = engine.clone();
            let interval_ms = self.message_store_config.timer_store_config.scheduler_interval_ms;
            let max_messages = self.message_store_config.timer_store_config.materialize_batch_messages;
            let max_bytes = self.message_store_config.timer_store_config.materialize_batch_bytes;
            if let Err(error) = scheduled_tasks.schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("timer-extended-materializer", Duration::from_millis(interval_ms)),
                move || {
                    let engine = engine.clone();
                    async move {
                        let budget = WorkBudget::try_new(
                            max_messages,
                            max_bytes,
                            Instant::now() + Duration::from_millis(interval_ms.max(100).saturating_mul(4)),
                        );
                        if let Err(error) = match budget {
                            Ok(budget) => engine.enqueue_source(budget).await.map(|_| ()),
                            Err(error) => Err(error),
                        } {
                            warn!("Extended Timeline materialization paused at its cleanup fence: {error}");
                        }
                    }
                },
            ) {
                self.scheduled_task_shutdown.cancel();
                task_group.cancel();
                error!("failed to schedule Extended Timeline materializer: {error}");
                return;
            }
        }

        #[cfg(feature = "extended_timeline")]
        if let Some(engine) = self.extended_timeline_engine.as_ref() {
            let engine = engine.clone();
            let role = Arc::clone(&self.extended_timeline_role);
            let interval_ms = self.message_store_config.timer_store_config.scheduler_interval_ms;
            let formal = self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline;
            let max_messages = self.message_store_config.timer_store_config.due_scan_messages;
            let max_bytes = self.message_store_config.timer_store_config.due_scan_bytes;
            if let Err(error) = scheduled_tasks.schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("timer-extended-due-scanner", Duration::from_millis(interval_ms)),
                move || {
                    let engine = engine.clone();
                    let role = Arc::clone(&role);
                    async move {
                        let Some(epoch) = (!formal).then_some(0).or_else(|| role.capture_delivery_epoch()) else {
                            return;
                        };
                        let budget = WorkBudget::try_new(
                            max_messages,
                            max_bytes,
                            Instant::now() + Duration::from_millis(interval_ms.max(100).saturating_mul(4)),
                        );
                        if let Err(error) = match budget {
                            Ok(budget) => engine.roll_due(TimerEngineEpoch::new(epoch), budget).await.map(|_| ()),
                            Err(error) => Err(error),
                        } {
                            warn!("Extended Timeline due scan will retry: {error}");
                        }
                    }
                },
            ) {
                self.scheduled_task_shutdown.cancel();
                task_group.cancel();
                error!("failed to schedule Extended Timeline due scanner: {error}");
                return;
            }
        }

        #[cfg(feature = "extended_timeline")]
        if let Some(completion) = self.extended_timeline_completion_reconciler.as_ref() {
            let completion = Arc::clone(completion);
            let runtime_scope = self.runtime_scope.clone();
            let interval_ms = self.message_store_config.timer_store_config.scheduler_interval_ms;
            let max_records = self.message_store_config.timer_store_config.due_scan_messages;
            let max_bytes = self.message_store_config.timer_store_config.due_scan_bytes;
            if let Err(error) = scheduled_tasks.schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay(
                    "timer-extended-completion-reconciler",
                    Duration::from_millis(interval_ms),
                ),
                move || {
                    let completion = Arc::clone(&completion);
                    let runtime_scope = runtime_scope.clone();
                    async move {
                        let _ = run_blocking_scheduled_task(
                            &runtime_scope,
                            "timer extended completion reconciler",
                            move || {
                                if let Err(error) = completion.run_once(max_records, max_bytes) {
                                    warn!("Extended Timeline completion replay will retry: {error}");
                                }
                            },
                        )
                        .await;
                    }
                },
            ) {
                self.scheduled_task_shutdown.cancel();
                task_group.cancel();
                error!("failed to schedule Extended Timeline completion reconciler: {error}");
                return;
            }
        }

        #[cfg(feature = "extended_timeline")]
        if let Some(delivery) = self.extended_timeline_delivery.as_ref() {
            let delivery = Arc::clone(delivery);
            let interval_ms = self.message_store_config.timer_store_config.scheduler_interval_ms;
            if let Err(error) = scheduled_tasks.schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("timer-extended-delivery", Duration::from_millis(interval_ms)),
                move || {
                    let delivery = Arc::clone(&delivery);
                    async move {
                        if let Err(error) = delivery.run_once().await {
                            warn!("Extended Timeline delivery will retry: {error}");
                        }
                    }
                },
            ) {
                self.scheduled_task_shutdown.cancel();
                task_group.cancel();
                error!("failed to schedule Extended Timeline delivery: {error}");
                return;
            }
        }

        #[cfg(feature = "extended_timeline")]
        if let (Some(gc), Some(completion)) = (
            self.extended_timeline_gc.as_ref(),
            self.extended_timeline_completion_reconciler.as_ref(),
        ) {
            let gc = Arc::clone(gc);
            let completion = Arc::clone(completion);
            let commit_log = self.commit_log.read_handle();
            let runtime_scope = self.runtime_scope.clone();
            let interval_ms = self
                .message_store_config
                .timer_store_config
                .scheduler_interval_ms
                .max(60_000);
            let max_records = self.message_store_config.timer_store_config.due_scan_messages;
            if let Err(error) = scheduled_tasks.schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay("timer-extended-gc", Duration::from_millis(interval_ms)),
                move || {
                    let gc = Arc::clone(&gc);
                    let completion = Arc::clone(&completion);
                    let commit_log = commit_log.clone();
                    let runtime_scope = runtime_scope.clone();
                    async move {
                        let _ = run_blocking_scheduled_task(&runtime_scope, "timer extended gc", move || {
                            let completion_cursor = match completion.completion_physical_cursor() {
                                Ok(cursor) => cursor,
                                Err(error) => {
                                    warn!("Extended Timeline GC cannot read completion cursor: {error}");
                                    return;
                                }
                            };
                            let replicated_cursor = commit_log.get_confirm_offset().max(0);
                            let now_ms = rocketmq_runtime::common::time_utils::current_millis() as i64;
                            if let Err(error) = gc.run_once(now_ms, completion_cursor, replicated_cursor, max_records) {
                                warn!("Extended Timeline GC will retry: {error}");
                            }
                        })
                        .await;
                    }
                },
            ) {
                self.scheduled_task_shutdown.cancel();
                task_group.cancel();
                error!("failed to schedule Extended Timeline GC: {error}");
                return;
            }
        }

        // clean files  Periodically
        let clean_commit_log_service_arc = self.clean_commit_log_service.clone();
        let clean_resource_interval = self.message_store_config.clean_resource_interval as u64;
        let clean_commit_log_active = Arc::new(AtomicBool::new(true));
        let clean_commit_log_runtime_scope = self.runtime_scope.clone();
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay(
                "clean-commit-log-scheduler",
                Duration::from_millis(clean_resource_interval.max(1)),
            ),
            move || {
                let service = Arc::clone(&clean_commit_log_service_arc);
                let active = Arc::clone(&clean_commit_log_active);
                let runtime_scope = clean_commit_log_runtime_scope.clone();
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task(&runtime_scope, "clean commit log", move || service.run()).await {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task clean commit log: {error}");
            return;
        }

        let store_self_check_active = Arc::new(AtomicBool::new(true));
        let store_self_check_runtime_scope = self.runtime_scope.clone();
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("store-self-check-scheduler", Duration::from_secs(10 * 60)),
            move || {
                let commit_log = self_check_commit_log.clone();
                let consume_queue_store = self_check_consume_queue_store.clone();
                let active = Arc::clone(&store_self_check_active);
                let runtime_scope = store_self_check_runtime_scope.clone();
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task(&runtime_scope, "store self check", move || {
                        commit_log.check_self();
                        ConsumeQueueStoreTrait::check_self(&consume_queue_store);
                    })
                    .await
                    {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task self check: {error}");
            return;
        }

        // store check point flush
        let checkpoint_flush_active = Arc::new(AtomicBool::new(true));
        let checkpoint_flush_runtime_scope = self.runtime_scope.clone();
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay("store-checkpoint-flush-scheduler", Duration::from_secs(1)),
            move || {
                let checkpoint = Arc::clone(&store_checkpoint_arc);
                let active = Arc::clone(&checkpoint_flush_active);
                let runtime_scope = checkpoint_flush_runtime_scope.clone();
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task(&runtime_scope, "store checkpoint flush", move || {
                        let _ = checkpoint.flush();
                    })
                    .await
                    {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task checkpoint flush: {error}");
            return;
        }

        let correct_logic_offset_service_arc = self.correct_logic_offset_service.clone();
        let clean_consume_queue_service_arc = self.clean_consume_queue_service.clone();
        let clean_consume_queue_active = Arc::new(AtomicBool::new(true));
        let clean_consume_queue_runtime_scope = self.runtime_scope.clone();
        if let Err(error) = scheduled_tasks.schedule_fixed_delay(
            ScheduledTaskConfig::fixed_delay(
                "clean-consume-queue-scheduler",
                Duration::from_millis(clean_resource_interval.max(1)),
            ),
            move || {
                let correct_service = Arc::clone(&correct_logic_offset_service_arc);
                let clean_service = Arc::clone(&clean_consume_queue_service_arc);
                let active = Arc::clone(&clean_consume_queue_active);
                let runtime_scope = clean_consume_queue_runtime_scope.clone();
                async move {
                    if !active.load(Ordering::Acquire) {
                        return;
                    }
                    if !run_blocking_scheduled_task(&runtime_scope, "clean consume queue", move || {
                        correct_service.run();
                        clean_service.run();
                    })
                    .await
                    {
                        active.store(false, Ordering::Release);
                    }
                }
            },
        ) {
            self.scheduled_task_shutdown.cancel();
            task_group.cancel();
            error!("failed to schedule store task clean consume queue: {error}");
            return;
        }

        self.scheduled_tasks = Some(scheduled_tasks);
        self.scheduled_task_group = Some(task_group);
    }

    pub(super) async fn shutdown_schedule_tasks(&mut self) {
        self.scheduled_task_shutdown.cancel();
        self.scheduled_tasks.take();
        if let Some(task_group) = self.scheduled_task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("LocalFileMessageStore scheduled tasks", report)
            {
                error!("scheduled store task failed during shutdown: {error}");
            }
        }
        #[cfg(feature = "extended_timeline")]
        if let Some(engine) = self.extended_timeline_engine.as_ref() {
            if let Err(error) = engine.shutdown().await {
                error!("Extended Timeline engine shutdown failed: {error}");
            }
        }
    }

    #[cfg(test)]
    pub(super) fn has_scheduled_task_group(&self) -> bool {
        self.scheduled_task_group.is_some()
    }

    pub(crate) fn scheduled_task_count(&self) -> usize {
        if let Some(task_group) = self.scheduled_task_group.as_ref() {
            return task_group.task_count();
        }
        self.scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default()
    }

    pub(crate) fn scheduled_task_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    pub(super) async fn shutdown_ha_update_master_tasks(&self) {
        let task_group = match self.ha_update_master_group.lock() {
            Ok(mut task_group) => task_group.take(),
            Err(error) => {
                error!("failed to take HA master address update task group during shutdown: {error}");
                return;
            }
        };

        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("HA master address update tasks", report) {
                error!("HA master address update task failed during shutdown: {error}");
            }
        }
    }
}

impl LocalFileMessageStore {
    pub(super) async fn load_store(&mut self) -> bool {
        if let Err(error) = self.ensure_operational_root_lease(StoreOperation::Load) {
            error!(error = %error, "message store root lease is unavailable before load");
            return false;
        }
        if self.lifecycle_state() != LocalStoreState::Initialized || self.shutdown.load(Ordering::Acquire) {
            error!(
                state = ?self.lifecycle_state(),
                shutdown = self.shutdown.load(Ordering::Acquire),
                "message store must be initialized and non-shutdown before load"
            );
            return false;
        }
        let managed = self.store_root_mode == StoreRootMode::Managed;
        let last_exit_ok = match self.is_temp_file_exist() {
            Ok(present) => !present,
            Err(error) => {
                error!(error = %error, "failed to inspect the retained-root abort marker before load");
                return false;
            }
        };
        info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok { "normally" } else { "abnormally" },
            self.message_store_config.store_path_root_dir
        );
        let mut result = if managed {
            match self.activate_managed_queue_runtime() {
                Ok(()) => true,
                Err(error) => {
                    error!(error = %error, "managed queue activation failed before recovery");
                    return false;
                }
            }
        } else {
            //load Commit log-- init commit mapped file queue
            let loaded_commit_log = self.commit_log.load();
            if !loaded_commit_log {
                return false;
            }
            // load Consume Queue-- init Consume log mapped file queue
            loaded_commit_log && self.consume_queue_store.load()
        };

        if self.message_store_config.enable_compaction {
            let required_wal_position = self.commit_log.get_max_offset();
            let Some(compaction_service) = self.compaction_service.as_mut() else {
                error!("compaction is enabled but compaction service is not initialized");
                return false;
            };
            result &= compaction_service.load(last_exit_ok, required_wal_position).await;
            if !result {
                return result;
            }
        }

        if result {
            let Some(checkpoint) = self.store_checkpoint.as_ref() else {
                error!("message store checkpoint is not initialized");
                return false;
            };
            self.master_flushed_offset = Arc::new(AtomicI64::new(checkpoint.master_flushed_offset() as i64));
            self.set_confirm_offset(checkpoint.confirm_phy_offset() as i64);
            result = self.index_service.load(last_exit_ok);
            if !result {
                error!("index service load failed; aborting message store recovery");
                return false;
            }
            #[cfg(feature = "tieredstore")]
            if result {
                if let Some(tiered_store) = self.tiered_store.as_ref() {
                    if let Err(error) = tiered_store.load().await {
                        error!("tieredstore load failed: {}", error);
                        return false;
                    }
                }
            }

            //recover commit log and consume queue
            if !self.recover(last_exit_ok).await {
                error!("message store recovery cleanup failed; aborting load");
                return false;
            }
            if self.message_store_config.enable_compaction {
                self.compaction_store.finish_recovery(self.get_max_phy_offset());
            }
            info!(
                "message store recover end, and the max phy offset = {}",
                self.get_max_phy_offset()
            );
        }

        if result {
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                result &= timer_message_store.load();
            }
            #[cfg(feature = "extended_timeline")]
            if let Some(materializer) = self.extended_timeline_materializer.as_ref() {
                if let Err(error) = materializer.refresh_cleanup_fence() {
                    error!("Extended Timeline cleanup fence recovery failed: {error}");
                    result = false;
                }
            }
        }

        let max_offset = self.get_max_phy_offset();
        self.set_broker_init_max_offset(max_offset);
        info!("load over, and the max phy offset = {}", max_offset);

        if !result {
            // self.allocate_mapped_file_service.shutdown();
        }
        result
    }

    pub(super) async fn start_store(&mut self) -> Result<(), StoreError> {
        self.validate_supported_configuration()?;
        self.ensure_operational_root_lease(StoreOperation::Start)?;
        if self.store_root_mode == StoreRootMode::Managed
            && (self.managed_lifecycle_runtime.is_none() || self.mapped_file_retirement_service.is_none())
        {
            return Err(lifecycle_invalid(
                StoreOperation::Start,
                "managed mapped-file lifecycle has not completed queue/runtime/reaper activation",
            ));
        }
        match self.lifecycle_state() {
            LocalStoreState::Initialized => {}
            LocalStoreState::Created => {
                return Err(lifecycle_invalid(
                    StoreOperation::Start,
                    "message store must be initialized before start".to_string(),
                ));
            }
            LocalStoreState::Started => {
                return Err(lifecycle_invalid(
                    StoreOperation::Start,
                    "message store is already started",
                ));
            }
            LocalStoreState::Shutdown => {
                return Err(lifecycle_invalid(
                    StoreOperation::Start,
                    "message store is shutdown; call init before start".to_string(),
                ));
            }
            LocalStoreState::RecoveringConsumeQueue
            | LocalStoreState::RecoveringCommitLog
            | LocalStoreState::RecoveringTopicQueueTable => {
                return Err(lifecycle_invalid(
                    StoreOperation::Start,
                    "message store is recovering; start is not allowed".to_string(),
                ));
            }
        }
        self.create_temp_file()?;

        let start_result: Result<(), StoreError> = async {
            self.allocate_mapped_file_service.start();

            self.index_service.start();

            #[cfg(feature = "tieredstore")]
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                tiered_store.start().await?;
            }

            self.reput_message_service
                .set_reput_from_offset(self.commit_log.get_confirm_offset());
            self.ensure_root_dependencies_wired("start")?;
            let reput_runtime_context = self.reput_runtime_context();
            self.reput_message_service.start(
                &self.runtime_scope,
                self.commit_log.read_handle(),
                self.composition.reput(),
                self.dispatcher.handle(),
                self.notify_message_arrive_in_batch,
                reput_runtime_context,
            );
            self.do_recheck_reput_offset_from_dispatchers();
            self.flush_consume_queue_service.start();
            self.commit_log.start();
            self.background_index_rebuild_service.start(
                &self.runtime_scope,
                self.commit_log.read_handle(),
                self.message_store_config.clone(),
                self.index_service.clone(),
                self.delay_level_table_ref().clone(),
                self.max_delay_level,
            );
            self.consume_queue_store.start();
            self.store_stats_service.start();
            if let Some(compaction_service) = self.compaction_service.as_mut() {
                compaction_service.start();
            }
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                timer_message_store.start();
            }
            self.sync_timer_message_store_role();
            #[cfg(feature = "extended_timeline")]
            self.sync_extended_timeline_role();

            if let Some(ha_service) = self.ha_service.as_mut() {
                ha_service.start().await.map_err(|e| {
                    error!("HA service start failed: {:?}", e);
                    StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Start)
                        .in_component(StoreComponent::HighAvailability)
                        .with_source(e)
                })?;
            }
            if let Some(service) = self.mapped_file_retirement_service.as_mut() {
                service.start()?;
            }
            self.add_schedule_task();
            // self.perfs.start();
            Ok(())
        }
        .await;

        match start_result {
            Ok(()) => {
                self.shutdown.store(false, Ordering::Release);
                self.set_lifecycle_state(LocalStoreState::Started);
                Ok(())
            }
            Err(error) => {
                if self.store_root_mode == StoreRootMode::Managed {
                    if let Some(runtime) = self.managed_lifecycle_runtime.as_ref() {
                        runtime.begin_shutdown();
                    }
                    if let Some(service) = self.mapped_file_retirement_service.as_mut() {
                        let _ = service.cancel_drain_and_await().await;
                    }
                    self.allocate_mapped_file_service.shutdown().await;
                }
                if self.background_index_rebuild_service.has_task_group() {
                    self.background_index_rebuild_service.shutdown().await;
                }
                #[cfg(feature = "tieredstore")]
                if let Some(tiered_store) = self.tiered_store.as_ref() {
                    if let Err(shutdown_error) = tiered_store.shutdown().await {
                        warn!("tieredstore shutdown after start failure failed: {}", shutdown_error);
                    }
                }
                Err(error)
            }
        }
    }

    pub(super) async fn initialize_store(&mut self) -> Result<(), StoreError> {
        self.validate_supported_configuration()?;
        self.ensure_operational_root_lease(StoreOperation::Load)?;
        match self.lifecycle_state() {
            LocalStoreState::Created | LocalStoreState::Shutdown => {}
            LocalStoreState::Initialized => return Ok(()),
            LocalStoreState::Started => {
                return Err(lifecycle_invalid(
                    StoreOperation::Load,
                    "message store cannot be initialized while started".to_string(),
                ));
            }
            LocalStoreState::RecoveringConsumeQueue
            | LocalStoreState::RecoveringCommitLog
            | LocalStoreState::RecoveringTopicQueueTable => {
                return Err(lifecycle_invalid(
                    StoreOperation::Load,
                    "message store cannot be initialized while recovering".to_string(),
                ));
            }
        }

        if !Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref())
            && !self.message_store_config.duplication_enable
        {
            self.ensure_root_dependencies_wired("init")?;
            let pending_ha_service = self.pending_ha_service.take().ok_or_else(|| {
                lifecycle_invalid(
                    StoreOperation::Load,
                    "HA service was not constructed while wiring root dependencies",
                )
            })?;
            let mut ha_service = match pending_ha_service {
                PendingHAService::Default(service) => GeneralHAService::new_with_default_ha_service(*service),
                PendingHAService::AutoSwitch(service) => GeneralHAService::new_with_auto_switch_ha_service(service),
            };
            let _ = ha_service.init();
            self.ha_service = Some(ha_service);
        }
        if let Some(ha_service) = self.ha_service.as_ref() {
            self.commit_log.publish_ha_service(ha_service.clone());
        }

        let storage_capability = crate::platform::current_store_platform_capability();
        info!(
            "Store platform capability snapshot: os={} page_size={} memory_lock_limit_bytes={} \
             effective_memory_lock_budget_bytes={} file_preallocate_supported={} io_hint_branch={} \
             mmap_advice_supported={} file_prefetch_supported={} lazy_mmap_supported={} \
             hint_failure_affects_correctness={}",
            storage_capability.os_name,
            storage_capability.page_size,
            storage_capability
                .memory_lock_limit_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            Self::effective_linux_memory_lock_budget_bytes(self.message_store_config.as_ref()),
            storage_capability.file_preallocate_supported,
            storage_capability.optimization.io_hint_branch.as_str(),
            storage_capability.optimization.mmap_advice_supported,
            storage_capability.optimization.file_prefetch_supported,
            storage_capability.optimization.lazy_mmap_supported,
            storage_capability.optimization.hint_failure_affects_correctness
        );

        if self.is_transient_store_pool_enable() {
            match self.transient_store_pool.init() {
                Ok(_) => {}
                Err(source) => {
                    return Err(
                        StoreError::new(&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Load)
                            .in_component(StoreComponent::MappedFile)
                            .with_source(source),
                    );
                }
            }
        }
        self.shutdown.store(false, Ordering::Release);
        self.set_lifecycle_state(LocalStoreState::Initialized);
        Ok(())
    }

    pub(super) async fn shutdown_store_gracefully(&mut self) -> Result<MessageStoreShutdownReport, StoreError> {
        let mut report = MessageStoreShutdownReport::default();
        let mut shutdown_error = None;
        let allow_root_path_persistence = match self.validate_current_root_mode(StoreOperation::Shutdown) {
            Ok(()) => self.store_root_lease_state == StoreRootLeaseState::Operational,
            Err(error) => {
                if self.store_root_lease_state != StoreRootLeaseState::Destroyed {
                    self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
                }
                warn!(error = %error, "message store root lease is invalid before shutdown");
                shutdown_error = Some(error);
                false
            }
        };
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown.store(true, Ordering::Release);
            self.set_lifecycle_state(LocalStoreState::Shutdown);

            if let Some(ha_service) = self.ha_service.as_ref() {
                self.shutdown_ha_update_master_tasks().await;
                ha_service.shutdown().await;
            }

            #[cfg(feature = "extended_timeline")]
            if allow_root_path_persistence {
                if let Err(error) = self.extended_timeline_role.transition(false) {
                    warn!("Extended Timeline shutdown fence persistence failed: {error}");
                }
            } else {
                self.extended_timeline_role.fence_in_memory();
            }

            if let Some(service) = self.mapped_file_retirement_service.as_mut() {
                let service_result = service.cancel_drain_and_await().await;
                let retirement = match service_result {
                    Ok(retirement) => retirement,
                    Err(error) => {
                        if shutdown_error.is_none() {
                            shutdown_error = Some(error);
                        } else {
                            warn!(error = %error, "mapped-file retirement service also failed during shutdown");
                        }
                        service.snapshot()
                    }
                };
                report.mapped_file_retirement_pending_tickets = retirement.pending_tickets;
                report.mapped_file_retirement_tombstone_backlog = retirement.tombstone_backlog;
                report.mapped_file_retirement_oldest_pending_age = retirement.oldest_pending_age;
                report.mapped_file_retirement_last_failure_stage = retirement.last_failure_stage;
                report.mapped_file_retirement_recovery_required = retirement.recovery_required;
            }
            self.shutdown_schedule_tasks().await;
            #[cfg(feature = "extended_timeline")]
            if let Some(materializer) = self.extended_timeline_materializer.as_ref() {
                materializer.close();
            }
            self.store_stats_service.shutdown_gracefully().await;
            self.background_index_rebuild_service.shutdown().await;
            match self.commit_log.shutdown_gracefully().await {
                Ok(final_flush) => report.final_flush = Some(final_flush),
                Err(error) => {
                    self.record_flush_failure(&error);
                    if shutdown_error.is_none() {
                        shutdown_error = Some(error);
                    } else {
                        warn!(error = %error, "commitlog flush also failed after Store root lease validation failed");
                    }
                }
            }

            self.reput_message_service.shutdown().await;
            #[cfg(feature = "tieredstore")]
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                if let Err(error) = tiered_store.shutdown().await {
                    error!("tieredstore shutdown failed: {}", error);
                }
            }
            self.consume_queue_store.shutdown();

            // dispatch-related services must be shut down after reputMessageService
            self.index_service.shutdown();

            if let Some(compaction_service) = self.compaction_service.as_mut() {
                compaction_service.shutdown_gracefully().await;
            }

            if self.message_store_config.rocksdb_cq_double_write_enable {
                // this.rocksDBMessageStore.consumeQueueStore.shutdown();
            }
            if let Some(timer_message_store) = self.timer_message_store.clone() {
                let _ = timer_message_store.stop_gracefully().await;
                let persist_timer_storage = if allow_root_path_persistence {
                    match self.validate_current_root_mode(StoreOperation::Shutdown) {
                        Ok(()) => self.store_root_lease_state == StoreRootLeaseState::Operational,
                        Err(error) => {
                            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
                            warn!(error = %error, "message store root lease changed before Timer persistence");
                            if shutdown_error.is_none() {
                                shutdown_error = Some(error);
                            }
                            false
                        }
                    }
                } else {
                    false
                };
                if persist_timer_storage {
                    timer_message_store.shutdown_storage_with_persistence();
                }
            }
            self.flush_consume_queue_service.shutdown().await;
            self.allocate_mapped_file_service.shutdown().await;
            if let Some(store_checkpoint) = self.store_checkpoint.as_ref() {
                let _ = store_checkpoint.shutdown();
            }
            // Service shutdown above drains already-owned handles. Only a currently validated
            // lease may mutate the root namespace, and the mutation stays relative to its
            // retained directory handle.
            if allow_root_path_persistence
                && self.store_root_lease_state == StoreRootLeaseState::Operational
                && self.running_flags.is_writeable()
                && self.dispatch_behind_bytes() == 0
            {
                match self.validate_current_root_mode(StoreOperation::Shutdown) {
                    Ok(()) => {
                        if let Err(error) = self.store_root_lease.remove_abort_marker(StoreOperation::Shutdown) {
                            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
                            warn!(error = %error, "failed to remove abort marker through retained Store root");
                            if shutdown_error.is_none() {
                                shutdown_error = Some(error);
                            }
                        }
                    }
                    Err(error) => {
                        self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
                        warn!(error = %error, "message store root lease changed while shutdown services were draining");
                        if shutdown_error.is_none() {
                            shutdown_error = Some(error);
                        }
                    }
                }
            }
        }

        match self.transient_store_pool.shutdown(Duration::ZERO) {
            Ok(pool_report) => {
                report.transient_pool_outstanding_leases = pool_report.outstanding_leases();
            }
            Err(source) => {
                let error = StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Shutdown)
                    .in_component(StoreComponent::MappedFile)
                    .with_source(source);
                if shutdown_error.is_none() {
                    shutdown_error = Some(error);
                } else {
                    warn!(error = %error, "transient store pool shutdown also failed");
                }
            }
        }
        match shutdown_error {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }

    pub(super) async fn shutdown_store(&mut self) {
        if let Err(error) = self.shutdown_store_gracefully().await {
            warn!(error = %error, "message store shutdown failed");
        }
    }

    pub(super) fn destroy_store(&mut self) {
        if !self.destroy_store_with_outcome() {
            warn!(
                "message store cleanup remains pending; retaining storage and metadata until durable retirement handoff"
            );
        }
    }

    /// Durably retires every managed mapped-file segment after ordinary Store shutdown.
    ///
    /// Lifecycle sidecars remain as the replayable audit trail. Legacy configurations continue to
    /// fail closed through [`Self::destroy_store_with_outcome`].
    pub(super) async fn destroy_store_gracefully(&mut self) -> Result<bool, StoreError> {
        if self.store_root_lease_state == StoreRootLeaseState::Destroyed {
            return Ok(true);
        }
        if self.lifecycle_state() != LocalStoreState::Shutdown {
            return Err(lifecycle_invalid(
                StoreOperation::Admin,
                "managed Store destroy requires ordinary shutdown to complete first",
            ));
        }
        if !self.message_store_config.enable_mapped_file_lifecycle_wave_b {
            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
            return Ok(false);
        }
        if let Err(error) = self.validate_current_root_mode(StoreOperation::Admin) {
            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
            return Err(error);
        }
        let Some(runtime) = self.managed_lifecycle_runtime.clone() else {
            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
            return Err(lifecycle_invalid(
                StoreOperation::Admin,
                "managed lifecycle runtime is unavailable after shutdown",
            ));
        };
        let Some(service) = self.mapped_file_retirement_service.as_mut() else {
            self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
            return Err(lifecycle_invalid(
                StoreOperation::Admin,
                "managed mapped-file retirement service is unavailable after shutdown",
            ));
        };

        self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
        let report = service.destroy_all_and_await().await?;
        let complete = report.pending_tickets == 0 && !report.recovery_required && runtime.store_destroy_complete();
        if complete {
            self.store_root_lease_state = StoreRootLeaseState::Destroyed;
        }
        Ok(complete)
    }

    /// Attempts an explicit Store-level destroy without bypassing mapped-file retirement.
    ///
    /// Wave-B enables durable per-segment retirement, but whole-Store destruction remains disabled
    /// until every Store-owned namespace and metadata artifact participates in the same durable
    /// handoff. An eligible request is fenced as retry-pending and performs no consume queue,
    /// commitlog, index, or metadata namespace mutation.
    #[must_use]
    pub(super) fn destroy_store_with_outcome(&mut self) -> bool {
        if self.store_root_lease_state == StoreRootLeaseState::Destroyed {
            return true;
        }
        if matches!(
            self.lifecycle_state(),
            LocalStoreState::Started
                | LocalStoreState::RecoveringConsumeQueue
                | LocalStoreState::RecoveringCommitLog
                | LocalStoreState::RecoveringTopicQueueTable
        ) {
            warn!(
                state = ?self.lifecycle_state(),
                "message store must finish shutdown or recovery before destroy"
            );
            return false;
        }
        self.store_root_lease_state = StoreRootLeaseState::DestroyRetryPending;
        warn!("message store explicit destroy is write-disabled until all Store namespaces support durable retirement");
        false
    }
}
