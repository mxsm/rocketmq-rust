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
    }

    #[cfg(test)]
    pub(super) fn has_scheduled_task_group(&self) -> bool {
        self.scheduled_task_group.is_some()
    }

    pub(crate) fn scheduled_task_count(&self) -> usize {
        let root_count = self
            .scheduled_task_group
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
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

pub(super) struct FlushConsumeQueueService {
    runtime_scope: StoreRuntimeScope,
    message_store_config: Arc<MessageStoreConfig>,
    consume_queue_store: ConsumeQueueStore,
    store_checkpoint: Arc<StoreCheckpoint>,
    worker_group: parking_lot::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    shutdown_token: parking_lot::Mutex<CancellationToken>,
    wakeup: Arc<Notify>,
}

impl FlushConsumeQueueService {
    pub(super) fn new(
        runtime_scope: StoreRuntimeScope,
        message_store_config: Arc<MessageStoreConfig>,
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            runtime_scope,
            message_store_config,
            consume_queue_store,
            store_checkpoint,
            worker_group: parking_lot::Mutex::new(None),
            shutdown_token: parking_lot::Mutex::new(CancellationToken::new()),
            wakeup: Arc::new(Notify::new()),
        }
    }

    pub(super) fn flush_once_blocking(
        consume_queue_store: &ConsumeQueueStore,
        store_checkpoint: &StoreCheckpoint,
        flush_least_pages: i32,
    ) {
        let consume_queue_table = consume_queue_store.get_consume_queue_table().lock().clone();
        for consume_queue_table in consume_queue_table.values() {
            for consume_queue in consume_queue_table.values() {
                let consume_queue = consume_queue.read();
                let _ = consume_queue_store.flush(consume_queue.as_ref(), flush_least_pages);
            }
        }

        if let Err(error) = store_checkpoint.flush() {
            error!("flush consume queue service failed to flush store checkpoint: {error}");
        }
    }

    pub(super) async fn flush_once(
        runtime_scope: StoreRuntimeScope,
        consume_queue_store: ConsumeQueueStore,
        store_checkpoint: Arc<StoreCheckpoint>,
        flush_least_pages: i32,
    ) {
        if let Err(error) = crate::runtime::spawn_io(&runtime_scope, "flush-consume-queue", move || {
            Self::flush_once_blocking(&consume_queue_store, &store_checkpoint, flush_least_pages);
        })
        .await
        {
            error!("flush consume queue service task failed: {error}");
        }
    }

    pub(super) fn start(&self) {
        let mut worker_group = self.worker_group.lock();
        if worker_group.is_some() {
            return;
        }

        let group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.flush-consume-queue");

        let message_store_config = self.message_store_config.clone();
        let consume_queue_store = self.consume_queue_store.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let runtime_scope = self.runtime_scope.clone();
        let shutdown_token = CancellationToken::new();
        *self.shutdown_token.lock() = shutdown_token.clone();
        let wakeup = self.wakeup.clone();

        match group.spawn_service("flush-consume-queue", async move {
            let interval = message_store_config.flush_interval_consume_queue.max(1) as u64;
            let thorough_interval = message_store_config.flush_consume_queue_thorough_interval as u64;
            let default_least_pages = message_store_config.flush_consume_queue_least_pages as i32;
            let mut last_thorough_flush_timestamp = current_millis();

            loop {
                let now = current_millis();
                let flush_least_pages =
                    if thorough_interval == 0 || now >= last_thorough_flush_timestamp + thorough_interval {
                        last_thorough_flush_timestamp = now;
                        0
                    } else {
                        default_least_pages
                    };

                Self::flush_once(
                    runtime_scope.clone(),
                    consume_queue_store.clone(),
                    store_checkpoint.clone(),
                    flush_least_pages,
                )
                .await;

                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = wakeup.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(interval)) => {}
                }
            }

            Self::flush_once(runtime_scope, consume_queue_store, store_checkpoint, 0).await;
        }) {
            Ok(_) => {
                *worker_group = Some(group);
            }
            Err(error) => {
                error!("failed to start flush consume queue service task: {error}");
            }
        }
    }

    pub(super) async fn shutdown(&self) {
        self.shutdown_token.lock().cancel();
        self.wakeup.notify_waiters();

        let worker_group = self.worker_group.lock().take();
        if let Some(worker_group) = worker_group {
            let report = worker_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("FlushConsumeQueueService", report) {
                error!("flush consume queue service task failed during shutdown: {error}");
            }
        }
    }
}

impl LocalFileMessageStore {
    pub(super) async fn load_store(&mut self) -> bool {
        let last_exit_ok = !self.is_temp_file_exist();
        info!(
            "last shutdown {}, store path root dir: {}",
            if last_exit_ok { "normally" } else { "abnormally" },
            self.message_store_config.store_path_root_dir
        );
        //load Commit log-- init commit mapped file queue
        let mut result = self.commit_log.load();
        if !result {
            return result;
        }
        // load Consume Queue-- init Consume log mapped file queue
        result &= self.consume_queue_store.load();

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
        match self.lifecycle_state() {
            LocalStoreState::Initialized => {}
            LocalStoreState::Created => {
                return Err(StoreError::invalid_state(
                    StoreOperation::Start,
                    "message store must be initialized before start".to_string(),
                ));
            }
            LocalStoreState::Started => {
                return Err(StoreError::invalid_state(
                    StoreOperation::Start,
                    "message store is already started",
                ));
            }
            LocalStoreState::Shutdown => {
                return Err(StoreError::invalid_state(
                    StoreOperation::Start,
                    "message store is shutdown; call init before start".to_string(),
                ));
            }
            LocalStoreState::RecoveringConsumeQueue
            | LocalStoreState::RecoveringCommitLog
            | LocalStoreState::RecoveringTopicQueueTable => {
                return Err(StoreError::invalid_state(
                    StoreOperation::Start,
                    "message store is recovering; start is not allowed".to_string(),
                ));
            }
        }

        self.acquire_store_lock()?;
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

            if let Some(ha_service) = self.ha_service.as_mut() {
                ha_service.start().await.map_err(|e| {
                    error!("HA service start failed: {:?}", e);
                    StoreError::high_availability(StoreOperation::Start, e)
                })?;
            }
            self.create_temp_file();
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
                if self.background_index_rebuild_service.has_task_group() {
                    self.background_index_rebuild_service.shutdown().await;
                }
                #[cfg(feature = "tieredstore")]
                if let Some(tiered_store) = self.tiered_store.as_ref() {
                    if let Err(shutdown_error) = tiered_store.shutdown().await {
                        warn!("tieredstore shutdown after start failure failed: {}", shutdown_error);
                    }
                }
                self.release_store_lock();
                Err(error)
            }
        }
    }

    pub(super) async fn initialize_store(&mut self) -> Result<(), StoreError> {
        self.validate_supported_configuration()?;
        match self.lifecycle_state() {
            LocalStoreState::Created | LocalStoreState::Shutdown => {}
            LocalStoreState::Initialized => return Ok(()),
            LocalStoreState::Started => {
                return Err(StoreError::invalid_state(
                    StoreOperation::Load,
                    "message store cannot be initialized while started".to_string(),
                ));
            }
            LocalStoreState::RecoveringConsumeQueue
            | LocalStoreState::RecoveringCommitLog
            | LocalStoreState::RecoveringTopicQueueTable => {
                return Err(StoreError::invalid_state(
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
                StoreError::invalid_state(
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
                    return Err(StoreError::new(StoreErrorKind::Storage, StoreOperation::Load)
                        .in_component(StoreComponent::MappedFile)
                        .with_source(source));
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
        let previous_state = self.lifecycle_state();
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown.store(true, Ordering::Release);
            self.set_lifecycle_state(LocalStoreState::Shutdown);

            if matches!(previous_state, LocalStoreState::Created | LocalStoreState::Shutdown) {
                self.release_store_lock();
                let _ = self.transient_store_pool.destroy();
                return Ok(report);
            }

            if let Some(ha_service) = self.ha_service.as_ref() {
                self.shutdown_ha_update_master_tasks().await;
                ha_service.shutdown().await;
            }

            self.shutdown_schedule_tasks().await;
            self.store_stats_service.shutdown_gracefully().await;
            self.background_index_rebuild_service.shutdown().await;
            match self.commit_log.shutdown_gracefully().await {
                Ok(final_flush) => report.final_flush = Some(final_flush),
                Err(error) => {
                    self.record_flush_failure(&error);
                    shutdown_error = Some(error);
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
            if let Some(timer_message_store) = self.timer_message_store.as_ref() {
                timer_message_store.shutdown_gracefully().await;
            }
            self.flush_consume_queue_service.shutdown().await;
            self.allocate_mapped_file_service.shutdown().await;
            if let Some(store_checkpoint) = self.store_checkpoint.as_ref() {
                let _ = store_checkpoint.shutdown();
            }
            if self.running_flags.is_writeable() && self.dispatch_behind_bytes() == 0 {
                //delete abort file
                self.delete_file(get_abort_file(self.message_store_config.store_path_root_dir.as_str()))
            }
        }

        self.release_store_lock();
        let _ = self.transient_store_pool.destroy();
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
            warn!("message store cleanup remains pending; retaining metadata and queue progress for retry");
        }
    }

    #[must_use]
    pub(super) fn destroy_store_with_outcome(&mut self) -> bool {
        self.release_store_lock();
        let consume_queue_destroyed = self.consume_queue_store.destroy_with_outcome();
        let commit_log_destroyed = self.commit_log.destroy_with_outcome();
        let index_destroyed = self.index_service.destroy_with_outcome();
        let storage_destroyed = consume_queue_destroyed && commit_log_destroyed && index_destroyed;

        let metadata_destroyed = if storage_destroyed {
            let store_root = self.message_store_config.store_path_root_dir.clone();
            let abort_removed = self.delete_file_with_outcome(get_abort_file(store_root.as_str()));
            let checkpoint_removed = self.delete_file_with_outcome(get_store_checkpoint(store_root.as_str()));
            abort_removed && checkpoint_removed
        } else {
            warn!(
                consume_queue_destroyed,
                commit_log_destroyed,
                index_destroyed,
                "message store cleanup is incomplete; abort and checkpoint files are retained"
            );
            false
        };
        storage_destroyed && metadata_destroyed
    }
}
