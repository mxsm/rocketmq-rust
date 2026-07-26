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
    pub(super) fn is_temp_file_exist(&self) -> bool {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        Path::new(&file_name).exists()
    }

    pub(super) fn create_temp_file(&self) {
        let file_name = get_abort_file(self.message_store_config.store_path_root_dir.as_str());
        let pid = std::process::id();
        match fs::File::create(file_name.as_str()) {
            Ok(_) => {}
            Err(e) => {
                error!("create temp file error: {}", e);
            }
        }
        let _ = string_to_file(pid.to_string().as_str(), file_name.as_str());
    }

    pub(super) async fn recover(&mut self, last_exit_ok: bool) {
        let previous_state = self.lifecycle_state();
        let recover_concurrently = self.is_recover_concurrently();
        let mut recovery_plan = RecoveryPlan::new(
            self.message_store_config.recovery_mode,
            RecoveryExit::from_last_exit_ok(last_exit_ok),
            recover_concurrently,
            self.message_store_config.max_recovery_commit_log_files,
        );
        recovery_plan.crc_policy = RecoveryCrcPolicy::new(
            self.message_store_config.check_crc_on_recover,
            self.message_store_config.force_verify_prop_crc,
        );
        recovery_plan.index_repair_policy = if self.message_store_config.message_index_enable {
            RecoveryIndexRepairPolicy::Synchronous
        } else {
            RecoveryIndexRepairPolicy::Disabled
        };
        recovery_plan.set_commit_log_offsets(
            self.get_min_phy_offset(),
            self.get_max_phy_offset(),
            self.get_confirm_offset(),
        );
        recovery_plan.set_index_safe_offset(self.current_index_safe_offset());
        recovery_plan.set_consume_queue_recovery_concurrency(ConsumeQueueRecoveryConcurrency::new(
            self.message_store_config
                .enable_local_file_consume_queue_recovery_concurrently,
            self.composition.config().recovery.consume_queue_parallelism,
        ));
        let mut recovery_executor = RecoveryExecutor::new(recovery_plan);
        info!(
            "message store recover mode: {}, recoveryMode: {}, lastExit: {}, maxRecoveryCommitLogFiles: {}, \
             crcPolicy: message={}, property={}, indexRepairPolicy: {}, localFileCqRecoveryConcurrent: {}, \
             localFileCqRecoveryParallelism: {}",
            if recover_concurrently { "concurrent" } else { "normal" },
            self.message_store_config.recovery_mode.as_str(),
            recovery_executor.plan().exit.as_str(),
            recovery_executor.plan().max_recovery_commit_log_files,
            recovery_executor.plan().crc_policy.check_message_crc,
            recovery_executor.plan().crc_policy.check_property_crc,
            recovery_executor.plan().index_repair_policy.as_str(),
            recovery_executor
                .plan()
                .consume_queue_recovery_concurrency
                .local_file_enabled,
            recovery_executor
                .plan()
                .consume_queue_recovery_concurrency
                .local_file_parallelism
        );
        self.set_lifecycle_state(LocalStoreState::RecoveringConsumeQueue);
        let recover_consume_queue = recovery_executor
            .run_phase(RecoveryPhase::ConsumeQueue, self.recover_consume_queue())
            .await;
        let dispatch_recovery_offset = self.get_dispatch_recovery_offset();
        recovery_executor
            .plan_mut()
            .set_dispatch_recovery_offset(dispatch_recovery_offset);
        recovery_executor
            .plan_mut()
            .set_max_consume_queue_physical_offset(dispatch_recovery_offset);

        self.set_lifecycle_state(LocalStoreState::RecoveringCommitLog);
        let recover_commit_log = if last_exit_ok {
            recovery_executor
                .run_phase(
                    RecoveryPhase::CommitLog,
                    self.recover_normally(dispatch_recovery_offset),
                )
                .await
        } else {
            recovery_executor
                .run_phase(
                    RecoveryPhase::CommitLog,
                    self.recover_abnormally(dispatch_recovery_offset),
                )
                .await
        };

        self.set_lifecycle_state(LocalStoreState::RecoveringTopicQueueTable);
        let recover_topic_queue_table = recovery_executor
            .run_phase(RecoveryPhase::TopicQueueTable, async {
                self.recover_topic_queue_table();
            })
            .await;
        if self.lifecycle_state() != LocalStoreState::Shutdown {
            self.set_lifecycle_state(previous_state);
        }
        recovery_executor.plan_mut().set_commit_log_offsets(
            self.get_min_phy_offset(),
            self.get_max_phy_offset(),
            self.get_confirm_offset(),
        );
        recovery_executor
            .plan_mut()
            .set_index_safe_offset(self.current_index_safe_offset());
        let recovery_report = recovery_executor.finish();
        info!(
            "message store recover total cost: {} ms, recoverConsumeQueue: {} ms, recoverCommitLog: {} ms, \
             recoverOffsetTable: {} ms, recoveryMode: {}, lastExit: {}, dispatchRecoveryOffset: {:?}, commitLogRange: \
             {:?}-{:?}, confirmOffset: {:?}, indexSafeOffset: {:?}",
            recovery_report.total_duration_ms,
            recover_consume_queue,
            recover_commit_log,
            recover_topic_queue_table,
            recovery_report.plan.mode.as_str(),
            recovery_report.plan.exit.as_str(),
            recovery_report.plan.dispatch_recovery_offset,
            recovery_report.plan.offsets.commit_log_min_offset,
            recovery_report.plan.offsets.commit_log_max_offset,
            recovery_report.plan.offsets.confirm_offset,
            recovery_report.plan.offsets.index_safe_offset
        );
        self.last_recovery_report = Some(recovery_report);
    }

    pub(super) fn current_index_safe_offset(&self) -> i64 {
        let Some(store_checkpoint) = self.store_checkpoint.as_ref() else {
            return 0;
        };
        let checkpoint_safe_offset = store_checkpoint.index_safe_phy_offset();
        let confirm_offset = self.get_confirm_offset().max(0) as u64;
        checkpoint_safe_offset.min(confirm_offset) as i64
    }

    pub async fn recover_normally(&mut self, max_phy_offset_of_consume_queue: i64) {
        let optimized_recovery_value = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY").ok();
        let use_optimized = optimized_recovery_requested(optimized_recovery_value.as_deref());

        drive_commit_log_recovery(use_optimized, |step| async move {
            match step {
                CommitLogRecoveryStep::Optimized => {
                    self.commit_log
                        .recover_normally_optimized(max_phy_offset_of_consume_queue)
                        .await;
                }
                CommitLogRecoveryStep::Standard => {
                    self.commit_log.recover_normally(max_phy_offset_of_consume_queue).await;
                }
            }
        })
        .await;
    }

    pub async fn recover_abnormally(&mut self, max_phy_offset_of_consume_queue: i64) {
        let optimized_recovery_value = std::env::var("ROCKETMQ_USE_OPTIMIZED_RECOVERY").ok();
        let use_optimized = optimized_recovery_requested(optimized_recovery_value.as_deref());

        drive_commit_log_recovery(use_optimized, |step| async move {
            match step {
                CommitLogRecoveryStep::Optimized => {
                    self.commit_log
                        .recover_abnormally_optimized(max_phy_offset_of_consume_queue)
                        .await;
                }
                CommitLogRecoveryStep::Standard => {
                    self.commit_log
                        .recover_abnormally(max_phy_offset_of_consume_queue)
                        .await;
                }
            }
        })
        .await;
    }

    pub(super) fn is_recover_concurrently(&self) -> bool {
        self.broker_config.recover_concurrently
            && (self.message_store_config.is_enable_rocksdb_store()
                || self.is_local_file_consume_queue_recover_concurrently())
    }

    pub(super) fn is_local_file_consume_queue_recover_concurrently(&self) -> bool {
        !self.message_store_config.is_enable_rocksdb_store()
            && self
                .message_store_config
                .enable_local_file_consume_queue_recovery_concurrently
    }

    pub(super) async fn recover_consume_queue(&mut self) {
        if self.broker_config.recover_concurrently && self.message_store_config.is_enable_rocksdb_store() {
            self.consume_queue_store.recover_concurrently().await;
        } else if self.broker_config.recover_concurrently && self.is_local_file_consume_queue_recover_concurrently() {
            let parallelism = self.composition.config().recovery.consume_queue_parallelism;
            let summary = self
                .consume_queue_store
                .recover_concurrently_with_summary(parallelism)
                .await;
            if !summary.is_success() {
                warn!(
                    "local file consume queue concurrent recovery failed, fallback to serial recovery, \
                     parallelism={}, queues={}, success={}, failed={}, failures={}",
                    parallelism,
                    summary.queue_count,
                    summary.success_count,
                    summary.failure_count,
                    summary.failure_description()
                );
                self.consume_queue_store.recover().await;
            }
        } else {
            self.consume_queue_store.recover().await;
        }
    }

    pub(super) fn get_dispatch_recovery_offset(&self) -> i64 {
        let commit_log_min_offset = self.commit_log.get_min_offset();
        let dispatch_recovery_offset = self
            .dispatcher
            .min_dispatch_progress_offset(commit_log_min_offset)
            .unwrap_or(commit_log_min_offset)
            .max(commit_log_min_offset);
        let controller_epoch_start_offset = self.controller_epoch_start_offset.load(Ordering::SeqCst);
        if controller_epoch_start_offset >= 0 {
            dispatch_recovery_offset.max(controller_epoch_start_offset.max(commit_log_min_offset))
        } else {
            dispatch_recovery_offset
        }
    }

    pub fn set_state_machine_version(&mut self, state_machine_version: i64) {
        self.publish_state_machine_version(state_machine_version);
    }

    pub(crate) fn publish_state_machine_version(&self, state_machine_version: i64) {
        self.state_machine_version
            .store(state_machine_version, Ordering::SeqCst);
    }

    pub fn set_controller_epoch_start_offset(&mut self, epoch_start_offset: i64) {
        self.publish_controller_epoch_start_offset(epoch_start_offset);
    }

    pub(crate) fn publish_controller_epoch_start_offset(&self, epoch_start_offset: i64) {
        self.controller_epoch_start_offset
            .store(epoch_start_offset, Ordering::SeqCst);
    }

    pub(crate) fn publish_confirm_offset(&self, phy_offset: i64) {
        self.commit_log.publish_confirm_offset(phy_offset);
    }

    pub fn get_controller_epoch_start_offset(&self) -> i64 {
        self.controller_epoch_start_offset.load(Ordering::SeqCst)
    }

    pub fn next_offset_correction(&self, old_offset: i64, new_offset: i64) -> i64 {
        let mut next_offset = old_offset;
        if self.store_runtime_state.broker_role() != BrokerRole::Slave
            || self.message_store_config.offset_check_in_slave
        {
            next_offset = new_offset;
        }
        next_offset
    }

    pub(super) fn check_in_mem_by_commit_offset(&self, offset_py: i64, size: i32) -> bool {
        let message = self.commit_log.get_message(offset_py, size);
        match message {
            None => false,
            Some(msg) => msg.is_in_mem(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackgroundIndexRebuildState {
    #[default]
    Idle,
    Running,
    Paused,
    Completed,
    Retrying,
    Failed,
    Shutdown,
}

impl BackgroundIndexRebuildState {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Running => 1,
            Self::Paused => 2,
            Self::Completed => 3,
            Self::Retrying => 4,
            Self::Failed => 5,
            Self::Shutdown => 6,
        }
    }

    pub(super) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Paused,
            3 => Self::Completed,
            4 => Self::Retrying,
            5 => Self::Failed,
            6 => Self::Shutdown,
            _ => Self::Idle,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Retrying => "retrying",
            Self::Failed => "failed",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundIndexRebuildSnapshot {
    pub state: BackgroundIndexRebuildState,
    pub current_safe_offset: i64,
    pub target_offset: i64,
    pub backlog_bytes: i64,
    pub rebuilt_bytes: u64,
    pub rebuilt_messages: u64,
    pub failure_count: u64,
    pub last_error: Option<String>,
    pub bytes_per_second: u64,
}

pub(super) struct BackgroundIndexRebuildProgress {
    state: AtomicU8,
    paused: AtomicBool,
    current_safe_offset: AtomicI64,
    target_offset: AtomicI64,
    rebuilt_bytes: AtomicU64,
    rebuilt_messages: AtomicU64,
    failure_count: AtomicU64,
    bytes_per_second: AtomicU64,
    last_error: StdMutex<Option<String>>,
    resume_notify: Notify,
}

impl BackgroundIndexRebuildProgress {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicU8::new(BackgroundIndexRebuildState::Idle.as_u8()),
            paused: AtomicBool::new(false),
            current_safe_offset: AtomicI64::new(0),
            target_offset: AtomicI64::new(0),
            rebuilt_bytes: AtomicU64::new(0),
            rebuilt_messages: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            bytes_per_second: AtomicU64::new(0),
            last_error: StdMutex::new(None),
            resume_notify: Notify::new(),
        }
    }

    pub(super) fn reset(&self, current_safe_offset: i64, target_offset: i64, bytes_per_second: u64) {
        self.paused.store(false, Ordering::Release);
        self.current_safe_offset
            .store(current_safe_offset.max(0), Ordering::Release);
        self.target_offset.store(target_offset.max(0), Ordering::Release);
        self.rebuilt_bytes.store(0, Ordering::Release);
        self.rebuilt_messages.store(0, Ordering::Release);
        self.failure_count.store(0, Ordering::Release);
        self.bytes_per_second.store(bytes_per_second, Ordering::Release);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = None;
        }
        self.set_state(BackgroundIndexRebuildState::Idle);
    }

    pub(super) fn set_state(&self, state: BackgroundIndexRebuildState) {
        self.state.store(state.as_u8(), Ordering::Release);
    }

    pub(super) fn state(&self) -> BackgroundIndexRebuildState {
        BackgroundIndexRebuildState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub(super) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
        if paused {
            self.set_state(BackgroundIndexRebuildState::Paused);
        } else {
            if self.state() == BackgroundIndexRebuildState::Paused {
                self.set_state(BackgroundIndexRebuildState::Idle);
            }
            self.resume_notify.notify_waiters();
        }
    }

    pub(super) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub(super) fn update_current_safe_offset(&self, current_safe_offset: i64) {
        self.current_safe_offset
            .store(current_safe_offset.max(0), Ordering::Release);
    }

    pub(super) fn record_rebuild(&self, bytes: u64, messages: u64) {
        self.rebuilt_bytes.fetch_add(bytes, Ordering::AcqRel);
        self.rebuilt_messages.fetch_add(messages, Ordering::AcqRel);
    }

    pub(super) fn record_error(&self, error: impl Into<String>) {
        self.failure_count.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.into());
        }
    }

    pub(super) fn snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        let current_safe_offset = self.current_safe_offset.load(Ordering::Acquire);
        let target_offset = self.target_offset.load(Ordering::Acquire);
        let last_error = self.last_error.lock().ok().and_then(|error| error.clone());
        BackgroundIndexRebuildSnapshot {
            state: self.state(),
            current_safe_offset,
            target_offset,
            backlog_bytes: target_offset.saturating_sub(current_safe_offset).max(0),
            rebuilt_bytes: self.rebuilt_bytes.load(Ordering::Acquire),
            rebuilt_messages: self.rebuilt_messages.load(Ordering::Acquire),
            failure_count: self.failure_count.load(Ordering::Acquire),
            last_error,
            bytes_per_second: self.bytes_per_second.load(Ordering::Acquire),
        }
    }
}

pub(super) struct BackgroundIndexRebuildService {
    shutdown_token: CancellationToken,
    progress: Arc<BackgroundIndexRebuildProgress>,
    task_group: Option<rocketmq_runtime::TaskGroup>,
}

impl BackgroundIndexRebuildService {
    pub(super) fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
            progress: Arc::new(BackgroundIndexRebuildProgress::new()),
            task_group: None,
        }
    }

    pub(super) fn start(
        &mut self,
        runtime_scope: &StoreRuntimeScope,
        commit_log: CommitLogReadHandle,
        message_store_config: Arc<MessageStoreConfig>,
        index_service: IndexService,
        delay_level_table: BTreeMap<i32, i64>,
        max_delay_level: i32,
    ) {
        if self.task_group.is_some() || !message_store_config.effective_background_index_rebuild_enable() {
            return;
        }
        if !message_store_config.message_index_enable {
            return;
        }

        let current_safe_offset = index_service.index_safe_phy_offset().min(i64::MAX as u64) as i64;
        let target_offset = commit_log.get_confirm_offset().max(0);
        let bytes_per_second = message_store_config.background_index_rebuild_bytes_per_second as u64;
        self.progress
            .reset(current_safe_offset, target_offset, bytes_per_second);

        if current_safe_offset >= target_offset {
            self.progress.set_state(BackgroundIndexRebuildState::Completed);
            return;
        }

        let task_group =
            crate::runtime::task_group(runtime_scope, "rocketmq-store.local-file.background-index-rebuild");

        self.shutdown_token = CancellationToken::new();
        let batch_size = message_store_config.background_index_rebuild_batch_size.max(1);
        let max_retries = message_store_config.background_index_rebuild_max_retries;
        let worker = BackgroundIndexRebuildWorker {
            commit_log,
            message_store_config,
            index_service,
            delay_level_table,
            max_delay_level,
            progress: self.progress.clone(),
            shutdown_token: self.shutdown_token.clone(),
            batch_size,
            bytes_per_second,
            max_retries,
        };

        if let Err(error) = task_group.spawn_service("background-index-rebuild", async move {
            worker.run().await;
        }) {
            self.shutdown_token.cancel();
            self.progress.record_error(error.to_string());
            self.progress.set_state(BackgroundIndexRebuildState::Failed);
            error!("failed to spawn BackgroundIndexRebuildService: {error}");
            return;
        }

        self.task_group = Some(task_group);
    }

    pub(super) fn pause(&self) {
        self.progress.set_paused(true);
    }

    pub(super) fn resume(&self) {
        self.progress.set_paused(false);
    }

    pub(super) fn snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        self.progress.snapshot()
    }

    pub(super) async fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.progress.resume_notify.notify_waiters();
        if let Some(task_group) = self.task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            match crate::runtime::shutdown_report_result("BackgroundIndexRebuildService", report) {
                Ok(()) => info!("BackgroundIndexRebuildService tasks shut down successfully"),
                Err(error) => warn!("BackgroundIndexRebuildService task shutdown reported an error: {error}"),
            }
        }
        self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
    }

    pub(super) fn has_task_group(&self) -> bool {
        self.task_group.is_some()
    }
}

pub(super) struct BackgroundIndexRebuildWorker {
    commit_log: CommitLogReadHandle,
    message_store_config: Arc<MessageStoreConfig>,
    index_service: IndexService,
    delay_level_table: BTreeMap<i32, i64>,
    max_delay_level: i32,
    progress: Arc<BackgroundIndexRebuildProgress>,
    shutdown_token: CancellationToken,
    batch_size: usize,
    bytes_per_second: u64,
    max_retries: usize,
}

pub(super) struct BackgroundIndexRebuildBatch {
    bytes: u64,
    messages: u64,
    completed: bool,
}

impl BackgroundIndexRebuildWorker {
    pub(super) async fn run(self) {
        let mut retry_count = 0usize;
        loop {
            if !self.wait_if_paused().await {
                return;
            }
            self.progress.set_state(BackgroundIndexRebuildState::Running);
            let started = Instant::now();
            match self.rebuild_batch() {
                Ok(batch) => {
                    retry_count = 0;
                    if batch.bytes > 0 || batch.messages > 0 {
                        self.progress.record_rebuild(batch.bytes, batch.messages);
                    }
                    if batch.completed {
                        if let Err(error) = self.index_service.flush_index_safe_offset() {
                            self.progress.record_error(error.to_string());
                            self.progress.set_state(BackgroundIndexRebuildState::Failed);
                            return;
                        }
                        self.progress.set_state(BackgroundIndexRebuildState::Completed);
                        return;
                    }
                    self.throttle(batch.bytes, started).await;
                }
                Err(error) => {
                    self.progress.record_error(error);
                    if retry_count >= self.max_retries {
                        self.progress.set_state(BackgroundIndexRebuildState::Failed);
                        return;
                    }
                    retry_count += 1;
                    self.progress.set_state(BackgroundIndexRebuildState::Retrying);
                    tokio::select! {
                        _ = self.shutdown_token.cancelled() => {
                            self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    }
                }
            }
        }
    }

    pub(super) async fn wait_if_paused(&self) -> bool {
        while self.progress.is_paused() {
            self.progress.set_state(BackgroundIndexRebuildState::Paused);
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                    return false;
                }
                _ = self.progress.resume_notify.notified() => {}
            }
        }
        if self.shutdown_token.is_cancelled() {
            self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
            return false;
        }
        true
    }

    pub(super) fn rebuild_batch(&self) -> Result<BackgroundIndexRebuildBatch, String> {
        let target_offset = self.progress.target_offset.load(Ordering::Acquire);
        let mut current_offset = self.progress.current_safe_offset.load(Ordering::Acquire);
        if current_offset >= target_offset {
            return Ok(BackgroundIndexRebuildBatch {
                bytes: 0,
                messages: 0,
                completed: true,
            });
        }

        let min_offset = self.commit_log.get_min_offset();
        if current_offset < min_offset {
            info!(
                "background index rebuild offset {current_offset} is smaller than commitlog min offset {min_offset}, \
                 advancing to retained range"
            );
            current_offset = min_offset;
            self.index_service.advance_index_safe_offset_to(current_offset);
            self.progress.update_current_safe_offset(current_offset);
            if current_offset >= target_offset {
                return Ok(BackgroundIndexRebuildBatch {
                    bytes: 0,
                    messages: 0,
                    completed: true,
                });
            }
        }

        let mut result = self
            .commit_log
            .get_data(current_offset)
            .ok_or_else(|| format!("commitlog data unavailable at offset {current_offset}"))?;
        current_offset = result.start_offset as i64;
        self.progress.update_current_safe_offset(current_offset);

        let mut read_size = 0i32;
        let mut rebuilt_bytes = 0u64;
        let mut rebuilt_messages = 0u64;
        while read_size < result.size
            && current_offset < target_offset
            && rebuilt_messages < self.batch_size as u64
            && !self.shutdown_token.is_cancelled()
        {
            let Some(bytes) = result.bytes.as_mut() else {
                return Err("commitlog data buffer is missing during background index rebuild".to_string());
            };
            let mut dispatch_request = commit_log::check_message_and_return_size(
                bytes,
                false,
                false,
                false,
                &self.message_store_config,
                self.max_delay_level,
                &self.delay_level_table,
            );
            let size = if dispatch_request.buffer_size == -1 {
                dispatch_request.msg_size
            } else {
                dispatch_request.buffer_size
            };

            if dispatch_request.success && dispatch_request.msg_size == 0 {
                current_offset = self.commit_log.roll_next_file(current_offset);
                self.index_service.advance_index_safe_offset_to(current_offset);
                self.progress.update_current_safe_offset(current_offset);
                read_size = result.size;
                continue;
            }
            if size <= 0 {
                return Err(format!("invalid message size {size} at offset {current_offset}"));
            }
            if current_offset.saturating_add(i64::from(size)) > target_offset {
                break;
            }

            if dispatch_request.success {
                match dispatch_request.msg_size.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        dispatch_request.commit_log_offset = current_offset;
                        self.index_service.build_index(&dispatch_request);
                        current_offset = current_offset.saturating_add(i64::from(size));
                        self.progress.update_current_safe_offset(current_offset);
                        rebuilt_bytes = rebuilt_bytes.saturating_add(size as u64);
                        rebuilt_messages = rebuilt_messages.saturating_add(1);
                        read_size += size;
                    }
                    std::cmp::Ordering::Equal => {}
                    std::cmp::Ordering::Less => {
                        return Err(format!(
                            "negative message size {} at offset {current_offset}",
                            dispatch_request.msg_size
                        ));
                    }
                }
            } else {
                return Err(format!("invalid message at offset {current_offset}"));
            }
        }

        Ok(BackgroundIndexRebuildBatch {
            bytes: rebuilt_bytes,
            messages: rebuilt_messages,
            completed: current_offset >= target_offset,
        })
    }

    pub(super) async fn throttle(&self, bytes: u64, started: Instant) {
        if self.bytes_per_second == 0 || bytes == 0 {
            return;
        }
        let expected = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64);
        let elapsed = started.elapsed();
        if expected > elapsed {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    self.progress.set_state(BackgroundIndexRebuildState::Shutdown);
                }
                _ = tokio::time::sleep(expected - elapsed) => {}
            }
        }
    }
}
