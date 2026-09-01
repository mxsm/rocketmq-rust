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
    pub(super) fn store_health_snapshot(&self) -> BackendHealthSnapshot {
        let ha_runtime_info = self
            .ha_service
            .as_ref()
            .map_or_else(Default::default, GeneralHAService::group_transfer_runtime_info);
        BackendHealthSnapshot {
            writeable: self.store_health_recorder.writeable(),
            last_flush_error: self.store_health_recorder.last_flush_error(),
            os_page_cache_busy: self.is_os_page_cache_busy(),
            transient_store_pool_deficient: self.is_transient_store_pool_deficient(),
            sync_flush: self.sync_flush_runtime_info(),
            dispatch_behind_bytes: self.dispatch_behind_bytes(),
            shutdown: self.is_shutdown(),
            ha_pending_request_count: ha_runtime_info.pending_request_count,
            ha_pending_oldest_wait_millis: ha_runtime_info.pending_request_oldest_wait_millis,
        }
    }

    pub fn get_message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    pub fn last_recovery_report(&self) -> Option<&RecoveryReport> {
        self.last_recovery_report.as_ref()
    }

    pub fn background_index_rebuild_snapshot(&self) -> BackgroundIndexRebuildSnapshot {
        self.background_index_rebuild_service.snapshot()
    }

    pub fn pause_background_index_rebuild(&self) {
        self.background_index_rebuild_service.pause();
    }

    pub fn resume_background_index_rebuild(&self) {
        self.background_index_rebuild_service.resume();
    }

    pub(crate) fn record_flush_failure(&self, error: &StoreError) {
        self.store_health_recorder.record_flush_failure(error);
    }
}

pub(super) async fn run_blocking_scheduled_task<F>(
    runtime_scope: &StoreRuntimeScope,
    task_name: &'static str,
    task: F,
) -> bool
where
    F: FnOnce() + Send + 'static,
{
    match crate::runtime::spawn_io(runtime_scope, task_name, task).await {
        Ok(()) => true,
        Err(error) => {
            error!("scheduled store task {task_name} failed: {error}");
            false
        }
    }
}

pub(super) fn store_path_disk_used_ratio(path: &str) -> f64 {
    let path = path.trim();
    if path.is_empty() {
        error!("Error when measuring disk space usage, path is null or empty");
        return -1.0;
    }

    let path = Path::new(path);
    if !path.exists() {
        error!(
            "Error when measuring disk space usage, file doesn't exist on this path: {}",
            path.to_string_lossy()
        );
        return -1.0;
    }

    match (fs2::total_space(path), fs2::available_space(path)) {
        (Ok(total_space), Ok(available_space)) if total_space > 0 => {
            total_space.saturating_sub(available_space) as f64 / total_space as f64
        }
        (Ok(_), Ok(_)) => -1.0,
        (Err(error), _) | (_, Err(error)) => {
            error!("Error when measuring disk space usage, got exception: {:?}", error);
            -1.0
        }
    }
}

pub(super) type CommitLogWalPin = dyn Fn() -> Option<u64> + Send + Sync;

pub(super) struct CleanCommitLogService {
    message_store_config: Arc<MessageStoreConfig>,
    commit_log: CommitLogCleanupHandle,
    running_flags: Arc<RunningFlags>,
    cleanup_policy: LocalCleanupPolicy,
    manual_delete_tracker: ManualDeleteTracker,
    minimum_pinned_wal_segment: Option<Arc<CommitLogWalPin>>,
    #[cfg(test)]
    disk_clean_decision_override: StdMutex<Option<DiskCleanDecision>>,
}

impl CleanCommitLogService {
    pub(super) const MAX_MANUAL_DELETE_FILE_TIMES: i32 = 20;

    pub(super) fn new(
        message_store_config: Arc<MessageStoreConfig>,
        commit_log: CommitLogCleanupHandle,
        running_flags: Arc<RunningFlags>,
        cleanup_policy: LocalCleanupPolicy,
        minimum_pinned_wal_segment: Option<Arc<CommitLogWalPin>>,
    ) -> Self {
        Self {
            message_store_config,
            commit_log,
            running_flags,
            cleanup_policy,
            manual_delete_tracker: ManualDeleteTracker::new(Self::MAX_MANUAL_DELETE_FILE_TIMES),
            minimum_pinned_wal_segment,
            #[cfg(test)]
            disk_clean_decision_override: StdMutex::new(None),
        }
    }

    pub(super) fn run(&self) {
        let expired_time = (self.message_store_config.file_reserved_time as i64)
            .saturating_mul(60)
            .saturating_mul(60)
            .saturating_mul(1000);
        let is_time_up = util_all::is_it_time_to_do(&self.message_store_config.delete_when);
        let disk_decision = self.is_space_to_delete();
        let is_manual_delete = self.consume_manual_delete_request();
        let clean_at_once = self.cleanup_policy.clean_file_forcibly_enabled() && disk_decision.clean_immediately;
        let minimum_pinned_wal_segment = self
            .minimum_pinned_wal_segment
            .as_ref()
            .and_then(|minimum_pinned_wal_segment| minimum_pinned_wal_segment());
        let delete_count = if is_time_up || disk_decision.should_delete || is_manual_delete {
            self.commit_log.delete_expired_files_by_time_before(
                expired_time,
                self.message_store_config.delete_commit_log_files_interval as i32,
                self.message_store_config.destroy_mapped_file_interval_forcibly as i64,
                clean_at_once,
                self.message_store_config.delete_file_batch_max as i32,
                minimum_pinned_wal_segment,
            )
        } else {
            0
        };
        if delete_count > 0 {
            info!(
                "clean commit log service deleted {} expired commitlog file(s), is_time_up={}, is_space_to_delete={}, \
                 is_manual_delete={}, clean_at_once={}",
                delete_count, is_time_up, disk_decision.should_delete, is_manual_delete, clean_at_once
            );
        } else if disk_decision.should_delete {
            warn!("disk space will be full soon, but delete commitlog file failed");
        }

        let first_file_is_before_pin = minimum_pinned_wal_segment
            .is_none_or(|pinned| u64::try_from(self.commit_log.get_min_offset()).is_ok_and(|minimum| minimum < pinned));
        if first_file_is_before_pin {
            let _ = self
                .commit_log
                .retry_delete_first_file(self.message_store_config.redelete_hanged_file_interval as i64);
        }
    }

    pub(super) fn execute_delete_files_manually(&self) {
        self.manual_delete_tracker.request();
        info!("executeDeleteFilesManually was invoked");
    }

    pub(super) fn consume_manual_delete_request(&self) -> bool {
        self.manual_delete_tracker.consume()
    }

    #[cfg(test)]
    pub(super) fn remaining_manual_delete_requests(&self) -> i32 {
        self.manual_delete_tracker.remaining()
    }

    #[cfg(test)]
    pub(super) fn set_disk_clean_decision_override(&self, decision: Option<DiskCleanDecision>) {
        *self
            .disk_clean_decision_override
            .lock()
            .expect("lock disk clean decision override") = decision;
    }

    pub(super) fn is_space_to_delete(&self) -> DiskCleanDecision {
        #[cfg(test)]
        if let Some(decision) = *self
            .disk_clean_decision_override
            .lock()
            .expect("lock disk clean decision override")
        {
            return decision;
        }

        let (min_physic_ratio, min_store_path) = self.min_physic_disk_ratio();

        match self.cleanup_policy.classify(min_physic_ratio) {
            DiskUsageState::Warning => {
                if self.running_flags.get_and_make_disk_full() {
                    error!(
                        "physic disk maybe full soon {}, so mark disk full, storePathPhysic={}",
                        min_physic_ratio,
                        min_store_path.as_deref().unwrap_or("")
                    );
                }
                return DiskCleanDecision {
                    should_delete: true,
                    clean_immediately: true,
                };
            }
            DiskUsageState::Forcible => {
                return DiskCleanDecision {
                    should_delete: true,
                    clean_immediately: true,
                };
            }
            DiskUsageState::Reclaim | DiskUsageState::Healthy => {
                if !self.running_flags.get_and_make_disk_ok() {
                    info!(
                        "physic disk space OK {}, so mark disk ok, storePathPhysic={}",
                        min_physic_ratio,
                        min_store_path.as_deref().unwrap_or("")
                    );
                }
            }
        }

        let store_path_logics = LocalFileMessageStore::get_store_path_logic(&self.message_store_config);
        let logics_ratio = store_path_disk_used_ratio(store_path_logics.as_str());
        match self.cleanup_policy.classify(logics_ratio) {
            DiskUsageState::Warning => {
                if self.running_flags.get_and_make_logic_disk_full() {
                    error!("logics disk maybe full soon {}, so mark disk full", logics_ratio);
                }
                return DiskCleanDecision {
                    should_delete: true,
                    clean_immediately: true,
                };
            }
            DiskUsageState::Forcible => {
                return DiskCleanDecision {
                    should_delete: true,
                    clean_immediately: true,
                };
            }
            DiskUsageState::Reclaim | DiskUsageState::Healthy => {
                if !self.running_flags.get_and_make_logic_disk_ok() {
                    info!("logics disk space OK {}, so mark disk ok", logics_ratio);
                }
            }
        }

        let decision = self.cleanup_policy.decide(min_physic_ratio, logics_ratio);
        if self.cleanup_policy.classify(min_physic_ratio) == DiskUsageState::Reclaim {
            info!("commitLog disk maybe full soon, so reclaim space, {}", min_physic_ratio);
            return decision;
        }

        if self.cleanup_policy.classify(logics_ratio) == DiskUsageState::Reclaim {
            info!("consumeQueue disk maybe full soon, so reclaim space, {}", logics_ratio);
            return decision;
        }

        decision
    }

    pub(super) fn min_physic_disk_ratio(&self) -> (f64, Option<String>) {
        let commit_log_store_path = LocalFileMessageStore::get_store_path_physic(&self.message_store_config);
        let mut min_ratio = f64::MAX;
        let mut min_store_path = None;

        for store_path in commit_log_store_path.split(mix_all::MULTI_PATH_SPLITTER.as_str()) {
            let store_path = store_path.trim();
            if store_path.is_empty() {
                continue;
            }

            let ratio = store_path_disk_used_ratio(store_path);
            if min_ratio > ratio {
                min_ratio = ratio;
                min_store_path = Some(store_path.to_string());
            }
        }

        if min_ratio == f64::MAX {
            (-1.0, None)
        } else {
            (min_ratio, min_store_path)
        }
    }

    pub(super) fn disk_space_warning_level_ratio(&self) -> f64 {
        self.cleanup_policy.disk_warning_ratio()
    }

    pub(super) fn disk_space_clean_forcibly_ratio(&self) -> f64 {
        self.cleanup_policy.disk_clean_forcibly_ratio()
    }

    pub(super) fn disk_max_used_space_ratio(&self) -> f64 {
        self.cleanup_policy.disk_max_used_ratio()
    }
}

pub(super) struct CleanConsumeQueueService {
    commit_log: CommitLogCleanupHandle,
    consume_queue_store: ConsumeQueueStore,
    index_service: IndexService,
}

impl CleanConsumeQueueService {
    pub(super) fn new(
        commit_log: CommitLogCleanupHandle,
        consume_queue_store: ConsumeQueueStore,
        index_service: IndexService,
    ) -> Self {
        Self {
            commit_log,
            consume_queue_store,
            index_service,
        }
    }

    pub(super) fn run(&self) {
        let min_commit_log_offset = self.commit_log.get_min_offset();
        if min_commit_log_offset < 0 {
            return;
        }

        let consume_queue_table = self.consume_queue_store.get_consume_queue_table().lock().clone();
        for queue_table in consume_queue_table.values() {
            for consume_queue in queue_table.values() {
                let consume_queue = consume_queue.read();
                let _ = self
                    .consume_queue_store
                    .delete_expired_file(consume_queue.as_ref(), min_commit_log_offset);
                self.consume_queue_store
                    .correct_min_offset(consume_queue.as_ref(), min_commit_log_offset);
            }
        }

        self.index_service
            .delete_expired_file(min_commit_log_offset.max(0) as u64);

        self.consume_queue_store.clean_expired_sync(min_commit_log_offset);
    }
}

pub(super) struct CorrectLogicOffsetService {
    commit_log: CommitLogCleanupHandle,
    consume_queue_store: ConsumeQueueStore,
}

impl CorrectLogicOffsetService {
    pub(super) fn new(commit_log: CommitLogCleanupHandle, consume_queue_store: ConsumeQueueStore) -> Self {
        Self {
            commit_log,
            consume_queue_store,
        }
    }

    pub(super) fn run(&self) {
        let min_commit_log_offset = self.commit_log.get_min_offset();
        if min_commit_log_offset < 0 {
            return;
        }

        let consume_queue_table = self.consume_queue_store.get_consume_queue_table().lock().clone();
        for queue_table in consume_queue_table.values() {
            for consume_queue in queue_table.values() {
                let consume_queue = consume_queue.read();
                self.consume_queue_store
                    .correct_min_offset(consume_queue.as_ref(), min_commit_log_offset);
            }
        }
    }
}
