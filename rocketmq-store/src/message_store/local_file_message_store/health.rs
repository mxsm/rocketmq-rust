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

    /// Returns the last recovery failure with its original diagnostic source.
    pub fn last_recovery_error(&self) -> Option<&StoreError> {
        self.last_recovery_error.as_ref()
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

#[derive(Clone, Copy, Debug)]
enum DiskSpaceSample {
    Known {
        total: u64,
        available: u64,
        observed_at: std::time::Instant,
    },
    Unknown {
        reason: &'static str,
        observed_at: std::time::Instant,
    },
}

impl DiskSpaceSample {
    fn sample(path: &Path) -> Self {
        let observed_at = std::time::Instant::now();
        // Windows volume probes may succeed for a missing path on an existing drive.
        if !path.is_dir() {
            return Self::Unknown {
                reason: "storage root unavailable",
                observed_at,
            };
        }
        match (fs2::total_space(path), fs2::available_space(path)) {
            (Ok(total), Ok(available)) if total > 0 && available <= total => Self::Known {
                total,
                available,
                observed_at,
            },
            (Ok(_), Ok(_)) => Self::Unknown {
                reason: "invalid capacity sample",
                observed_at,
            },
            _ => Self::Unknown {
                reason: "disk probe unavailable",
                observed_at,
            },
        }
    }

    fn ratio(self) -> Option<f64> {
        match self {
            Self::Known { total, available, .. } => Some((total - available) as f64 / total as f64),
            Self::Unknown { .. } => None,
        }
    }

    fn state(self, policy: LocalCleanupPolicy) -> DiskUsageState {
        match self {
            Self::Known { observed_at, .. } => {
                tracing::trace!(
                    sample_age_ms = observed_at.elapsed().as_millis() as u64,
                    "disk capacity sampled"
                );
                policy.classify(self.ratio().unwrap_or(f64::NAN))
            }
            Self::Unknown { reason, observed_at } => {
                warn!(
                    reason,
                    sample_age_ms = observed_at.elapsed().as_millis() as u64,
                    "disk capacity is unknown; retaining write protection"
                );
                DiskUsageState::Unknown
            }
        }
    }
}

struct DiskAvailabilitySummary {
    active: Option<DiskUsageState>,
    allocation: Vec<DiskUsageState>,
    cleanup: Vec<DiskUsageState>,
    logic: DiskUsageState,
}

impl DiskAvailabilitySummary {
    fn physical_state(&self) -> DiskUsageState {
        // An unhealthy active segment cannot be made safe by a healthy spare.
        // Once the segment is full, normal allocation validates the next root again.
        self.active.unwrap_or_else(|| {
            self.allocation
                .iter()
                .copied()
                .find(|state| matches!(state, DiskUsageState::Healthy | DiskUsageState::Reclaim))
                .or_else(|| self.allocation.first().copied())
                .unwrap_or(DiskUsageState::Unknown)
        })
    }

    fn apply(&self, flags: &RunningFlags) -> DiskCleanDecision {
        match self.physical_state() {
            DiskUsageState::Unknown | DiskUsageState::Warning => {
                flags.get_and_make_disk_full();
            }
            DiskUsageState::Healthy | DiskUsageState::Reclaim => {
                flags.get_and_make_disk_ok();
            }
            DiskUsageState::Forcible => {}
        }
        match self.logic {
            DiskUsageState::Unknown | DiskUsageState::Warning => {
                flags.get_and_make_logic_disk_full();
            }
            DiskUsageState::Healthy | DiskUsageState::Reclaim => {
                flags.get_and_make_logic_disk_ok();
            }
            DiskUsageState::Forcible => {}
        }
        let states = self.cleanup.iter().copied().chain([self.physical_state(), self.logic]);
        let mut decision = DiskCleanDecision::default();
        for state in states {
            decision.should_delete |= !matches!(state, DiskUsageState::Healthy);
            decision.clean_immediately |= matches!(state, DiskUsageState::Warning | DiskUsageState::Forcible);
        }
        decision
    }
}

// Legacy runtime-info projection only. Admission uses explicit samples above.
pub(super) fn store_path_disk_used_ratio(path: &str) -> f64 {
    DiskSpaceSample::sample(Path::new(path.trim())).ratio().unwrap_or(-1.0)
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
        if is_time_up || disk_decision.should_delete || is_manual_delete {
            let outcome = self.commit_log.delete_expired_files_by_time_before(
                expired_time,
                self.message_store_config.delete_commit_log_files_interval as i32,
                self.message_store_config.destroy_mapped_file_interval_forcibly as i64,
                clean_at_once,
                self.message_store_config.delete_file_batch_max as i32,
                minimum_pinned_wal_segment,
            );
            use crate::consume_queue::mapped_file_queue::CleanupOutcome;
            match outcome {
                CleanupOutcome::ManagedSubmitted { selected, submitted } => {
                    if selected > 0 {
                        info!(
                            selected,
                            submitted, "commitlog retirement tickets submitted; completion belongs to the reaper"
                        );
                    }
                }
                CleanupOutcome::LegacyCompleted { namespace_removed } => {
                    if namespace_removed > 0 {
                        info!(namespace_removed, "expired commitlog namespace entries removed; physical free space requires a new disk sample");
                    } else if disk_decision.should_delete {
                        warn!("disk capacity requires reclaim; no commitlog namespace entries removed");
                    }
                }
            }
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

        let sample = |path: &Path| DiskSpaceSample::sample(path).state(self.cleanup_policy);
        let active = self.commit_log.active_root().as_deref().map(sample);
        let allocation = self
            .commit_log
            .allocation_candidates()
            .iter()
            .map(|path| sample(path))
            .collect();
        let commit_log_path = LocalFileMessageStore::get_store_path_physic(&self.message_store_config);
        let cleanup = commit_log_path
            .split(mix_all::MULTI_PATH_SPLITTER.as_str())
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(|path| sample(Path::new(path)))
            .collect();
        let logic_path = LocalFileMessageStore::get_store_path_logic(&self.message_store_config);
        DiskAvailabilitySummary {
            active,
            allocation,
            cleanup,
            logic: sample(Path::new(logic_path.as_str())),
        }
        .apply(&self.running_flags)
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

#[cfg(test)]
mod disk_tests {
    use super::*;

    fn summary(
        active: Option<DiskUsageState>,
        allocation: Vec<DiskUsageState>,
        logic: DiskUsageState,
    ) -> DiskAvailabilitySummary {
        DiskAvailabilitySummary {
            active,
            allocation,
            cleanup: Vec::new(),
            logic,
        }
    }

    #[test]
    fn warning_unknown_and_recovery_preserve_independent_write_failures() {
        let flags = RunningFlags::new();
        summary(Some(DiskUsageState::Warning), vec![], DiskUsageState::Healthy).apply(&flags);
        assert!(!flags.is_writeable());
        summary(
            Some(DiskUsageState::Unknown),
            vec![DiskUsageState::Healthy],
            DiskUsageState::Healthy,
        )
        .apply(&flags);
        assert!(!flags.is_writeable(), "a spare cannot clear the active segment fence");
        summary(Some(DiskUsageState::Healthy), vec![], DiskUsageState::Unknown).apply(&flags);
        assert!(!flags.is_writeable(), "CQ capacity is independent");
        flags.get_and_make_not_writeable();
        summary(Some(DiskUsageState::Healthy), vec![], DiskUsageState::Healthy).apply(&flags);
        assert!(
            !flags.is_writeable(),
            "capacity recovery does not clear a flush failure"
        );
        flags.get_and_make_writeable();
        assert!(flags.is_writeable());
    }

    #[test]
    fn allocation_and_cleanup_do_not_share_a_minimum_ratio() {
        let flags = RunningFlags::new();
        let mut disks = summary(None, vec![DiskUsageState::Healthy], DiskUsageState::Healthy);
        disks.cleanup = vec![DiskUsageState::Warning, DiskUsageState::Healthy];
        let decision = disks.apply(&flags);
        assert!(decision.clean_immediately);
        assert!(
            flags.is_writeable(),
            "a full inactive root does not block a verified new-segment candidate"
        );
        disks.allocation.clear();
        disks.cleanup = vec![DiskUsageState::Healthy];
        disks.apply(&flags);
        assert!(
            !flags.is_writeable(),
            "healthy readonly storage cannot authorize allocation"
        );
    }
}
