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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_store_api::StoreError;

use crate::column_family::RocksDbColumnFamily;
use crate::message::IndexRocksDbRecord;
use crate::message::MessageRocksDbStorage;

pub trait RocksDbIndexDispatch {
    fn topic(&self) -> &str;

    fn commit_log_offset(&self) -> i64;

    fn message_size(&self) -> i32;

    fn store_timestamp(&self) -> i64;

    fn is_transaction_rollback(&self) -> bool;

    fn keys(&self) -> &str;

    fn uniq_key(&self) -> Option<&str>;

    fn tags(&self) -> Option<&str>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbIndexBuildConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
}

impl Default for RocksDbIndexBuildConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100_000,
            batch_size: 1000,
        }
    }
}

impl RocksDbIndexBuildConfig {
    fn validate(self) -> Result<Self, StoreError> {
        if self.queue_capacity == 0 {
            return Err(crate::error::request_invalid(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        if self.batch_size == 0 {
            return Err(crate::error::request_invalid(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        Ok(self)
    }
}

pub struct RocksDbIndexBuildService {
    storage: Arc<MessageRocksDbStorage>,
    config: RocksDbIndexBuildConfig,
    pending: Mutex<VecDeque<PendingIndexRecord>>,
    pending_safe_offset: AtomicI64,
    accepted_safe_offset: AtomicI64,
    safe_progress_failed: AtomicBool,
    safe_progress_lock: Mutex<()>,
    #[cfg(test)]
    before_safe_persist_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    fail_next_safe_invalidation: AtomicBool,
}

struct PendingIndexRecord {
    record: IndexRocksDbRecord,
    safe_offset: i64,
}

impl RocksDbIndexBuildService {
    pub fn new(storage: Arc<MessageRocksDbStorage>, config: RocksDbIndexBuildConfig) -> Result<Self, StoreError> {
        let config = config.validate()?;
        let accepted_safe_offset = storage.get_last_safe_offset_py(
            rocketmq_store_api::StoreOperation::QueryOffset,
            RocksDbColumnFamily::Default.name(),
        )?;
        Ok(Self {
            storage,
            config,
            pending: Mutex::new(VecDeque::with_capacity(config.queue_capacity.min(1024))),
            pending_safe_offset: AtomicI64::new(0),
            accepted_safe_offset: AtomicI64::new(accepted_safe_offset),
            safe_progress_failed: AtomicBool::new(accepted_safe_offset < 0),
            safe_progress_lock: Mutex::new(()),
            #[cfg(test)]
            before_safe_persist_hook: Mutex::new(None),
            #[cfg(test)]
            fail_next_safe_invalidation: AtomicBool::new(false),
        })
    }

    pub fn build_index<R>(&self, dispatch_request: &R) -> Result<usize, StoreError>
    where
        R: RocksDbIndexDispatch + ?Sized,
    {
        // Lock order is safe-progress, then pending. Every durable frontier writer follows it.
        let _safe_progress = self
            .safe_progress_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        if self.safe_progress_failed.load(Ordering::Acquire) {
            return Err(crate::error::unavailable(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        let safe_offset = (dispatch_request.commit_log_offset() >= 0
            && dispatch_request.message_size() > 0
            && !dispatch_request.topic().is_empty()
            && dispatch_request.store_timestamp() > 0)
            .then(|| {
                dispatch_request
                    .commit_log_offset()
                    .saturating_add(i64::from(dispatch_request.message_size()))
            });
        let accepted_safe_offset = self.accepted_safe_offset.load(Ordering::Acquire);
        if let Some(safe_offset) = safe_offset {
            if safe_offset <= accepted_safe_offset {
                return Ok(0);
            }
            if dispatch_request.commit_log_offset() != accepted_safe_offset {
                self.mark_safe_progress_failed_locked()?;
                return Err(crate::error::state_corrupted(
                    rocketmq_store_api::StoreOperation::AppendDerived,
                ));
            }
        }
        let records = match self.records_for_dispatch(dispatch_request) {
            Ok(records) => records,
            Err(error) => {
                self.mark_safe_progress_failed_locked()?;
                return Err(error);
            }
        };
        if records.is_empty() {
            if let Some(safe_offset) = safe_offset {
                self.pending_safe_offset.fetch_max(safe_offset, Ordering::AcqRel);
                self.accepted_safe_offset.store(safe_offset, Ordering::Release);
            }
            return Ok(0);
        }

        let mut pending = self
            .pending
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        let available = self.config.queue_capacity.saturating_sub(pending.len());
        if records.len() > available {
            drop(pending);
            self.mark_safe_progress_failed_locked()?;
            return Err(crate::error::capacity_exhausted(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }

        let count = records.len();
        let safe_offset = safe_offset.unwrap_or_default();
        let last_record = records.len() - 1;
        pending.extend(
            records
                .into_iter()
                .enumerate()
                .map(|(index, record)| PendingIndexRecord {
                    record,
                    safe_offset: if index == last_record { safe_offset } else { 0 },
                }),
        );
        self.accepted_safe_offset.store(safe_offset, Ordering::Release);
        Ok(count)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.lock().map_or(0, |pending| pending.len())
    }

    pub fn flush_pending(&self) -> Result<usize, StoreError> {
        let _safe_progress = self
            .safe_progress_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        self.flush_pending_locked()
    }

    fn flush_pending_locked(&self) -> Result<usize, StoreError> {
        if self.safe_progress_failed.load(Ordering::Acquire) {
            // INVALID must reach durable storage before any pending index record. Once
            // persisted, the storage helpers intentionally refuse to replace it with a
            // positive frontier, so pending records may still be drained for diagnostics
            // and a future explicit rebuild without making queries appear complete.
            self.persist_safe_progress_failure_locked()?;
        }
        let mut flushed = 0;
        loop {
            let batch = self.drain_batch()?;
            if batch.is_empty() {
                let safe_offset = self.pending_safe_offset.swap(0, Ordering::AcqRel);
                if let Err(error) = self.storage.write_index_safe_offset(safe_offset) {
                    self.pending_safe_offset.fetch_max(safe_offset, Ordering::AcqRel);
                    return Err(error);
                }
                return Ok(flushed);
            }

            let batch_len = batch.len();
            let safe_offset = batch
                .iter()
                .map(|pending| pending.safe_offset)
                .max()
                .unwrap_or_default();
            let records: Vec<_> = batch.iter().map(|pending| pending.record.clone()).collect();
            #[cfg(test)]
            if let Some(hook) = self
                .before_safe_persist_hook
                .lock()
                .expect("safe-persist test hook lock")
                .as_ref()
            {
                hook();
            }
            if let Err(error) = self
                .storage
                .write_records_for_index_with_safe_offset(&records, safe_offset)
            {
                if let Err(requeue_error) = self.requeue_front(batch) {
                    self.mark_safe_progress_failed_locked()?;
                    return Err(requeue_error);
                }
                return Err(error);
            }
            flushed += batch_len;
        }
    }

    pub async fn flush_pending_blocking(
        self: Arc<Self>,
        runtime_scope: &crate::runtime::RocksDbRuntimeScope,
    ) -> Result<usize, StoreError> {
        crate::runtime::spawn_io(
            runtime_scope,
            "rocksdb.index.flush_pending",
            rocketmq_store_api::StoreOperation::AppendDerived,
            move || self.flush_pending(),
        )
        .await?
    }

    pub fn get_dispatch_from_phy_offset(&self) -> Result<Option<i64>, StoreError> {
        let last_offset = self.storage.get_last_offset_py(
            rocketmq_store_api::StoreOperation::QueryOffset,
            RocksDbColumnFamily::Default.name(),
        )?;
        Ok((last_offset > 0).then_some(last_offset))
    }

    pub fn get_safe_dispatch_offset(&self) -> Result<i64, StoreError> {
        let _safe_progress = self
            .safe_progress_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::QueryOffset))?;
        if self.safe_progress_failed.load(Ordering::Acquire) {
            return Ok(-1);
        }
        self.storage.get_last_safe_offset_py(
            rocketmq_store_api::StoreOperation::QueryOffset,
            RocksDbColumnFamily::Default.name(),
        )
    }

    /// Seeds the contiguous frontier at the first retained CommitLog byte during recovery.
    ///
    /// Offsets below this boundary have already expired and therefore do not represent an
    /// index gap. This does not publish query safety; the frontier becomes durable only as
    /// recovered requests are written successfully.
    pub fn initialize_dispatch_frontier(&self, commit_log_min_offset: i64) -> Result<i64, StoreError> {
        let _safe_progress = self
            .safe_progress_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::QueryOffset))?;
        if self.safe_progress_failed.load(Ordering::Acquire) {
            return Err(crate::error::unavailable(
                rocketmq_store_api::StoreOperation::QueryOffset,
            ));
        }
        self.accepted_safe_offset
            .fetch_max(commit_log_min_offset.max(0), Ordering::AcqRel);
        Ok(self.accepted_safe_offset.load(Ordering::Acquire))
    }

    /// Persists a CommitLog BLANK transition after the scanner has verified the padding record.
    pub fn advance_safe_frontier_over_blank(
        &self,
        blank_start_offset: i64,
        next_file_offset: i64,
    ) -> Result<(), StoreError> {
        let _safe_progress = self
            .safe_progress_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        if self.safe_progress_failed.load(Ordering::Acquire)
            || blank_start_offset < 0
            || next_file_offset <= blank_start_offset
        {
            return Err(crate::error::state_corrupted(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        if let Err(error) = self.flush_pending_locked() {
            self.mark_safe_progress_failed_locked()?;
            return Err(error);
        }
        if self.accepted_safe_offset.load(Ordering::Acquire) != blank_start_offset {
            self.mark_safe_progress_failed_locked()?;
            return Err(crate::error::state_corrupted(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        if let Err(error) = self.storage.write_index_safe_offset(next_file_offset) {
            self.mark_safe_progress_failed_locked()?;
            return Err(error);
        }
        self.accepted_safe_offset.store(next_file_offset, Ordering::Release);
        Ok(())
    }

    fn mark_safe_progress_failed_locked(&self) -> Result<(), StoreError> {
        self.safe_progress_failed.store(true, Ordering::Release);
        self.accepted_safe_offset.store(-1, Ordering::Release);
        self.persist_safe_progress_failure_locked()
    }

    fn persist_safe_progress_failure_locked(&self) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_next_safe_invalidation.swap(false, Ordering::AcqRel) {
            return Err(crate::error::unavailable(
                rocketmq_store_api::StoreOperation::AppendDerived,
            ));
        }
        self.storage.invalidate_index_safe_offset()
    }

    fn records_for_dispatch<R>(&self, dispatch_request: &R) -> Result<Vec<IndexRocksDbRecord>, StoreError>
    where
        R: RocksDbIndexDispatch + ?Sized,
    {
        if dispatch_request.commit_log_offset() < 0
            || dispatch_request.message_size() <= 0
            || dispatch_request.topic().is_empty()
            || dispatch_request.store_timestamp() <= 0
        {
            return Ok(Vec::new());
        }

        if dispatch_request.is_transaction_rollback() {
            return Ok(Vec::new());
        }

        let uniq_key = dispatch_request.uniq_key().filter(|key| !key.is_empty());
        let compatibility_uniq_key = uniq_key.unwrap_or_default();

        let topic = dispatch_request.topic();
        let mut records = Vec::new();
        let mut seen_keys = HashSet::new();
        for key in dispatch_request.keys().split(' ') {
            if key.is_empty() || !seen_keys.insert(key) {
                continue;
            }
            records.push(IndexRocksDbRecord::normal_key(
                topic,
                key,
                compatibility_uniq_key,
                dispatch_request.store_timestamp(),
                dispatch_request.commit_log_offset(),
            ));
        }

        if let Some(tag) = dispatch_request.tags().filter(|tag| !tag.is_empty()) {
            records.push(IndexRocksDbRecord::tag_key(
                topic,
                tag,
                compatibility_uniq_key,
                dispatch_request.store_timestamp(),
                dispatch_request.commit_log_offset(),
            ));
        }

        if let Some(uniq_key) = uniq_key {
            records.push(IndexRocksDbRecord::unique_key(
                topic,
                uniq_key,
                dispatch_request.store_timestamp(),
                dispatch_request.commit_log_offset(),
            ));
        }
        Ok(records)
    }

    fn drain_batch(&self) -> Result<Vec<PendingIndexRecord>, StoreError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        let batch_len = self.config.batch_size.min(pending.len());
        Ok(pending.drain(..batch_len).collect())
    }

    fn requeue_front(&self, mut batch: Vec<PendingIndexRecord>) -> Result<(), StoreError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        while let Some(record) = batch.pop() {
            pending.push_front(record);
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_before_safe_persist_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .before_safe_persist_hook
            .lock()
            .expect("safe-persist test hook lock") = Some(hook);
    }

    #[cfg(test)]
    fn fail_next_safe_invalidation(&self) {
        self.fail_next_safe_invalidation.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;

    use tempfile::tempdir;

    use super::*;
    use crate::config::RocksDbConfig;

    struct TestDispatch {
        offset: i64,
        size: i32,
        keys: &'static str,
    }

    impl RocksDbIndexDispatch for TestDispatch {
        fn topic(&self) -> &str {
            "safe-progress-topic"
        }

        fn commit_log_offset(&self) -> i64 {
            self.offset
        }

        fn message_size(&self) -> i32 {
            self.size
        }

        fn store_timestamp(&self) -> i64 {
            1_000_000_000_000 + self.offset
        }

        fn is_transaction_rollback(&self) -> bool {
            false
        }

        fn keys(&self) -> &str {
            self.keys
        }

        fn uniq_key(&self) -> Option<&str> {
            None
        }

        fn tags(&self) -> Option<&str> {
            None
        }
    }

    fn open_storage(path: &Path) -> Arc<MessageRocksDbStorage> {
        Arc::new(
            MessageRocksDbStorage::open(RocksDbConfig {
                enabled: true,
                path: path.to_path_buf(),
                ..RocksDbConfig::default()
            })
            .expect("open message RocksDB")
            .expect("enabled RocksDB storage"),
        )
    }

    #[test]
    fn safe_progress_serialization_prevents_a_stale_flush_from_overwriting_invalid() {
        let temp = tempdir().expect("safe-progress temp dir");
        let database_path = temp.path().join("message-index");
        let storage = open_storage(&database_path);
        let service = Arc::new(
            RocksDbIndexBuildService::new(Arc::clone(&storage), RocksDbIndexBuildConfig::default())
                .expect("create index service"),
        );
        service
            .build_index(&TestDispatch {
                offset: 0,
                size: 100,
                keys: "first-key",
            })
            .expect("enqueue first index record");

        let flush_entered = Arc::new(Barrier::new(2));
        let release_flush = Arc::new(Barrier::new(2));
        let entered_for_hook = Arc::clone(&flush_entered);
        let release_for_hook = Arc::clone(&release_flush);
        service.set_before_safe_persist_hook(Arc::new(move || {
            entered_for_hook.wait();
            release_for_hook.wait();
        }));
        let service_for_flush = Arc::clone(&service);
        let flush = thread::spawn(move || service_for_flush.flush_pending());
        flush_entered.wait();

        let (gap_started_tx, gap_started_rx) = mpsc::channel();
        let (gap_done_tx, gap_done_rx) = mpsc::channel();
        let service_for_gap = Arc::clone(&service);
        let gap = thread::spawn(move || {
            gap_started_tx.send(()).expect("signal gap attempt");
            let result = service_for_gap.build_index(&TestDispatch {
                offset: 200,
                size: 100,
                keys: "after-gap",
            });
            gap_done_tx.send(result.is_err()).expect("report gap result");
        });
        gap_started_rx.recv().expect("gap attempt started");
        assert!(
            service.safe_progress_lock.try_lock().is_err(),
            "flush must own the safe-progress serialization lock before persisting"
        );
        assert!(
            gap_done_rx.try_recv().is_err(),
            "gap failure must wait behind the flush"
        );

        release_flush.wait();
        assert_eq!(flush.join().expect("join flush").expect("flush first record"), 1);
        assert!(gap_done_rx.recv().expect("gap result"));
        gap.join().expect("join gap attempt");
        assert_eq!(service.get_safe_dispatch_offset().expect("read invalid frontier"), -1);

        drop(service);
        storage.store().close();
        drop(storage);
        let reopened_storage = open_storage(&database_path);
        let reopened = RocksDbIndexBuildService::new(reopened_storage, RocksDbIndexBuildConfig::default())
            .expect("reopen index service");
        assert_eq!(reopened.get_safe_dispatch_offset().expect("reopened frontier"), -1);
    }

    #[test]
    fn concurrent_flushes_cannot_regress_the_durable_safe_frontier() {
        let temp = tempdir().expect("concurrent-flush temp dir");
        let database_path = temp.path().join("message-index");
        let storage = open_storage(&database_path);
        let service = Arc::new(
            RocksDbIndexBuildService::new(Arc::clone(&storage), RocksDbIndexBuildConfig::default())
                .expect("create index service"),
        );
        for (offset, key) in [(0, "first-key"), (100, "second-key")] {
            service
                .build_index(&TestDispatch {
                    offset,
                    size: 100,
                    keys: key,
                })
                .expect("enqueue contiguous index record");
        }

        let start = Arc::new(Barrier::new(3));
        let mut flushes = Vec::new();
        for _ in 0..2 {
            let service = Arc::clone(&service);
            let start = Arc::clone(&start);
            flushes.push(thread::spawn(move || {
                start.wait();
                service.flush_pending()
            }));
        }
        start.wait();
        for flush in flushes {
            flush.join().expect("join concurrent flush").expect("concurrent flush");
        }
        assert_eq!(service.get_safe_dispatch_offset().expect("durable frontier"), 200);

        drop(service);
        storage.store().close();
        drop(storage);
        let reopened_storage = open_storage(&database_path);
        let reopened = RocksDbIndexBuildService::new(reopened_storage, RocksDbIndexBuildConfig::default())
            .expect("reopen index service");
        assert_eq!(reopened.get_safe_dispatch_offset().expect("reopened frontier"), 200);
    }

    #[test]
    fn failed_invalid_write_is_retried_before_pending_records_can_flush() {
        let temp = tempdir().expect("failed-invalidation temp dir");
        let database_path = temp.path().join("message-index");
        let storage = open_storage(&database_path);
        let service = RocksDbIndexBuildService::new(Arc::clone(&storage), RocksDbIndexBuildConfig::default())
            .expect("create index service");
        service
            .build_index(&TestDispatch {
                offset: 0,
                size: 100,
                keys: "pending-before-gap",
            })
            .expect("enqueue record before the gap");

        service.fail_next_safe_invalidation();
        assert!(service
            .build_index(&TestDispatch {
                offset: 200,
                size: 100,
                keys: "after-gap",
            })
            .is_err());
        assert_eq!(
            service.pending_len(),
            1,
            "failed INVALID write must retain pending records"
        );
        assert_eq!(
            storage
                .get_last_safe_offset_py(
                    rocketmq_store_api::StoreOperation::QueryOffset,
                    RocksDbColumnFamily::Default.name(),
                )
                .expect("frontier before retry"),
            0,
            "the injected failure must leave the durable frontier unchanged"
        );

        assert_eq!(
            service.flush_pending().expect("retry INVALID and flush pending record"),
            1
        );
        assert_eq!(service.pending_len(), 0);
        assert_eq!(service.get_safe_dispatch_offset().expect("latched frontier"), -1);
        assert_eq!(service.flush_pending().expect("repeat fail-closed flush"), 0);

        drop(service);
        storage.store().close();
        drop(storage);
        let reopened_storage = open_storage(&database_path);
        let reopened = RocksDbIndexBuildService::new(reopened_storage, RocksDbIndexBuildConfig::default())
            .expect("reopen index service");
        assert_eq!(reopened.get_safe_dispatch_offset().expect("reopened frontier"), -1);
    }
}
