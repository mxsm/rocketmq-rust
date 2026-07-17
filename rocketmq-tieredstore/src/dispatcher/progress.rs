// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_store_api::CursorAdvanceDisposition;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedRecordId;
use tokio::sync::Mutex;

use super::progress_persistence::PersistedTieredProgress;
use super::progress_persistence::TieredProgressPersistence;
use crate::config::TieredStoreConfig;
use crate::dispatcher::TieredDispatchRequest;

/// Readiness state of the Tiered CommitLog-derived delivery path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TieredDispatchReadiness {
    Healthy,
    RetryLedgerFull,
    RetryLedgerBytesExceeded,
    RetryLedgerExpired,
    ProgressPersistenceFailed,
    RetryPayloadUnavailable,
    CursorInvariantViolated,
    DispatcherFailed,
    Shutdown,
}

/// Bounded progress and WAL-pin snapshot exposed to readiness and alert owners.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredDispatchHealth {
    readiness: TieredDispatchReadiness,
    cursor: Option<DerivedCursor>,
    retry_count: usize,
    retry_source_bytes: u64,
    oldest_retry_age: Duration,
    minimum_pinned_offset: Option<u64>,
    minimum_pinned_wal_segment: Option<u64>,
}

impl TieredDispatchHealth {
    pub(crate) const fn initial() -> Self {
        Self {
            readiness: TieredDispatchReadiness::Healthy,
            cursor: None,
            retry_count: 0,
            retry_source_bytes: 0,
            oldest_retry_age: Duration::ZERO,
            minimum_pinned_offset: None,
            minimum_pinned_wal_segment: None,
        }
    }

    /// Returns whether intake may continue without violating a hard retry bound.
    pub const fn is_ready(&self) -> bool {
        matches!(self.readiness, TieredDispatchReadiness::Healthy)
    }

    /// Returns the stable readiness/alert reason.
    pub const fn readiness(&self) -> TieredDispatchReadiness {
        self.readiness
    }

    /// Returns the last atomically persisted global Tiered cursor.
    pub const fn cursor(&self) -> Option<DerivedCursor> {
        self.cursor
    }

    /// Returns the number of payload-free retry records.
    pub const fn retry_count(&self) -> usize {
        self.retry_count
    }

    /// Returns the source CommitLog bytes represented by retry records.
    pub const fn retry_source_bytes(&self) -> u64 {
        self.retry_source_bytes
    }

    /// Returns the age of the oldest retry record.
    pub const fn oldest_retry_age(&self) -> Duration {
        self.oldest_retry_age
    }

    /// Returns the minimum physical offset that must remain available for retry.
    pub const fn minimum_pinned_offset(&self) -> Option<u64> {
        self.minimum_pinned_offset
    }

    /// Returns the base offset of the minimum source WAL segment that must remain pinned.
    pub const fn minimum_pinned_wal_segment(&self) -> Option<u64> {
        self.minimum_pinned_wal_segment
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub(crate) struct TieredRetryEntry {
    source_epoch: u64,
    physical_offset: u64,
    length: u32,
    topic: String,
    queue_id: i32,
    queue_offset: i64,
    message_size: i32,
    tags_code: i64,
    store_timestamp: i64,
    keys: Option<String>,
    uniq_key: Option<String>,
    offset_id: Option<String>,
    sys_flag: i32,
    attempts: u32,
    first_failed_at_millis: u64,
    next_attempt_at_millis: u64,
}

impl TieredRetryEntry {
    fn new(
        record: DerivedRecordId,
        request: &TieredDispatchRequest,
        now_millis: u64,
        initial_backoff: Duration,
    ) -> Self {
        Self {
            source_epoch: record.source_epoch(),
            physical_offset: record.physical_offset(),
            length: record.length(),
            topic: request.topic.clone(),
            queue_id: request.queue_id,
            queue_offset: request.queue_offset,
            message_size: request.message_size,
            tags_code: request.tags_code,
            store_timestamp: request.store_timestamp,
            keys: request.keys.clone(),
            uniq_key: request.uniq_key.clone(),
            offset_id: request.offset_id.clone(),
            sys_flag: request.sys_flag,
            attempts: 1,
            first_failed_at_millis: now_millis,
            next_attempt_at_millis: now_millis.saturating_add(duration_millis(initial_backoff)),
        }
    }

    pub(crate) fn record(&self) -> Result<DerivedRecordId, RocketMQError> {
        DerivedRecordId::try_new(self.source_epoch, self.physical_offset, self.length)
            .map_err(|error| crate::error::storage_corrupted(format!("tiered retry record: {error}")))
    }

    pub(crate) fn request_with_body(&self, body: Bytes) -> TieredDispatchRequest {
        TieredDispatchRequest {
            topic: self.topic.clone(),
            queue_id: self.queue_id,
            queue_offset: self.queue_offset,
            commit_log_offset: i64::try_from(self.physical_offset).unwrap_or(i64::MAX),
            message_size: self.message_size,
            tags_code: self.tags_code,
            store_timestamp: self.store_timestamp,
            keys: self.keys.clone(),
            uniq_key: self.uniq_key.clone(),
            offset_id: self.offset_id.clone(),
            sys_flag: self.sys_flag,
            body: Some(body),
        }
    }

    pub(crate) const fn next_attempt_at_millis(&self) -> u64 {
        self.next_attempt_at_millis
    }

    fn bump(&mut self, now_millis: u64, initial: Duration, maximum: Duration) {
        self.attempts = self.attempts.saturating_add(1);
        let exponent = self.attempts.saturating_sub(1).min(31);
        let delay = duration_millis(initial)
            .saturating_mul(1_u64 << exponent)
            .min(duration_millis(maximum));
        self.next_attempt_at_millis = now_millis.saturating_add(delay);
    }
}

#[derive(Clone, Default)]
struct TieredProgressState {
    cursor: Option<DerivedCursor>,
    retries: BTreeMap<DerivedRecordId, TieredRetryEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordDisposition {
    Deliver,
    RetryPending,
    AlreadyCommitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureRecordOutcome {
    Recorded,
    AlreadyCommitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryCapacityError {
    Entries,
    Bytes,
    Age,
}

pub(crate) struct TieredProgressTracker {
    source_epoch: u64,
    retry_max_entries: usize,
    retry_max_bytes: u64,
    retry_max_age: Duration,
    retry_backoff_initial: Duration,
    retry_backoff_max: Duration,
    source_wal_segment_size: u64,
    persistence: TieredProgressPersistence,
    state: Mutex<TieredProgressState>,
    health: Arc<RwLock<TieredDispatchHealth>>,
    loaded: AtomicBool,
}

impl TieredProgressTracker {
    pub(crate) fn new(config: &TieredStoreConfig, health: Arc<RwLock<TieredDispatchHealth>>) -> Self {
        Self {
            source_epoch: config.source_epoch,
            retry_max_entries: config.retry_ledger_max_entries.max(1),
            retry_max_bytes: config.retry_ledger_max_bytes.max(1),
            retry_max_age: config.retry_ledger_max_age,
            retry_backoff_initial: config.retry_backoff_initial.max(Duration::from_millis(1)),
            retry_backoff_max: config.retry_backoff_max.max(config.retry_backoff_initial),
            source_wal_segment_size: config.source_wal_segment_size.max(1),
            persistence: TieredProgressPersistence::new(config.store_path_root_dir.clone()),
            state: Mutex::new(TieredProgressState::default()),
            health,
            loaded: AtomicBool::new(false),
        }
    }

    pub(crate) async fn load(&self, now_millis: u64) -> Result<(), RocketMQError> {
        if self
            .loaded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let result = self.load_inner(now_millis).await;
        if result.is_err() {
            self.loaded.store(false, Ordering::Release);
        }
        result
    }

    async fn load_inner(&self, now_millis: u64) -> Result<(), RocketMQError> {
        let Some(persisted) = self.persistence.load().await? else {
            self.refresh_health(&TieredProgressState::default(), now_millis, None);
            return Ok(());
        };
        let checkpoint = DerivedCheckpoint::decode(&persisted.checkpoint, DerivedEngine::Tiered)
            .map_err(|error| crate::error::storage_corrupted(format!("tiered progress checkpoint: {error}")))?;
        let cursor = checkpoint.cursor();
        if cursor.source_epoch() != self.source_epoch {
            return Err(crate::error::storage_corrupted(format!(
                "tiered progress source epoch mismatch: expected {}, got {}",
                self.source_epoch,
                cursor.source_epoch()
            )));
        }

        let mut retries = BTreeMap::new();
        for entry in persisted.retries {
            let record = entry.record()?;
            if record.source_epoch() != self.source_epoch || record.end_offset() > cursor.next_offset() {
                return Err(crate::error::storage_corrupted(
                    "tiered retry record is outside the persisted cursor prefix",
                ));
            }
            if retries.insert(record, entry).is_some() {
                return Err(crate::error::storage_corrupted(
                    "tiered retry ledger contains duplicate record identity",
                ));
            }
        }
        let state = TieredProgressState {
            cursor: Some(cursor),
            retries,
        };
        let readiness = self.health_readiness(&state, now_millis);
        self.refresh_health(&state, now_millis, readiness);
        *self.state.lock().await = state;
        Ok(())
    }

    pub(crate) async fn destroy(&self) -> Result<(), RocketMQError> {
        self.persistence.destroy().await?;
        let state = TieredProgressState::default();
        *self.state.lock().await = state.clone();
        self.refresh_health(&state, 0, None);
        self.loaded.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) const fn retry_poll_interval(&self) -> Duration {
        self.retry_backoff_initial
    }

    pub(crate) async fn classify(&self, record: DerivedRecordId) -> Result<RecordDisposition, RocketMQError> {
        self.validate_epoch(record)?;
        let state = self.state.lock().await;
        if state.retries.contains_key(&record) {
            return Ok(RecordDisposition::RetryPending);
        }
        let Some(cursor) = state.cursor else {
            return Ok(RecordDisposition::Deliver);
        };
        match cursor
            .prepare(record)
            .map_err(|error| RocketMQError::illegal_argument(format!("tiered cursor invariant: {error}")))?
        {
            CursorAdvanceDisposition::AlreadyCommitted => Ok(RecordDisposition::AlreadyCommitted),
            CursorAdvanceDisposition::Advance(_) => Ok(RecordDisposition::Deliver),
        }
    }

    pub(crate) async fn record_success(&self, record: DerivedRecordId, now_millis: u64) -> Result<(), RocketMQError> {
        self.validate_epoch(record)?;
        let mut state_guard = self.state.lock().await;
        let mut candidate = state_guard.clone();
        let current = candidate
            .cursor
            .unwrap_or_else(|| DerivedCursor::restore(record.source_epoch(), record.physical_offset()));
        match current
            .prepare(record)
            .map_err(|error| RocketMQError::illegal_argument(format!("tiered cursor invariant: {error}")))?
        {
            CursorAdvanceDisposition::AlreadyCommitted => {
                if candidate.retries.remove(&record).is_none() {
                    return Ok(());
                }
            }
            CursorAdvanceDisposition::Advance(advance) => {
                candidate.cursor = Some(advance.next_cursor());
                candidate.retries.remove(&record);
            }
        }
        self.persist_candidate(&candidate).await?;
        *state_guard = candidate.clone();
        self.refresh_health(&candidate, now_millis, self.health_readiness(&candidate, now_millis));
        Ok(())
    }

    pub(crate) async fn record_failure(
        &self,
        record: DerivedRecordId,
        request: &TieredDispatchRequest,
        now_millis: u64,
    ) -> Result<Result<FailureRecordOutcome, RetryCapacityError>, RocketMQError> {
        self.validate_epoch(record)?;
        let mut state_guard = self.state.lock().await;
        let mut candidate = state_guard.clone();
        let current = candidate
            .cursor
            .unwrap_or_else(|| DerivedCursor::restore(record.source_epoch(), record.physical_offset()));
        let advance = match current
            .prepare(record)
            .map_err(|error| RocketMQError::illegal_argument(format!("tiered cursor invariant: {error}")))?
        {
            CursorAdvanceDisposition::AlreadyCommitted => {
                if !candidate.retries.contains_key(&record) {
                    return Ok(Ok(FailureRecordOutcome::AlreadyCommitted));
                }
                None
            }
            CursorAdvanceDisposition::Advance(advance) => Some(advance),
        };

        match candidate.retries.get_mut(&record) {
            Some(entry) => entry.bump(now_millis, self.retry_backoff_initial, self.retry_backoff_max),
            None => {
                candidate.retries.insert(
                    record,
                    TieredRetryEntry::new(record, request, now_millis, self.retry_backoff_initial),
                );
            }
        }
        if let Some(advance) = advance {
            candidate.cursor = Some(advance.next_cursor());
        }
        if let Some(readiness) = self.capacity_violation(&candidate, now_millis) {
            let capacity = match readiness {
                TieredDispatchReadiness::RetryLedgerFull => RetryCapacityError::Entries,
                TieredDispatchReadiness::RetryLedgerBytesExceeded => RetryCapacityError::Bytes,
                TieredDispatchReadiness::RetryLedgerExpired => RetryCapacityError::Age,
                _ => unreachable!("only hard retry limits are returned here"),
            };
            self.refresh_health(
                &state_guard,
                now_millis,
                self.health_readiness(&state_guard, now_millis),
            );
            return Ok(Err(capacity));
        }

        self.persist_candidate(&candidate).await?;
        *state_guard = candidate.clone();
        self.refresh_health(&candidate, now_millis, self.health_readiness(&candidate, now_millis));
        Ok(Ok(FailureRecordOutcome::Recorded))
    }

    pub(crate) async fn due_retries(&self, now_millis: u64) -> Vec<TieredRetryEntry> {
        let state = self.state.lock().await;
        let mut due = state
            .retries
            .values()
            .filter(|entry| entry.next_attempt_at_millis() <= now_millis)
            .cloned()
            .collect::<Vec<_>>();
        due.sort_by_key(|entry| (entry.next_attempt_at_millis, entry.topic.clone(), entry.queue_id));
        due
    }

    pub(crate) async fn record_retry_failure(
        &self,
        record: DerivedRecordId,
        now_millis: u64,
    ) -> Result<(), RocketMQError> {
        let mut state_guard = self.state.lock().await;
        let Some(_) = state_guard.retries.get(&record) else {
            return Ok(());
        };
        let mut candidate = state_guard.clone();
        if let Some(entry) = candidate.retries.get_mut(&record) {
            entry.bump(now_millis, self.retry_backoff_initial, self.retry_backoff_max);
        }
        self.persist_candidate(&candidate).await?;
        *state_guard = candidate.clone();
        self.refresh_health(&candidate, now_millis, self.health_readiness(&candidate, now_millis));
        Ok(())
    }

    pub(crate) async fn record_retry_success(
        &self,
        record: DerivedRecordId,
        now_millis: u64,
    ) -> Result<(), RocketMQError> {
        let mut state_guard = self.state.lock().await;
        if !state_guard.retries.contains_key(&record) {
            return Ok(());
        }
        let mut candidate = state_guard.clone();
        candidate.retries.remove(&record);
        self.persist_candidate(&candidate).await?;
        *state_guard = candidate.clone();
        self.refresh_health(&candidate, now_millis, self.health_readiness(&candidate, now_millis));
        Ok(())
    }

    pub(crate) fn mark_unready(&self, readiness: TieredDispatchReadiness) {
        self.health.write().readiness = readiness;
    }

    pub(crate) fn mark_unready_if_healthy(&self, readiness: TieredDispatchReadiness) {
        let mut health = self.health.write();
        if health.readiness == TieredDispatchReadiness::Healthy {
            health.readiness = readiness;
        }
    }

    pub(crate) fn hard_backpressure(&self) -> bool {
        matches!(
            self.health.read().readiness,
            TieredDispatchReadiness::RetryLedgerFull
                | TieredDispatchReadiness::RetryLedgerBytesExceeded
                | TieredDispatchReadiness::RetryLedgerExpired
        )
    }

    async fn persist_candidate(&self, candidate: &TieredProgressState) -> Result<(), RocketMQError> {
        let Some(cursor) = candidate.cursor else {
            return Ok(());
        };
        let persisted = PersistedTieredProgress {
            checkpoint: DerivedCheckpoint::new(DerivedEngine::Tiered, cursor).encode().to_vec(),
            retries: candidate.retries.values().cloned().collect(),
        };
        if let Err(error) = self.persistence.persist(&persisted).await {
            self.mark_unready(TieredDispatchReadiness::ProgressPersistenceFailed);
            return Err(error);
        }
        Ok(())
    }

    fn validate_epoch(&self, record: DerivedRecordId) -> Result<(), RocketMQError> {
        if record.source_epoch() == self.source_epoch {
            return Ok(());
        }
        self.mark_unready(TieredDispatchReadiness::CursorInvariantViolated);
        Err(RocketMQError::illegal_argument(format!(
            "tiered source epoch mismatch: expected {}, got {}",
            self.source_epoch,
            record.source_epoch()
        )))
    }

    fn capacity_violation(&self, state: &TieredProgressState, now_millis: u64) -> Option<TieredDispatchReadiness> {
        if state.retries.len() > self.retry_max_entries {
            return Some(TieredDispatchReadiness::RetryLedgerFull);
        }
        if retry_source_bytes(&state.retries) > self.retry_max_bytes {
            return Some(TieredDispatchReadiness::RetryLedgerBytesExceeded);
        }
        let oldest_age = oldest_retry_age(&state.retries, now_millis);
        if oldest_age > self.retry_max_age {
            return Some(TieredDispatchReadiness::RetryLedgerExpired);
        }
        None
    }

    fn health_readiness(&self, state: &TieredProgressState, now_millis: u64) -> Option<TieredDispatchReadiness> {
        if state.retries.len() >= self.retry_max_entries {
            return Some(TieredDispatchReadiness::RetryLedgerFull);
        }
        if retry_source_bytes(&state.retries) >= self.retry_max_bytes {
            return Some(TieredDispatchReadiness::RetryLedgerBytesExceeded);
        }
        let oldest_age = oldest_retry_age(&state.retries, now_millis);
        if oldest_age > self.retry_max_age {
            return Some(TieredDispatchReadiness::RetryLedgerExpired);
        }
        None
    }

    fn refresh_health(&self, state: &TieredProgressState, now_millis: u64, readiness: Option<TieredDispatchReadiness>) {
        let minimum_pinned_offset = state.retries.keys().map(|record| record.physical_offset()).min();
        let minimum_pinned_wal_segment =
            minimum_pinned_offset.map(|offset| offset / self.source_wal_segment_size * self.source_wal_segment_size);
        *self.health.write() = TieredDispatchHealth {
            readiness: readiness.unwrap_or(TieredDispatchReadiness::Healthy),
            cursor: state.cursor,
            retry_count: state.retries.len(),
            retry_source_bytes: retry_source_bytes(&state.retries),
            oldest_retry_age: oldest_retry_age(&state.retries, now_millis),
            minimum_pinned_offset,
            minimum_pinned_wal_segment,
        };
    }
}

fn retry_source_bytes(retries: &BTreeMap<DerivedRecordId, TieredRetryEntry>) -> u64 {
    retries
        .keys()
        .map(|record| u64::from(record.length()))
        .fold(0_u64, u64::saturating_add)
}

fn oldest_retry_age(retries: &BTreeMap<DerivedRecordId, TieredRetryEntry>, now_millis: u64) -> Duration {
    retries
        .values()
        .map(|entry| now_millis.saturating_sub(entry.first_failed_at_millis))
        .max()
        .map(Duration::from_millis)
        .unwrap_or(Duration::ZERO)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
