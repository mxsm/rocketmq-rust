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

#![deny(missing_docs)]

//! Storage capabilities and their contract-violation model.

mod capability;
pub mod checkpoint;
mod checkpoint_artifact;
mod contract;
mod error;
mod ha_contract;
mod progress;
mod timer;
mod timer_snapshot;
mod wal;

pub use capability::AdminStore;
pub use capability::DerivedRecordSink;
pub use capability::MessageAppender;
pub use capability::MessageReader;
pub use capability::OffsetIndex;
pub use capability::ReleaseCheckpointStore;
pub use capability::ReplicationControl;
pub use capability::StoreHealth;
pub use capability::StoreLifecycle;
pub use checkpoint::CheckpointArtifact;
pub use checkpoint::CheckpointBackend;
pub use checkpoint::CheckpointManifest;
pub use checkpoint::CheckpointOffsets;
pub use checkpoint::CheckpointRequest;
pub use checkpoint::CheckpointRestoreVerification;
pub use checkpoint::CheckpointStorageIdentity;
pub use checkpoint::CHECKPOINT_SCHEMA_VERSION;
pub use checkpoint_artifact::file_uri_to_path;
pub use checkpoint_artifact::hash_checkpoint_directory;
pub use checkpoint_artifact::path_to_file_uri;
pub use checkpoint_artifact::CheckpointDirectoryDigest;
pub use checkpoint_artifact::RELEASE_CHECKPOINT_MANIFEST_FILE;
pub use contract::StoreContractViolation;
pub use error::StoreComponent;
pub use error::StoreError;
pub use error::StoreErrorKind;
pub use error::StoreOperation;
pub use ha_contract::decide_replication;
pub use ha_contract::AckPolicy;
pub use ha_contract::HaRejectReason;
pub use ha_contract::MasterEpoch;
pub use ha_contract::ReplicaAck;
pub use ha_contract::ReplicaCount;
pub use ha_contract::ReplicationAcknowledgement;
pub use ha_contract::ReplicationDecision;
pub use ha_contract::ReplicationObservation;
pub use ha_contract::SyncStateSet;
pub use ha_contract::SyncStateSetEpoch;
pub use ha_contract::WriteAuthority;
pub use ha_contract::WriteLeaseToken;
pub use progress::CursorAdvance;
pub use progress::CursorAdvanceDisposition;
pub use progress::DerivedCheckpoint;
pub use progress::DerivedCursor;
pub use progress::DerivedEngine;
pub use progress::DerivedRecordId;
pub use progress::LegacyDerivedCursorV0;
pub use progress::DERIVED_CHECKPOINT_ENCODED_LEN;
pub use progress::DERIVED_CHECKPOINT_FORMAT_VERSION;
pub use timer::PersistedTimerRoute;
pub use timer::TimerEngineEpoch;
pub use timer::TimerEngineId;
pub use timer::TimerGeneration;
pub use timer::TimerId;
pub use timer::TimerPayloadLocator;
pub use timer::TimerPayloadStoreLocator;
pub use timer::TimerRecallRequest;
pub use timer::TimerRecallStatus;
pub use timer::TimerSourceCqOffset;
pub use timer::TimerStoreMode;
pub use timer::TimerTimelineCursor;
pub use timer::EXTENDED_TIMELINE_FORMAT_VERSION;
pub use timer::JAVA_COMPAT_TIMER_FORMAT_VERSION;
pub use timer_snapshot::TimerSnapshotFile;
pub use timer_snapshot::TimerSnapshotManifest;
pub use timer_snapshot::TimerTimelineIndexKind;
pub use timer_snapshot::TIMER_SNAPSHOT_SCHEMA_VERSION;
pub use wal::WalPort;

use std::fmt;
use std::ops::Range;

use bytes::Bytes;

/// Durability reached by a primary-log append.
///
/// Derived-record progress never upgrades this value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Durability {
    /// The primary log accepted the bytes, without a durable-write guarantee.
    #[default]
    Memory,
    /// The durable watermark covers the complete appended range locally.
    Local,
    /// The configured replica acknowledgement condition was also satisfied.
    Replicated,
}

/// Backend-neutral outcome of a legacy-compatible append attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AppendStatus {
    #[default]
    /// Represents the put ok case.
    PutOk,
    /// Represents the flush disk timeout case.
    FlushDiskTimeout,
    /// Represents the flush replica timeout case.
    FlushReplicaTimeout,
    /// Represents the replica unavailable case.
    ReplicaUnavailable,
    /// Represents the service unavailable case.
    ServiceUnavailable,
    /// Represents the storage unavailable case.
    StorageUnavailable,
    /// Represents the invalid message case.
    InvalidMessage,
    /// Represents the properties too large case.
    PropertiesTooLarge,
    /// Represents the page cache busy case.
    PageCacheBusy,
    /// Represents the unknown case.
    Unknown,
    /// Represents the insufficient replicas case.
    InsufficientReplicas,
    /// Represents the remote append failed case.
    RemoteAppendFailed,
    /// Represents the queue limit exceeded case.
    QueueLimitExceeded,
    /// Represents the schedule flow control case.
    ScheduleFlowControl,
    /// Represents the schedule message illegal case.
    ScheduleMessageIllegal,
    /// Represents the schedule disabled case.
    ScheduleDisabled,
}

impl AppendStatus {
    /// Returns whether the primary log accepted the append.
    pub const fn is_accepted(self) -> bool {
        matches!(
            self,
            Self::PutOk | Self::FlushDiskTimeout | Self::FlushReplicaTimeout | Self::ReplicaUnavailable
        )
    }
}

/// Canonical receipt for one primary-log append attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendReceipt {
    status: AppendStatus,
    appended_range: Option<Range<i64>>,
    appended_watermark: i64,
    durable_watermark: i64,
    durability: Durability,
}

impl AppendReceipt {
    /// Creates a receipt after validating range, status, watermark, and durability invariants.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation`] when any receipt field contradicts another field.
    pub fn try_new(
        status: AppendStatus,
        appended_range: Range<i64>,
        appended_watermark: i64,
        durable_watermark: i64,
        durability: Durability,
    ) -> Result<Self, StoreContractViolation> {
        if appended_range.start == appended_range.end {
            return Err(StoreContractViolation::AppendReceiptEmptyRange);
        }
        if appended_range.start > appended_range.end {
            return Err(StoreContractViolation::AppendReceiptReversedRange);
        }
        if !status.is_accepted() {
            return Err(StoreContractViolation::AppendReceiptRejectedStatusWithRange);
        }
        if appended_watermark < appended_range.end {
            return Err(StoreContractViolation::AppendReceiptAppendedWatermarkBehindRange);
        }
        if durable_watermark > appended_watermark {
            return Err(StoreContractViolation::AppendReceiptDurableWatermarkAheadOfAppended);
        }
        if durability == Durability::Replicated {
            return Err(StoreContractViolation::AppendReceiptReplicatedDurabilityRequiresDecision);
        }
        match durability {
            Durability::Memory if durable_watermark >= appended_range.end => {
                return Err(StoreContractViolation::AppendReceiptMemoryDurabilityAlreadyCovered);
            }
            Durability::Local | Durability::Replicated if durable_watermark < appended_range.end => {
                return Err(StoreContractViolation::AppendReceiptDurableWatermarkBehindRange);
            }
            Durability::Memory | Durability::Local | Durability::Replicated => {}
        }
        Ok(Self {
            status,
            appended_range: Some(appended_range),
            appended_watermark,
            durable_watermark,
            durability,
        })
    }

    /// Creates a receipt from a proof produced by [`decide_replication`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation`] when receipt fields contradict each other or the supplied
    /// acknowledgement does not cover the complete appended range.
    pub fn try_new_with_replication(
        status: AppendStatus,
        appended_range: Range<i64>,
        appended_watermark: i64,
        durable_watermark: i64,
        acknowledgement: ReplicationAcknowledgement,
    ) -> Result<Self, StoreContractViolation> {
        if acknowledgement.acknowledged_offset() < appended_range.end {
            return Err(StoreContractViolation::AppendReceiptReplicationDecisionBehindRange);
        }
        let mut receipt = Self::try_new(
            status,
            appended_range,
            appended_watermark,
            durable_watermark,
            Durability::Local,
        )?;
        receipt.durability = acknowledgement.durability();
        Ok(receipt)
    }

    /// Creates a rejected receipt after validating status and watermark invariants.
    ///
    /// # Errors
    ///
    /// Returns [`StoreContractViolation`] for an accepted status or reversed progress watermarks.
    pub const fn try_rejected(
        status: AppendStatus,
        appended_watermark: i64,
        durable_watermark: i64,
    ) -> Result<Self, StoreContractViolation> {
        if status.is_accepted() {
            return Err(StoreContractViolation::AppendReceiptAcceptedStatusWithoutRange);
        }
        if durable_watermark > appended_watermark {
            return Err(StoreContractViolation::AppendReceiptDurableWatermarkAheadOfAppended);
        }
        Ok(Self {
            status,
            appended_range: None,
            appended_watermark,
            durable_watermark,
            durability: Durability::Memory,
        })
    }

    /// Returns the neutral append outcome.
    pub const fn status(&self) -> AppendStatus {
        self.status
    }

    /// Returns the half-open physical range written by this operation.
    pub fn appended_range(&self) -> Option<Range<i64>> {
        self.appended_range.clone()
    }

    /// Returns the first physical byte appended by this operation.
    pub fn first_appended_offset(&self) -> Option<i64> {
        self.appended_range.as_ref().map(|range| range.start)
    }

    /// Returns the last physical byte appended by this operation.
    pub fn last_appended_offset(&self) -> Option<i64> {
        self.appended_range.as_ref().map(|range| range.end - 1)
    }

    /// Returns the exclusive primary-log append watermark observed after the operation.
    pub const fn appended_watermark(&self) -> i64 {
        self.appended_watermark
    }

    /// Returns the exclusive durable watermark observed after the operation.
    pub const fn durable_watermark(&self) -> i64 {
        self.durable_watermark
    }

    /// Returns the explicitly reached durability level.
    pub const fn durability(&self) -> Durability {
        self.durability
    }

    /// Returns whether the primary log accepted a non-empty appended range.
    pub fn is_accepted(&self) -> bool {
        self.status.is_accepted() && self.appended_range.is_some()
    }

    /// Returns whether the complete appended range reached the reported durable watermark.
    pub fn is_durable(&self) -> bool {
        self.is_accepted()
            && self.durability != Durability::Memory
            && self
                .appended_range
                .as_ref()
                .is_some_and(|range| self.durable_watermark >= range.end)
    }
}

/// Progress of derived records, independent from primary-log acknowledgement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DerivedProgress {
    source_watermark: i64,
    derived_watermark: i64,
}

impl DerivedProgress {
    /// Creates an independent derived-progress observation.
    pub const fn new(source_watermark: i64, derived_watermark: i64) -> Self {
        Self {
            source_watermark,
            derived_watermark,
        }
    }

    /// Returns the exclusive primary-log source watermark observed by derivation.
    pub const fn source_watermark(self) -> i64 {
        self.source_watermark
    }

    /// Returns the exclusive watermark completed by the derived store.
    pub const fn derived_watermark(self) -> i64 {
        self.derived_watermark
    }

    /// Derived progress is never a primary append acknowledgement.
    pub const fn acknowledges_primary_append(self) -> bool {
        false
    }

    /// Derived progress is never a primary durability condition.
    pub const fn satisfies_primary_durability(self) -> bool {
        false
    }
}

/// Bytes whose backend lease remains held for the lifetime of this value.
pub struct LeasedBytes<L> {
    // Bytes drop before the lease, so a backend view is gone before its guard is released.
    bytes: Bytes,
    lease: L,
}

impl<L> LeasedBytes<L> {
    /// Couples bytes with the guard that keeps their source alive.
    pub const fn new(bytes: Bytes, lease: L) -> Self {
        Self { bytes, lease }
    }

    /// Returns the readable bytes while retaining the lease.
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Consumes the lease and returns independently owned bytes.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl<L: fmt::Debug> fmt::Debug for LeasedBytes<L> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeasedBytes")
            .field("bytes", &self.bytes)
            .field("lease", &self.lease)
            .finish()
    }
}

/// Neutral cache-residency observation for a selected byte range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadCacheState {
    #[default]
    /// Represents the unknown case.
    Unknown,
    /// Represents the hot case.
    Hot,
    /// Represents the cold case.
    Cold,
}

/// One selected physical byte range coupled to its backend lease.
#[derive(Debug)]
pub struct SelectResult<L> {
    start_offset: u64,
    data: LeasedBytes<L>,
    cache_state: ReadCacheState,
}

impl<L> SelectResult<L> {
    /// Creates a selected range from neutral bytes and location metadata.
    pub const fn new(start_offset: u64, data: LeasedBytes<L>, cache_state: ReadCacheState) -> Self {
        Self {
            start_offset,
            data,
            cache_state,
        }
    }

    /// Returns the physical start offset.
    pub const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Returns the selected byte length.
    pub fn size(&self) -> usize {
        self.data.bytes().len()
    }

    /// Returns the leased data.
    pub const fn data(&self) -> &LeasedBytes<L> {
        &self.data
    }

    /// Consumes the selected result and returns its leased data.
    pub fn into_data(self) -> LeasedBytes<L> {
        self.data
    }

    /// Returns the neutral cache observation.
    pub const fn cache_state(&self) -> ReadCacheState {
        self.cache_state
    }
}

/// Canonical result status for a bounded logical get.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GetStatus {
    #[default]
    /// Represents the found case.
    Found,
    /// Represents the no matched message case.
    NoMatchedMessage,
    /// Represents the message was removing case.
    MessageWasRemoving,
    /// Represents the offset found null case.
    OffsetFoundNull,
    /// Represents the offset overflow badly case.
    OffsetOverflowBadly,
    /// Represents the offset overflow one case.
    OffsetOverflowOne,
    /// Represents the offset too small case.
    OffsetTooSmall,
    /// Represents the no matched logic queue case.
    NoMatchedLogicQueue,
    /// Represents the no message in queue case.
    NoMessageInQueue,
    /// Represents the offset reset case.
    OffsetReset,
}

/// Canonical neutral projection of a legacy logical get result.
#[derive(Debug)]
pub struct GetResult<L> {
    /// The records value.
    pub records: Vec<SelectResult<L>>,
    /// The queue offsets value.
    pub queue_offsets: Vec<u64>,
    /// The status value.
    pub status: Option<GetStatus>,
    /// The next begin offset value.
    pub next_begin_offset: i64,
    /// The min offset value.
    pub min_offset: i64,
    /// The max offset value.
    pub max_offset: i64,
    /// The buffer total size value.
    pub buffer_total_size: i32,
    /// The number of message entries.
    pub message_count: i32,
    /// Whether suggest pulling from replica.
    pub suggest_pulling_from_replica: bool,
    /// The number of commercial message entries.
    pub commercial_message_count: i32,
    /// The commercial size per message value.
    pub commercial_size_per_message: i32,
    /// The cold data sum value.
    pub cold_data_sum: i64,
}

impl<L> Default for GetResult<L> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            queue_offsets: Vec::new(),
            status: None,
            next_begin_offset: 0,
            min_offset: 0,
            max_offset: 0,
            buffer_total_size: 0,
            message_count: 0,
            suggest_pulling_from_replica: false,
            commercial_message_count: 0,
            commercial_size_per_message: 4 * 1024,
            cold_data_sum: 0,
        }
    }
}

/// Owned logical-read outcome after backend leases have been decoded by an adapter.
///
/// This projection keeps storage navigation semantics in the storage boundary while allowing the
/// caller to own decoded records. `None` records remain distinct from a successful read containing
/// an empty record collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOutcome<T> {
    status: GetStatus,
    next_begin_offset: i64,
    min_offset: i64,
    max_offset: i64,
    records: Option<Vec<T>>,
}

impl<T> ReadOutcome<T> {
    /// Creates an owned read outcome from canonical store navigation fields.
    pub fn new<R>(status: GetStatus, next_begin_offset: i64, min_offset: i64, max_offset: i64, records: R) -> Self
    where
        R: Into<Option<Vec<T>>>,
    {
        Self {
            status,
            next_begin_offset,
            min_offset,
            max_offset,
            records: records.into(),
        }
    }

    /// Returns the exact backend-neutral logical-read status.
    pub const fn status(&self) -> GetStatus {
        self.status
    }

    /// Returns the next logical offset suggested by the store.
    pub const fn next_begin_offset(&self) -> i64 {
        self.next_begin_offset
    }

    /// Returns the minimum readable logical offset.
    pub const fn min_offset(&self) -> i64 {
        self.min_offset
    }

    /// Returns the maximum logical offset observed by the store.
    pub const fn max_offset(&self) -> i64 {
        self.max_offset
    }

    /// Returns decoded records without transferring ownership.
    pub fn records(&self) -> Option<&[T]> {
        self.records.as_deref()
    }

    /// Consumes the outcome and returns its decoded records.
    pub fn into_records(self) -> Option<Vec<T>> {
        self.records
    }
}

/// Canonical neutral projection of a legacy key query result.
#[derive(Debug)]
pub struct QueryResult<L> {
    /// The records value.
    pub records: Vec<SelectResult<L>>,
    /// The index last update timestamp value.
    pub index_last_update_timestamp: i64,
    /// The index last update physical offset value.
    pub index_last_update_physical_offset: i64,
    /// The buffer total size value.
    pub buffer_total_size: i32,
    /// Whether index query safe.
    pub index_query_safe: bool,
    /// The index safe physical offset value.
    pub index_safe_physical_offset: i64,
    /// The index confirm physical offset value.
    pub index_confirm_physical_offset: i64,
}

impl<L> Default for QueryResult<L> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            index_last_update_timestamp: 0,
            index_last_update_physical_offset: 0,
            buffer_total_size: 0,
            index_query_safe: true,
            index_safe_physical_offset: 0,
            index_confirm_physical_offset: 0,
        }
    }
}

/// Compact durable-write pressure projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushBacklog {
    /// The queue depth value.
    pub queue_depth: u64,
    /// The oldest wait duration in milliseconds.
    pub oldest_wait_millis: u64,
}

/// Canonical backend-neutral health projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreHealthSnapshot {
    /// Whether writable.
    pub writable: bool,
    /// The last error value.
    pub last_error: Option<StoreErrorKind>,
    /// Whether page cache busy.
    pub page_cache_busy: bool,
    /// Whether transient pool deficient.
    pub transient_pool_deficient: bool,
    /// The flush backlog value.
    pub flush_backlog: FlushBacklog,
    /// The dispatch behind size in bytes.
    pub dispatch_behind_bytes: i64,
    /// Whether shutdown.
    pub shutdown: bool,
    /// The number of replication pending entries.
    pub replication_pending_count: u64,
    /// The replication oldest wait duration in milliseconds.
    pub replication_oldest_wait_millis: u64,
    /// The appended watermark value.
    pub appended_watermark: i64,
    /// The durable watermark value.
    pub durable_watermark: i64,
}

impl Default for StoreHealthSnapshot {
    fn default() -> Self {
        Self {
            writable: true,
            last_error: None,
            page_cache_busy: false,
            transient_pool_deficient: false,
            flush_backlog: FlushBacklog::default(),
            dispatch_behind_bytes: 0,
            shutdown: false,
            replication_pending_count: 0,
            replication_oldest_wait_millis: 0,
            appended_watermark: 0,
            durable_watermark: 0,
        }
    }
}

impl StoreHealthSnapshot {
    /// Returns whether the store currently accepts primary writes.
    pub const fn writable(&self) -> bool {
        self.writable
    }

    /// Returns the neutral classification of the latest health failure.
    pub const fn last_error(&self) -> Option<StoreErrorKind> {
        self.last_error
    }

    /// Returns the exclusive primary-log append watermark.
    pub const fn appended_watermark(&self) -> i64 {
        self.appended_watermark
    }

    /// Returns the exclusive durable watermark.
    pub const fn durable_watermark(&self) -> i64 {
        self.durable_watermark
    }
}
