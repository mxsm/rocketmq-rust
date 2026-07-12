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

//! Runtime-neutral storage capability contracts.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use bytes::Bytes;
use rocketmq_model::message::MessageQueue;

/// Result returned by storage capabilities.
pub type StoreResult<T> = Result<T, StoreError>;

/// Runtime-neutral boxed future used by asynchronous storage capabilities.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = StoreResult<T>> + Send + 'a>>;

/// Stable, low-cardinality storage failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreErrorKind {
    NotStarted,
    Unavailable,
    InvalidRequest,
    NotFound,
    Capacity,
    Storage,
    Io,
    Corruption,
    Timeout,
    Unsupported,
    Internal,
}

impl StoreErrorKind {
    /// Returns the stable machine-readable name for this classification.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Unavailable => "unavailable",
            Self::InvalidRequest => "invalid_request",
            Self::NotFound => "not_found",
            Self::Capacity => "capacity",
            Self::Storage => "storage",
            Self::Io => "io",
            Self::Corruption => "corruption",
            Self::Timeout => "timeout",
            Self::Unsupported => "unsupported",
            Self::Internal => "internal",
        }
    }
}

/// Backend-neutral storage error.
///
/// Native failure objects and sensitive location details stay in implementation adapters. This
/// error carries only a stable classification and operation name across the capability boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreError {
    kind: StoreErrorKind,
    operation: &'static str,
}

impl StoreError {
    /// Creates a backend-neutral error for a stable operation name.
    pub const fn new(kind: StoreErrorKind, operation: &'static str) -> Self {
        Self { kind, operation }
    }

    /// Returns the stable failure classification.
    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    /// Returns the stable operation name without backend details.
    pub const fn operation(&self) -> &'static str {
        self.operation
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "store operation {} failed: {}",
            self.operation,
            self.kind.as_str()
        )
    }
}

impl Error for StoreError {}

/// Durability reached when an append result was produced.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Durability {
    #[default]
    Memory,
    Local,
    Replicated,
}

/// Backend-neutral outcome of a message append attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AppendStatus {
    #[default]
    PutOk,
    FlushDiskTimeout,
    FlushReplicaTimeout,
    ReplicaUnavailable,
    ServiceUnavailable,
    StorageUnavailable,
    InvalidMessage,
    PageCacheBusy,
    ReplicasUnavailable,
    RemoteAppendFailed,
    QueueLimit,
    TimerFlowControl,
    TimerMessageIllegal,
    TimerNotEnabled,
    Unknown,
}

impl AppendStatus {
    /// Whether the append was accepted even if the requested durability was not reached in time.
    pub const fn is_accepted(self) -> bool {
        matches!(
            self,
            Self::PutOk | Self::FlushDiskTimeout | Self::FlushReplicaTimeout | Self::ReplicaUnavailable
        )
    }
}

/// Receipt for one accepted or rejected append operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendReceipt {
    pub status: AppendStatus,
    /// Half-open physical byte range written by this append.
    pub offsets: Range<i64>,
    /// Exclusive watermark of bytes appended to the primary log.
    pub appended_watermark: i64,
    /// Exclusive watermark known to satisfy the reported durability.
    pub durable_watermark: i64,
    pub durability: Durability,
    pub wrote_bytes: i32,
    pub message_id: Option<String>,
    pub store_timestamp: i64,
    pub logical_offset: i64,
    pub message_count: i32,
}

impl AppendReceipt {
    /// Creates a receipt while keeping append and durable progress independent.
    pub fn new(
        status: AppendStatus,
        offsets: Range<i64>,
        appended_watermark: i64,
        durable_watermark: i64,
        durability: Durability,
    ) -> Self {
        let wrote_bytes = i32::try_from(offsets.end.saturating_sub(offsets.start)).unwrap_or(i32::MAX);
        Self {
            status,
            offsets,
            appended_watermark,
            durable_watermark,
            durability,
            wrote_bytes,
            message_id: None,
            store_timestamp: 0,
            logical_offset: 0,
            message_count: 1,
        }
    }

    /// Whether the append was accepted by the primary log.
    pub const fn is_accepted(&self) -> bool {
        self.status.is_accepted()
    }

    /// Whether this append's exclusive end offset is covered by the durable watermark.
    pub fn is_durable(&self) -> bool {
        self.is_accepted() && self.durable_watermark >= self.offsets.end
    }
}

/// Compact sync-flush pressure projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushBacklog {
    pub queue_depth: u64,
    pub oldest_wait_millis: u64,
}

/// Backend-neutral health projection used by hot-path admission decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreHealthSnapshot {
    pub writable: bool,
    pub last_error: Option<StoreErrorKind>,
    pub page_cache_busy: bool,
    pub transient_pool_deficient: bool,
    pub flush_backlog: FlushBacklog,
    pub dispatch_behind_bytes: i64,
    pub shutdown: bool,
    pub replication_pending_count: u64,
    pub replication_oldest_wait_millis: u64,
    pub appended_watermark: i64,
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

/// Request for a bounded logical queue read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRequest {
    pub queue: MessageQueue,
    pub offset: i64,
    pub max_messages: u32,
    pub max_bytes: u32,
}

/// One backend-neutral stored message value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredMessage {
    pub payload: Bytes,
    pub physical_offset: i64,
    pub logical_offset: i64,
    pub store_timestamp: i64,
}

/// Result of a bounded logical queue read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadResult {
    pub messages: Vec<StoredMessage>,
    pub next_offset: i64,
}

/// Available half-open logical offset range for a queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OffsetRange {
    pub min: i64,
    pub max: i64,
}

/// Backend-neutral replication progress.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationState {
    pub confirmed_offset: i64,
    pub replicated_offset: i64,
    pub replica_count: u32,
}

/// One derived record tied to a primary-log source offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedRecord {
    pub source_offset: i64,
    pub payload: Bytes,
}

/// Progress of derived data independent from primary append acknowledgement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DerivedProgress {
    pub source_watermark: i64,
    pub derived_watermark: i64,
}

/// Stable administrative operation category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdminOperation {
    Flush,
    Compact,
    DeleteExpired,
    ResetOffset,
}

/// Backend-neutral administrative request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminRequest {
    pub operation: AdminOperation,
}

/// Backend-neutral administrative result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminResponse {
    pub operation: AdminOperation,
    pub affected: u64,
}

/// Storage lifecycle capability.
pub trait StoreLifecycle: Send + Sync {
    /// Loads existing store state.
    ///
    /// # Errors
    ///
    /// Returns a typed error when persisted state cannot be loaded safely.
    fn load(&mut self) -> StoreFuture<'_, bool>;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns a typed error when startup cannot establish a usable store.
    fn start(&mut self) -> StoreFuture<'_, ()>;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns a typed error when final durable progress or shutdown fails.
    fn shutdown(&mut self) -> StoreFuture<'_, ()>;
}

/// Message append capability generic over a consumer-owned input value.
pub trait MessageAppender<M>: Send {
    /// Appends one input and reports independent append and durable progress.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the capability cannot produce an append outcome.
    fn append_message(&mut self, message: M) -> StoreFuture<'_, AppendReceipt>;
}

/// Message read capability.
pub trait MessageReader: Send + Sync {
    /// Reads a bounded logical queue window.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the requested range cannot be read safely.
    fn read(&self, request: ReadRequest) -> StoreFuture<'_, ReadResult>;
}

/// Logical queue offset-index capability.
pub trait OffsetIndex: Send + Sync {
    /// Returns the currently readable logical offset range.
    ///
    /// # Errors
    ///
    /// Returns a typed error when index state is unavailable or inconsistent.
    fn offset_range(&self, queue: &MessageQueue) -> StoreResult<OffsetRange>;
}

/// Health snapshot capability.
pub trait StoreHealth: Send + Sync {
    /// Returns a low-cardinality snapshot without backend implementation details.
    fn health_snapshot(&self) -> StoreHealthSnapshot;
}

/// Replication-control capability.
pub trait ReplicationControl: Send + Sync {
    /// Returns current replication progress.
    fn replication_state(&self) -> ReplicationState;

    /// Advances or restores the confirmed primary-log offset.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the offset violates implementation invariants.
    fn set_confirmed_offset(&mut self, offset: i64) -> StoreResult<()>;
}

/// Derived-record append capability.
pub trait DerivedRecordSink: Send {
    /// Appends derived data without changing primary append acknowledgement semantics.
    ///
    /// # Errors
    ///
    /// Returns a typed error when derived progress cannot be persisted.
    fn append_derived(&mut self, record: DerivedRecord) -> StoreFuture<'_, DerivedProgress>;
}

/// Administrative storage capability.
pub trait AdminStore: Send {
    /// Executes one bounded administrative operation.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the operation is rejected or fails.
    fn execute_admin(&mut self, request: AdminRequest) -> StoreFuture<'_, AdminResponse>;
}
