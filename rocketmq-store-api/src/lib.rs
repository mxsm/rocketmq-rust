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

//! Storage capability contracts.

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::ops::Range;

use bytes::Bytes;

/// Closed vocabulary for operations that may cross the capability boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOperation {
    Load,
    Start,
    Shutdown,
    Append,
    Read,
    QueryOffset,
    Replicate,
    AppendDerived,
    Admin,
}

impl StoreOperation {
    /// Returns the stable machine-readable operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Start => "start",
            Self::Shutdown => "shutdown",
            Self::Append => "append",
            Self::Read => "read",
            Self::QueryOffset => "query_offset",
            Self::Replicate => "replicate",
            Self::AppendDerived => "append_derived",
            Self::Admin => "admin",
        }
    }
}

/// Stable, low-cardinality storage failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// Returns the stable machine-readable classification name.
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

/// Storage error containing only closed, non-sensitive vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreError {
    kind: StoreErrorKind,
    operation: StoreOperation,
}

impl StoreError {
    /// Creates an error from closed operation and kind vocabularies.
    pub const fn new(kind: StoreErrorKind, operation: StoreOperation) -> Self {
        Self { kind, operation }
    }

    /// Returns the stable failure classification.
    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    /// Returns the operation that failed.
    pub const fn operation(&self) -> StoreOperation {
        self.operation
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "store operation {} failed: {}",
            self.operation.as_str(),
            self.kind.as_str()
        )
    }
}

impl StdError for StoreError {}

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
    PutOk,
    FlushDiskTimeout,
    FlushReplicaTimeout,
    ReplicaUnavailable,
    ServiceUnavailable,
    StorageUnavailable,
    InvalidMessage,
    PropertiesTooLarge,
    PageCacheBusy,
    Unknown,
    InsufficientReplicas,
    RemoteAppendFailed,
    QueueLimitExceeded,
    ScheduleFlowControl,
    ScheduleMessageIllegal,
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
    /// Creates a receipt for a non-empty half-open physical byte range.
    pub fn new(
        status: AppendStatus,
        appended_range: Range<i64>,
        appended_watermark: i64,
        durable_watermark: i64,
        durability: Durability,
    ) -> Self {
        let appended_range = (appended_range.start < appended_range.end).then_some(appended_range);
        Self {
            status,
            appended_range,
            appended_watermark,
            durable_watermark,
            durability,
        }
    }

    /// Creates a rejected receipt without inventing an appended range.
    pub const fn rejected(status: AppendStatus, appended_watermark: i64, durable_watermark: i64) -> Self {
        Self {
            status,
            appended_range: None,
            appended_watermark,
            durable_watermark,
            durability: Durability::Memory,
        }
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
        self.bytes.clone()
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
    Unknown,
    Hot,
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

    /// Returns the neutral cache observation.
    pub const fn cache_state(&self) -> ReadCacheState {
        self.cache_state
    }
}

/// Canonical result status for a bounded logical get.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GetStatus {
    #[default]
    Found,
    NoMatchedMessage,
    MessageWasRemoving,
    OffsetFoundNull,
    OffsetOverflowBadly,
    OffsetOverflowOne,
    OffsetTooSmall,
    NoMatchedLogicQueue,
    NoMessageInQueue,
    OffsetReset,
}

/// Canonical neutral projection of a legacy logical get result.
#[derive(Debug)]
pub struct GetResult<L> {
    pub records: Vec<SelectResult<L>>,
    pub queue_offsets: Vec<u64>,
    pub status: Option<GetStatus>,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub buffer_total_size: i32,
    pub message_count: i32,
    pub suggest_pulling_from_replica: bool,
    pub commercial_message_count: i32,
    pub commercial_size_per_message: i32,
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

/// Canonical neutral projection of a legacy key query result.
#[derive(Debug)]
pub struct QueryResult<L> {
    pub records: Vec<SelectResult<L>>,
    pub index_last_update_timestamp: i64,
    pub index_last_update_physical_offset: i64,
    pub buffer_total_size: i32,
    pub index_query_safe: bool,
    pub index_safe_physical_offset: i64,
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
    pub queue_depth: u64,
    pub oldest_wait_millis: u64,
}

/// Canonical backend-neutral health projection.
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

/// Storage lifecycle capability.
pub trait StoreLifecycle: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    /// Loads existing state.
    ///
    /// # Errors
    ///
    /// Returns a typed error when state cannot be loaded safely.
    fn load(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns a typed error when startup cannot establish a usable store.
    fn start(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns a typed error when final progress or shutdown fails.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Message append capability generic over a consumer-owned input.
pub trait MessageAppender<M: Send>: Send {
    type Receipt: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Appends one input and returns the implementation-owned receipt projection.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the append cannot produce an outcome.
    fn append_message(&mut self, message: M) -> impl Future<Output = Result<Self::Receipt, Self::Error>> + Send;
}

/// Message read capability with implementation-owned request and output values.
pub trait MessageReader: Send + Sync {
    type Request: Send;
    type Output: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Reads a bounded message window.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the requested data cannot be read safely.
    fn read(&self, request: Self::Request) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Logical offset lookup capability.
pub trait OffsetIndex: Send + Sync {
    type Query: Send + Sync;
    type Output;
    type Error: StdError + Send + Sync + 'static;

    /// Queries the current logical offset projection.
    ///
    /// # Errors
    ///
    /// Returns a typed error when index state is unavailable or inconsistent.
    fn query_offset(&self, query: &Self::Query) -> Result<Self::Output, Self::Error>;
}

/// Health snapshot capability with an implementation-owned projection.
pub trait StoreHealth: Send + Sync {
    type Snapshot;

    /// Returns the current health projection.
    fn health_snapshot(&self) -> Self::Snapshot;
}

/// Replication control capability with implementation-owned command and state values.
pub trait ReplicationControl: Send + Sync {
    type Command: Send;
    type State: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Returns current replication state.
    fn replication_state(&self) -> Self::State;

    /// Applies one replication command.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the command violates store invariants.
    fn apply_replication(
        &mut self,
        command: Self::Command,
    ) -> impl Future<Output = Result<Self::State, Self::Error>> + Send;
}

/// Derived-record append capability with implementation-owned values.
pub trait DerivedRecordSink: Send {
    type Record: Send;
    type Progress: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Appends derived data independently from primary append acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns a typed error when derived progress cannot be persisted.
    fn append_derived(
        &mut self,
        record: Self::Record,
    ) -> impl Future<Output = Result<Self::Progress, Self::Error>> + Send;
}

/// Administrative storage capability with implementation-owned values.
pub trait AdminStore: Send {
    type Request: Send;
    type Response: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Executes one bounded administrative operation.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the operation is rejected or fails.
    fn execute_admin(
        &mut self,
        request: Self::Request,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;
}
