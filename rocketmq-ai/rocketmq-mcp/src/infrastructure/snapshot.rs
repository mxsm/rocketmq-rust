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

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::mem::size_of;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::infrastructure::cache::CacheMetricsSnapshot;
use crate::model::contract::observed_at;
use crate::model::contract::CacheStatus;
use crate::model::contract::Page;
use crate::model::contract::PageRequest;
use crate::model::contract::QueryPayload;
use crate::model::contract::SourceFailure;
use crate::model::contract::DEFAULT_PAGE_LIMIT;
use crate::model::contract::MAX_PAGE_LIMIT;
use crate::model::contract::SCHEMA_VERSION;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;

const CURSOR_PREFIX: &str = "rmq-s2-";
const MAX_SNAPSHOT_ENTRIES: usize = 10_000;
const MAX_SNAPSHOT_ROWS: usize = 10_000;
const MAX_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_SCOPE_BYTES: usize = MAX_TOTAL_BYTES / 4;
const MAX_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAX_CLUSTER_BYTES: usize = 255;
const MAX_FILTER_BYTES: usize = 1_024;
const MAX_VISIBILITY_BYTES: usize = 128;
const MAX_CURSOR_BYTES: usize = 256;
// These per-record envelopes deliberately exceed the smallest HashMap/BTreeMap
// allocations and VecDeque growth blocks. Containers are compacted after removals,
// so charging the full envelope to every live record remains conservative.
const RETAINED_ENTRY_BOOKKEEPING_OVERHEAD: usize = 2 * 1024;
const RETAINED_TOMBSTONE_OVERHEAD: usize = 1024;
const RETAINED_FLIGHT_BOOKKEEPING_OVERHEAD: usize = 2 * 1024;
const BTREE_MAP_ENTRY_ALLOCATION_OVERHEAD: usize = 1024;
const ARC_ALLOCATION_OVERHEAD: usize = 2 * size_of::<usize>();

pub(crate) trait RetainedSize {
    fn retained_heap_size(&self) -> usize;

    fn retained_size(&self) -> usize
    where
        Self: Sized,
    {
        size_of::<Self>().saturating_add(self.retained_heap_size())
    }
}

impl RetainedSize for u8 {
    fn retained_heap_size(&self) -> usize {
        0
    }
}

impl RetainedSize for String {
    fn retained_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T: RetainedSize> RetainedSize for Vec<T> {
    fn retained_heap_size(&self) -> usize {
        self.capacity().saturating_mul(size_of::<T>()).saturating_add(
            self.iter()
                .fold(0usize, |total, item| total.saturating_add(item.retained_heap_size())),
        )
    }
}

impl<T: RetainedSize> RetainedSize for Option<T> {
    fn retained_heap_size(&self) -> usize {
        self.as_ref().map_or(0, RetainedSize::retained_heap_size)
    }
}

impl RetainedSize for BTreeMap<String, String> {
    fn retained_heap_size(&self) -> usize {
        self.iter().fold(0usize, |total, (key, value)| {
            total
                .saturating_add(size_of::<(String, String)>())
                .saturating_add(BTREE_MAP_ENTRY_ALLOCATION_OVERHEAD)
                .saturating_add(key.retained_heap_size())
                .saturating_add(value.retained_heap_size())
        })
    }
}

impl RetainedSize for SourceFailure {
    fn retained_heap_size(&self) -> usize {
        self.logical_target.retained_heap_size()
    }
}

impl RetainedSize for TopicRouteBroker {
    fn retained_heap_size(&self) -> usize {
        self.cluster
            .retained_heap_size()
            .saturating_add(self.broker_name.retained_heap_size())
            .saturating_add(self.broker_addrs.retained_heap_size())
            .saturating_add(self.zone_name.retained_heap_size())
    }
}

impl RetainedSize for TopicRouteQueue {
    fn retained_heap_size(&self) -> usize {
        self.broker_name.retained_heap_size()
    }
}

impl RetainedSize for QueueLag {
    fn retained_heap_size(&self) -> usize {
        self.topic
            .retained_heap_size()
            .saturating_add(self.broker_name.retained_heap_size())
            .saturating_add(self.last_observed_at.retained_heap_size())
            .saturating_add(self.client_ip.retained_heap_size())
    }
}

impl RetainedSize for SessionTopicRoute {
    fn retained_heap_size(&self) -> usize {
        self.brokers
            .retained_heap_size()
            .saturating_add(self.queues.retained_heap_size())
    }
}

impl RetainedSize for SessionConsumerLag {
    fn retained_heap_size(&self) -> usize {
        self.queues.retained_heap_size()
    }
}

impl<T: RetainedSize> RetainedSize for QueryPayload<T> {
    fn retained_heap_size(&self) -> usize {
        self.data
            .retained_heap_size()
            .saturating_add(self.warnings.retained_heap_size())
            .saturating_add(self.source_failures.retained_heap_size())
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotLimits {
    max_entries: usize,
    max_rows: usize,
    max_snapshot_bytes: usize,
    max_total_bytes: usize,
    max_scope_bytes: usize,
    max_lifetime: Duration,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_entries: MAX_SNAPSHOT_ENTRIES,
            max_rows: MAX_SNAPSHOT_ROWS,
            max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
            max_total_bytes: MAX_TOTAL_BYTES,
            max_scope_bytes: MAX_SCOPE_BYTES,
            max_lifetime: MAX_LIFETIME,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotKind {
    TopicInventory,
    ConsumerGroupInventory,
    TopicRoute,
    ConsumerLag,
    ConsumerConnections,
    ProducerConnections,
}

impl SnapshotKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::TopicInventory => "topic_inventory",
            Self::ConsumerGroupInventory => "consumer_group_inventory",
            Self::TopicRoute => "topic_route",
            Self::ConsumerLag => "consumer_lag",
            Self::ConsumerConnections => "consumer_connections",
            Self::ProducerConnections => "producer_connections",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotSelectionMode {
    LiteralFilter,
    ExactIdentifier,
}

impl SnapshotSelectionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::LiteralFilter => "literal_filter",
            Self::ExactIdentifier => "exact_identifier",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SnapshotRequest {
    kind: SnapshotKind,
    cluster: String,
    normalized_filter: String,
    selection_mode: SnapshotSelectionMode,
    page_limit: u32,
    schema_version: &'static str,
    visibility: String,
}

impl SnapshotRequest {
    pub(crate) fn try_new(
        kind: SnapshotKind,
        cluster: impl Into<String>,
        normalized_filter: impl Into<String>,
        page: &PageRequest,
        visibility: impl Into<String>,
    ) -> Result<Self, SnapshotError> {
        Self::try_new_with_selection(
            kind,
            cluster,
            normalized_filter,
            SnapshotSelectionMode::LiteralFilter,
            page,
            visibility,
        )
    }

    pub(crate) fn try_new_with_selection(
        kind: SnapshotKind,
        cluster: impl Into<String>,
        normalized_filter: impl Into<String>,
        selection_mode: SnapshotSelectionMode,
        page: &PageRequest,
        visibility: impl Into<String>,
    ) -> Result<Self, SnapshotError> {
        let page_limit = page.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
        if !(1..=MAX_PAGE_LIMIT).contains(&page_limit) {
            return Err(SnapshotError::InvalidLimit);
        }
        let cluster = cluster.into();
        let normalized_filter = normalized_filter.into();
        let visibility = visibility.into();
        if cluster.trim().is_empty()
            || cluster.len() > MAX_CLUSTER_BYTES
            || normalized_filter.len() > MAX_FILTER_BYTES
            || visibility.trim().is_empty()
            || visibility.len() > MAX_VISIBILITY_BYTES
        {
            return Err(SnapshotError::ContextTooLarge);
        }
        Ok(Self {
            kind,
            cluster,
            normalized_filter,
            selection_mode,
            page_limit,
            schema_version: SCHEMA_VERSION,
            visibility,
        })
    }

    fn scope(&self) -> (&str, &str) {
        (&self.cluster, &self.visibility)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotWeight {
    entries: usize,
    rows: usize,
}

impl SnapshotWeight {
    pub(crate) const fn inventory(entries: usize) -> Self {
        Self { entries, rows: 0 }
    }

    pub(crate) const fn detail(rows: usize) -> Self {
        Self { entries: 1, rows }
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotError {
    #[error("limit must be between 1 and {MAX_PAGE_LIMIT}")]
    InvalidLimit,
    #[error("snapshot query context is blank or exceeds its bounded size")]
    ContextTooLarge,
    #[error("cursor is invalid or has been tampered with")]
    InvalidCursor,
    #[error("cursor snapshot has expired")]
    Expired,
    #[error("cursor snapshot was evicted")]
    Evicted,
    #[error("cursor snapshot was invalidated")]
    Invalidated,
    #[error("cursor does not match the requested query context")]
    ContextMismatch,
    #[error("cursor does not match the requested page contract")]
    PageContractMismatch,
    #[error("snapshot exceeds the bounded entry budget")]
    EntryBudgetExceeded,
    #[error("snapshot exceeds the bounded row budget")]
    RowBudgetExceeded,
    #[error("snapshot exceeds the bounded byte budget")]
    ByteBudgetExceeded,
}

impl From<SnapshotError> for ToolExecutionError {
    fn from(error: SnapshotError) -> Self {
        ToolExecutionError::InvalidArguments(error.to_string())
    }
}

#[derive(Debug, Default)]
struct SnapshotMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    coalesced_waiters: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
    capacity: usize,
    per_scope_capacity: usize,
    limits: SnapshotLimits,
    generation: AtomicU64,
    sequence: AtomicU64,
    flight_sequence: AtomicU64,
    primary_key: RandomState,
    secondary_key: RandomState,
    state: StdMutex<StoreState>,
    metrics: SnapshotMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotKey {
    request: SnapshotRequest,
    generation: u64,
}

#[derive(Default)]
struct StoreState {
    snapshots: SnapshotState,
    flights: FlightState,
}

#[derive(Default)]
struct SnapshotState {
    entries: HashMap<Arc<str>, SnapshotEntry>,
    insertion_order: VecDeque<Arc<str>>,
    tombstones: HashMap<Arc<str>, TombstoneReason>,
    tombstone_order: VecDeque<Arc<str>>,
    total_bytes: usize,
}

#[derive(Default)]
struct FlightState {
    joinable: HashMap<SnapshotKey, u64>,
    records: HashMap<u64, FlightRecord>,
    total_bytes: usize,
}

struct FlightRecord {
    key: SnapshotKey,
    cell: Arc<FlightCell>,
    bytes: usize,
    phase: FlightPhase,
    outcome: FlightOutcome,
}

enum FlightPhase {
    Loading { participants: usize },
    Completed { remaining: usize },
}

struct FlightCell {
    completed: Notify,
}

enum FlightOutcome {
    Loading,
    Success(Arc<str>),
    Failure(SharedFlightFailure),
}

struct FlightTicket {
    inner: Arc<SnapshotStoreInner>,
    id: u64,
    cell: Arc<FlightCell>,
    leader: bool,
}

impl std::fmt::Debug for FlightTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlightTicket")
            .field("id", &self.id)
            .field("leader", &self.leader)
            .finish_non_exhaustive()
    }
}

enum BeginFlight<T> {
    Cached(SnapshotView<T>),
    Leader(FlightTicket),
    Waiter(FlightTicket),
}

#[derive(Clone, Copy)]
enum SharedFlightFailure {
    InvalidArguments,
    Backend,
    PermissionDenied,
    UnauthorizedScope,
    TenantMismatch,
    ClusterNotAllowed,
    RateLimited,
    ChangePlanningDisabled,
    Internal,
    OutputTooLarge { actual_bytes: usize, max_bytes: usize },
    TimedOut { timeout_ms: u64 },
    Cancelled,
}

impl SharedFlightFailure {
    fn from_error(error: &ToolExecutionError) -> Self {
        match error {
            ToolExecutionError::InvalidArguments(_) => Self::InvalidArguments,
            ToolExecutionError::Backend(_) => Self::Backend,
            ToolExecutionError::PermissionDenied(_) => Self::PermissionDenied,
            ToolExecutionError::UnauthorizedScope(_) => Self::UnauthorizedScope,
            ToolExecutionError::TenantMismatch(_) => Self::TenantMismatch,
            ToolExecutionError::ClusterNotAllowed(_) => Self::ClusterNotAllowed,
            ToolExecutionError::RateLimited(_) => Self::RateLimited,
            ToolExecutionError::ChangePlanningDisabled(_) => Self::ChangePlanningDisabled,
            ToolExecutionError::Internal(_) => Self::Internal,
            ToolExecutionError::OutputTooLarge {
                actual_bytes,
                max_bytes,
            } => Self::OutputTooLarge {
                actual_bytes: *actual_bytes,
                max_bytes: *max_bytes,
            },
            ToolExecutionError::TimedOut { timeout_ms } => Self::TimedOut {
                timeout_ms: *timeout_ms,
            },
            ToolExecutionError::Cancelled => Self::Cancelled,
        }
    }

    fn into_error(self) -> ToolExecutionError {
        const MESSAGE: &str = "coalesced upstream load failed";
        match self {
            Self::InvalidArguments => ToolExecutionError::InvalidArguments(MESSAGE.to_string()),
            Self::Backend => ToolExecutionError::Backend(MESSAGE.to_string()),
            Self::PermissionDenied => ToolExecutionError::PermissionDenied(MESSAGE.to_string()),
            Self::UnauthorizedScope => ToolExecutionError::UnauthorizedScope(MESSAGE.to_string()),
            Self::TenantMismatch => ToolExecutionError::TenantMismatch(MESSAGE.to_string()),
            Self::ClusterNotAllowed => ToolExecutionError::ClusterNotAllowed(MESSAGE.to_string()),
            Self::RateLimited => ToolExecutionError::RateLimited(MESSAGE.to_string()),
            Self::ChangePlanningDisabled => ToolExecutionError::ChangePlanningDisabled(MESSAGE.to_string()),
            Self::Internal => ToolExecutionError::Internal(MESSAGE.to_string()),
            Self::OutputTooLarge {
                actual_bytes,
                max_bytes,
            } => ToolExecutionError::OutputTooLarge {
                actual_bytes,
                max_bytes,
            },
            Self::TimedOut { timeout_ms } => ToolExecutionError::TimedOut { timeout_ms },
            Self::Cancelled => ToolExecutionError::Cancelled,
        }
    }
}

struct SnapshotEntry {
    key: SnapshotKey,
    value: Arc<dyn Any + Send + Sync>,
    observed_at: String,
    inserted_at: Instant,
    reusable_until: Option<Instant>,
    expires_at: Instant,
    bytes: usize,
    pinned_by_flight: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstoneReason {
    Expired,
    Evicted,
    Invalidated,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotView<T> {
    id: Arc<str>,
    request: SnapshotRequest,
    generation: u64,
    position: usize,
    pub(crate) payload: QueryPayload<T>,
    pub(crate) observed_at: String,
    pub(crate) freshness_ms: u64,
    pub(crate) cache_status: CacheStatus,
}

impl<T> SnapshotView<T> {
    pub(crate) fn identity(&self) -> &str {
        &self.id
    }
}

impl SnapshotStore {
    pub(crate) fn new(capacity: usize) -> Self {
        Self::with_limits(capacity, SnapshotLimits::default())
    }

    fn with_limits(capacity: usize, limits: SnapshotLimits) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new(SnapshotStoreInner {
                capacity,
                per_scope_capacity: (capacity / 4).max(1),
                limits,
                generation: AtomicU64::new(0),
                sequence: AtomicU64::new(0),
                flight_sequence: AtomicU64::new(0),
                primary_key: RandomState::new(),
                secondary_key: RandomState::new(),
                state: StdMutex::new(StoreState::default()),
                metrics: SnapshotMetrics::default(),
            }),
        }
    }

    pub(crate) async fn get_or_load<T, Load, LoadFuture>(
        &self,
        request: SnapshotRequest,
        cursor: Option<&str>,
        cursor_ttl: Duration,
        response_cache_ttl: Option<Duration>,
        weight: impl FnOnce(&T) -> SnapshotWeight,
        cancellation: &CancellationToken,
        load: Load,
    ) -> Result<SnapshotView<T>, ToolExecutionError>
    where
        T: Clone + RetainedSize + Send + Sync + 'static,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<QueryPayload<T>, ToolExecutionError>>,
    {
        if let Some(cursor) = cursor {
            return self.resolve_cursor(&request, cursor).map_err(Into::into);
        }

        let cursor_ttl = cursor_ttl.min(self.inner.limits.max_lifetime);
        if cursor_ttl.is_zero() {
            return Err(SnapshotError::Expired.into());
        }
        let response_cache_ttl = response_cache_ttl
            .filter(|ttl| !ttl.is_zero())
            .map(|ttl| ttl.min(cursor_ttl));
        let generation = self.inner.generation.load(Ordering::Acquire);
        let key = SnapshotKey {
            request: request.clone(),
            generation,
        };
        let ticket = if response_cache_ttl.is_some() {
            match self.begin_flight::<T>(&key)? {
                BeginFlight::Cached(view) => return Ok(view),
                BeginFlight::Leader(ticket) => Some(ticket),
                BeginFlight::Waiter(ticket) => {
                    self.inner.metrics.coalesced_waiters.fetch_add(1, Ordering::Relaxed);
                    return ticket.wait::<T>(&key, cancellation).await;
                }
            }
        } else {
            None
        };

        if cancellation.is_cancelled() {
            return Err(ToolExecutionError::Cancelled);
        }

        let payload = match load().await {
            Ok(payload) => payload,
            Err(error) => {
                if let Some(ticket) = ticket.as_ref() {
                    self.finish_failure(ticket, SharedFlightFailure::from_error(&error));
                }
                return Err(error);
            }
        };
        let snapshot_weight = weight(&payload.data);
        let budget_error = if snapshot_weight.entries > self.inner.limits.max_entries {
            Some(SnapshotError::EntryBudgetExceeded)
        } else if snapshot_weight.rows > self.inner.limits.max_rows {
            Some(SnapshotError::RowBudgetExceeded)
        } else {
            None
        };
        if let Some(error) = budget_error {
            let tool_error: ToolExecutionError = error.into();
            if let Some(ticket) = ticket.as_ref() {
                self.finish_failure(ticket, SharedFlightFailure::from_error(&tool_error));
            }
            return Err(tool_error);
        }
        let observed_at = observed_at();
        let id = self.snapshot_id(&key);
        let bytes = retained_entry_bytes(&payload, &key, &id, &observed_at);
        if bytes > self.inner.limits.max_snapshot_bytes
            || bytes > self.inner.limits.max_scope_bytes
            || bytes > self.inner.limits.max_total_bytes
        {
            let error: ToolExecutionError = SnapshotError::ByteBudgetExceeded.into();
            if let Some(ticket) = ticket.as_ref() {
                self.finish_failure(ticket, SharedFlightFailure::from_error(&error));
            }
            return Err(error);
        }
        let inserted_at = Instant::now();
        if let Some(ticket) = ticket.as_ref() {
            if let Err(error) = self.finish_success(
                ticket,
                id.clone(),
                Arc::new(payload.clone()),
                observed_at.clone(),
                inserted_at,
                cursor_ttl,
                response_cache_ttl,
                bytes,
            ) {
                return Err(error.into());
            }
            self.inner.metrics.misses.fetch_add(1, Ordering::Relaxed);
        } else {
            self.insert(
                id.clone(),
                key.clone(),
                Arc::new(payload.clone()),
                observed_at.clone(),
                inserted_at,
                cursor_ttl,
                response_cache_ttl,
                bytes,
                generation,
            )?;
            self.inner.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
        }
        let cache_status = if response_cache_ttl.is_some() {
            CacheStatus::Miss
        } else {
            CacheStatus::Bypass
        };
        Ok(SnapshotView {
            id,
            request,
            generation,
            position: 0,
            payload,
            observed_at,
            freshness_ms: 0,
            cache_status,
        })
    }

    pub(crate) fn page<T: Clone>(
        &self,
        view: &SnapshotView<impl Clone>,
        items: &[T],
    ) -> Result<Page<T>, SnapshotError> {
        let total_count = items.len();
        if view.position > total_count {
            return Err(SnapshotError::InvalidCursor);
        }
        let end = view
            .position
            .saturating_add(view.request.page_limit as usize)
            .min(total_count);
        let page_items = items[view.position..end].to_vec();
        let count = page_items.len();
        let has_more = end < total_count;
        let next_cursor = has_more.then(|| self.encode_cursor(&view.id, end, view.generation));
        Ok(Page {
            items: page_items,
            count,
            total_count,
            has_more,
            next_cursor,
        })
    }

    pub(crate) fn metrics(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.inner.metrics.hits.load(Ordering::Relaxed),
            misses: self.inner.metrics.misses.load(Ordering::Relaxed),
            bypasses: self.inner.metrics.bypasses.load(Ordering::Relaxed),
            evictions: self.inner.metrics.evictions.load(Ordering::Relaxed),
            invalidations: self.inner.metrics.invalidations.load(Ordering::Relaxed),
            coalesced_waiters: self.inner.metrics.coalesced_waiters.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn clear(&self) -> usize {
        let mut store = self.lock_state();
        // The store lock is the invalidation linearization point shared with
        // flight admission and completion.
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        let completed_cells = store
            .flights
            .records
            .values_mut()
            .filter_map(|record| match record.phase {
                FlightPhase::Completed { .. } => {
                    if matches!(record.outcome, FlightOutcome::Success(_)) {
                        record.outcome = FlightOutcome::Failure(SharedFlightFailure::InvalidArguments);
                    }
                    Some(record.cell.clone())
                }
                FlightPhase::Loading { .. } => None,
            })
            .collect::<Vec<_>>();
        let state = &mut store.snapshots;
        let ids = state.entries.keys().cloned().collect::<Vec<_>>();
        let removed = ids.len();
        for id in ids {
            remove_entry(
                state,
                &id,
                Some(TombstoneReason::Invalidated),
                self.tombstone_capacity(),
            );
        }
        compact_state_containers(state);
        self.inner.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
        drop(store);
        for cell in completed_cells {
            cell.completed.notify_waiters();
        }
        removed
    }

    fn lock_state(&self) -> MutexGuard<'_, StoreState> {
        self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn get_by_key_locked<T>(&self, state: &mut SnapshotState, key: &SnapshotKey) -> Option<SnapshotView<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        purge_expired(state, self.tombstone_capacity());
        let now = Instant::now();
        let id = state.insertion_order.iter().rev().find_map(|id| {
            state
                .entries
                .get(id)
                .filter(|entry| entry.key == *key && entry.reusable_until.is_some_and(|until| until > now))
                .map(|_| id.clone())
        })?;
        let entry = state.entries.get(&id)?;
        let payload = entry.value.downcast_ref::<QueryPayload<T>>()?.clone();
        let freshness_ms = Instant::now()
            .saturating_duration_since(entry.inserted_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
        Some(SnapshotView {
            id,
            request: key.request.clone(),
            generation: key.generation,
            position: 0,
            payload,
            observed_at: entry.observed_at.clone(),
            freshness_ms,
            cache_status: CacheStatus::Hit,
        })
    }

    fn resolve_cursor<T>(&self, request: &SnapshotRequest, cursor: &str) -> Result<SnapshotView<T>, SnapshotError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let claims = self.decode_cursor(cursor)?;
        let mut store = self.lock_state();
        let state = &mut store.snapshots;
        purge_expired(state, self.tombstone_capacity());
        if let Some(entry) = state.entries.get(claims.id.as_str()) {
            return self.view_from_entry(request, &claims, entry);
        }
        Err(match state.tombstones.get(claims.id.as_str()) {
            Some(TombstoneReason::Expired) => SnapshotError::Expired,
            Some(TombstoneReason::Evicted) => SnapshotError::Evicted,
            Some(TombstoneReason::Invalidated) => SnapshotError::Invalidated,
            None => SnapshotError::InvalidCursor,
        })
    }

    fn view_from_entry<T>(
        &self,
        request: &SnapshotRequest,
        claims: &CursorClaims,
        entry: &SnapshotEntry,
    ) -> Result<SnapshotView<T>, SnapshotError>
    where
        T: Clone + Send + Sync + 'static,
    {
        if claims.generation != entry.key.generation {
            return Err(SnapshotError::InvalidCursor);
        }
        validate_request(request, &entry.key.request)?;
        let payload = entry
            .value
            .downcast_ref::<QueryPayload<T>>()
            .ok_or(SnapshotError::ContextMismatch)?
            .clone();
        let freshness_ms = Instant::now()
            .saturating_duration_since(entry.inserted_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
        Ok(SnapshotView {
            id: Arc::from(claims.id.as_str()),
            request: request.clone(),
            generation: claims.generation,
            position: claims.position,
            payload,
            observed_at: entry.observed_at.clone(),
            freshness_ms,
            cache_status: CacheStatus::Hit,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "snapshot retention metadata is validated together"
    )]
    fn insert(
        &self,
        id: Arc<str>,
        key: SnapshotKey,
        value: Arc<dyn Any + Send + Sync>,
        observed_at: String,
        inserted_at: Instant,
        cursor_ttl: Duration,
        response_cache_ttl: Option<Duration>,
        bytes: usize,
        generation: u64,
    ) -> Result<(), SnapshotError> {
        let mut store = self.lock_state();
        if self.inner.generation.load(Ordering::Acquire) != generation {
            return Err(SnapshotError::Invalidated);
        }
        let StoreState { snapshots, flights } = &mut *store;
        purge_expired(snapshots, self.tombstone_capacity());
        let scope = key.request.scope();
        let mut evicted_any = false;
        while scope_entry_count(snapshots, scope).saturating_add(flight_scope_count(flights, scope))
            >= self.inner.per_scope_capacity
            || scope_bytes(snapshots, scope)
                .saturating_add(flight_scope_bytes(flights, scope))
                .saturating_add(bytes)
                > self.inner.limits.max_scope_bytes
        {
            let Some(oldest) = oldest_in_scope(snapshots, scope) else {
                break;
            };
            remove_entry(
                snapshots,
                &oldest,
                Some(TombstoneReason::Evicted),
                self.tombstone_capacity(),
            );
            self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            evicted_any = true;
        }
        if scope_entry_count(snapshots, scope).saturating_add(flight_scope_count(flights, scope))
            >= self.inner.per_scope_capacity
        {
            return Err(SnapshotError::EntryBudgetExceeded);
        }
        if scope_bytes(snapshots, scope)
            .saturating_add(flight_scope_bytes(flights, scope))
            .saturating_add(bytes)
            > self.inner.limits.max_scope_bytes
        {
            return Err(SnapshotError::ByteBudgetExceeded);
        }
        while snapshots.entries.len().saturating_add(flights.records.len()) >= self.inner.capacity
            || snapshots
                .total_bytes
                .saturating_add(flights.total_bytes)
                .saturating_add(bytes)
                > self.inner.limits.max_total_bytes
        {
            let Some(oldest) = oldest_evictable(snapshots) else {
                break;
            };
            remove_entry(
                snapshots,
                &oldest,
                Some(TombstoneReason::Evicted),
                self.tombstone_capacity(),
            );
            self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            evicted_any = true;
        }
        if evicted_any {
            compact_state_containers(snapshots);
        }
        trim_tombstones_for_bytes(
            snapshots,
            flights.total_bytes.saturating_add(bytes),
            self.inner.limits.max_total_bytes,
        );
        if snapshots.entries.len().saturating_add(flights.records.len()) >= self.inner.capacity {
            return Err(SnapshotError::EntryBudgetExceeded);
        }
        if snapshots
            .total_bytes
            .saturating_add(flights.total_bytes)
            .saturating_add(bytes)
            > self.inner.limits.max_total_bytes
        {
            return Err(SnapshotError::ByteBudgetExceeded);
        }
        snapshots.insertion_order.push_back(id.clone());
        snapshots.total_bytes = snapshots.total_bytes.saturating_add(bytes);
        snapshots.entries.insert(
            id,
            SnapshotEntry {
                key,
                value,
                observed_at,
                inserted_at,
                reusable_until: response_cache_ttl.map(|ttl| inserted_at + ttl),
                expires_at: inserted_at + cursor_ttl,
                bytes,
                pinned_by_flight: false,
            },
        );
        Ok(())
    }

    fn begin_flight<T>(&self, key: &SnapshotKey) -> Result<BeginFlight<T>, SnapshotError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut store = self.lock_state();
        if self.inner.generation.load(Ordering::Acquire) != key.generation {
            return Err(SnapshotError::Invalidated);
        }
        let StoreState { snapshots, flights } = &mut *store;
        purge_expired(snapshots, self.tombstone_capacity());
        if let Some(view) = self.get_by_key_locked(snapshots, key) {
            return Ok(BeginFlight::Cached(view));
        }
        if let Some(id) = flights.joinable.get(key).copied() {
            if let Some(record) = flights.records.get_mut(&id) {
                if let FlightPhase::Loading { participants } = &mut record.phase {
                    *participants = participants.saturating_add(1);
                    return Ok(BeginFlight::Waiter(FlightTicket {
                        inner: self.inner.clone(),
                        id,
                        cell: record.cell.clone(),
                        leader: false,
                    }));
                }
            }
            flights.joinable.remove(key);
        }

        let bytes = retained_flight_bytes(key);
        if bytes > self.inner.limits.max_scope_bytes || bytes > self.inner.limits.max_total_bytes {
            return Err(SnapshotError::ByteBudgetExceeded);
        }
        let scope = key.request.scope();
        // Every flight record owns an active loading or completed cohort and is
        // therefore non-evictable. Only retained snapshots can make room.
        let mut evicted_any = false;
        while scope_entry_count(snapshots, scope).saturating_add(flight_scope_count(flights, scope))
            >= self.inner.per_scope_capacity
            || scope_bytes(snapshots, scope)
                .saturating_add(flight_scope_bytes(flights, scope))
                .saturating_add(bytes)
                > self.inner.limits.max_scope_bytes
        {
            let Some(oldest) = oldest_in_scope(snapshots, scope) else {
                break;
            };
            remove_entry(
                snapshots,
                &oldest,
                Some(TombstoneReason::Evicted),
                self.tombstone_capacity(),
            );
            self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            evicted_any = true;
        }
        while snapshots.entries.len().saturating_add(flights.records.len()) >= self.inner.capacity
            || snapshots
                .total_bytes
                .saturating_add(flights.total_bytes)
                .saturating_add(bytes)
                > self.inner.limits.max_total_bytes
        {
            let Some(oldest) = oldest_evictable(snapshots) else {
                break;
            };
            remove_entry(
                snapshots,
                &oldest,
                Some(TombstoneReason::Evicted),
                self.tombstone_capacity(),
            );
            self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            evicted_any = true;
        }
        if evicted_any {
            compact_state_containers(snapshots);
        }
        trim_tombstones_for_bytes(
            snapshots,
            flights.total_bytes.saturating_add(bytes),
            self.inner.limits.max_total_bytes,
        );
        if scope_entry_count(snapshots, scope).saturating_add(flight_scope_count(flights, scope))
            >= self.inner.per_scope_capacity
            || flights.records.len() >= self.inner.capacity
            || snapshots.entries.len().saturating_add(flights.records.len()) >= self.inner.capacity
        {
            return Err(SnapshotError::EntryBudgetExceeded);
        }
        if scope_bytes(snapshots, scope)
            .saturating_add(flight_scope_bytes(flights, scope))
            .saturating_add(bytes)
            > self.inner.limits.max_scope_bytes
            || snapshots
                .total_bytes
                .saturating_add(flights.total_bytes)
                .saturating_add(bytes)
                > self.inner.limits.max_total_bytes
        {
            return Err(SnapshotError::ByteBudgetExceeded);
        }

        let id = self.inner.flight_sequence.fetch_add(1, Ordering::Relaxed);
        let cell = Arc::new(FlightCell {
            completed: Notify::new(),
        });
        flights.joinable.insert(key.clone(), id);
        flights.records.insert(
            id,
            FlightRecord {
                key: key.clone(),
                cell: cell.clone(),
                bytes,
                phase: FlightPhase::Loading { participants: 1 },
                outcome: FlightOutcome::Loading,
            },
        );
        flights.total_bytes = flights.total_bytes.saturating_add(bytes);
        Ok(BeginFlight::Leader(FlightTicket {
            inner: self.inner.clone(),
            id,
            cell,
            leader: true,
        }))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "flight completion transfers validated snapshot metadata atomically"
    )]
    fn finish_success(
        &self,
        ticket: &FlightTicket,
        id: Arc<str>,
        value: Arc<dyn Any + Send + Sync>,
        observed_at: String,
        inserted_at: Instant,
        cursor_ttl: Duration,
        response_cache_ttl: Option<Duration>,
        entry_bytes: usize,
    ) -> Result<(), SnapshotError> {
        let mut store = self.lock_state();
        let StoreState { snapshots, flights } = &mut *store;
        let Some(record) = flights.records.get(&ticket.id) else {
            return Err(SnapshotError::Invalidated);
        };
        let FlightPhase::Loading { participants } = record.phase else {
            return Err(SnapshotError::Invalidated);
        };
        if !ticket.leader || !Arc::ptr_eq(&ticket.cell, &record.cell) {
            return Err(SnapshotError::Invalidated);
        }
        let key = record.key.clone();
        let old_bytes = record.bytes;
        let keeps_terminal_flight = participants > 1;
        let terminal_bytes = keeps_terminal_flight.then(|| retained_terminal_flight_bytes(&key, &id));
        let incoming_bytes = entry_bytes.saturating_add(terminal_bytes.unwrap_or(0));
        let error = if self.inner.generation.load(Ordering::Acquire) != key.generation {
            Some(SnapshotError::Invalidated)
        } else if incoming_bytes > self.inner.limits.max_scope_bytes
            || incoming_bytes > self.inner.limits.max_total_bytes
        {
            Some(SnapshotError::ByteBudgetExceeded)
        } else {
            purge_expired(snapshots, self.tombstone_capacity());
            let scope = key.request.scope();
            let mut evicted_any = false;
            while scope_entry_count(snapshots, scope)
                .saturating_add(flight_scope_count(flights, scope))
                .saturating_add(usize::from(keeps_terminal_flight))
                > self.inner.per_scope_capacity
                || scope_bytes(snapshots, scope)
                    .saturating_add(flight_scope_bytes(flights, scope))
                    .saturating_sub(old_bytes)
                    .saturating_add(incoming_bytes)
                    > self.inner.limits.max_scope_bytes
            {
                let Some(oldest) = oldest_in_scope(snapshots, scope) else {
                    break;
                };
                remove_entry(
                    snapshots,
                    &oldest,
                    Some(TombstoneReason::Evicted),
                    self.tombstone_capacity(),
                );
                self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                evicted_any = true;
            }
            while snapshots
                .entries
                .len()
                .saturating_add(flights.records.len())
                .saturating_add(usize::from(keeps_terminal_flight))
                > self.inner.capacity
                || snapshots
                    .total_bytes
                    .saturating_add(flights.total_bytes)
                    .saturating_sub(old_bytes)
                    .saturating_add(incoming_bytes)
                    > self.inner.limits.max_total_bytes
            {
                let Some(oldest) = oldest_evictable(snapshots) else {
                    break;
                };
                remove_entry(
                    snapshots,
                    &oldest,
                    Some(TombstoneReason::Evicted),
                    self.tombstone_capacity(),
                );
                self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                evicted_any = true;
            }
            if evicted_any {
                compact_state_containers(snapshots);
            }
            trim_tombstones_for_bytes(
                snapshots,
                flights
                    .total_bytes
                    .saturating_sub(old_bytes)
                    .saturating_add(incoming_bytes),
                self.inner.limits.max_total_bytes,
            );
            if scope_entry_count(snapshots, scope)
                .saturating_add(flight_scope_count(flights, scope))
                .saturating_add(usize::from(keeps_terminal_flight))
                > self.inner.per_scope_capacity
                || snapshots
                    .entries
                    .len()
                    .saturating_add(flights.records.len())
                    .saturating_add(usize::from(keeps_terminal_flight))
                    > self.inner.capacity
            {
                Some(SnapshotError::EntryBudgetExceeded)
            } else if scope_bytes(snapshots, scope)
                .saturating_add(flight_scope_bytes(flights, scope))
                .saturating_sub(old_bytes)
                .saturating_add(incoming_bytes)
                > self.inner.limits.max_scope_bytes
                || snapshots
                    .total_bytes
                    .saturating_add(flights.total_bytes)
                    .saturating_sub(old_bytes)
                    .saturating_add(incoming_bytes)
                    > self.inner.limits.max_total_bytes
            {
                Some(SnapshotError::ByteBudgetExceeded)
            } else {
                None
            }
        };
        if let Some(error) = error {
            let shared = SharedFlightFailure::from_error(&ToolExecutionError::from(error));
            let notify = transition_failure(flights, ticket.id, shared);
            drop(store);
            if let Some(cell) = notify {
                cell.completed.notify_waiters();
            }
            return Err(error);
        }
        if flights.joinable.get(&key) == Some(&ticket.id) {
            flights.joinable.remove(&key);
        }
        let mut notify = None;
        if let Some(terminal_bytes) = terminal_bytes {
            let record = flights.records.get_mut(&ticket.id).ok_or(SnapshotError::Invalidated)?;
            record.bytes = terminal_bytes;
            record.phase = FlightPhase::Completed {
                remaining: participants,
            };
            flights.total_bytes = flights
                .total_bytes
                .saturating_sub(old_bytes)
                .saturating_add(terminal_bytes);
            record.outcome = FlightOutcome::Success(id.clone());
            notify = Some(record.cell.clone());
        } else {
            remove_flight_record(flights, ticket.id);
        }
        snapshots.insertion_order.push_back(id.clone());
        snapshots.total_bytes = snapshots.total_bytes.saturating_add(entry_bytes);
        snapshots.entries.insert(
            id,
            SnapshotEntry {
                key,
                value,
                observed_at,
                inserted_at,
                reusable_until: response_cache_ttl.map(|ttl| inserted_at + ttl),
                expires_at: inserted_at + cursor_ttl,
                bytes: entry_bytes,
                pinned_by_flight: keeps_terminal_flight,
            },
        );
        drop(store);
        if let Some(cell) = notify {
            cell.completed.notify_waiters();
        }
        Ok(())
    }

    fn finish_failure(&self, ticket: &FlightTicket, error: SharedFlightFailure) {
        let mut store = self.lock_state();
        let notify = transition_failure(&mut store.flights, ticket.id, error);
        drop(store);
        if let Some(cell) = notify {
            cell.completed.notify_waiters();
        }
    }

    #[cfg(test)]
    async fn flight_lock(&self, key: &SnapshotKey) -> Result<(FlightTicket, bool), SnapshotError> {
        match self.begin_flight::<Vec<u8>>(key)? {
            BeginFlight::Leader(ticket) => Ok((ticket, false)),
            BeginFlight::Waiter(ticket) => Ok((ticket, true)),
            BeginFlight::Cached(_) => Err(SnapshotError::Invalidated),
        }
    }

    #[cfg(test)]
    async fn prune_flights(&self) {}

    fn snapshot_id(&self, key: &SnapshotKey) -> Arc<str> {
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let sequence = sequence.to_le_bytes();
        let generation = key.generation.to_le_bytes();
        let page_limit = key.request.page_limit.to_le_bytes();
        let parts = [
            sequence.as_slice(),
            generation.as_slice(),
            key.request.kind.as_str().as_bytes(),
            key.request.cluster.as_bytes(),
            key.request.normalized_filter.as_bytes(),
            key.request.selection_mode.as_str().as_bytes(),
            page_limit.as_slice(),
            key.request.schema_version.as_bytes(),
            key.request.visibility.as_bytes(),
        ];
        Arc::from(hex_encode(&self.authentication_tag(0x51, &parts)))
    }

    fn encode_cursor(&self, id: &str, position: usize, generation: u64) -> String {
        let claims = format!("{id}:{position}:{generation}");
        let tag = hex_encode(&self.authentication_tag(0x72, &[claims.as_bytes()]));
        format!("{CURSOR_PREFIX}{}.{}", hex_encode(claims.as_bytes()), tag)
    }

    fn decode_cursor(&self, cursor: &str) -> Result<CursorClaims, SnapshotError> {
        if cursor.len() > MAX_CURSOR_BYTES {
            return Err(SnapshotError::InvalidCursor);
        }
        let encoded = cursor.strip_prefix(CURSOR_PREFIX).ok_or(SnapshotError::InvalidCursor)?;
        let (claims_hex, tag) = encoded.split_once('.').ok_or(SnapshotError::InvalidCursor)?;
        let claims_bytes = hex_decode(claims_hex).ok_or(SnapshotError::InvalidCursor)?;
        let claims = std::str::from_utf8(&claims_bytes).map_err(|_| SnapshotError::InvalidCursor)?;
        let supplied_tag = hex_decode(tag)
            .filter(|tag| tag.len() == 16)
            .ok_or(SnapshotError::InvalidCursor)?;
        let expected = self.authentication_tag(0x72, &[claims.as_bytes()]);
        if !constant_time_eq(&supplied_tag, &expected) {
            return Err(SnapshotError::InvalidCursor);
        }
        let mut fields = claims.split(':');
        let id = fields
            .next()
            .filter(|id| id.len() == 32)
            .ok_or(SnapshotError::InvalidCursor)?;
        let position = fields
            .next()
            .ok_or(SnapshotError::InvalidCursor)?
            .parse()
            .map_err(|_| SnapshotError::InvalidCursor)?;
        let generation = fields
            .next()
            .ok_or(SnapshotError::InvalidCursor)?
            .parse()
            .map_err(|_| SnapshotError::InvalidCursor)?;
        if fields.next().is_some() || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SnapshotError::InvalidCursor);
        }
        Ok(CursorClaims {
            id: id.to_string(),
            position,
            generation,
        })
    }

    fn tombstone_capacity(&self) -> usize {
        self.inner.capacity.saturating_mul(4).clamp(32, 4_096)
    }

    fn authentication_tag(&self, domain: u8, parts: &[&[u8]]) -> [u8; 16] {
        let primary = keyed_hash(&self.inner.primary_key, domain, parts);
        let secondary = keyed_hash(&self.inner.secondary_key, domain ^ 0xa5, parts);
        let mut tag = [0u8; 16];
        tag[..8].copy_from_slice(&primary.to_le_bytes());
        tag[8..].copy_from_slice(&secondary.to_le_bytes());
        tag
    }
}

impl FlightTicket {
    async fn wait<T>(
        &self,
        key: &SnapshotKey,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotView<T>, ToolExecutionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        loop {
            let notified = self.cell.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let outcome = {
                let store = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(record) = store.flights.records.get(&self.id) else {
                    return Err(ToolExecutionError::Cancelled);
                };
                if !Arc::ptr_eq(&self.cell, &record.cell) {
                    return Err(ToolExecutionError::Cancelled);
                }
                match &record.outcome {
                    FlightOutcome::Loading => None,
                    FlightOutcome::Failure(error) => Some(Err(error.into_error())),
                    FlightOutcome::Success(id) => {
                        let entry =
                            store.snapshots.entries.get(id).ok_or_else(|| {
                                ToolExecutionError::internal("pinned coalesced snapshot is unavailable")
                            })?;
                        let payload = entry
                            .value
                            .downcast_ref::<QueryPayload<T>>()
                            .ok_or_else(|| ToolExecutionError::internal("coalesced snapshot type mismatch"))?
                            .clone();
                        let freshness_ms = Instant::now()
                            .saturating_duration_since(entry.inserted_at)
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX);
                        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
                        Some(Ok(SnapshotView {
                            id: id.clone(),
                            request: key.request.clone(),
                            generation: key.generation,
                            position: 0,
                            payload,
                            observed_at: entry.observed_at.clone(),
                            freshness_ms,
                            cache_status: CacheStatus::Hit,
                        }))
                    }
                }
            };
            if let Some(outcome) = outcome {
                return outcome;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(ToolExecutionError::Cancelled),
                _ = &mut notified => {}
            }
        }
    }
}

impl Drop for FlightTicket {
    fn drop(&mut self) {
        let mut store = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let flights = &mut store.flights;
        let Some(record) = flights.records.get(&self.id) else {
            return;
        };
        if !Arc::ptr_eq(&self.cell, &record.cell) {
            return;
        }

        let mut notify = None;
        let loading = matches!(record.phase, FlightPhase::Loading { .. });
        if loading && self.leader {
            notify = transition_failure(flights, self.id, SharedFlightFailure::Cancelled);
        }
        let remove = if let Some(record) = flights.records.get_mut(&self.id) {
            match &mut record.phase {
                FlightPhase::Loading { participants } => {
                    *participants = participants.saturating_sub(1);
                    false
                }
                FlightPhase::Completed { remaining } => {
                    *remaining = remaining.saturating_sub(1);
                    *remaining == 0
                }
            }
        } else {
            false
        };

        if remove {
            release_flight_record(&self.inner, &mut store, self.id);
        }
        drop(store);
        if let Some(cell) = notify {
            cell.completed.notify_waiters();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CursorClaims {
    id: String,
    position: usize,
    generation: u64,
}

fn validate_request(request: &SnapshotRequest, retained: &SnapshotRequest) -> Result<(), SnapshotError> {
    if request.page_limit != retained.page_limit {
        return Err(SnapshotError::PageContractMismatch);
    }
    if request.kind != retained.kind
        || request.cluster != retained.cluster
        || request.normalized_filter != retained.normalized_filter
        || request.selection_mode != retained.selection_mode
        || request.schema_version != retained.schema_version
        || request.visibility != retained.visibility
    {
        return Err(SnapshotError::ContextMismatch);
    }
    Ok(())
}

fn scope_entry_count(state: &SnapshotState, scope: (&str, &str)) -> usize {
    state
        .entries
        .values()
        .filter(|entry| entry.key.request.scope() == scope)
        .count()
}

fn flight_scope_count(state: &FlightState, scope: (&str, &str)) -> usize {
    state
        .records
        .values()
        .filter(|record| record.key.request.scope() == scope)
        .count()
}

#[cfg(test)]
fn flight_entry_reservation_count(state: &FlightState) -> usize {
    state.records.len()
}

fn flight_scope_bytes(state: &FlightState, scope: (&str, &str)) -> usize {
    state
        .records
        .values()
        .filter(|record| record.key.request.scope() == scope)
        .fold(0usize, |total, record| total.saturating_add(record.bytes))
}

fn transition_failure(state: &mut FlightState, id: u64, error: SharedFlightFailure) -> Option<Arc<FlightCell>> {
    let record = state.records.get_mut(&id)?;
    let FlightPhase::Loading { participants } = record.phase else {
        return None;
    };
    let key = record.key.clone();
    record.phase = FlightPhase::Completed {
        remaining: participants,
    };
    record.outcome = FlightOutcome::Failure(error);
    let cell = record.cell.clone();
    if state.joinable.get(&key) == Some(&id) {
        state.joinable.remove(&key);
    }
    Some(cell)
}

fn remove_flight_record(state: &mut FlightState, id: u64) -> Option<FlightRecord> {
    let record = state.records.remove(&id)?;
    if state.joinable.get(&record.key) == Some(&id) {
        state.joinable.remove(&record.key);
    }
    state.total_bytes = state.total_bytes.saturating_sub(record.bytes);
    state.records.shrink_to_fit();
    state.joinable.shrink_to_fit();
    Some(record)
}

fn release_flight_record(inner: &SnapshotStoreInner, store: &mut StoreState, id: u64) {
    let snapshot_id = store.flights.records.get(&id).and_then(|record| match &record.outcome {
        FlightOutcome::Success(id) => Some(id.clone()),
        FlightOutcome::Loading | FlightOutcome::Failure(_) => None,
    });
    remove_flight_record(&mut store.flights, id);
    if let Some(id) = snapshot_id {
        if let Some(entry) = store.snapshots.entries.get_mut(&id) {
            entry.pinned_by_flight = false;
        }
        purge_expired(&mut store.snapshots, inner.capacity.saturating_mul(4).clamp(32, 4_096));
    }
}

fn scope_bytes(state: &SnapshotState, scope: (&str, &str)) -> usize {
    state
        .entries
        .values()
        .filter(|entry| entry.key.request.scope() == scope)
        .fold(0usize, |total, entry| total.saturating_add(entry.bytes))
}

fn oldest_in_scope(state: &SnapshotState, scope: (&str, &str)) -> Option<Arc<str>> {
    state.insertion_order.iter().find_map(|id| {
        state
            .entries
            .get(id)
            .filter(|entry| !entry.pinned_by_flight && entry.key.request.scope() == scope)
            .map(|_| id.clone())
    })
}

fn oldest_evictable(state: &SnapshotState) -> Option<Arc<str>> {
    state.insertion_order.iter().find_map(|id| {
        state
            .entries
            .get(id)
            .filter(|entry| !entry.pinned_by_flight)
            .map(|_| id.clone())
    })
}

fn purge_expired(state: &mut SnapshotState, tombstone_capacity: usize) {
    let now = Instant::now();
    let expired = state
        .entries
        .iter()
        .filter(|(_, entry)| !entry.pinned_by_flight && entry.expires_at <= now)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let removed_any = !expired.is_empty();
    for id in expired {
        remove_entry(state, &id, Some(TombstoneReason::Expired), tombstone_capacity);
    }
    if removed_any {
        compact_state_containers(state);
    }
}

fn remove_entry(state: &mut SnapshotState, id: &str, tombstone: Option<TombstoneReason>, tombstone_capacity: usize) {
    if let Some(entry) = state.entries.remove(id) {
        state.insertion_order.retain(|candidate| candidate.as_ref() != id);
        state.total_bytes = state.total_bytes.saturating_sub(entry.bytes);
    }
    if let Some(reason) = tombstone {
        let id: Arc<str> = Arc::from(id);
        if state.tombstones.insert(id.clone(), reason).is_none() {
            state.total_bytes = state
                .total_bytes
                .saturating_add(RETAINED_TOMBSTONE_OVERHEAD.saturating_add(id.len()));
        }
        state
            .tombstone_order
            .retain(|candidate| candidate.as_ref() != id.as_ref());
        state.tombstone_order.push_back(id);
        while state.tombstone_order.len() > tombstone_capacity {
            if let Some(oldest) = state.tombstone_order.pop_front() {
                if state.tombstones.remove(&oldest).is_some() {
                    state.total_bytes = state
                        .total_bytes
                        .saturating_sub(RETAINED_TOMBSTONE_OVERHEAD.saturating_add(oldest.len()));
                }
            }
        }
    }
}

fn trim_tombstones_for_bytes(state: &mut SnapshotState, incoming: usize, max_total_bytes: usize) {
    let mut removed_any = false;
    while state.total_bytes.saturating_add(incoming) > max_total_bytes {
        let Some(oldest) = state.tombstone_order.pop_front() else {
            break;
        };
        if state.tombstones.remove(&oldest).is_some() {
            removed_any = true;
            state.total_bytes = state
                .total_bytes
                .saturating_sub(RETAINED_TOMBSTONE_OVERHEAD.saturating_add(oldest.len()));
        }
    }
    if removed_any {
        compact_state_containers(state);
    }
}

fn compact_state_containers(state: &mut SnapshotState) {
    state.entries.shrink_to_fit();
    state.insertion_order.shrink_to_fit();
    state.tombstones.shrink_to_fit();
    state.tombstone_order.shrink_to_fit();
}

fn retained_entry_bytes<T: RetainedSize>(
    payload: &QueryPayload<T>,
    key: &SnapshotKey,
    id: &str,
    observed_at: &String,
) -> usize {
    payload
        .retained_size()
        .saturating_add(key.request.cluster.capacity())
        .saturating_add(key.request.normalized_filter.capacity())
        .saturating_add(key.request.visibility.capacity())
        .saturating_add(key.request.schema_version.len())
        .saturating_add(id.len())
        .saturating_add(ARC_ALLOCATION_OVERHEAD)
        .saturating_add(observed_at.capacity())
        .saturating_add(size_of::<SnapshotEntry>())
        .saturating_add(size_of::<SnapshotKey>())
        .saturating_add(size_of::<SnapshotRequest>())
        .saturating_add(size_of::<SnapshotState>())
        .saturating_add(ARC_ALLOCATION_OVERHEAD)
        .saturating_add(RETAINED_ENTRY_BOOKKEEPING_OVERHEAD)
}

fn retained_flight_bytes(key: &SnapshotKey) -> usize {
    size_of::<SnapshotKey>()
        .saturating_mul(2)
        .saturating_add(size_of::<FlightRecord>())
        .saturating_add(size_of::<FlightCell>())
        .saturating_add(key.request.cluster.len())
        .saturating_add(key.request.normalized_filter.len())
        .saturating_add(key.request.visibility.len())
        .saturating_add(ARC_ALLOCATION_OVERHEAD)
        .saturating_add(RETAINED_FLIGHT_BOOKKEEPING_OVERHEAD)
}

fn retained_terminal_flight_bytes(key: &SnapshotKey, id: &str) -> usize {
    retained_flight_bytes(key)
        .saturating_add(id.len())
        .saturating_add(ARC_ALLOCATION_OVERHEAD)
}

fn keyed_hash(key: &RandomState, domain: u8, parts: &[&[u8]]) -> u64 {
    let mut hasher = key.build_hasher();
    hasher.write_u8(domain);
    for part in parts {
        hasher.write_usize(part.len());
        hasher.write(part);
    }
    hasher.finish()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use tokio::sync::Barrier;
    use tokio::sync::Notify;

    fn request(kind: SnapshotKind, cluster: &str, limit: u32) -> SnapshotRequest {
        SnapshotRequest::try_new(
            kind,
            cluster,
            "",
            &PageRequest {
                limit: Some(limit),
                cursor: None,
            },
            "standard",
        )
        .unwrap()
    }

    fn short_string_with_capacity(capacity: usize) -> String {
        let mut value = String::with_capacity(capacity);
        value.push('x');
        value
    }

    #[tokio::test]
    async fn cursor_is_stable_context_bound_and_tamper_evident() {
        let store = SnapshotStore::new(8);
        let cancellation = CancellationToken::new();
        let view = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 2),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8, 2, 3])) },
            )
            .await
            .unwrap();
        let first = store.page(&view, &view.payload.data).unwrap();
        let cursor = first.next_cursor.unwrap();
        let resumed = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 2),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("cursor hit must not reload") },
            )
            .await
            .unwrap();
        assert_eq!(store.page(&resumed, &resumed.payload.data).unwrap().items, [3]);

        let mut tampered = cursor.clone().into_bytes();
        *tampered.last_mut().unwrap() ^= 1;
        let tampered = String::from_utf8(tampered).unwrap();
        assert!(store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 2),
                Some(&tampered),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("tampered cursor must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("tampered"));

        let claims = store.decode_cursor(&cursor).unwrap();
        let forged_claims = format!("{}:{}:{}", claims.id, claims.position + 1, claims.generation);
        let (_, original_tag) = cursor.rsplit_once('.').unwrap();
        let forged = format!(
            "{CURSOR_PREFIX}{}.{}",
            hex_encode(forged_claims.as_bytes()),
            original_tag
        );
        assert_eq!(store.decode_cursor(&forged), Err(SnapshotError::InvalidCursor));

        let (encoded_claims, _) = cursor.rsplit_once('.').unwrap();
        assert_eq!(
            store.decode_cursor(&format!("{encoded_claims}.not-hex")),
            Err(SnapshotError::InvalidCursor)
        );

        let other_store = SnapshotStore::new(8);
        assert_eq!(other_store.decode_cursor(&cursor), Err(SnapshotError::InvalidCursor));
        assert_eq!(
            store.decode_cursor(&"x".repeat(MAX_CURSOR_BYTES + 1)),
            Err(SnapshotError::InvalidCursor)
        );
        assert!(store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "b", 2),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("context mismatch must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("context"));

        assert!(store
            .get_or_load(
                request(SnapshotKind::ConsumerGroupInventory, "a", 2),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("cross-query cursor must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("context"));

        assert!(store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("page-contract mismatch must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("page contract"));

        let mut visibility_mismatch = request(SnapshotKind::TopicInventory, "a", 2);
        visibility_mismatch.visibility = "sensitive".to_string();
        assert!(store
            .get_or_load(
                visibility_mismatch,
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("visibility mismatch must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("context"));
    }

    #[tokio::test]
    async fn expiry_eviction_and_invalidation_are_lazy_and_stable() {
        let cancellation = CancellationToken::new();
        let store = SnapshotStore::new(4);
        let view = store
            .get_or_load(
                request(SnapshotKind::TopicRoute, "a", 1),
                None,
                Duration::from_millis(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::detail(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8, 2])) },
            )
            .await
            .unwrap();
        let cursor = store.page(&view, &view.payload.data).unwrap().next_cursor.unwrap();
        store
            .lock_state()
            .snapshots
            .entries
            .get_mut(view.id.as_ref())
            .unwrap()
            .expires_at = Instant::now();
        let error = store
            .get_or_load(
                request(SnapshotKind::TopicRoute, "a", 1),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::detail(items.len()),
                &cancellation,
                || async { panic!("expired cursor must not reload") },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("expired"));

        let view = store
            .get_or_load(
                request(SnapshotKind::ConsumerLag, "b", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::detail(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8, 2])) },
            )
            .await
            .unwrap();
        let cursor = store.page(&view, &view.payload.data).unwrap().next_cursor.unwrap();
        assert_eq!(store.clear().await, 1);
        let error = store
            .get_or_load(
                request(SnapshotKind::ConsumerLag, "b", 1),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::detail(items.len()),
                &cancellation,
                || async { panic!("invalidated cursor must not reload") },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalidated"));
    }

    #[tokio::test]
    async fn deterministic_scope_eviction_rejects_old_cursor_without_reloading() {
        let store = SnapshotStore::new(4);
        let cancellation = CancellationToken::new();
        let first = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8, 2])) },
            )
            .await
            .unwrap();
        let cursor = store.page(&first, &first.payload.data).unwrap().next_cursor.unwrap();
        store
            .get_or_load(
                request(SnapshotKind::ConsumerLag, "a", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::detail(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![3u8])) },
            )
            .await
            .unwrap();
        let error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("evicted cursor must not reload") },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("evicted"));
    }

    #[tokio::test]
    async fn per_snapshot_entry_row_and_byte_budgets_are_enforced() {
        let cancellation = CancellationToken::new();
        let store = SnapshotStore::new(8);
        let entry_error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "entries", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |_| SnapshotWeight::inventory(MAX_SNAPSHOT_ENTRIES + 1),
                &cancellation,
                || async { Ok(QueryPayload::complete(Vec::<u8>::new())) },
            )
            .await
            .unwrap_err();
        assert!(entry_error.to_string().contains("entry budget"));

        let row_error = store
            .get_or_load(
                request(SnapshotKind::ConsumerLag, "rows", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |_| SnapshotWeight::detail(MAX_SNAPSHOT_ROWS + 1),
                &cancellation,
                || async { Ok(QueryPayload::complete(Vec::<u8>::new())) },
            )
            .await
            .unwrap_err();
        assert!(row_error.to_string().contains("row budget"));

        let byte_error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "bytes", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |_| SnapshotWeight::inventory(1),
                &cancellation,
                || async { Ok(QueryPayload::complete("x".repeat(MAX_SNAPSHOT_BYTES + 1))) },
            )
            .await
            .unwrap_err();
        assert!(byte_error.to_string().contains("byte budget"));

        let metadata_store = SnapshotStore::with_limits(
            8,
            SnapshotLimits {
                max_snapshot_bytes: 1_024,
                ..SnapshotLimits::default()
            },
        );
        let metadata_error = metadata_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "metadata", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |_| SnapshotWeight::inventory(0),
                &cancellation,
                || async {
                    Ok(QueryPayload::new(
                        Vec::<u8>::new(),
                        true,
                        vec!["w".repeat(2_048)],
                        vec![crate::model::contract::SourceFailure::new(
                            crate::model::contract::QuerySource::TopicRoute,
                            crate::model::contract::SourceFailureCode::SourceUnavailable,
                            true,
                            "broker-a",
                        )],
                    ))
                },
            )
            .await
            .unwrap_err();
        assert!(metadata_error.to_string().contains("byte budget"));
    }

    #[tokio::test]
    async fn retained_size_rejects_maximum_count_short_strings_with_large_capacities() {
        let store = SnapshotStore::new(8);
        let error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "short-strings", 1),
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<String>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async {
                    Ok(QueryPayload::complete(
                        (0..MAX_SNAPSHOT_ENTRIES)
                            .map(|_| short_string_with_capacity(512))
                            .collect::<Vec<_>>(),
                    ))
                },
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("byte budget"));
    }

    #[tokio::test]
    async fn retained_size_rejects_metadata_capacity_not_visible_on_the_wire() {
        let store = SnapshotStore::new(8);
        let error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "metadata-capacity", 1),
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async {
                    Ok(QueryPayload::new(
                        Vec::<u8>::new(),
                        true,
                        vec![short_string_with_capacity(MAX_SNAPSHOT_BYTES + 1)],
                        Vec::new(),
                    ))
                },
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("byte budget"));
    }

    #[test]
    fn snapshot_context_components_are_bounded() {
        let page = PageRequest {
            limit: Some(1),
            cursor: None,
        };
        assert_eq!(
            SnapshotRequest::try_new(
                SnapshotKind::TopicInventory,
                "c".repeat(MAX_CLUSTER_BYTES + 1),
                "",
                &page,
                "standard",
            ),
            Err(SnapshotError::ContextTooLarge)
        );
        assert_eq!(
            SnapshotRequest::try_new(
                SnapshotKind::TopicInventory,
                "cluster",
                "f".repeat(MAX_FILTER_BYTES + 1),
                &page,
                "standard",
            ),
            Err(SnapshotError::ContextTooLarge)
        );
        assert_eq!(
            SnapshotRequest::try_new(
                SnapshotKind::TopicInventory,
                "cluster",
                "",
                &page,
                "v".repeat(MAX_VISIBILITY_BYTES + 1),
            ),
            Err(SnapshotError::ContextTooLarge)
        );
    }

    #[tokio::test]
    async fn per_scope_and_global_byte_limits_evict_deterministically() {
        let cancellation = CancellationToken::new();
        let payload = QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)]);
        let sample_key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "a", 1),
            generation: 0,
        };
        let observed_at = "2026-01-01T00:00:00.000Z".to_string();
        let sample_bytes =
            retained_entry_bytes(&payload, &sample_key, "00000000000000000000000000000000", &observed_at);

        let scope_store = SnapshotStore::with_limits(
            16,
            SnapshotLimits {
                max_snapshot_bytes: sample_bytes + 256,
                max_scope_bytes: sample_bytes * 2 - 1,
                ..SnapshotLimits::default()
            },
        );
        let first = scope_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                None,
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(2),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        let cursor = scope_store.page(&first, &[1u8, 2]).unwrap().next_cursor.unwrap();
        scope_store
            .get_or_load(
                request(SnapshotKind::ConsumerGroupInventory, "a", 1),
                None,
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(1),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        assert!(scope_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                Some(&cursor),
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(2),
                &cancellation,
                || async { panic!("evicted cursor must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("evicted"));

        let too_large_for_scope = SnapshotStore::with_limits(
            16,
            SnapshotLimits {
                max_snapshot_bytes: sample_bytes + 256,
                max_scope_bytes: sample_bytes - 1,
                ..SnapshotLimits::default()
            },
        );
        assert!(too_large_for_scope
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                None,
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(1),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("byte budget"));

        let global_store = SnapshotStore::with_limits(
            16,
            SnapshotLimits {
                max_snapshot_bytes: sample_bytes + 256,
                max_total_bytes: sample_bytes * 2 - 1,
                max_scope_bytes: sample_bytes * 2,
                ..SnapshotLimits::default()
            },
        );
        let first = global_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                None,
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(2),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        let cursor = global_store.page(&first, &[1u8, 2]).unwrap().next_cursor.unwrap();
        global_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "b", 1),
                None,
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(1),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        assert!(global_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "a", 1),
                Some(&cursor),
                Duration::from_secs(1),
                None,
                |_: &Vec<String>| SnapshotWeight::inventory(2),
                &cancellation,
                || async { panic!("globally evicted cursor must not reload") },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("evicted"));
    }

    #[tokio::test]
    async fn concurrent_initial_load_is_singleflight_and_budgeted() {
        let store = SnapshotStore::new(8);
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(9));
        let release_loader = Arc::new(Notify::new());
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let calls = calls.clone();
            let start = start.clone();
            let release_loader = release_loader.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                store
                    .get_or_load(
                        request(SnapshotKind::ConsumerGroupInventory, "a", 2),
                        None,
                        Duration::from_secs(1),
                        Some(Duration::from_secs(1)),
                        |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                        &CancellationToken::new(),
                        || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            release_loader.notified().await;
                            Ok(QueryPayload::complete(vec![1u8, 2, 3]))
                        },
                    )
                    .await
                    .unwrap()
                    .cache_status
            }));
        }
        start.wait().await;
        for _ in 0..1_000 {
            if store.metrics().coalesced_waiters == 7 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(store.metrics().coalesced_waiters, 7);
        release_loader.notify_one();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.metrics().coalesced_waiters, 7);
        assert!(store.lock_state().flights.records.is_empty());
    }

    #[tokio::test]
    async fn concurrent_failed_load_is_shared_without_duplicate_upstream_work() {
        let store = SnapshotStore::new(8);
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(9));
        let release_loader = Arc::new(Notify::new());
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let calls = calls.clone();
            let start = start.clone();
            let release_loader = release_loader.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                store
                    .get_or_load(
                        request(SnapshotKind::ConsumerGroupInventory, "failed", 2),
                        None,
                        Duration::from_secs(1),
                        Some(Duration::from_secs(1)),
                        |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                        &CancellationToken::new(),
                        || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            release_loader.notified().await;
                            Err(ToolExecutionError::backend("shared failure"))
                        },
                    )
                    .await
                    .unwrap_err()
                    .to_string()
            }));
        }
        start.wait().await;
        for _ in 0..1_000 {
            if store.metrics().coalesced_waiters == 7 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(store.metrics().coalesced_waiters, 7);
        release_loader.notify_one();
        for task in tasks {
            assert!(task.await.unwrap().contains("backend error"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = store.lock_state();
        assert!(state.flights.records.is_empty());
        assert_eq!(state.flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn active_flights_are_rejected_at_scope_and_global_count_boundaries() {
        let scope_store = SnapshotStore::new(4);
        let first_key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "scope-a", 1),
            generation: 0,
        };
        let (first, coalesced) = scope_store.flight_lock(&first_key).await.unwrap();
        assert!(!coalesced);
        let (same, coalesced) = scope_store.flight_lock(&first_key).await.unwrap();
        assert!(coalesced);

        let same_scope_key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "scope-a", 1),
            generation: 0,
        };
        assert_eq!(
            scope_store.flight_lock(&same_scope_key).await.unwrap_err(),
            SnapshotError::EntryBudgetExceeded
        );

        drop(first);
        drop(same);
        scope_store.prune_flights().await;

        let global_store = SnapshotStore::new(1);
        let (active, coalesced) = global_store.flight_lock(&first_key).await.unwrap();
        assert!(!coalesced);
        let other_scope_key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "scope-b", 1),
            generation: 0,
        };
        assert_eq!(
            global_store.flight_lock(&other_scope_key).await.unwrap_err(),
            SnapshotError::EntryBudgetExceeded
        );
        drop(active);
        global_store.prune_flights().await;
    }

    #[tokio::test]
    async fn active_flight_bytes_are_rejected_before_insertion() {
        let key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "scope-a", 1),
            generation: 0,
        };
        let bytes = retained_flight_bytes(&key);
        let total_store = SnapshotStore::with_limits(
            4,
            SnapshotLimits {
                max_total_bytes: bytes - 1,
                max_scope_bytes: bytes,
                ..SnapshotLimits::default()
            },
        );
        assert_eq!(
            total_store.flight_lock(&key).await.unwrap_err(),
            SnapshotError::ByteBudgetExceeded
        );
        {
            let store = total_store.lock_state();
            let flights = &store.flights;
            assert!(flights.records.is_empty());
            assert_eq!(flights.total_bytes, 0);
        }

        let scope_store = SnapshotStore::with_limits(
            4,
            SnapshotLimits {
                max_total_bytes: bytes,
                max_scope_bytes: bytes - 1,
                ..SnapshotLimits::default()
            },
        );
        assert_eq!(
            scope_store.flight_lock(&key).await.unwrap_err(),
            SnapshotError::ByteBudgetExceeded
        );
        let store = scope_store.lock_state();
        let flights = &store.flights;
        assert!(flights.records.is_empty());
        assert_eq!(flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn pruning_dead_flights_releases_accounting_and_bucket_high_water() {
        let store = SnapshotStore::new(64);
        let mut active = Vec::new();
        for index in 0..32 {
            let key = SnapshotKey {
                request: request(SnapshotKind::TopicInventory, &format!("scope-{index}"), 1),
                generation: 0,
            };
            let (flight, coalesced) = store.flight_lock(&key).await.unwrap();
            assert!(!coalesced);
            active.push(flight);
        }
        {
            let state = store.lock_state();
            let flights = &state.flights;
            assert_eq!(flights.records.len(), 32);
            assert!(flights.records.capacity() >= 32);
            assert!(flights.total_bytes > 0);
        }

        drop(active);
        store.prune_flights().await;
        let state = store.lock_state();
        let flights = &state.flights;
        assert!(flights.records.is_empty());
        assert_eq!(flights.records.capacity(), 0);
        assert_eq!(flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn cancellation_before_flight_acquisition_releases_the_flight_record() {
        let store = SnapshotStore::new(8);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "scope-a", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { panic!("cancelled request must not load") },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolExecutionError::Cancelled));

        let state = store.lock_state();
        let flights = &state.flights;
        assert!(flights.records.is_empty());
        assert_eq!(flights.records.capacity(), 0);
        assert_eq!(flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn existing_snapshots_and_new_flights_share_scope_and_global_count_reservations() {
        let cancellation = CancellationToken::new();
        let scope_store = SnapshotStore::new(4);
        scope_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "scope-a", 1),
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8])) },
            )
            .await
            .unwrap();
        let scope_key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "scope-a", 1),
            generation: 0,
        };
        let (scope_flight, coalesced) = scope_store.flight_lock(&scope_key).await.unwrap();
        assert!(!coalesced);
        {
            let state = scope_store.lock_state();
            assert_eq!(state.snapshots.entries.len(), 0);
            assert_eq!(flight_entry_reservation_count(&state.flights), 1);
        }
        drop(scope_flight);
        scope_store.prune_flights().await;

        let global_store = SnapshotStore::new(1);
        global_store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "scope-a", 1),
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8])) },
            )
            .await
            .unwrap();
        let global_key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "scope-b", 1),
            generation: 0,
        };
        let (global_flight, coalesced) = global_store.flight_lock(&global_key).await.unwrap();
        assert!(!coalesced);
        {
            let state = global_store.lock_state();
            assert_eq!(state.snapshots.entries.len(), 0);
            assert_eq!(flight_entry_reservation_count(&state.flights), 1);
        }
        drop(global_flight);
        global_store.prune_flights().await;
    }

    #[tokio::test]
    async fn existing_snapshots_and_new_flights_share_scope_and_global_byte_reservations() {
        let cancellation = CancellationToken::new();
        let payload = QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)]);
        let snapshot_key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "scope-a", 1),
            generation: 0,
        };
        let observed_at = "2026-01-01T00:00:00.000Z".to_string();
        let snapshot_bytes = retained_entry_bytes(
            &payload,
            &snapshot_key,
            "00000000000000000000000000000000",
            &observed_at,
        );
        let scope_key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "scope-a", 1),
            generation: 0,
        };
        let scope_flight_bytes = retained_flight_bytes(&scope_key);
        let scope_store = SnapshotStore::with_limits(
            8,
            SnapshotLimits {
                max_snapshot_bytes: snapshot_bytes + 256,
                max_scope_bytes: snapshot_bytes + scope_flight_bytes - 1,
                ..SnapshotLimits::default()
            },
        );
        scope_store
            .get_or_load(
                snapshot_key.request.clone(),
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<String>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        let (scope_flight, _) = scope_store.flight_lock(&scope_key).await.unwrap();
        {
            let state = scope_store.lock_state();
            assert!(state.snapshots.entries.is_empty());
            assert!(
                state.snapshots.total_bytes.saturating_add(state.flights.total_bytes)
                    <= scope_store.inner.limits.max_total_bytes
            );
            assert!(
                flight_scope_bytes(&state.flights, scope_key.request.scope())
                    <= scope_store.inner.limits.max_scope_bytes
            );
        }
        drop(scope_flight);
        scope_store.prune_flights().await;

        let global_key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "scope-b", 1),
            generation: 0,
        };
        let global_flight_bytes = retained_flight_bytes(&global_key);
        let global_store = SnapshotStore::with_limits(
            8,
            SnapshotLimits {
                max_snapshot_bytes: snapshot_bytes + 256,
                max_scope_bytes: snapshot_bytes + 256,
                max_total_bytes: snapshot_bytes + global_flight_bytes - 1,
                ..SnapshotLimits::default()
            },
        );
        global_store
            .get_or_load(
                snapshot_key.request,
                None,
                Duration::from_secs(1),
                None,
                |items: &Vec<String>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![short_string_with_capacity(8 * 1024)])) },
            )
            .await
            .unwrap();
        let (global_flight, _) = global_store.flight_lock(&global_key).await.unwrap();
        {
            let state = global_store.lock_state();
            assert!(state.snapshots.entries.is_empty());
            assert!(
                state.snapshots.total_bytes.saturating_add(state.flights.total_bytes)
                    <= global_store.inner.limits.max_total_bytes
            );
        }
        drop(global_flight);
        global_store.prune_flights().await;
    }

    #[tokio::test]
    async fn successful_transfer_and_failed_load_release_shared_flight_reservations() {
        let cancellation = CancellationToken::new();
        let store = SnapshotStore::new(1);
        store
            .get_or_load(
                request(SnapshotKind::TopicInventory, "scope-a", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Ok(QueryPayload::complete(vec![1u8])) },
            )
            .await
            .unwrap();
        {
            let state = store.lock_state();
            assert_eq!(state.snapshots.entries.len(), 1);
            assert!(state.flights.records.is_empty());
            assert_eq!(state.flights.total_bytes, 0);
        }

        store.clear().await;
        let error = store
            .get_or_load(
                request(SnapshotKind::ConsumerGroupInventory, "scope-a", 1),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &cancellation,
                || async { Err(ToolExecutionError::backend("load failed")) },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("load failed"));
        let state = store.lock_state();
        assert!(state.flights.records.is_empty());
        assert_eq!(state.flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn registered_cohort_consumes_one_nanosecond_success_and_immediate_cursor() {
        let store = SnapshotStore::new(8);
        let key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "one-nanosecond", 1),
            generation: 0,
        };
        let (leader, coalesced) = store.flight_lock(&key).await.unwrap();
        assert!(!coalesced);
        let (waiter, coalesced) = store.flight_lock(&key).await.unwrap();
        assert!(coalesced);

        let upstream_calls = AtomicUsize::new(0);
        upstream_calls.fetch_add(1, Ordering::SeqCst);
        let payload = QueryPayload::complete(vec![1u8, 2]);
        let observed_at = observed_at();
        let id = store.snapshot_id(&key);
        let bytes = retained_entry_bytes(&payload, &key, &id, &observed_at);
        let inserted_at = Instant::now();
        store
            .finish_success(
                &leader,
                id.clone(),
                Arc::new(payload.clone()),
                observed_at.clone(),
                inserted_at,
                Duration::from_nanos(1),
                Some(Duration::from_nanos(1)),
                bytes,
            )
            .unwrap();
        drop(leader);

        let leader_view = SnapshotView {
            id: id.clone(),
            request: key.request.clone(),
            generation: key.generation,
            position: 0,
            payload: payload.clone(),
            observed_at,
            freshness_ms: 0,
            cache_status: CacheStatus::Miss,
        };
        let cursor = store
            .page(&leader_view, &leader_view.payload.data)
            .unwrap()
            .next_cursor
            .unwrap();
        let resumed = store
            .get_or_load(
                key.request.clone(),
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async { panic!("pinned cursor continuation must not reload") },
            )
            .await
            .unwrap();
        assert_eq!(resumed.payload.data, [1, 2]);

        let waiter_view = waiter.wait::<Vec<u8>>(&key, &CancellationToken::new()).await.unwrap();
        assert_eq!(waiter_view.payload.data, [1, 2]);
        assert_eq!(upstream_calls.load(Ordering::SeqCst), 1);
        drop(waiter);
        let state = store.lock_state();
        assert!(state.flights.records.is_empty());
        assert!(state.snapshots.entries.is_empty());
    }

    #[tokio::test]
    async fn completed_failure_is_closed_to_new_requests_and_recovery_uses_a_new_cohort() {
        let store = SnapshotStore::new(8);
        let key = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "recover", 1),
            generation: 0,
        };
        let (leader, _) = store.flight_lock(&key).await.unwrap();
        let (old_waiter, coalesced) = store.flight_lock(&key).await.unwrap();
        assert!(coalesced);
        store.finish_failure(&leader, SharedFlightFailure::Backend);
        let old_cohort = leader.id;
        drop(leader);

        let new_calls = Arc::new(AtomicUsize::new(0));
        let calls = new_calls.clone();
        let recovered = store
            .get_or_load(
                key.request.clone(),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(QueryPayload::complete(vec![7u8]))
                },
            )
            .await
            .unwrap();
        assert_eq!(recovered.payload.data, [7]);
        assert_eq!(new_calls.load(Ordering::SeqCst), 1);
        assert_ne!(store.inner.flight_sequence.load(Ordering::Relaxed) - 1, old_cohort);

        let old_error = old_waiter
            .wait::<Vec<u8>>(&key, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(old_error, ToolExecutionError::Backend(_)));
        drop(old_waiter);
        assert!(store.lock_state().flights.records.is_empty());
    }

    #[tokio::test]
    async fn completed_flights_remain_inside_scope_global_and_byte_budgets() {
        let key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "bounded", 1),
            generation: 0,
        };
        let other_scope = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "other", 1),
            generation: 0,
        };
        let global = SnapshotStore::new(1);
        let (leader, _) = global.flight_lock(&key).await.unwrap();
        let (waiter, _) = global.flight_lock(&key).await.unwrap();
        global.finish_failure(&leader, SharedFlightFailure::Backend);
        drop(leader);
        assert_eq!(
            global.flight_lock(&other_scope).await.unwrap_err(),
            SnapshotError::EntryBudgetExceeded
        );
        drop(waiter);

        let scope = SnapshotStore::new(4);
        let same_scope = SnapshotKey {
            request: request(SnapshotKind::ConsumerGroupInventory, "bounded", 1),
            generation: 0,
        };
        let (leader, _) = scope.flight_lock(&key).await.unwrap();
        let (waiter, _) = scope.flight_lock(&key).await.unwrap();
        scope.finish_failure(&leader, SharedFlightFailure::Backend);
        drop(leader);
        assert_eq!(
            scope.flight_lock(&same_scope).await.unwrap_err(),
            SnapshotError::EntryBudgetExceeded
        );
        drop(waiter);

        let first_bytes = retained_flight_bytes(&key);
        let second_bytes = retained_flight_bytes(&other_scope);
        let bytes = SnapshotStore::with_limits(
            4,
            SnapshotLimits {
                max_total_bytes: first_bytes + second_bytes - 1,
                max_scope_bytes: first_bytes + second_bytes,
                ..SnapshotLimits::default()
            },
        );
        let (leader, _) = bytes.flight_lock(&key).await.unwrap();
        let (waiter, _) = bytes.flight_lock(&key).await.unwrap();
        bytes.finish_failure(&leader, SharedFlightFailure::Backend);
        drop(leader);
        assert_eq!(
            bytes.flight_lock(&other_scope).await.unwrap_err(),
            SnapshotError::ByteBudgetExceeded
        );
        drop(waiter);
        let state = bytes.lock_state();
        assert!(state.flights.records.is_empty());
        assert_eq!(state.flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn dropping_leader_future_without_waiters_releases_container_high_water() {
        let store = SnapshotStore::new(8);
        let entered = Arc::new(Notify::new());
        let task_store = store.clone();
        let task_entered = entered.clone();
        let handle = tokio::spawn(async move {
            task_store
                .get_or_load(
                    request(SnapshotKind::TopicInventory, "abort-alone", 1),
                    None,
                    Duration::from_secs(1),
                    Some(Duration::from_secs(1)),
                    |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                    &CancellationToken::new(),
                    || async move {
                        task_entered.notify_one();
                        pending::<Result<QueryPayload<Vec<u8>>, ToolExecutionError>>().await
                    },
                )
                .await
        });
        entered.notified().await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        let state = store.lock_state();
        assert!(state.flights.records.is_empty());
        assert_eq!(state.flights.records.capacity(), 0);
        assert_eq!(state.flights.joinable.capacity(), 0);
        assert_eq!(state.flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn dropping_leader_future_wakes_registered_waiter_and_releases_container() {
        let store = SnapshotStore::new(8);
        let entered = Arc::new(Notify::new());
        let leader_store = store.clone();
        let leader_entered = entered.clone();
        let leader = tokio::spawn(async move {
            leader_store
                .get_or_load(
                    request(SnapshotKind::TopicInventory, "abort-shared", 1),
                    None,
                    Duration::from_secs(1),
                    Some(Duration::from_secs(1)),
                    |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                    &CancellationToken::new(),
                    || async move {
                        leader_entered.notify_one();
                        pending::<Result<QueryPayload<Vec<u8>>, ToolExecutionError>>().await
                    },
                )
                .await
        });
        entered.notified().await;
        let waiter_store = store.clone();
        let waiter = tokio::spawn(async move {
            waiter_store
                .get_or_load(
                    request(SnapshotKind::TopicInventory, "abort-shared", 1),
                    None,
                    Duration::from_secs(1),
                    Some(Duration::from_secs(1)),
                    |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                    &CancellationToken::new(),
                    || async { panic!("registered waiter must not load") },
                )
                .await
        });
        for _ in 0..1_000 {
            if store.metrics().coalesced_waiters == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(store.metrics().coalesced_waiters, 1);
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        let waiter_error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must be woken")
            .unwrap()
            .unwrap_err();
        assert!(matches!(waiter_error, ToolExecutionError::Cancelled));
        let state = store.lock_state();
        assert!(state.flights.records.is_empty());
        assert_eq!(state.flights.records.capacity(), 0);
        assert_eq!(state.flights.joinable.capacity(), 0);
        assert_eq!(state.flights.total_bytes, 0);
    }

    #[tokio::test]
    async fn clear_atomically_invalidates_terminal_success_before_waiter_consumes_it() {
        let store = SnapshotStore::new(8);
        let key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "clear-terminal", 1),
            generation: 0,
        };
        let (leader, _) = store.flight_lock(&key).await.unwrap();
        let (waiter, _) = store.flight_lock(&key).await.unwrap();
        let payload = QueryPayload::complete(vec![1u8, 2]);
        let observed_at = observed_at();
        let id = store.snapshot_id(&key);
        let bytes = retained_entry_bytes(&payload, &key, &id, &observed_at);
        store
            .finish_success(
                &leader,
                id.clone(),
                Arc::new(payload.clone()),
                observed_at.clone(),
                Instant::now(),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                bytes,
            )
            .unwrap();
        drop(leader);
        let leader_view = SnapshotView {
            id,
            request: key.request.clone(),
            generation: key.generation,
            position: 0,
            payload,
            observed_at,
            freshness_ms: 0,
            cache_status: CacheStatus::Miss,
        };
        let cursor = store
            .page(&leader_view, &leader_view.payload.data)
            .unwrap()
            .next_cursor
            .unwrap();

        assert_eq!(store.clear().await, 1);
        let waiter_error = waiter
            .wait::<Vec<u8>>(&key, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(waiter_error, ToolExecutionError::InvalidArguments(_)));
        drop(waiter);
        let cursor_error = store
            .get_or_load(
                key.request,
                Some(&cursor),
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async { panic!("invalidated cursor must not reload") },
            )
            .await
            .unwrap_err();
        assert!(cursor_error.to_string().contains("invalidated"));
        assert!(store.lock_state().flights.records.is_empty());
    }

    #[tokio::test]
    async fn stale_admission_and_late_leader_cannot_pollute_new_clear_generation() {
        let store = SnapshotStore::new(8);
        let stale_key = SnapshotKey {
            request: request(SnapshotKind::TopicInventory, "clear-generation", 1),
            generation: 0,
        };
        let (stale_leader, _) = store.flight_lock(&stale_key).await.unwrap();
        let (stale_waiter, _) = store.flight_lock(&stale_key).await.unwrap();
        store.clear().await;
        assert_eq!(
            store.flight_lock(&stale_key).await.unwrap_err(),
            SnapshotError::Invalidated
        );

        let current = store
            .get_or_load(
                stale_key.request.clone(),
                None,
                Duration::from_secs(1),
                Some(Duration::from_secs(1)),
                |items: &Vec<u8>| SnapshotWeight::inventory(items.len()),
                &CancellationToken::new(),
                || async { Ok(QueryPayload::complete(vec![9u8])) },
            )
            .await
            .unwrap();
        assert_eq!(current.generation, 1);

        let stale_payload = QueryPayload::complete(vec![1u8]);
        let stale_observed_at = observed_at();
        let stale_id = store.snapshot_id(&stale_key);
        let stale_bytes = retained_entry_bytes(&stale_payload, &stale_key, &stale_id, &stale_observed_at);
        assert_eq!(
            store
                .finish_success(
                    &stale_leader,
                    stale_id,
                    Arc::new(stale_payload),
                    stale_observed_at,
                    Instant::now(),
                    Duration::from_secs(1),
                    Some(Duration::from_secs(1)),
                    stale_bytes,
                )
                .unwrap_err(),
            SnapshotError::Invalidated
        );
        drop(stale_leader);
        assert!(matches!(
            stale_waiter
                .wait::<Vec<u8>>(&stale_key, &CancellationToken::new())
                .await
                .unwrap_err(),
            ToolExecutionError::InvalidArguments(_)
        ));
        drop(stale_waiter);
        let state = store.lock_state();
        assert_eq!(state.snapshots.entries.len(), 1);
        assert!(state.snapshots.entries.values().all(|entry| entry.key.generation == 1));
        assert!(state.flights.records.is_empty());
    }
}
