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
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::mem::size_of;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use rocketmq_observability::metrics::mcp::McpCacheEvent;
use rocketmq_observability::metrics::mcp::McpMetricsRecorder;

use crate::infrastructure::metrics::ComponentMetrics;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::observed_at;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;

mod failure;

pub(crate) use failure::CachedFailure;
use failure::Flight;

const MAX_CACHE_ENTRY_BYTES: usize = 4 * 1024 * 1024;
const MAX_CACHE_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const RETAINED_CACHE_ENTRY_OVERHEAD: usize = 2 * 1024;
const ARC_ALLOCATION_OVERHEAD: usize = 2 * size_of::<usize>();
/// How long a kept failure answers the calls for its key, and never longer than a value would.
const FAILURE_KEPT_FOR: Duration = Duration::from_secs(1);
/// The warning on a result that is the expired value of a source that failed.
pub(crate) const STALE_CACHE_VALUE_WARNING: &str = "stale_cache_value";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bypasses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub coalesced_waiters: u64,
}

impl CacheMetricsSnapshot {
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            hits: self.hits.saturating_add(other.hits),
            misses: self.misses.saturating_add(other.misses),
            bypasses: self.bypasses.saturating_add(other.bypasses),
            evictions: self.evictions.saturating_add(other.evictions),
            invalidations: self.invalidations.saturating_add(other.invalidations),
            coalesced_waiters: self.coalesced_waiters.saturating_add(other.coalesced_waiters),
        }
    }
}

#[derive(Debug, Default)]
struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    coalesced_waiters: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct QueryCache {
    inner: Arc<QueryCacheInner>,
}

struct QueryCacheInner {
    enabled: bool,
    capacity: usize,
    max_entry_bytes: usize,
    max_total_bytes: usize,
    /// How long past its expiry a value still answers for a source that fails. Zero is never.
    stale_if_error: Duration,
    generation: AtomicU64,
    state: Mutex<CacheState>,
    flights: Mutex<HashMap<String, Weak<Flight>>>,
    metrics: CacheMetrics,
    telemetry: ComponentMetrics,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>,
    insertion_order: VecDeque<String>,
    retained_bytes: usize,
    /// The failures that answer their key for a moment. There are at most as many as the cache
    /// has entries, and they hold no payload, so the byte limits do not count them.
    kept_failures: HashMap<String, KeptFailure>,
}

struct CacheEntry {
    value: Arc<dyn Any + Send + Sync>,
    observed_at: String,
    inserted_at: Instant,
    expires_at: Instant,
    /// Until when the value answers for a source that fails; `expires_at` when it never does.
    stale_until: Instant,
    retained_bytes: usize,
}

impl CacheEntry {
    fn age_ms(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.inserted_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

struct KeptFailure {
    failure: Arc<dyn Any + Send + Sync>,
    expires_at: Instant,
}

/// What a key holds for a caller.
enum Held {
    Value {
        value: Arc<dyn Any + Send + Sync>,
        observed_at: String,
        freshness_ms: u64,
    },
    Failure(Arc<dyn Any + Send + Sync>),
    Nothing,
}

enum Lookup<T, E> {
    Fresh(QueryResult<T>),
    Failed(E),
    Absent,
}

impl QueryCache {
    pub(crate) fn new(enabled: bool, capacity: usize) -> Self {
        Self::with_byte_limits(enabled, capacity, MAX_CACHE_ENTRY_BYTES, MAX_CACHE_TOTAL_BYTES)
    }

    /// Returns an empty cache with the same limits that reports its events through `metrics`.
    ///
    /// Apply this before the cache is used: entries and counters do not carry over.
    pub(crate) fn with_metrics(self, metrics: McpMetricsRecorder) -> Self {
        Self::build(
            self.inner.enabled,
            self.inner.capacity,
            self.inner.max_entry_bytes,
            self.inner.max_total_bytes,
            self.inner.stale_if_error,
            ComponentMetrics::new(metrics),
        )
    }

    /// Returns an empty cache with the same limits whose values answer for `window` past their
    /// expiry when their source fails. A zero `window` turns that off.
    ///
    /// Apply this before the cache is used: entries and counters do not carry over.
    pub(crate) fn with_stale_if_error(self, window: Duration) -> Self {
        Self::build(
            self.inner.enabled,
            self.inner.capacity,
            self.inner.max_entry_bytes,
            self.inner.max_total_bytes,
            window,
            self.inner.telemetry.clone(),
        )
    }

    fn with_byte_limits(enabled: bool, capacity: usize, max_entry_bytes: usize, max_total_bytes: usize) -> Self {
        Self::build(
            enabled,
            capacity,
            max_entry_bytes,
            max_total_bytes,
            Duration::ZERO,
            ComponentMetrics::default(),
        )
    }

    fn build(
        enabled: bool,
        capacity: usize,
        max_entry_bytes: usize,
        max_total_bytes: usize,
        stale_if_error: Duration,
        telemetry: ComponentMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(QueryCacheInner {
                enabled,
                capacity,
                max_entry_bytes,
                max_total_bytes,
                stale_if_error,
                generation: AtomicU64::new(0),
                state: Mutex::new(CacheState::default()),
                flights: Mutex::new(HashMap::new()),
                metrics: CacheMetrics::default(),
                telemetry,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) async fn get_or_try_init<T, E, Load, LoadFuture>(
        &self,
        key: String,
        ttl: Duration,
        load: Load,
    ) -> Result<QueryResult<T>, E>
    where
        T: Clone + RetainedSize + Send + Sync + 'static,
        E: CachedFailure,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<T, E>>,
    {
        let cancellation = CancellationToken::new();
        self.get_or_try_init_cancellable(
            key,
            ttl,
            &cancellation,
            || unreachable!(),
            || async { load().await.map(QueryPayload::complete) },
        )
        .await
    }

    /// Returns the value of `key`, loading it when the cache holds none that is current.
    ///
    /// The callers of one key load one at a time. Those that waited for a load read the value it
    /// left, or get its failure when that [is shared](CachedFailure::is_shared). A failure that
    /// [is kept](CachedFailure::is_kept) also answers the calls of the next second, so a source
    /// that is down is asked once in that time.
    ///
    /// A cache built [`with_stale_if_error`](Self::with_stale_if_error) answers a failure that
    /// [allows it](CachedFailure::allows_stale) with the value that expired within that window,
    /// marked with [`STALE_CACHE_VALUE_WARNING`] and reported at its real age.
    pub(crate) async fn get_or_try_init_cancellable<T, E, Load, LoadFuture, Cancelled>(
        &self,
        key: String,
        ttl: Duration,
        cancellation: &CancellationToken,
        cancelled: Cancelled,
        load: Load,
    ) -> Result<QueryResult<T>, E>
    where
        T: Clone + RetainedSize + Send + Sync + 'static,
        E: CachedFailure,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<QueryPayload<T>, E>>,
        Cancelled: FnOnce() -> E,
    {
        if !self.inner.enabled || self.inner.capacity == 0 || ttl.is_zero() {
            self.inner.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
            self.inner.telemetry.record_cache_event(McpCacheEvent::Bypass);
            return load()
                .await
                .map(|payload| QueryResult::from_payload(payload, observed_at(), 0, CacheStatus::Bypass));
        }

        match self.lookup(&key).await {
            Lookup::Fresh(result) => return Ok(result),
            Lookup::Failed(failure) => return self.answer_failure(&key, failure).await,
            Lookup::Absent => {}
        }

        let (flight, coalesced) = self.flight(&key).await;
        if coalesced {
            self.inner.metrics.coalesced_waiters.fetch_add(1, Ordering::Relaxed);
            self.inner.telemetry.record_cache_event(McpCacheEvent::CoalescedWaiter);
        }
        let failed_loads = flight.failed_loads();
        let _turn = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled()),
            turn = flight.turn.lock() => turn,
        };
        match self.lookup(&key).await {
            Lookup::Fresh(result) => return Ok(result),
            Lookup::Failed(failure) => return self.answer_failure(&key, failure).await,
            Lookup::Absent => {}
        }
        if let Some(failure) = flight.failure_since(failed_loads) {
            return self.answer_failure(&key, failure).await;
        }

        let generation = self.inner.generation.load(Ordering::Acquire);
        let payload = match load().await {
            Ok(payload) => payload,
            Err(failure) => {
                if failure.is_shared() {
                    flight.fail(&failure);
                }
                if failure.is_kept() {
                    self.keep_failure(&key, &failure, ttl, generation).await;
                }
                return self.answer_failure(&key, failure).await;
            }
        };
        let source_retained_bytes = payload.retained_size();
        let observed_at = observed_at();
        self.insert_if_current(
            key,
            payload.clone(),
            source_retained_bytes,
            observed_at.clone(),
            ttl,
            generation,
        )
        .await;
        self.inner.metrics.misses.fetch_add(1, Ordering::Relaxed);
        self.inner.telemetry.record_cache_event(McpCacheEvent::Miss);
        Ok(QueryResult::from_payload(payload, observed_at, 0, CacheStatus::Miss))
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
        let mut state = self.inner.state.lock().await;
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        let removed = state.entries.len();
        state.entries.clear();
        state.insertion_order.clear();
        state.entries.shrink_to_fit();
        state.insertion_order.shrink_to_fit();
        state.retained_bytes = 0;
        state.kept_failures = HashMap::new();
        self.inner.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
        self.inner.telemetry.record_cache_event(McpCacheEvent::Invalidation);
        removed
    }

    /// Reads what `key` holds: a current value, a failure kept for it, or neither.
    async fn lookup<T, E>(&self, key: &str) -> Lookup<T, E>
    where
        T: Clone + Send + Sync + 'static,
        E: CachedFailure,
    {
        let held = {
            let mut state = self.inner.state.lock().await;
            held(&mut state, key, Instant::now())
        };
        // The lock is released: this caller's copy of the entry is made without holding it.
        match held {
            Held::Value {
                value,
                observed_at,
                freshness_ms,
            } => match value.downcast_ref::<QueryPayload<T>>() {
                Some(payload) => Lookup::Fresh(self.hit(payload.clone(), observed_at, freshness_ms)),
                None => Lookup::Absent,
            },
            Held::Failure(failure) => failure
                .downcast_ref::<E>()
                .cloned()
                .map_or(Lookup::Absent, Lookup::Failed),
            Held::Nothing => Lookup::Absent,
        }
    }

    fn hit<T>(&self, payload: QueryPayload<T>, observed_at: String, freshness_ms: u64) -> QueryResult<T> {
        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
        self.inner.telemetry.record_cache_event(McpCacheEvent::Hit);
        QueryResult::from_payload(payload, observed_at, freshness_ms, CacheStatus::Hit)
    }

    /// Answers a failed load of `key`: with the value that expired a short while ago when the
    /// failure allows that and there is one, otherwise with the failure.
    async fn answer_failure<T, E>(&self, key: &str, failure: E) -> Result<QueryResult<T>, E>
    where
        T: Clone + Send + Sync + 'static,
        E: CachedFailure,
    {
        if self.inner.stale_if_error.is_zero() || !failure.allows_stale() {
            return Err(failure);
        }
        let stale = {
            let state = self.inner.state.lock().await;
            let now = Instant::now();
            state
                .entries
                .get(key)
                .filter(|entry| entry.stale_until > now)
                .map(|entry| (Arc::clone(&entry.value), entry.observed_at.clone(), entry.age_ms(now)))
        };
        let Some((value, observed_at, freshness_ms)) = stale else {
            return Err(failure);
        };
        let Some(payload) = value.downcast_ref::<QueryPayload<T>>() else {
            return Err(failure);
        };
        let mut payload = payload.clone();
        mark_stale(&mut payload.warnings);
        Ok(self.hit(payload, observed_at, freshness_ms))
    }

    /// Keeps `failure` as the answer for `key` for a moment, unless the cache was invalidated
    /// while the load ran.
    async fn keep_failure<E: CachedFailure>(&self, key: &str, failure: &E, ttl: Duration, generation: u64) {
        let mut state = self.inner.state.lock().await;
        if self.inner.generation.load(Ordering::Acquire) != generation {
            return;
        }
        let now = Instant::now();
        state.kept_failures.retain(|_, kept| kept.expires_at > now);
        if state.kept_failures.len() < self.inner.capacity {
            state.kept_failures.insert(
                key.to_string(),
                KeptFailure {
                    failure: Arc::new(failure.clone()),
                    expires_at: now + ttl.min(FAILURE_KEPT_FOR),
                },
            );
        }
    }

    async fn insert_if_current<T>(
        &self,
        key: String,
        value: QueryPayload<T>,
        source_retained_bytes: usize,
        observed_at: String,
        ttl: Duration,
        generation: u64,
    ) where
        T: RetainedSize + Send + Sync + 'static,
    {
        // The larger of what the loaded value and the stored copy retain is charged.
        let retained_bytes =
            retained_cache_entry_bytes(&key, &observed_at, source_retained_bytes.max(value.retained_size()));
        if retained_bytes > self.inner.max_entry_bytes || retained_bytes > self.inner.max_total_bytes {
            return;
        }
        let mut state = self.inner.state.lock().await;
        if self.inner.generation.load(Ordering::Acquire) != generation {
            return;
        }
        remove_entry(&mut state, &key);
        while state.entries.len() >= self.inner.capacity
            || state.retained_bytes.saturating_add(retained_bytes) > self.inner.max_total_bytes
        {
            let Some(oldest) = state.insertion_order.pop_front() else {
                break;
            };
            if let Some(entry) = state.entries.remove(&oldest) {
                state.retained_bytes = state.retained_bytes.saturating_sub(entry.retained_bytes);
                self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                self.inner.telemetry.record_cache_event(McpCacheEvent::Eviction);
            }
        }
        let inserted_at = Instant::now();
        state.kept_failures.retain(|_, kept| kept.expires_at > inserted_at);
        state.retained_bytes = state.retained_bytes.saturating_add(retained_bytes);
        state.insertion_order.push_back(key.clone());
        state.entries.insert(
            key,
            CacheEntry {
                value: Arc::new(value),
                observed_at,
                inserted_at,
                expires_at: inserted_at + ttl,
                stale_until: inserted_at + ttl + self.inner.stale_if_error,
                retained_bytes,
            },
        );
    }

    /// Joins the loads of `key`; `true` when a caller is already there.
    async fn flight(&self, key: &str) -> (Arc<Flight>, bool) {
        let mut flights = self.inner.flights.lock().await;
        flights.retain(|_, flight| flight.strong_count() > 0);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            return (flight, true);
        }
        let flight = Arc::new(Flight::default());
        flights.insert(key.to_string(), Arc::downgrade(&flight));
        (flight, false)
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.inner.state.lock().await.entries.len()
    }

    #[cfg(test)]
    async fn retained_bytes(&self) -> usize {
        self.inner.state.lock().await.retained_bytes
    }
}

/// Adds [`STALE_CACHE_VALUE_WARNING`] to the warnings of a result, which stay sorted.
pub(crate) fn mark_stale(warnings: &mut Vec<String>) {
    if !warnings.iter().any(|warning| warning == STALE_CACHE_VALUE_WARNING) {
        warnings.push(STALE_CACHE_VALUE_WARNING.to_string());
        warnings.sort();
    }
}

/// Reads `key`, dropping what it holds past any use: a value past its stale window, a failure
/// past its moment.
fn held(state: &mut CacheState, key: &str, now: Instant) -> Held {
    if let Some(entry) = state.entries.get(key) {
        if entry.expires_at > now {
            return Held::Value {
                value: Arc::clone(&entry.value),
                observed_at: entry.observed_at.clone(),
                freshness_ms: entry.age_ms(now),
            };
        }
        if entry.stale_until <= now {
            remove_entry(state, key);
        }
    }
    match state.kept_failures.get(key) {
        Some(kept) if kept.expires_at > now => Held::Failure(Arc::clone(&kept.failure)),
        Some(_) => {
            state.kept_failures.remove(key);
            Held::Nothing
        }
        None => Held::Nothing,
    }
}

fn remove_entry(state: &mut CacheState, key: &str) {
    if let Some(entry) = state.entries.remove(key) {
        state.retained_bytes = state.retained_bytes.saturating_sub(entry.retained_bytes);
        state.insertion_order.retain(|candidate| candidate != key);
        if state.entries.is_empty() {
            state.entries.shrink_to_fit();
            state.insertion_order.shrink_to_fit();
        }
    }
}

fn retained_cache_entry_bytes(key: &String, observed_at: &String, payload_bytes: usize) -> usize {
    size_of::<CacheEntry>()
        .saturating_add(RETAINED_CACHE_ENTRY_OVERHEAD)
        .saturating_add(ARC_ALLOCATION_OVERHEAD)
        .saturating_add(key.capacity().saturating_mul(2))
        .saturating_add(observed_at.capacity())
        .saturating_add(payload_bytes)
}

#[cfg(test)]
mod policy_tests;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use serde::Serialize;
    use tokio::sync::Notify;

    use crate::model::contract::CacheStatus;
    use crate::model::contract::QuerySource;
    use crate::model::contract::SourceFailure;
    use crate::model::contract::SourceFailureCode;

    use super::*;

    #[derive(Clone, Serialize)]
    struct HiddenCapacityPayload {
        visible: String,
        #[serde(skip_serializing)]
        hidden: String,
    }

    impl RetainedSize for HiddenCapacityPayload {
        fn retained_heap_size(&self) -> usize {
            self.visible
                .retained_heap_size()
                .saturating_add(self.hidden.retained_heap_size())
        }
    }

    /// The plain failures of these tests: the waiters of a load get them, and nothing else.
    impl CachedFailure for () {
        fn is_shared(&self) -> bool {
            true
        }

        fn is_kept(&self) -> bool {
            false
        }

        fn allows_stale(&self) -> bool {
            false
        }
    }

    impl CachedFailure for &'static str {
        fn is_shared(&self) -> bool {
            true
        }

        fn is_kept(&self) -> bool {
            false
        }

        fn allows_stale(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn cache_returns_miss_then_hit_without_reloading() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));

        let first = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        let second = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;

        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(first.data, second.data);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn binding_metrics_keeps_the_configured_limits_and_behavior() {
        let cache =
            QueryCache::with_byte_limits(true, 3, 64 * 1024, 128 * 1024).with_metrics(McpMetricsRecorder::noop());
        assert!(cache.inner.enabled);
        assert_eq!(cache.inner.capacity, 3);
        assert_eq!(cache.inner.max_entry_bytes, 64 * 1024);
        assert_eq!(cache.inner.max_total_bytes, 128 * 1024);

        let calls = Arc::new(AtomicUsize::new(0));
        let first = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        let second = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.metrics().hits, 1);

        let disabled = QueryCache::new(false, 8).with_metrics(McpMetricsRecorder::noop());
        let bypassed = load_string(&disabled, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        assert_eq!(bypassed.cache_status, CacheStatus::Bypass);
    }

    #[tokio::test]
    async fn cache_hit_preserves_partial_evidence_exactly() {
        let cache = QueryCache::new(true, 8);
        let cancellation = CancellationToken::new();
        let payload = QueryPayload::new(
            "usable".to_string(),
            true,
            vec!["enrichment_incomplete".to_string()],
            vec![SourceFailure::new(
                QuerySource::ConsumerStatistics,
                SourceFailureCode::Timeout,
                true,
                "broker-b",
            )],
        );
        let first = cache
            .get_or_try_init_cancellable(
                "group:orders".to_string(),
                Duration::from_secs(1),
                &cancellation,
                || (),
                || async { Ok::<_, ()>(payload) },
            )
            .await
            .unwrap();
        let second: QueryResult<String> = cache
            .get_or_try_init_cancellable(
                "group:orders".to_string(),
                Duration::from_secs(1),
                &cancellation,
                || (),
                || async { panic!("cache hit must not reload") },
            )
            .await
            .unwrap();

        assert_eq!(first.cache_status, CacheStatus::Miss);
        assert_eq!(second.cache_status, CacheStatus::Hit);
        assert_eq!(first.data, second.data);
        assert_eq!(first.partial, second.partial);
        assert_eq!(first.warnings, second.warnings);
        assert_eq!(first.source_failures, second.source_failures);
        assert_eq!(first.observed_at, second.observed_at);
    }

    #[tokio::test]
    async fn cache_reloads_expired_entries() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));

        let _ = load_string(&cache, "topic:orders", Duration::from_millis(5), calls.clone()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = load_string(&cache, "topic:orders", Duration::from_millis(5), calls.clone()).await;

        assert_eq!(result.cache_status, CacheStatus::Miss);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cache_evicts_oldest_entry_at_capacity() {
        let cache = QueryCache::new(true, 2);
        let calls = Arc::new(AtomicUsize::new(0));

        let _ = load_string(&cache, "a", Duration::from_secs(1), calls.clone()).await;
        let _ = load_string(&cache, "b", Duration::from_secs(1), calls.clone()).await;
        let _ = load_string(&cache, "c", Duration::from_secs(1), calls.clone()).await;
        let result = load_string(&cache, "a", Duration::from_secs(1), calls.clone()).await;

        assert_eq!(result.cache_status, CacheStatus::Miss);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(cache.len().await, 2);
    }

    #[tokio::test]
    async fn cache_enforces_exact_per_entry_and_global_retained_byte_limits() {
        let key = "entry-a".to_string();
        let observed = "2026-01-01T00:00:00Z".to_string();
        let payload = QueryPayload::complete("x".repeat(128));
        let entry_bytes = retained_cache_entry_bytes(&key, &observed, payload.retained_size());
        let ttl = Duration::from_secs(60);

        let exact = QueryCache::with_byte_limits(true, 8, entry_bytes, entry_bytes * 2);
        exact
            .insert_if_current(
                key.clone(),
                payload.clone(),
                payload.retained_size(),
                observed.clone(),
                ttl,
                0,
            )
            .await;
        assert_eq!(exact.len().await, 1);
        assert_eq!(exact.retained_bytes().await, entry_bytes);

        let below_entry = QueryCache::with_byte_limits(true, 8, entry_bytes - 1, entry_bytes * 2);
        below_entry
            .insert_if_current(
                key.clone(),
                payload.clone(),
                payload.retained_size(),
                observed.clone(),
                ttl,
                0,
            )
            .await;
        assert_eq!(below_entry.len().await, 0);
        assert_eq!(below_entry.retained_bytes().await, 0);

        let global = QueryCache::with_byte_limits(true, 8, entry_bytes + 1, entry_bytes * 2);
        global
            .insert_if_current(key, payload.clone(), payload.retained_size(), observed.clone(), ttl, 0)
            .await;
        global
            .insert_if_current(
                "entry-b".to_string(),
                payload.clone(),
                payload.retained_size(),
                observed.clone(),
                ttl,
                0,
            )
            .await;
        assert_eq!(global.len().await, 2);
        assert_eq!(global.retained_bytes().await, entry_bytes * 2);

        let mut plus_one_observed = String::with_capacity(observed.capacity() + 1);
        plus_one_observed.push_str(&observed);
        plus_one_observed.push('x');
        let plus_one_bytes =
            retained_cache_entry_bytes(&"entry-c".to_string(), &plus_one_observed, payload.retained_size());
        assert_eq!(plus_one_bytes, entry_bytes + 1);
        global
            .insert_if_current(
                "entry-c".to_string(),
                payload.clone(),
                payload.retained_size(),
                plus_one_observed,
                ttl,
                0,
            )
            .await;
        assert_eq!(
            global.len().await,
            1,
            "one extra retained byte must evict both exact-size entries"
        );
        assert_eq!(global.retained_bytes().await, plus_one_bytes);
        assert_eq!(global.metrics().evictions, 2);
        assert_eq!(global.clear().await, 1);
        assert_eq!(global.retained_bytes().await, 0);
    }

    #[tokio::test]
    async fn oversized_payload_is_returned_but_never_retained_or_reused() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let calls = calls.clone();
            let result = cache
                .get_or_try_init("infra:oversized".to_string(), Duration::from_secs(60), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>("x".repeat(MAX_CACHE_ENTRY_BYTES))
                })
                .await
                .unwrap();
            assert_eq!(result.cache_status, CacheStatus::Miss);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.len().await, 0);
        assert_eq!(cache.retained_bytes().await, 0);
    }

    #[tokio::test]
    async fn hidden_excess_capacity_is_charged_and_never_cached() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let calls = calls.clone();
            let result = cache
                .get_or_try_init("capacity:hidden".to_string(), Duration::from_secs(60), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mut hidden = String::with_capacity(MAX_CACHE_ENTRY_BYTES + 1);
                    hidden.push('x');
                    Ok::<_, ()>(HiddenCapacityPayload {
                        visible: "ok".to_string(),
                        hidden,
                    })
                })
                .await
                .unwrap();
            assert_eq!(result.cache_status, CacheStatus::Miss);
            assert!(result.data.hidden.capacity() > MAX_CACHE_ENTRY_BYTES);
            assert!(serde_json::to_vec(&result.data).unwrap().len() < 64);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.len().await, 0);
        assert_eq!(cache.retained_bytes().await, 0);
    }

    #[tokio::test]
    async fn cache_singleflight_coalesces_concurrent_misses() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_try_init("shared".to_string(), Duration::from_secs(1), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok::<_, ()>("value".to_string())
                    })
                    .await
                    .unwrap()
            }));
        }

        let mut misses = 0;
        let mut hits = 0;
        for task in tasks {
            match task.await.unwrap().cache_status {
                CacheStatus::Miss => misses += 1,
                CacheStatus::Hit => hits += 1,
                CacheStatus::Bypass => panic!("enabled cache should not bypass"),
            }
        }
        assert_eq!(misses, 1);
        assert_eq!(hits, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disabled_cache_bypasses_without_changing_results() {
        let cache = QueryCache::new(false, 0);
        let calls = Arc::new(AtomicUsize::new(0));

        let first = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        let second = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;

        assert_eq!(first.cache_status, CacheStatus::Bypass);
        assert_eq!(second.cache_status, CacheStatus::Bypass);
        assert_eq!(first.data, "value-1");
        assert_eq!(second.data, "value-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_loads_are_not_cached() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));
        let failed_calls = calls.clone();

        let first = cache
            .get_or_try_init("topic:orders".to_string(), Duration::from_secs(1), || async move {
                failed_calls.fetch_add(1, Ordering::SeqCst);
                Err::<String, _>("backend unavailable")
            })
            .await;
        let second = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;

        assert_eq!(first, Err("backend unavailable"));
        assert_eq!(second.cache_status, CacheStatus::Miss);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.len().await, 1);
    }

    #[tokio::test]
    async fn explicit_invalidation_forces_reload() {
        let cache = QueryCache::new(true, 8);
        let calls = Arc::new(AtomicUsize::new(0));

        let _ = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;
        let removed = cache.clear().await;
        let reloaded = load_string(&cache, "topic:orders", Duration::from_secs(1), calls.clone()).await;

        assert_eq!(removed, 1);
        assert_eq!(reloaded.cache_status, CacheStatus::Miss);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.metrics().invalidations, 1);
    }

    #[tokio::test]
    async fn invalidation_during_load_prevents_stale_reinsert() {
        let cache = QueryCache::new(true, 8);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let task_cache = cache.clone();
        let task_started = started.clone();
        let task_release = release.clone();
        let task = tokio::spawn(async move {
            task_cache
                .get_or_try_init("topic:orders".to_string(), Duration::from_secs(1), || async move {
                    task_started.notify_one();
                    task_release.notified().await;
                    Ok::<_, ()>("stale".to_string())
                })
                .await
                .unwrap()
        });

        started.notified().await;
        assert_eq!(cache.clear().await, 0);
        release.notify_one();
        let result = task.await.unwrap();

        assert_eq!(result.data, "stale");
        assert_eq!(result.cache_status, CacheStatus::Miss);
        assert_eq!(cache.len().await, 0);
    }

    async fn load_string(cache: &QueryCache, key: &str, ttl: Duration, calls: Arc<AtomicUsize>) -> QueryResult<String> {
        cache
            .get_or_try_init(key.to_string(), ttl, || async move {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, ()>(format!("value-{call}"))
            })
            .await
            .unwrap()
    }
}
