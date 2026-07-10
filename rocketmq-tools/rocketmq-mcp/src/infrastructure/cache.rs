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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::model::contract::observed_at;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryResult;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bypasses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub coalesced_waiters: u64,
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
    generation: AtomicU64,
    state: Mutex<CacheState>,
    flights: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    metrics: CacheMetrics,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>,
    insertion_order: VecDeque<String>,
}

struct CacheEntry {
    value: Arc<dyn Any + Send + Sync>,
    observed_at: String,
    inserted_at: Instant,
    expires_at: Instant,
}

impl QueryCache {
    pub(crate) fn new(enabled: bool, capacity: usize) -> Self {
        Self {
            inner: Arc::new(QueryCacheInner {
                enabled,
                capacity,
                generation: AtomicU64::new(0),
                state: Mutex::new(CacheState::default()),
                flights: Mutex::new(HashMap::new()),
                metrics: CacheMetrics::default(),
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
        T: Clone + Send + Sync + 'static,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<T, E>>,
    {
        let cancellation = CancellationToken::new();
        self.get_or_try_init_cancellable(key, ttl, &cancellation, || unreachable!(), load)
            .await
    }

    pub(crate) async fn get_or_try_init_cancellable<T, E, Load, LoadFuture, Cancelled>(
        &self,
        key: String,
        ttl: Duration,
        cancellation: &CancellationToken,
        cancelled: Cancelled,
        load: Load,
    ) -> Result<QueryResult<T>, E>
    where
        T: Clone + Send + Sync + 'static,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<T, E>>,
        Cancelled: FnOnce() -> E,
    {
        if !self.inner.enabled || self.inner.capacity == 0 || ttl.is_zero() {
            self.inner.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
            return load().await.map(|data| QueryResult {
                data,
                observed_at: observed_at(),
                freshness_ms: 0,
                cache_status: CacheStatus::Bypass,
            });
        }

        if let Some(result) = self.get(&key).await {
            return Ok(result);
        }

        let (flight, coalesced) = self.flight_lock(&key).await;
        if coalesced {
            self.inner.metrics.coalesced_waiters.fetch_add(1, Ordering::Relaxed);
        }
        let _flight_guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled()),
            guard = flight.lock() => guard,
        };
        if let Some(result) = self.get(&key).await {
            return Ok(result);
        }

        let generation = self.inner.generation.load(Ordering::Acquire);
        let data = load().await?;
        let observed_at = observed_at();
        self.insert_if_current(key, data.clone(), observed_at.clone(), ttl, generation)
            .await;
        self.inner.metrics.misses.fetch_add(1, Ordering::Relaxed);
        Ok(QueryResult {
            data,
            observed_at,
            freshness_ms: 0,
            cache_status: CacheStatus::Miss,
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
        let mut state = self.inner.state.lock().await;
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        let removed = state.entries.len();
        state.entries.clear();
        state.insertion_order.clear();
        self.inner.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
        removed
    }

    async fn get<T>(&self, key: &str) -> Option<QueryResult<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut state = self.inner.state.lock().await;
        let now = Instant::now();
        let expired = state.entries.get(key).is_some_and(|entry| entry.expires_at <= now);
        if expired {
            remove_entry(&mut state, key);
            return None;
        }
        let entry = state.entries.get(key)?;
        let data = entry.value.downcast_ref::<T>()?.clone();
        let freshness_ms = now
            .saturating_duration_since(entry.inserted_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let result = QueryResult {
            data,
            observed_at: entry.observed_at.clone(),
            freshness_ms,
            cache_status: CacheStatus::Hit,
        };
        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
        Some(result)
    }

    async fn insert_if_current<T>(&self, key: String, value: T, observed_at: String, ttl: Duration, generation: u64)
    where
        T: Send + Sync + 'static,
    {
        let mut state = self.inner.state.lock().await;
        if self.inner.generation.load(Ordering::Acquire) != generation {
            return;
        }
        remove_entry(&mut state, &key);
        while state.entries.len() >= self.inner.capacity {
            let Some(oldest) = state.insertion_order.pop_front() else {
                break;
            };
            if state.entries.remove(&oldest).is_some() {
                self.inner.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        let inserted_at = Instant::now();
        state.insertion_order.push_back(key.clone());
        state.entries.insert(
            key,
            CacheEntry {
                value: Arc::new(value),
                observed_at,
                inserted_at,
                expires_at: inserted_at + ttl,
            },
        );
    }

    async fn flight_lock(&self, key: &str) -> (Arc<Mutex<()>>, bool) {
        let mut flights = self.inner.flights.lock().await;
        flights.retain(|_, flight| flight.strong_count() > 0);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            return (flight, true);
        }
        let flight = Arc::new(Mutex::new(()));
        flights.insert(key.to_string(), Arc::downgrade(&flight));
        (flight, false)
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.inner.state.lock().await.entries.len()
    }
}

fn remove_entry(state: &mut CacheState, key: &str) {
    if state.entries.remove(key).is_some() {
        state.insertion_order.retain(|candidate| candidate != key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Notify;

    use crate::model::contract::CacheStatus;

    use super::*;

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
