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

//! How the query cache answers the callers of a key whose load failed.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    /// The source cannot be reached.
    Unavailable,
    /// The source did not answer in time.
    TimedOut,
    /// The source answered that it cannot serve the request.
    Refused,
    /// The caller that loaded gave up.
    Cancelled,
}

impl CachedFailure for Failure {
    fn is_shared(&self) -> bool {
        !matches!(self, Self::Cancelled)
    }

    fn is_kept(&self) -> bool {
        matches!(self, Self::Unavailable)
    }

    fn allows_stale(&self) -> bool {
        matches!(self, Self::Unavailable | Self::TimedOut)
    }
}

/// A value that has no serialized form, so the cache can only size it by what it retains.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Opaque(String);

impl RetainedSize for Opaque {
    fn retained_heap_size(&self) -> usize {
        self.0.retained_heap_size()
    }
}

const KEY: &str = "broker:a";
const TTL: Duration = Duration::from_secs(5);

/// Calls the cache for [`KEY`] with a load that counts itself and ends with `outcome`.
async fn call(
    cache: &QueryCache,
    loads: &Arc<AtomicUsize>,
    outcome: Result<&'static str, Failure>,
) -> Result<QueryResult<String>, Failure> {
    let loads = loads.clone();
    cache
        .get_or_try_init(KEY.to_string(), TTL, || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            outcome.map(str::to_string)
        })
        .await
}

/// Starts a load of [`KEY`] that ends with `failure` once `release` is notified, then starts
/// `waiters` callers whose own load would succeed, and returns after all of them wait for it.
async fn fail_a_load_with_waiters(
    cache: &QueryCache,
    loads: &Arc<AtomicUsize>,
    failure: Failure,
    waiters: usize,
) -> Vec<tokio::task::JoinHandle<Result<QueryResult<String>, Failure>>> {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut calls = Vec::new();
    calls.push(tokio::spawn({
        let cache = cache.clone();
        let loads = loads.clone();
        let started = started.clone();
        let release = release.clone();
        async move {
            cache
                .get_or_try_init(KEY.to_string(), TTL, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                    Err::<String, _>(failure)
                })
                .await
        }
    }));
    started.notified().await;
    for _ in 0..waiters {
        calls.push(tokio::spawn({
            let cache = cache.clone();
            let loads = loads.clone();
            async move { call(&cache, &loads, Ok("loaded by a waiter")).await }
        }));
    }
    while cache.metrics().coalesced_waiters < waiters as u64 {
        tokio::task::yield_now().await;
    }
    release.notify_one();
    calls
}

#[tokio::test]
async fn failed_leader_shares_its_failure_with_waiters() {
    let cache = QueryCache::new(true, 8);
    let loads = Arc::new(AtomicUsize::new(0));

    for call in fail_a_load_with_waiters(&cache, &loads, Failure::Refused, 4).await {
        assert_eq!(call.await.unwrap(), Err(Failure::Refused));
    }
    assert_eq!(loads.load(Ordering::SeqCst), 1, "no waiter may load again");

    // The failure belonged to that load: a call that comes later loads.
    let later = call(&cache, &loads, Ok("recovered")).await.unwrap();
    assert_eq!(later.cache_status, CacheStatus::Miss);
    assert_eq!(later.data, "recovered");
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_failure_that_is_not_shared_leaves_the_load_to_the_next_waiter() {
    let cache = QueryCache::new(true, 8);
    let loads = Arc::new(AtomicUsize::new(0));

    let mut calls = fail_a_load_with_waiters(&cache, &loads, Failure::Cancelled, 3).await;
    assert_eq!(calls.remove(0).await.unwrap(), Err(Failure::Cancelled));
    let mut statuses = Vec::new();
    for call in calls {
        let result = call.await.unwrap().unwrap();
        assert_eq!(result.data, "loaded by a waiter");
        statuses.push(result.cache_status);
    }
    statuses.sort_by_key(|status| *status == CacheStatus::Hit);

    // One waiter loaded in place of the caller that gave up; the others read what it left.
    assert_eq!(statuses, [CacheStatus::Miss, CacheStatus::Hit, CacheStatus::Hit]);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn an_unavailable_source_is_asked_once_in_the_time_its_failure_is_kept() {
    let cache = QueryCache::new(true, 8);
    let loads = Arc::new(AtomicUsize::new(0));

    assert_eq!(
        call(&cache, &loads, Err(Failure::Unavailable)).await,
        Err(Failure::Unavailable)
    );
    tokio::time::advance(FAILURE_KEPT_FOR - Duration::from_millis(1)).await;
    assert_eq!(call(&cache, &loads, Ok("unused")).await, Err(Failure::Unavailable));
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_millis(1)).await;
    let recovered = call(&cache, &loads, Ok("recovered")).await.unwrap();
    assert_eq!(recovered.cache_status, CacheStatus::Miss);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert!(cache.inner.state.lock().await.kept_failures.is_empty());

    // A failure that says nothing about the source is not kept at all.
    cache.clear().await;
    assert_eq!(
        call(&cache, &loads, Err(Failure::TimedOut)).await,
        Err(Failure::TimedOut)
    );
    assert_eq!(call(&cache, &loads, Err(Failure::Refused)).await, Err(Failure::Refused));
    assert_eq!(loads.load(Ordering::SeqCst), 4);

    // An invalidation asks for a reload, so it drops a kept failure too.
    assert_eq!(
        call(&cache, &loads, Err(Failure::Unavailable)).await,
        Err(Failure::Unavailable)
    );
    cache.clear().await;
    assert_eq!(call(&cache, &loads, Ok("reloaded")).await.unwrap().data, "reloaded");
    assert_eq!(loads.load(Ordering::SeqCst), 6);
}

#[tokio::test(start_paused = true)]
async fn a_failure_is_kept_no_longer_than_a_value_would_be() {
    let cache = QueryCache::new(true, 8);
    let ttl = Duration::from_millis(200);
    let loads = Arc::new(AtomicUsize::new(0));
    let fail = || {
        let loads = loads.clone();
        cache.get_or_try_init("short".to_string(), ttl, || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Err::<String, _>(Failure::Unavailable)
        })
    };

    assert_eq!(fail().await, Err(Failure::Unavailable));
    tokio::time::advance(ttl).await;
    assert_eq!(fail().await, Err(Failure::Unavailable));
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn kept_failures_are_bounded_by_the_capacity_of_the_cache() {
    let cache = QueryCache::new(true, 2);
    for key in ["a", "b", "c"] {
        let failed = cache
            .get_or_try_init(key.to_string(), TTL, || async {
                Err::<String, _>(Failure::Unavailable)
            })
            .await;
        assert_eq!(failed, Err(Failure::Unavailable));
    }
    assert_eq!(cache.inner.state.lock().await.kept_failures.len(), 2);

    // The next failure after their moment takes the place of the ones that are over.
    tokio::time::advance(FAILURE_KEPT_FOR).await;
    let failed = cache
        .get_or_try_init("d".to_string(), TTL, || async {
            Err::<String, _>(Failure::Unavailable)
        })
        .await;
    assert_eq!(failed, Err(Failure::Unavailable));
    let state = cache.inner.state.lock().await;
    assert_eq!(state.kept_failures.keys().collect::<Vec<_>>(), ["d"]);
}

#[tokio::test(start_paused = true)]
async fn stale_value_is_served_with_a_warning_when_the_backend_fails() {
    let window = Duration::from_secs(30);
    let cache = QueryCache::new(true, 8).with_stale_if_error(window);
    let loads = Arc::new(AtomicUsize::new(0));
    let loaded = call(&cache, &loads, Ok("value")).await.unwrap();
    assert!(loaded.warnings.is_empty());

    // Expired, and the source is down: the old value answers, and says so.
    tokio::time::advance(TTL + Duration::from_secs(1)).await;
    let stale = call(&cache, &loads, Err(Failure::Unavailable)).await.unwrap();
    assert_eq!(stale.data, "value");
    assert_eq!(stale.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(stale.cache_status, CacheStatus::Hit);
    assert_eq!(stale.freshness_ms, 6_000);
    assert_eq!(stale.observed_at, loaded.observed_at);
    assert!(!stale.partial);
    assert_eq!(loads.load(Ordering::SeqCst), 2);

    // While the failure is kept, the old value answers without another load.
    let again = call(&cache, &loads, Ok("unused")).await.unwrap();
    assert_eq!(again.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(loads.load(Ordering::SeqCst), 2);

    // A source that answers too late is treated the same; one that refuses the request is not.
    tokio::time::advance(FAILURE_KEPT_FOR).await;
    let late = call(&cache, &loads, Err(Failure::TimedOut)).await.unwrap();
    assert_eq!(late.warnings, [STALE_CACHE_VALUE_WARNING]);
    assert_eq!(late.freshness_ms, 7_000);
    assert_eq!(call(&cache, &loads, Err(Failure::Refused)).await, Err(Failure::Refused));

    // A value loaded again is current, and unmarked.
    let reloaded = call(&cache, &loads, Ok("newer")).await.unwrap();
    assert_eq!((reloaded.cache_status, reloaded.warnings.len()), (CacheStatus::Miss, 0));
    let hit = call(&cache, &loads, Ok("unused")).await.unwrap();
    assert_eq!((hit.data.as_str(), hit.warnings.len()), ("newer", 0));

    // Past the window the failure is the answer, and the value is gone.
    tokio::time::advance(TTL + window).await;
    assert_eq!(
        call(&cache, &loads, Err(Failure::Unavailable)).await,
        Err(Failure::Unavailable)
    );
    assert_eq!(cache.len().await, 0);
    assert_eq!(cache.retained_bytes().await, 0);
}

#[tokio::test(start_paused = true)]
async fn waiters_of_a_failed_load_get_the_stale_value_too() {
    let cache = QueryCache::new(true, 8).with_stale_if_error(Duration::from_secs(30));
    let loads = Arc::new(AtomicUsize::new(0));
    call(&cache, &loads, Ok("value")).await.unwrap();
    tokio::time::advance(TTL).await;

    for call in fail_a_load_with_waiters(&cache, &loads, Failure::TimedOut, 3).await {
        let stale = call.await.unwrap().unwrap();
        assert_eq!(stale.data, "value");
        assert_eq!(stale.warnings, [STALE_CACHE_VALUE_WARNING]);
    }
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn an_expired_value_is_not_served_without_a_window_or_after_an_invalidation() {
    let loads = Arc::new(AtomicUsize::new(0));

    let strict = QueryCache::new(true, 8);
    call(&strict, &loads, Ok("value")).await.unwrap();
    tokio::time::advance(TTL).await;
    assert_eq!(
        call(&strict, &loads, Err(Failure::Unavailable)).await,
        Err(Failure::Unavailable)
    );
    assert_eq!(strict.len().await, 0);

    let lenient = QueryCache::new(true, 8).with_stale_if_error(Duration::from_secs(30));
    call(&lenient, &loads, Ok("value")).await.unwrap();
    tokio::time::advance(TTL).await;
    assert_eq!(lenient.clear().await, 1);
    assert_eq!(
        call(&lenient, &loads, Err(Failure::Unavailable)).await,
        Err(Failure::Unavailable)
    );
}

#[tokio::test]
async fn a_value_is_sized_by_what_it_retains() {
    let cache = QueryCache::new(true, 8);
    let load = || async { Ok::<_, Failure>(Opaque("x".repeat(128))) };

    let first = cache.get_or_try_init(KEY.to_string(), TTL, load).await.unwrap();
    let second = cache.get_or_try_init(KEY.to_string(), TTL, load).await.unwrap();

    assert_eq!(
        (first.cache_status, second.cache_status),
        (CacheStatus::Miss, CacheStatus::Hit)
    );
    let observed = first.observed_at.clone();
    let payload = QueryPayload::complete(first.data);
    assert_eq!(
        cache.retained_bytes().await,
        retained_cache_entry_bytes(&KEY.to_string(), &observed, payload.retained_size())
    );
}
