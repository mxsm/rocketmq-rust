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

use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use moka::sync::SegmentedCache;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SharedRocketMQError;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use tokio::sync::watch;
use tokio::time::Instant;

use super::route_lookup::RouteLookupOutcome;
use crate::config::NamesrvConfig;

const CACHE_SHARDS: usize = 16;
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct LookupCacheKey {
    endpoint_generation: u64,
    topic: CheetahString,
}

impl LookupCacheKey {
    pub(super) fn new(endpoint_generation: u64, topic: CheetahString) -> Self {
        Self {
            endpoint_generation,
            topic,
        }
    }

    fn estimated_overhead(&self) -> usize {
        size_of::<u64>() + self.topic.len() + 32
    }
}

#[derive(Clone)]
enum CachedRoute {
    Found(Arc<TopicRouteData>),
    Missing,
}

impl CachedRoute {
    fn from_route(route: Option<TopicRouteData>) -> Self {
        route.map_or(Self::Missing, |route| Self::Found(Arc::new(route)))
    }

    fn into_owned(self) -> Option<TopicRouteData> {
        match self {
            Self::Found(route) => Some(route.as_ref().clone()),
            Self::Missing => None,
        }
    }

    fn is_found(&self) -> bool {
        matches!(self, Self::Found(_))
    }
}

#[derive(Clone)]
struct CachedEntry {
    route: CachedRoute,
    expires_at: Instant,
    response_bytes: usize,
}

#[derive(Clone)]
enum FlightState {
    Pending,
    Complete(Result<CachedRoute, SharedRocketMQError>),
    Unavailable,
    Cancelled,
}

struct LookupFlight {
    state: watch::Sender<FlightState>,
}

impl LookupFlight {
    fn new() -> Self {
        let (state, _receiver) = watch::channel(FlightState::Pending);
        Self { state }
    }
}

pub(super) struct ResolvedRoute {
    pub(super) route: Option<TopicRouteData>,
    pub(super) response_bytes: usize,
}

#[derive(Clone, Copy)]
pub(super) struct LookupCacheConfig {
    positive_ttl: Duration,
    negative_ttl: Duration,
    max_entries: u64,
    max_bytes: u64,
}

impl LookupCacheConfig {
    pub(super) fn from_namesrv_config(config: &NamesrvConfig) -> Self {
        Self {
            positive_ttl: Duration::from_millis(config.cluster_test_route_cache_positive_ttl_millis),
            negative_ttl: Duration::from_millis(config.cluster_test_route_cache_negative_ttl_millis),
            max_entries: config.cluster_test_route_cache_max_entries,
            max_bytes: config.cluster_test_route_cache_max_bytes,
        }
    }
}

impl Default for LookupCacheConfig {
    fn default() -> Self {
        Self {
            positive_ttl: Duration::from_secs(1),
            negative_ttl: Duration::from_millis(250),
            max_entries: 1_000,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

pub(super) struct ClusterTestLookupCache {
    entries: SegmentedCache<LookupCacheKey, CachedEntry>,
    flights: DashMap<LookupCacheKey, Arc<LookupFlight>>,
    positive_ttl: Duration,
    negative_ttl: Duration,
    max_bytes: usize,
}

impl ClusterTestLookupCache {
    pub(super) fn new(config: LookupCacheConfig) -> Self {
        let max_bytes = config.max_bytes.max(1);
        let max_entries = config.max_entries.max(1);
        let minimum_entry_weight = max_bytes.div_ceil(max_entries).clamp(1, u64::from(u32::MAX)) as u32;
        let entries = SegmentedCache::builder(CACHE_SHARDS)
            .max_capacity(max_bytes)
            .weigher(move |key: &LookupCacheKey, value: &CachedEntry| {
                let estimated = value.response_bytes.saturating_add(key.estimated_overhead());
                u32::try_from(estimated).unwrap_or(u32::MAX).max(minimum_entry_weight)
            })
            .build();
        Self {
            entries,
            flights: DashMap::new(),
            positive_ttl: config.positive_ttl,
            negative_ttl: config.negative_ttl,
            max_bytes: usize::try_from(max_bytes).unwrap_or(usize::MAX),
        }
    }

    pub(super) async fn get_or_resolve<F, Fut>(
        &self,
        key: LookupCacheKey,
        resolve: F,
    ) -> RocketMQResult<RouteLookupOutcome<Option<TopicRouteData>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = RocketMQResult<RouteLookupOutcome<ResolvedRoute>>>,
    {
        if let Some(entry) = self.entries.get(&key) {
            if entry.expires_at > Instant::now() {
                return Ok(RouteLookupOutcome::Resolved(entry.route.into_owned()));
            }
            self.entries.invalidate(&key);
        }

        let (flight, is_leader) = match self.flights.entry(key.clone()) {
            Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            Entry::Vacant(entry) => {
                let flight = Arc::new(LookupFlight::new());
                entry.insert(Arc::clone(&flight));
                (flight, true)
            }
        };

        if !is_leader {
            return wait_for_flight(&flight).await;
        }

        let mut leader = FlightLeaderGuard::new(&self.flights, key.clone(), Arc::clone(&flight));
        match resolve().await {
            Ok(RouteLookupOutcome::Resolved(resolved)) => {
                let response_bytes = resolved.response_bytes;
                let route = CachedRoute::from_route(resolved.route);
                if response_bytes <= self.max_bytes {
                    let ttl = if route.is_found() {
                        self.positive_ttl
                    } else {
                        self.negative_ttl
                    };
                    self.entries.insert(
                        key,
                        CachedEntry {
                            route: route.clone(),
                            expires_at: Instant::now() + ttl,
                            response_bytes,
                        },
                    );
                }
                leader.complete(FlightState::Complete(Ok(route.clone())));
                Ok(RouteLookupOutcome::Resolved(route.into_owned()))
            }
            Ok(RouteLookupOutcome::Unavailable) => {
                leader.complete(FlightState::Unavailable);
                Ok(RouteLookupOutcome::Unavailable)
            }
            Ok(RouteLookupOutcome::Cancelled) => {
                leader.complete(FlightState::Cancelled);
                Ok(RouteLookupOutcome::Cancelled)
            }
            Err(error) => {
                let error = SharedRocketMQError::new(error);
                leader.complete(FlightState::Complete(Err(error.clone())));
                Err(error.into_error())
            }
        }
    }

    #[cfg(test)]
    fn stats(&self) -> (u64, u64, usize) {
        self.entries.run_pending_tasks();
        (
            self.entries.entry_count(),
            self.entries.weighted_size(),
            self.flights.len(),
        )
    }
}

async fn wait_for_flight(flight: &LookupFlight) -> RocketMQResult<RouteLookupOutcome<Option<TopicRouteData>>> {
    let mut state = flight.state.subscribe();
    loop {
        match state.borrow_and_update().clone() {
            FlightState::Pending => {}
            FlightState::Complete(Ok(route)) => return Ok(RouteLookupOutcome::Resolved(route.into_owned())),
            FlightState::Complete(Err(error)) => return Err(error.into_error()),
            FlightState::Unavailable => return Ok(RouteLookupOutcome::Unavailable),
            FlightState::Cancelled => return Ok(RouteLookupOutcome::Cancelled),
        }
        if state.changed().await.is_err() {
            return Ok(RouteLookupOutcome::Cancelled);
        }
    }
}

struct FlightLeaderGuard<'a> {
    flights: &'a DashMap<LookupCacheKey, Arc<LookupFlight>>,
    key: LookupCacheKey,
    flight: Arc<LookupFlight>,
    completed: bool,
}

impl<'a> FlightLeaderGuard<'a> {
    fn new(
        flights: &'a DashMap<LookupCacheKey, Arc<LookupFlight>>,
        key: LookupCacheKey,
        flight: Arc<LookupFlight>,
    ) -> Self {
        Self {
            flights,
            key,
            flight,
            completed: false,
        }
    }

    fn complete(&mut self, state: FlightState) {
        let _ = self.flight.state.send(state);
        self.flights
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.flight));
        self.completed = true;
    }
}

impl Drop for FlightLeaderGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let _ = self.flight.state.send(FlightState::Cancelled);
        self.flights
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.flight));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use rocketmq_error::RocketMQError;
    use tokio::sync::Barrier;
    use tokio::sync::Notify;

    use super::*;

    fn key(topic: &str) -> LookupCacheKey {
        LookupCacheKey::new(1, CheetahString::from(topic))
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_resolution() {
        let cache = Arc::new(ClusterTestLookupCache::new(LookupCacheConfig::default()));
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(101));
        let release = Arc::new(Notify::new());
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                cache
                    .get_or_resolve(key("missing"), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        release.notified().await;
                        Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                            route: None,
                            response_bytes: 0,
                        }))
                    })
                    .await
            }));
        }

        start.wait().await;
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        release.notify_waiters();
        for task in tasks {
            assert!(matches!(
                task.await.unwrap().unwrap(),
                RouteLookupOutcome::Resolved(None)
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.stats().2, 0);
    }

    #[tokio::test]
    async fn positive_and_negative_entries_expire_independently() {
        let cache = ClusterTestLookupCache::new(LookupCacheConfig {
            positive_ttl: Duration::from_millis(20),
            negative_ttl: Duration::from_millis(5),
            max_entries: 10,
            max_bytes: 1024,
        });
        let calls = AtomicUsize::new(0);

        for _ in 0..2 {
            cache
                .get_or_resolve(key("negative"), || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                        route: None,
                        response_bytes: 0,
                    }))
                })
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        cache
            .get_or_resolve(key("negative"), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                    route: None,
                    response_bytes: 0,
                }))
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let positive_calls = AtomicUsize::new(0);
        for _ in 0..2 {
            cache
                .get_or_resolve(key("positive"), || async {
                    positive_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                        route: Some(TopicRouteData::default()),
                        response_bytes: 1,
                    }))
                })
                .await
                .unwrap();
        }
        assert_eq!(positive_calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(21)).await;
        cache
            .get_or_resolve(key("positive"), || async {
                positive_calls.fetch_add(1, Ordering::SeqCst);
                Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                    route: Some(TopicRouteData::default()),
                    response_bytes: 1,
                }))
            })
            .await
            .unwrap();
        assert_eq!(positive_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_and_cancelled_resolutions_leave_no_cache_or_flight() {
        let cache = Arc::new(ClusterTestLookupCache::new(LookupCacheConfig::default()));
        let error = cache
            .get_or_resolve(key("error"), || async {
                Err(RocketMQError::Network(Arc::new(rocketmq_error::Error::new(
                    &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                ))))
            })
            .await;
        assert!(error.is_err());
        assert_eq!(cache.stats(), (0, 0, 0));

        let entered = Arc::new(Notify::new());
        let leader = {
            let cache = Arc::clone(&cache);
            let entered = Arc::clone(&entered);
            tokio::spawn(async move {
                cache
                    .get_or_resolve(key("cancelled"), || async move {
                        entered.notify_one();
                        std::future::pending().await
                    })
                    .await
            })
        };
        entered.notified().await;
        leader.abort();
        let _ = leader.await;
        assert_eq!(cache.stats().2, 0);
    }

    #[tokio::test]
    async fn coalesced_failure_retains_the_canonical_network_allocation() {
        let flight = LookupFlight::new();
        let canonical = Arc::new(rocketmq_error::Error::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            std::io::Error::other("connection refused"),
        ));
        let expected = Arc::clone(&canonical);
        let shared = SharedRocketMQError::new(RocketMQError::Network(canonical));
        flight.state.send_replace(FlightState::Complete(Err(shared)));

        let error = wait_for_flight(&flight)
            .await
            .expect_err("coalesced failure should be returned");
        let RocketMQError::Shared(shared) = error else {
            panic!("coalesced failures must retain the shared typed error");
        };
        let RocketMQError::Network(actual) = shared.as_error() else {
            panic!("coalesced failures must retain the canonical Network error");
        };
        assert!(Arc::ptr_eq(actual, &expected));
        assert!(std::error::Error::source(actual.as_ref()).is_some());
    }

    #[tokio::test]
    async fn unavailable_and_cancelled_flights_are_normal_typed_outcomes() {
        let unavailable = LookupFlight::new();
        unavailable.state.send_replace(FlightState::Unavailable);
        assert!(matches!(
            wait_for_flight(&unavailable).await,
            Ok(RouteLookupOutcome::Unavailable)
        ));

        let cancelled = LookupFlight::new();
        cancelled.state.send_replace(FlightState::Cancelled);
        assert!(matches!(
            wait_for_flight(&cancelled).await,
            Ok(RouteLookupOutcome::Cancelled)
        ));
    }

    #[tokio::test]
    async fn unavailable_resolution_is_not_negative_cached() {
        let cache = ClusterTestLookupCache::new(LookupCacheConfig::default());
        let calls = AtomicUsize::new(0);

        for _ in 0..2 {
            let outcome = cache
                .get_or_resolve(key("unavailable"), || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(RouteLookupOutcome::Unavailable)
                })
                .await
                .expect("endpoint absence is a normal lookup outcome");
            assert!(matches!(outcome, RouteLookupOutcome::Unavailable));
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.stats(), (0, 0, 0));
    }

    #[tokio::test]
    async fn leader_drop_notifies_followers_with_typed_cancellation() {
        let cache = ClusterTestLookupCache::new(LookupCacheConfig::default());
        let lookup_key = key("leader-drop");
        let flight = Arc::new(LookupFlight::new());
        cache.flights.insert(lookup_key.clone(), Arc::clone(&flight));
        let _receiver = flight.state.subscribe();
        let leader = FlightLeaderGuard::new(&cache.flights, lookup_key, Arc::clone(&flight));

        drop(leader);

        assert!(matches!(
            wait_for_flight(&flight).await,
            Ok(RouteLookupOutcome::Cancelled)
        ));
        assert_eq!(cache.stats(), (0, 0, 0));
    }

    #[tokio::test]
    async fn entry_count_bytes_and_oversize_bypass_are_bounded() {
        let cache = ClusterTestLookupCache::new(LookupCacheConfig {
            positive_ttl: Duration::from_secs(1),
            negative_ttl: Duration::from_secs(1),
            max_entries: 2,
            max_bytes: 300,
        });
        for index in 0..3 {
            cache
                .get_or_resolve(key(&format!("topic-{index}")), || async {
                    Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                        route: None,
                        response_bytes: 80,
                    }))
                })
                .await
                .unwrap();
        }
        let (entries, weighted_bytes, _) = cache.stats();
        assert!(entries <= 2);
        assert!(weighted_bytes <= 300);

        let calls = AtomicUsize::new(0);
        for _ in 0..2 {
            cache
                .get_or_resolve(key("oversize"), || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(RouteLookupOutcome::Resolved(ResolvedRoute {
                        route: None,
                        response_bytes: 301,
                    }))
                })
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
