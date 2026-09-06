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

use std::error::Error as StdError;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use moka::sync::SegmentedCache;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;

use crate::config::NamesrvConfig;
use crate::route::topic_route_snapshot::RouteVariant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum JsonEncoding {
    Legacy,
    Standard,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RouteCacheKey {
    topic: CheetahString,
    snapshot_version: u64,
    variant: RouteVariant,
    encoding: JsonEncoding,
}

impl RouteCacheKey {
    pub(crate) fn new(
        topic: CheetahString,
        snapshot_version: u64,
        variant: RouteVariant,
        encoding: JsonEncoding,
    ) -> Self {
        Self {
            topic,
            snapshot_version,
            variant,
            encoding,
        }
    }

    fn estimated_overhead(&self) -> usize {
        self.topic.len() + size_of::<u64>() + size_of::<RouteVariant>() + size_of::<JsonEncoding>() + 32
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RouteCachePolicy {
    pub(crate) enabled: bool,
    pub(crate) zone_requested: bool,
    pub(crate) order_enabled: bool,
    pub(crate) external_route: bool,
}

impl RouteCachePolicy {
    pub(crate) fn is_eligible(self) -> bool {
        self.enabled && !self.zone_requested && !self.order_enabled && !self.external_route
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RouteCacheOutcomeKind {
    Hit,
    Miss,
    Oversize,
}

pub(crate) struct RouteCacheOutcome {
    pub(crate) body: Bytes,
    pub(crate) kind: RouteCacheOutcomeKind,
}

#[derive(Clone)]
enum CachedRouteBody {
    Ready(Bytes),
    Oversize(Bytes),
}

impl CachedRouteBody {
    fn bytes(&self) -> &Bytes {
        match self {
            Self::Ready(body) | Self::Oversize(body) => body,
        }
    }
}

#[derive(Debug)]
struct SharedRouteEncodeFailure {
    source: Arc<RocketMQError>,
}

impl fmt::Display for SharedRouteEncodeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NameServer route response encoding failed")
    }
}

impl StdError for SharedRouteEncodeFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

fn route_encode_failure(source: Arc<RocketMQError>) -> RocketMQError {
    RocketMQError::Serialization(SerializationError::source(
        "encode",
        "namesrv-route-response",
        SharedRouteEncodeFailure { source },
    ))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RouteResponseCacheStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) oversize: u64,
    pub(crate) evictions: u64,
    pub(crate) entry_count: u64,
    pub(crate) weighted_size: u64,
}

#[derive(Default)]
struct RouteResponseCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    oversize: AtomicU64,
    evictions: AtomicU64,
}

pub(crate) struct RouteResponseCache {
    cache: SegmentedCache<RouteCacheKey, CachedRouteBody>,
    max_single_response_bytes: usize,
    counters: Arc<RouteResponseCacheCounters>,
}

impl RouteResponseCache {
    pub(crate) fn from_namesrv_config(config: &NamesrvConfig) -> Self {
        Self::new(RouteResponseCacheConfig {
            max_bytes: config.namesrv_route_response_cache_max_bytes.max(1),
            max_entries: config.namesrv_route_response_cache_max_entries.max(1),
            max_single_response_bytes: config.namesrv_route_response_cache_max_single_response_bytes.max(1),
            shards: config.namesrv_route_response_cache_shards.max(1),
        })
    }

    fn new(config: RouteResponseCacheConfig) -> Self {
        let max_bytes = config.max_bytes.max(1);
        let max_entries = config.max_entries.max(1);
        let minimum_entry_weight = max_bytes.div_ceil(max_entries).clamp(1, u64::from(u32::MAX)) as u32;
        let counters = Arc::new(RouteResponseCacheCounters::default());
        let eviction_counters = Arc::clone(&counters);
        let cache = SegmentedCache::builder(config.shards.max(1))
            .max_capacity(max_bytes)
            .weigher(move |key: &RouteCacheKey, value: &CachedRouteBody| {
                let estimated = value.bytes().len().saturating_add(key.estimated_overhead());
                u32::try_from(estimated).unwrap_or(u32::MAX).max(minimum_entry_weight)
            })
            .eviction_listener(move |_key, _value, _cause| {
                eviction_counters.evictions.fetch_add(1, Ordering::Relaxed);
            })
            .build();
        Self {
            cache,
            max_single_response_bytes: usize::try_from(config.max_single_response_bytes).unwrap_or(usize::MAX),
            counters,
        }
    }

    pub(crate) fn get_or_try_insert_with(
        &self,
        key: RouteCacheKey,
        encode: impl FnOnce() -> RocketMQResult<Vec<u8>>,
    ) -> RocketMQResult<RouteCacheOutcome> {
        if let Some(value) = self.cache.get(&key) {
            return Ok(match value {
                CachedRouteBody::Ready(body) => {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    RouteCacheOutcome {
                        body,
                        kind: RouteCacheOutcomeKind::Hit,
                    }
                }
                CachedRouteBody::Oversize(body) => {
                    self.cache.invalidate(&key);
                    self.counters.oversize.fetch_add(1, Ordering::Relaxed);
                    RouteCacheOutcome {
                        body,
                        kind: RouteCacheOutcomeKind::Oversize,
                    }
                }
            });
        }

        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        let max_single_response_bytes = self.max_single_response_bytes;
        let value = self
            .cache
            .try_get_with(key.clone(), || {
                encode().map(|body| {
                    let body = Bytes::from(body);
                    if body.len() > max_single_response_bytes {
                        CachedRouteBody::Oversize(body)
                    } else {
                        CachedRouteBody::Ready(body)
                    }
                })
            })
            .map_err(route_encode_failure)?;

        match value {
            CachedRouteBody::Ready(body) => Ok(RouteCacheOutcome {
                body,
                kind: RouteCacheOutcomeKind::Miss,
            }),
            CachedRouteBody::Oversize(body) => {
                self.cache.invalidate(&key);
                self.counters.oversize.fetch_add(1, Ordering::Relaxed);
                Ok(RouteCacheOutcome {
                    body,
                    kind: RouteCacheOutcomeKind::Oversize,
                })
            }
        }
    }

    pub(crate) fn stats(&self) -> RouteResponseCacheStats {
        RouteResponseCacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            oversize: self.counters.oversize.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
            entry_count: self.cache.entry_count(),
            weighted_size: self.cache.weighted_size(),
        }
    }

    #[cfg(test)]
    fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

struct RouteResponseCacheConfig {
    max_bytes: u64,
    max_entries: u64,
    max_single_response_bytes: u64,
    shards: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use rocketmq_error::Error;
    use rocketmq_error::PublicErrorView;
    use rocketmq_error::CORE_SERIALIZATION_FAILED;

    use super::*;

    fn find_source<'a, T>(mut error: &'a (dyn StdError + 'static)) -> Option<&'a T>
    where
        T: StdError + 'static,
    {
        loop {
            if let Some(source) = error.downcast_ref::<T>() {
                return Some(source);
            }
            error = error.source()?;
        }
    }

    fn cache(max_bytes: u64, max_entries: u64, max_single_response_bytes: u64) -> RouteResponseCache {
        RouteResponseCache::new(RouteResponseCacheConfig {
            max_bytes,
            max_entries,
            max_single_response_bytes,
            shards: 2,
        })
    }

    fn key(version: u64, variant: RouteVariant, encoding: JsonEncoding) -> RouteCacheKey {
        RouteCacheKey::new(
            CheetahString::from_static_str("cache-topic"),
            version,
            variant,
            encoding,
        )
    }

    #[test]
    fn unchanged_generation_hits_without_reencoding() {
        let cache = cache(1024, 10, 512);
        let encodes = AtomicUsize::new(0);
        let key = key(1, RouteVariant::Base, JsonEncoding::Legacy);

        let first = cache
            .get_or_try_insert_with(key.clone(), || {
                encodes.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3])
            })
            .unwrap();
        let second = cache
            .get_or_try_insert_with(key, || {
                encodes.fetch_add(1, Ordering::Relaxed);
                Ok(vec![4, 5, 6])
            })
            .unwrap();

        assert_eq!(first.kind, RouteCacheOutcomeKind::Miss);
        assert_eq!(second.kind, RouteCacheOutcomeKind::Hit);
        assert_eq!(second.body, Bytes::from_static(&[1, 2, 3]));
        assert_eq!(encodes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn version_variant_and_encoding_are_independent_cache_keys() {
        let cache = cache(4096, 16, 512);
        for (index, key) in [
            key(1, RouteVariant::Base, JsonEncoding::Legacy),
            key(2, RouteVariant::Base, JsonEncoding::Legacy),
            key(2, RouteVariant::ActingMaster, JsonEncoding::Legacy),
            key(2, RouteVariant::ActingMaster, JsonEncoding::Standard),
        ]
        .into_iter()
        .enumerate()
        {
            let outcome = cache.get_or_try_insert_with(key, || Ok(vec![index as u8])).unwrap();
            assert_eq!(outcome.kind, RouteCacheOutcomeKind::Miss);
        }
    }

    #[test]
    fn oversize_body_is_returned_but_not_retained() {
        let cache = cache(1024, 10, 4);
        let key = key(1, RouteVariant::Base, JsonEncoding::Legacy);

        let outcome = cache.get_or_try_insert_with(key.clone(), || Ok(vec![0; 5])).unwrap();
        cache.run_pending_tasks();

        assert_eq!(outcome.kind, RouteCacheOutcomeKind::Oversize);
        assert_eq!(outcome.body.len(), 5);
        assert_eq!(cache.stats().entry_count, 0);
        let second = cache.get_or_try_insert_with(key, || Ok(vec![1; 5])).unwrap();
        assert_eq!(second.kind, RouteCacheOutcomeKind::Oversize);
    }

    #[test]
    fn weight_floor_enforces_entry_count_and_total_bytes() {
        let cache = cache(300, 2, 200);
        for version in 1..=3 {
            cache
                .get_or_try_insert_with(key(version, RouteVariant::Base, JsonEncoding::Legacy), || {
                    Ok(vec![0; 80])
                })
                .unwrap();
        }
        cache.run_pending_tasks();
        let stats = cache.stats();

        assert!(stats.entry_count <= 2, "{stats:?}");
        assert!(stats.weighted_size <= 300, "{stats:?}");
    }

    #[test]
    fn shared_encode_failure_preserves_typed_source_and_is_not_cached() {
        let cache = cache(1024, 10, 512);
        let key = key(1, RouteVariant::Base, JsonEncoding::Legacy);
        let canonical = Arc::new(Error::caused_by(
            &CORE_SERIALIZATION_FAILED,
            std::io::Error::other("private route encoder detail"),
        ));

        let error =
            match cache.get_or_try_insert_with(key.clone(), || Err(RocketMQError::Shared(Arc::clone(&canonical)))) {
                Ok(_) => panic!("route encoding failure must remain visible"),
                Err(error) => error,
            };

        assert_eq!(error.descriptor(), &CORE_SERIALIZATION_FAILED);
        assert_eq!(error.descriptor().projection().remoting().code.as_i32(), 1);
        let context = error.context();
        let view = PublicErrorView::try_new(error.descriptor(), &context).expect("source context must be valid");
        assert_eq!(view.message(), "Serialization failed");
        assert!(!error.to_string().contains("private route encoder detail"));
        let wrapper = find_source::<SharedRouteEncodeFailure>(&error)
            .expect("cache failure must retain its shared typed source wrapper");
        let RocketMQError::Shared(retained) = wrapper.source.as_ref() else {
            panic!("cache failure wrapper must retain the original canonical carrier")
        };
        assert!(Arc::ptr_eq(retained, &canonical));
        assert!(find_source::<std::io::Error>(&error).is_some());

        let retry = cache
            .get_or_try_insert_with(key, || Ok(vec![1, 2, 3]))
            .expect("failed route encoding must not be cached");
        assert_eq!(retry.kind, RouteCacheOutcomeKind::Miss);
        assert_eq!(retry.body, Bytes::from_static(&[1, 2, 3]));
    }

    #[test]
    fn zone_order_external_and_disabled_policies_bypass_cache() {
        let base = RouteCachePolicy {
            enabled: true,
            zone_requested: false,
            order_enabled: false,
            external_route: false,
        };
        assert!(base.is_eligible());
        assert!(!RouteCachePolicy { enabled: false, ..base }.is_eligible());
        assert!(!RouteCachePolicy {
            zone_requested: true,
            ..base
        }
        .is_eligible());
        assert!(!RouteCachePolicy {
            order_enabled: true,
            ..base
        }
        .is_eligible());
        assert!(!RouteCachePolicy {
            external_route: true,
            ..base
        }
        .is_eligible());
    }
}
