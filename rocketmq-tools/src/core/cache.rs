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

//! Caching layer for performance optimization
//!
//! Provides in-memory caching for frequently accessed data like cluster info
//! and topic routes to reduce network calls and improve response times.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use tokio::sync::RwLock;

/// Cache entry with expiration
#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

impl<T> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// Thread-safe cache for RocketMQ data
pub struct RocketMQCache {
    cluster_info: Arc<RwLock<Option<CacheEntry<ClusterInfo>>>>,
    topic_routes: Arc<RwLock<HashMap<String, CacheEntry<TopicRouteData>>>>,
    cluster_ttl: Duration,
    route_ttl: Duration,
}

impl Default for RocketMQCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(300), Duration::from_secs(60))
    }
}

impl RocketMQCache {
    /// Create new cache with custom TTL
    pub fn new(cluster_ttl: Duration, route_ttl: Duration) -> Self {
        Self {
            cluster_info: Arc::new(RwLock::new(None)),
            topic_routes: Arc::new(RwLock::new(HashMap::new())),
            cluster_ttl,
            route_ttl,
        }
    }

    /// Get cached cluster info
    pub async fn get_cluster_info(&self) -> Option<ClusterInfo> {
        let cache: tokio::sync::RwLockReadGuard<'_, Option<CacheEntry<ClusterInfo>>> = self.cluster_info.read().await;
        cache.as_ref().and_then(|entry: &CacheEntry<ClusterInfo>| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Cache cluster info
    pub async fn set_cluster_info(&self, info: ClusterInfo) {
        let mut cache = self.cluster_info.write().await;
        *cache = Some(CacheEntry::new(info, self.cluster_ttl));
    }

    /// Get cached topic route
    pub async fn get_topic_route(&self, topic: &str) -> Option<TopicRouteData> {
        let cache = self.topic_routes.read().await;
        cache.get(topic).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Cache topic route
    pub async fn set_topic_route(&self, topic: String, route: TopicRouteData) {
        let mut cache = self.topic_routes.write().await;
        cache.insert(topic, CacheEntry::new(route, self.route_ttl));
    }

    /// Clear all cached data
    pub async fn clear(&self) {
        let mut cluster_cache = self.cluster_info.write().await;
        *cluster_cache = None;

        let mut route_cache = self.topic_routes.write().await;
        route_cache.clear();
    }

    /// Remove expired entries
    pub async fn cleanup_expired(&self) {
        // Check cluster info
        {
            let cache: tokio::sync::RwLockReadGuard<'_, Option<CacheEntry<ClusterInfo>>> =
                self.cluster_info.read().await;
            if let Some(entry) = cache.as_ref() {
                if entry.is_expired() {
                    drop(cache);
                    let mut write_cache: tokio::sync::RwLockWriteGuard<'_, Option<CacheEntry<ClusterInfo>>> =
                        self.cluster_info.write().await;
                    *write_cache = None;
                }
            }
        }

        // Check topic routes
        {
            let cache = self.topic_routes.read().await;
            let expired_keys: Vec<_> = cache
                .iter()
                .filter(|(_, entry)| entry.is_expired())
                .map(|(key, _)| key.clone())
                .collect();

            if !expired_keys.is_empty() {
                drop(cache);
                let mut write_cache = self.topic_routes.write().await;
                for key in expired_keys {
                    write_cache.remove(&key);
                }
            }
        }
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cluster_cached: bool = {
            let guard: tokio::sync::RwLockReadGuard<'_, Option<CacheEntry<ClusterInfo>>> =
                self.cluster_info.read().await;
            guard.is_some()
        };
        let routes_count = self.topic_routes.read().await.len();

        CacheStats {
            cluster_info_cached: cluster_cached,
            topic_routes_count: routes_count,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub cluster_info_cached: bool,
    pub topic_routes_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_expiration() {
        let cache = RocketMQCache::new(Duration::from_millis(100), Duration::from_millis(100));

        let cluster_info = ClusterInfo::default();
        cache.set_cluster_info(cluster_info.clone()).await;

        // Should be cached immediately
        assert!(cache.get_cluster_info().await.is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired
        assert!(cache.get_cluster_info().await.is_none());
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = RocketMQCache::default();

        let cluster_info = ClusterInfo::default();
        cache.set_cluster_info(cluster_info).await;

        assert!(cache.get_cluster_info().await.is_some());

        cache.clear().await;

        assert!(cache.get_cluster_info().await.is_none());
    }
}
