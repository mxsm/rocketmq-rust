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

//! Performance optimization examples
//!
//! This module demonstrates how to use the performance optimization features
//! added in Phase 3.3.

#![allow(dead_code)]

use std::time::Duration;

use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;

use crate::core::admin::AdminBuilder;
use crate::core::cache::RocketMQCache;
use crate::core::concurrent;
use crate::core::RocketMQResult;

/// Example: Using cache to reduce network calls
///
/// ```rust,ignore
/// use rocketmq_tools::core::cache::RocketMQCache;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> RocketMQResult<()> {
///     // Create cache with 5-minute TTL for cluster info
///     let cache = RocketMQCache::new(
///         Duration::from_secs(300),  // cluster_ttl
///         Duration::from_secs(60),   // route_ttl
///     );
///
///     let mut admin = AdminBuilder::new()
///         .namesrv_addr("127.0.0.1:9876")
///         .build_with_guard()
///         .await?;
///
///     // First call - fetches from server and caches
///     let cluster_info = match cache.get_cluster_info().await {
///         Some(cached) => cached,
///         None => {
///             let info = admin.examine_broker_cluster_info().await?;
///             cache.set_cluster_info(info.clone()).await;
///             info
///         }
///     };
///
///     // Second call - uses cached data (much faster!)
///     let cluster_info = cache.get_cluster_info().await.unwrap();
///
///     Ok(())
/// }
/// ```
pub async fn example_cache_usage() -> RocketMQResult<()> {
    let cache = RocketMQCache::new(Duration::from_secs(300), Duration::from_secs(60));

    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_with_guard()
        .await?;

    // Check cache first
    let cluster_info = match cache.get_cluster_info().await {
        Some(cached) => {
            println!("Using cached cluster info");
            cached
        }
        None => {
            println!("Fetching fresh cluster info");
            let info = admin.examine_broker_cluster_info().await?;
            cache.set_cluster_info(info.clone()).await;
            info
        }
    };

    println!("Cluster info: {} brokers", cluster_info.broker_addr_table.len());

    Ok(())
}

/// Example: Concurrent queries for multiple topics
///
/// ```rust,ignore
/// use rocketmq_tools::core::concurrent::concurrent_query;
///
/// #[tokio::main]
/// async fn main() -> RocketMQResult<()> {
///     let mut admin = AdminBuilder::new()
///         .namesrv_addr("127.0.0.1:9876")
///         .build_with_guard()
///         .await?;
///
///     let topics = vec!["Topic1", "Topic2", "Topic3"];
///
///     // Sequential (slow) - 3 network calls one after another
///     // for topic in &topics {
///     //     let route = admin.examine_topic_route_info(topic.into()).await?;
///     // }
///
///     // Concurrent (fast) - 3 network calls simultaneously
///     let queries = topics.iter().map(|topic| {
///         let topic = topic.to_string();
///         admin.examine_topic_route_info(topic.into())
///     });
///
///     let results = concurrent_query(queries).await;
///     for (topic, result) in topics.iter().zip(results) {
///         match result {
///             Ok(Some(route)) => println!("{}: {} brokers", topic, route.broker_datas.len()),
///             Ok(None) => println!("{}: not found", topic),
///             Err(e) => println!("{}: error - {}", topic, e),
///         }
///     }
///
///     Ok(())
/// }
/// ```
pub async fn example_concurrent_queries() -> RocketMQResult<()> {
    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_with_guard()
        .await?;

    let topics = vec!["TestTopic1", "TestTopic2", "TestTopic3"];

    // Create concurrent queries
    let queries = topics.iter().map(|topic| {
        let topic_str = topic.to_string();
        async move {
            admin
                .examine_topic_route_info(topic_str.into())
                .await
                .map_err(|e| e.into())
        }
    });

    // Execute all queries concurrently
    let results = concurrent::concurrent_query(queries).await;

    // Process results
    for (topic, result) in topics.iter().zip(results) {
        match result {
            Ok(Some(route)) => {
                println!("{}: {} brokers", topic, route.broker_datas.len())
            }
            Ok(None) => println!("{}: not found", topic),
            Err(e) => eprintln!("{}: error - {}", topic, e),
        }
    }

    Ok(())
}

/// Example: Limited concurrency to avoid overwhelming server
///
/// ```rust,ignore
/// use rocketmq_tools::core::concurrent::concurrent_query_limited;
///
/// // Query 100 topics with max 10 concurrent connections
/// let results = concurrent_query_limited(queries, 10).await;
/// ```
pub async fn example_limited_concurrency() -> RocketMQResult<()> {
    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_with_guard()
        .await?;

    // Suppose we have many topics to query
    let topics: Vec<_> = (1..=50).map(|i| format!("Topic{}", i)).collect();

    let queries = topics.iter().map(|topic| {
        let topic = topic.clone();
        async move { admin.examine_topic_route_info(topic.into()).await.map_err(|e| e.into()) }
    });

    // Limit to 10 concurrent queries to avoid overwhelming the server
    let results = concurrent::concurrent_query_limited(queries, 10).await;

    let successful = results.iter().filter(|r| r.is_ok()).count();
    println!("Successfully queried {}/{} topics", successful, topics.len());

    Ok(())
}

/// Example: Batch processing with caching
///
/// Combines caching and concurrent queries for optimal performance
pub async fn example_cached_concurrent_queries() -> RocketMQResult<()> {
    let cache = RocketMQCache::new(Duration::from_secs(300), Duration::from_secs(60));

    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_with_guard()
        .await?;

    let topics = vec!["Topic1", "Topic2", "Topic3"];

    // Check cache first for each topic
    for topic in &topics {
        if let Some(route) = cache.get_topic_route(topic).await {
            println!("{}: using cached route ({} brokers)", topic, route.broker_datas.len());
        } else {
            // Not in cache - will fetch below
            println!("{}: will fetch from server", topic);
        }
    }

    // Fetch missing topics concurrently
    let topics_to_fetch: Vec<_> = topics.iter().filter(|t| cache.get_topic_route(t).is_none()).collect();

    if !topics_to_fetch.is_empty() {
        let queries = topics_to_fetch.iter().map(|topic| {
            let topic_str = topic.to_string();
            async move {
                admin
                    .examine_topic_route_info(topic_str.into())
                    .await
                    .map_err(|e| e.into())
            }
        });

        let results = concurrent::concurrent_query(queries).await;

        // Cache the results
        for (topic, result) in topics_to_fetch.iter().zip(results) {
            if let Ok(Some(route)) = result {
                cache.set_topic_route(topic.to_string(), route).await;
                println!("{}: fetched and cached", topic);
            }
        }
    }

    // Show cache stats
    let stats = cache.stats().await;
    println!(
        "Cache stats: cluster_cached={}, routes_cached={}",
        stats.cluster_info_cached, stats.topic_routes_count
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    // Note: These examples require a running RocketMQ server
    // They are here for documentation purposes
}
