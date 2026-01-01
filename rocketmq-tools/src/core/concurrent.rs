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

//! Concurrent query utilities for performance optimization

use std::future::Future;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::core::RocketMQResult;

/// Execute multiple async operations concurrently
///
/// # Example
/// ```rust,ignore
/// let results = concurrent_query(topics.iter().map(|topic| {
///     let admin = admin.clone();
///     let topic = topic.clone();
///     async move {
///         admin.examine_topic_route_info(topic).await
///     }
/// })).await;
/// ```
pub async fn concurrent_query<F, T>(queries: impl Iterator<Item = F>) -> Vec<RocketMQResult<T>>
where
    F: Future<Output = RocketMQResult<T>>,
{
    let mut futures = queries.collect::<FuturesUnordered<_>>();
    let mut results = Vec::new();

    while let Some(result) = futures.next().await {
        results.push(result);
    }

    results
}

/// Execute queries concurrently with a maximum concurrency limit
///
/// This prevents overwhelming the server with too many simultaneous connections.
pub async fn concurrent_query_limited<F, T>(
    queries: impl Iterator<Item = F>,
    max_concurrent: usize,
) -> Vec<RocketMQResult<T>>
where
    F: Future<Output = RocketMQResult<T>>,
{
    let mut futures = FuturesUnordered::new();
    let mut queries = queries.peekable();
    let mut results = Vec::new();

    // Fill initial batch
    for _ in 0..max_concurrent {
        if let Some(query) = queries.next() {
            futures.push(query);
        } else {
            break;
        }
    }

    // Process results and add new queries
    while let Some(result) = futures.next().await {
        results.push(result);

        // Add next query if available
        if let Some(query) = queries.next() {
            futures.push(query);
        }
    }

    results
}

/// Batch process items with concurrent queries
///
/// Splits items into batches and processes each batch concurrently.
pub async fn batch_query<I, F, T>(items: Vec<I>, batch_size: usize, query_fn: impl Fn(I) -> F) -> Vec<RocketMQResult<T>>
where
    F: Future<Output = RocketMQResult<T>>,
    I: Clone,
{
    let mut all_results = Vec::new();

    for chunk in items.chunks(batch_size) {
        let queries = chunk.iter().cloned().map(&query_fn);
        let batch_results = concurrent_query(queries).await;
        all_results.extend(batch_results);
    }

    all_results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_query() {
        let queries = (0..5).map(|i| async move { Ok(i) });
        let results = concurrent_query(queries).await;

        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_concurrent_query_limited() {
        let queries = (0..10).map(|i| async move { Ok(i) });
        let results = concurrent_query_limited(queries, 3).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_batch_query() {
        let items: Vec<i32> = (0..10).collect();
        let results = batch_query(items, 3, |i| async move { Ok(i * 2) }).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.is_ok()));
    }
}
