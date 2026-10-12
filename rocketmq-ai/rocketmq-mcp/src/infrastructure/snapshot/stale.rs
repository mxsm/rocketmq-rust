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

//! Answering a first page whose load failed with the snapshot that stopped being current a
//! short while ago.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::time::Instant;

use super::purge_expired;
use super::SnapshotKey;
use super::SnapshotStore;
use super::SnapshotView;
use crate::infrastructure::cache::mark_stale;
use crate::infrastructure::cache::CachedFailure;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryPayload;
use crate::tools::executor::ToolFailure;

impl SnapshotStore {
    /// Answers a failed load of `key` with the last snapshot of the same request, when the store
    /// was built [`with_stale_if_error`](Self::with_stale_if_error), the failure
    /// [allows it](CachedFailure::allows_stale), and that snapshot is still retained and stopped
    /// being current within the window. Otherwise the failure is the answer.
    ///
    /// The view reports the real age of the snapshot and carries the stale warning in a copy of
    /// its payload, so everything built from the view is marked too. Its cursors continue the
    /// retained snapshot for as long as that lives.
    pub(super) fn stale_or<T>(&self, key: &SnapshotKey, failure: ToolFailure) -> Result<SnapshotView<T>, ToolFailure>
    where
        T: Clone + Send + Sync + 'static,
    {
        let window = self.inner.stale_if_error;
        if window.is_zero() || !failure.allows_stale() {
            return Err(failure);
        }
        let retained = {
            let mut store = self.lock_state();
            let state = &mut store.snapshots;
            purge_expired(state, self.tombstone_capacity());
            let now = Instant::now();
            state.reusable.get(&self.key_hash(key)).and_then(|id| {
                let entry = state.entries.get(id)?;
                let current_until = entry.reusable_until?;
                (entry.key == *key && current_until + window > now).then(|| {
                    let age = now.saturating_duration_since(entry.inserted_at);
                    (id.clone(), Arc::clone(&entry.value), entry.observed_at.clone(), age)
                })
            })
        };
        let Some((id, value, observed_at, age)) = retained else {
            return Err(failure);
        };
        let Ok(payload) = Arc::downcast::<QueryPayload<T>>(value) else {
            return Err(failure);
        };
        // The store lock is released: the marked copy is made without holding it.
        let mut payload = QueryPayload::clone(&payload);
        mark_stale(&mut payload.warnings);
        self.inner.metrics.hits.fetch_add(1, Ordering::Relaxed);
        Ok(SnapshotView {
            id,
            request: key.request.clone(),
            generation: key.generation,
            position: 0,
            payload: Arc::new(payload),
            observed_at,
            freshness_ms: age.as_millis().try_into().unwrap_or(u64::MAX),
            cache_status: CacheStatus::Hit,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use crate::infrastructure::cache::STALE_CACHE_VALUE_WARNING;
    use crate::infrastructure::snapshot::SnapshotKind;
    use crate::infrastructure::snapshot::SnapshotRequest;
    use crate::infrastructure::snapshot::SnapshotStore;
    use crate::infrastructure::snapshot::SnapshotView;
    use crate::infrastructure::snapshot::SnapshotWeight;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::PageRequest;
    use crate::model::contract::QueryPayload;
    use crate::tools::executor::NotFoundEntity;
    use crate::tools::executor::ToolExecutionError;
    use crate::tools::executor::ToolFailure;
    use crate::tools::executor::ToolRejection;

    const CURRENT_FOR: Duration = Duration::from_secs(5);
    const CURSORS_FOR: Duration = Duration::from_secs(60);

    /// Reads the first page of one route, or the page `cursor` continues, with a load that
    /// counts itself and ends with `outcome`.
    async fn read(
        store: &SnapshotStore,
        loads: &Arc<AtomicUsize>,
        cursor: Option<&str>,
        outcome: Result<Vec<u8>, ToolFailure>,
    ) -> Result<SnapshotView<Vec<u8>>, ToolFailure> {
        let page = PageRequest {
            limit: Some(2),
            cursor: None,
        };
        let request =
            SnapshotRequest::try_new(SnapshotKind::TopicRoute, "cluster-a", "topic=orders", &page, "standard")
                .expect("the request is well formed");
        let loads = loads.clone();
        store
            .get_or_load(
                request,
                cursor,
                CURSORS_FOR,
                Some(CURRENT_FOR),
                |rows: &Vec<u8>| SnapshotWeight::detail(rows.len()),
                &CancellationToken::new(),
                || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    outcome.map(QueryPayload::complete)
                },
            )
            .await
    }

    fn unavailable() -> ToolFailure {
        ToolFailure::Operational(ToolExecutionError::Backend(None))
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_first_page_is_answered_by_the_snapshot_that_was_current_before() {
        let store = SnapshotStore::new(8).with_stale_if_error(Duration::from_secs(30));
        let loads = Arc::new(AtomicUsize::new(0));
        let loaded = read(&store, &loads, None, Ok(vec![1, 2, 3])).await.unwrap();
        assert!(loaded.payload.warnings.is_empty());

        tokio::time::advance(CURRENT_FOR + Duration::from_secs(1)).await;
        let stale = read(&store, &loads, None, Err(unavailable())).await.unwrap();
        assert_eq!(stale.payload.data, [1, 2, 3]);
        assert_eq!(stale.payload.warnings, [STALE_CACHE_VALUE_WARNING]);
        assert_eq!(stale.cache_status, CacheStatus::Hit);
        assert_eq!(stale.freshness_ms, 6_000);
        assert_eq!(stale.observed_at, loaded.observed_at);
        assert_eq!(stale.identity(), loaded.identity());
        assert_eq!(
            loads.load(Ordering::SeqCst),
            2,
            "the source is asked before the old snapshot answers"
        );

        // The retained snapshot itself is unmarked, and its cursors keep working.
        let cursor = store.page(&stale, &stale.payload.data).unwrap().next_cursor.unwrap();
        let resumed = read(&store, &loads, Some(&cursor), Err(unavailable())).await.unwrap();
        assert_eq!(store.page(&resumed, &resumed.payload.data).unwrap().items, [3]);
        assert!(resumed.payload.warnings.is_empty());

        // A source that answers too late is treated the same; a request it refuses is not.
        let timed_out = ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 25_000 });
        let late = read(&store, &loads, None, Err(timed_out)).await.unwrap();
        assert_eq!(late.payload.warnings, [STALE_CACHE_VALUE_WARNING]);
        let refused = read(&store, &loads, None, Err(ToolFailure::not_found(NotFoundEntity::Topic))).await;
        assert_eq!(refused.unwrap_err().code(), "not_found");

        // A snapshot that loads again is current and unmarked.
        let reloaded = read(&store, &loads, None, Ok(vec![7, 8])).await.unwrap();
        assert_eq!(reloaded.cache_status, CacheStatus::Miss);
        assert!(reloaded.payload.warnings.is_empty());
        assert_ne!(reloaded.identity(), loaded.identity());
    }

    #[tokio::test(start_paused = true)]
    async fn an_old_snapshot_answers_only_inside_the_window_and_while_it_is_retained() {
        let loads = Arc::new(AtomicUsize::new(0));

        // Past the window the failure is the answer although the snapshot is still retained.
        let short = SnapshotStore::new(8).with_stale_if_error(Duration::from_secs(10));
        read(&short, &loads, None, Ok(vec![1])).await.unwrap();
        tokio::time::advance(CURRENT_FOR + Duration::from_secs(10)).await;
        let failed = read(&short, &loads, None, Err(unavailable())).await;
        assert_eq!(failed.unwrap_err().code(), "source_unavailable");

        // A window longer than the snapshot lives ends when the snapshot is dropped.
        let long = SnapshotStore::new(8).with_stale_if_error(Duration::from_secs(300));
        read(&long, &loads, None, Ok(vec![1])).await.unwrap();
        tokio::time::advance(CURSORS_FOR - Duration::from_secs(1)).await;
        assert!(read(&long, &loads, None, Err(unavailable())).await.is_ok());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(read(&long, &loads, None, Err(unavailable())).await.is_err());

        // Without a window, and after an invalidation, the failure is the answer.
        let strict = SnapshotStore::new(8);
        read(&strict, &loads, None, Ok(vec![1])).await.unwrap();
        tokio::time::advance(CURRENT_FOR).await;
        assert!(read(&strict, &loads, None, Err(unavailable())).await.is_err());

        let invalidated = SnapshotStore::new(8).with_stale_if_error(Duration::from_secs(30));
        read(&invalidated, &loads, None, Ok(vec![1])).await.unwrap();
        tokio::time::advance(CURRENT_FOR).await;
        assert_eq!(invalidated.clear().await, 1);
        assert!(read(&invalidated, &loads, None, Err(unavailable())).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn waiters_of_a_failed_first_page_get_the_old_snapshot_too() {
        let store = SnapshotStore::new(8).with_stale_if_error(Duration::from_secs(30));
        let loads = Arc::new(AtomicUsize::new(0));
        read(&store, &loads, None, Ok(vec![1, 2])).await.unwrap();
        tokio::time::advance(CURRENT_FOR).await;

        let release = Arc::new(tokio::sync::Notify::new());
        let mut calls = Vec::new();
        for _ in 0..3 {
            let (store, loads, release) = (store.clone(), loads.clone(), release.clone());
            calls.push(tokio::spawn(async move {
                let page = PageRequest {
                    limit: Some(2),
                    cursor: None,
                };
                let request =
                    SnapshotRequest::try_new(SnapshotKind::TopicRoute, "cluster-a", "topic=orders", &page, "standard")
                        .expect("the request is well formed");
                store
                    .get_or_load(
                        request,
                        None,
                        CURSORS_FOR,
                        Some(CURRENT_FOR),
                        |rows: &Vec<u8>| SnapshotWeight::detail(rows.len()),
                        &CancellationToken::new(),
                        || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            release.notified().await;
                            Err::<QueryPayload<Vec<u8>>, _>(unavailable())
                        },
                    )
                    .await
            }));
        }
        while store.metrics().coalesced_waiters < 2 {
            tokio::task::yield_now().await;
        }
        release.notify_one();

        for call in calls {
            let stale = call.await.unwrap().unwrap();
            assert_eq!(stale.payload.data, [1, 2]);
            assert_eq!(stale.payload.warnings, [STALE_CACHE_VALUE_WARNING]);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }
}
