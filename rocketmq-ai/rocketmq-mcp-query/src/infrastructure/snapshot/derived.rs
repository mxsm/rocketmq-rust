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

//! Snapshots whose payload is selected from a value that was observed earlier.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::BegunLoad;
use super::RetainedSize;
use super::SnapshotRequest;
use super::SnapshotStore;
use super::SnapshotView;
use super::SnapshotWeight;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryPayload;
use crate::tools::executor::ToolFailure;

/// When and how the value was observed that the payload of a snapshot is derived from.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotOrigin {
    pub(crate) observed_at: String,
    /// How long ago that was when the payload was derived.
    pub(crate) age: Duration,
    pub(crate) cache_status: CacheStatus,
}

/// A payload derived from a value that was observed at `origin`.
pub(crate) struct DerivedPayload<T> {
    pub(crate) payload: QueryPayload<T>,
    pub(crate) origin: SnapshotOrigin,
}

impl SnapshotStore {
    /// Returns the snapshot of `request` like [`get_or_load`](Self::get_or_load), with a `load`
    /// that derives the payload from a value someone else keeps instead of reading its source.
    ///
    /// The snapshot reports the observation of that value and its age, and it stays current only
    /// as long as a snapshot read at that time would. Its cursors live as long as those of any
    /// new snapshot. The store counts no miss and no bypass for it: whoever keeps the value has
    /// counted how it was obtained.
    pub(crate) async fn get_or_derive<T, Load, LoadFuture>(
        &self,
        request: SnapshotRequest,
        cursor: Option<&str>,
        cursor_ttl: Duration,
        response_cache_ttl: Option<Duration>,
        weight: impl FnOnce(&T) -> SnapshotWeight,
        cancellation: &CancellationToken,
        load: Load,
    ) -> Result<SnapshotView<T>, ToolFailure>
    where
        T: Clone + RetainedSize + Send + Sync + 'static,
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<DerivedPayload<T>, ToolFailure>>,
    {
        let pending = match self
            .begin_load(request, cursor, cursor_ttl, response_cache_ttl, cancellation)
            .await?
        {
            BegunLoad::Ready(view) => return Ok(view),
            BegunLoad::Pending(pending) => pending,
        };
        match load().await {
            Ok(derived) => self.finish_load(pending, derived.payload, Some(derived.origin), weight),
            Err(failure) => self.fail_load(pending, failure),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use super::DerivedPayload;
    use super::SnapshotOrigin;
    use crate::infrastructure::snapshot::SnapshotKind;
    use crate::infrastructure::snapshot::SnapshotRequest;
    use crate::infrastructure::snapshot::SnapshotStore;
    use crate::infrastructure::snapshot::SnapshotView;
    use crate::infrastructure::snapshot::SnapshotWeight;
    use crate::model::contract::CacheStatus;
    use crate::model::contract::PageRequest;
    use crate::model::contract::QueryPayload;
    use crate::tools::executor::ToolFailure;

    const CURRENT_FOR: Duration = Duration::from_secs(5);
    const CURSORS_FOR: Duration = Duration::from_secs(60);

    /// Reads the first page of the names that contain `filter`, or the page `cursor` continues,
    /// deriving them from names that were observed `age` ago with `cache_status`.
    async fn derive(
        store: &SnapshotStore,
        derivations: &Arc<AtomicUsize>,
        filter: &str,
        cursor: Option<&str>,
        age: Duration,
        cache_status: CacheStatus,
    ) -> Result<SnapshotView<Vec<String>>, ToolFailure> {
        let page = PageRequest {
            limit: Some(1),
            cursor: None,
        };
        let request = SnapshotRequest::try_new(SnapshotKind::TopicInventory, "cluster-a", filter, &page, "standard")
            .expect("the request is well formed");
        let derivations = derivations.clone();
        store
            .get_or_derive(
                request,
                cursor,
                CURSORS_FOR,
                Some(CURRENT_FOR),
                |names: &Vec<String>| SnapshotWeight::inventory(names.len()),
                &CancellationToken::new(),
                || async move {
                    derivations.fetch_add(1, Ordering::SeqCst);
                    Ok(DerivedPayload {
                        payload: QueryPayload::complete(vec!["orders".to_string(), "payments".to_string()]),
                        origin: SnapshotOrigin {
                            observed_at: "2026-01-01T00:00:00Z".to_string(),
                            age,
                            cache_status,
                        },
                    })
                },
            )
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn a_derived_snapshot_is_as_old_and_as_current_as_what_it_comes_from() {
        let store = SnapshotStore::new(8);
        let derivations = Arc::new(AtomicUsize::new(0));
        let age = Duration::from_secs(3);

        let derived = derive(&store, &derivations, "", None, age, CacheStatus::Hit)
            .await
            .unwrap();
        assert_eq!(derived.observed_at, "2026-01-01T00:00:00Z");
        assert_eq!(derived.freshness_ms, 3_000);
        assert_eq!(derived.cache_status, CacheStatus::Hit);
        // Whoever keeps the value counted its use; the store adds nothing for the derivation.
        let metrics = store.metrics();
        assert_eq!((metrics.hits, metrics.misses, metrics.bypasses), (0, 0, 0));

        // It is current for the two seconds the value still was, and reports its growing age.
        tokio::time::advance(Duration::from_millis(1_999)).await;
        let reused = derive(&store, &derivations, "", None, age, CacheStatus::Hit)
            .await
            .unwrap();
        assert_eq!(reused.identity(), derived.identity());
        assert_eq!(reused.freshness_ms, 4_999);
        assert_eq!(derivations.load(Ordering::SeqCst), 1);
        assert_eq!(store.metrics().hits, 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        let rederived = derive(&store, &derivations, "", None, Duration::ZERO, CacheStatus::Miss)
            .await
            .unwrap();
        assert_ne!(rederived.identity(), derived.identity());
        assert_eq!((rederived.freshness_ms, rederived.cache_status), (0, CacheStatus::Miss));
        assert_eq!(derivations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_cursors_of_a_derived_snapshot_live_their_whole_lifetime() {
        let store = SnapshotStore::new(8);
        let derivations = Arc::new(AtomicUsize::new(0));
        let derived = derive(&store, &derivations, "", None, Duration::from_secs(4), CacheStatus::Hit)
            .await
            .unwrap();
        let cursor = store
            .page(&derived, &derived.payload.data)
            .unwrap()
            .next_cursor
            .unwrap();

        // The lifetime counts from the derivation, not from the older observation.
        tokio::time::advance(CURSORS_FOR - Duration::from_millis(1)).await;
        let resumed = derive(
            &store,
            &derivations,
            "",
            Some(&cursor),
            Duration::ZERO,
            CacheStatus::Miss,
        )
        .await
        .unwrap();
        assert_eq!(store.page(&resumed, &resumed.payload.data).unwrap().items, ["payments"]);
        assert_eq!(resumed.freshness_ms, 63_999);

        tokio::time::advance(Duration::from_millis(1)).await;
        let expired = derive(
            &store,
            &derivations,
            "",
            Some(&cursor),
            Duration::ZERO,
            CacheStatus::Miss,
        )
        .await;
        assert_eq!(expired.unwrap_err().code(), "cursor_expired");
        assert_eq!(derivations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_derivation_without_a_kept_snapshot_reports_how_the_value_was_obtained() {
        let store = SnapshotStore::new(8);
        let request = SnapshotRequest::try_new(
            SnapshotKind::TopicInventory,
            "cluster-a",
            "",
            &PageRequest::default(),
            "standard",
        )
        .expect("the request is well formed");

        // No snapshot is kept current: every first page derives again.
        let derived = store
            .get_or_derive(
                request,
                None,
                CURSORS_FOR,
                None,
                |names: &Vec<String>| SnapshotWeight::inventory(names.len()),
                &CancellationToken::new(),
                || async {
                    Ok(DerivedPayload {
                        payload: QueryPayload::complete(vec!["orders".to_string()]),
                        origin: SnapshotOrigin {
                            observed_at: "2026-01-01T00:00:00Z".to_string(),
                            age: Duration::ZERO,
                            cache_status: CacheStatus::Bypass,
                        },
                    })
                },
            )
            .await
            .unwrap();

        assert_eq!(derived.cache_status, CacheStatus::Bypass);
        let metrics = store.metrics();
        assert_eq!((metrics.hits, metrics.misses, metrics.bypasses), (0, 0, 0));
    }
}
