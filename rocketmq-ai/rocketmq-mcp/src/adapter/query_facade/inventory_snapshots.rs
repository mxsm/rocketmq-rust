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

//! The Topic and Consumer Group inventories of a cluster, and the snapshots that page them.
//!
//! The names of a cluster are read once for each visibility class and kept in the query cache
//! while they are current. A snapshot holds the names one request selects: there is one for each
//! filter, selection mode, and page size, because a cursor is bound to all three. It is selected
//! from the kept names in memory, so requests that differ only in those share one read.

use std::sync::Arc;
use std::time::Duration;

use super::query_result_from_snapshot;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use super::ResolvedCluster;
use super::WorkflowFuture;
use crate::infrastructure::snapshot::DerivedPayload;
use crate::infrastructure::snapshot::SnapshotKind;
use crate::infrastructure::snapshot::SnapshotOrigin;
use crate::infrastructure::snapshot::SnapshotRequest;
use crate::infrastructure::snapshot::SnapshotSelectionMode;
use crate::infrastructure::snapshot::SnapshotView;
use crate::infrastructure::snapshot::SnapshotWeight;
use crate::model::contract::observed_at;
use crate::model::contract::Page;
use crate::model::contract::PageRequest;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::tools::consumer_tools::ConsumerGroupName;
use crate::tools::consumer_tools::ListConsumerGroupNamesOutput;
use crate::tools::consumer_tools::ListConsumerGroupsArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// The sorted, distinct names of one inventory, shared by everyone who reads them.
type Names = Arc<Vec<String>>;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Returns the snapshot of the Topic names that contain `filter`, ignoring ASCII case.
    pub(super) async fn topic_inventory_snapshot(
        &self,
        cluster: ResolvedCluster,
        filter: Option<&str>,
        page: &PageRequest,
    ) -> Result<SnapshotView<Vec<String>>, ToolFailure> {
        let filter = normalized_filter(filter).unwrap_or_default();
        let request = SnapshotRequest::try_new(
            SnapshotKind::TopicInventory,
            cluster.name.clone(),
            filter.clone(),
            page,
            self.visibility_class.as_str(),
        )?;
        let capacity = self.snapshots.max_snapshot_entries();
        self.snapshots
            .get_or_derive(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.topic_list_ttl_ms),
                |topics: &Vec<String>| SnapshotWeight::inventory(topics.len()),
                &self.control.cancellation,
                || async {
                    let names = self.topic_names(cluster).await?;
                    Ok(select_names(names, capacity, |topic| {
                        filter.is_empty() || topic.to_ascii_lowercase().contains(&filter)
                    }))
                },
            )
            .await
    }

    /// Returns the snapshot of the Consumer Group names selected by `filter`: the one equal name
    /// when `exact`, otherwise every name that contains it, ignoring ASCII case.
    pub(super) async fn consumer_group_inventory_snapshot(
        &self,
        cluster: ResolvedCluster,
        filter: Option<&str>,
        exact: bool,
        page: &PageRequest,
    ) -> Result<SnapshotView<Vec<String>>, ToolFailure> {
        let filter = if exact {
            filter.map(str::trim).unwrap_or_default().to_string()
        } else {
            normalized_filter(filter).unwrap_or_default()
        };
        let selection_mode = if exact {
            SnapshotSelectionMode::ExactIdentifier
        } else {
            SnapshotSelectionMode::LiteralFilter
        };
        let request = SnapshotRequest::try_new_with_selection(
            SnapshotKind::ConsumerGroupInventory,
            cluster.name.clone(),
            filter.clone(),
            selection_mode,
            page,
            self.visibility_class.as_str(),
        )?;
        let capacity = self.snapshots.max_snapshot_entries();
        self.snapshots
            .get_or_derive(
                request,
                page.cursor.as_deref(),
                self.cursor_snapshot_ttl(),
                self.snapshot_response_ttl(self.config.cache.consumer_lag_ttl_ms),
                |groups: &Vec<String>| SnapshotWeight::inventory(groups.len()),
                &self.control.cancellation,
                || async {
                    let names = self.consumer_group_names(cluster).await?;
                    Ok(select_names(names, capacity, |group| {
                        if filter.is_empty() {
                            true
                        } else if exact {
                            group == filter
                        } else {
                            group.to_ascii_lowercase().contains(&filter)
                        }
                    }))
                },
            )
            .await
    }

    /// Returns one page of the Consumer Group names that contain the filter of `args`.
    ///
    /// The page comes from the snapshot that [`list_consumer_groups`](Self::list_consumer_groups)
    /// pages, so a cursor of either continues with the other. Nothing is read about the groups.
    pub(crate) async fn list_consumer_group_names(
        &self,
        args: ListConsumerGroupsArgs,
    ) -> Result<QueryResult<ListConsumerGroupNamesOutput>, ToolFailure> {
        let cluster = self.resolve_cluster(args.cluster.as_deref().unwrap_or_default())?;
        let snapshot = self
            .consumer_group_inventory_snapshot(cluster.clone(), args.filter.as_deref(), false, &args.page)
            .await?;
        let names = self.snapshots.page(&snapshot, &snapshot.payload.data)?;
        let page = Page {
            count: names.count,
            total_count: names.total_count,
            has_more: names.has_more,
            next_cursor: names.next_cursor,
            items: names
                .items
                .into_iter()
                .map(|group| ConsumerGroupName { group })
                .collect(),
        };
        Ok(query_result_from_snapshot(
            &snapshot,
            snapshot.payload.completeness().wrap(ListConsumerGroupNamesOutput {
                cluster: cluster.name,
                page,
                generated_at: observed_at(),
            }),
        ))
    }

    /// Counts the Topics of a cluster. An inventory too large for a snapshot is counted as well.
    pub(super) async fn topic_count(&self, cluster: ResolvedCluster) -> Result<QueryPayload<usize>, ToolFailure> {
        self.topic_names(cluster).await.map(counted)
    }

    /// Counts the Consumer Groups of a cluster, like [`topic_count`](Self::topic_count).
    pub(super) async fn consumer_group_count(
        &self,
        cluster: ResolvedCluster,
    ) -> Result<QueryPayload<usize>, ToolFailure> {
        self.consumer_group_names(cluster).await.map(counted)
    }

    /// The Topic names of a cluster, read at most once while the Topic list is current.
    ///
    /// The returned future is boxed for the reason [`run_workflow`](Self::run_workflow) gives.
    fn topic_names(&self, cluster: ResolvedCluster) -> WorkflowFuture<'_, QueryResult<Names>> {
        Box::pin(async move {
            let key = self.cache_key("topic_names", &cluster.name, "");
            let ttl = Duration::from_millis(self.config.cache.topic_list_ttl_ms);
            self.cache
                .get_or_try_init_cancellable(
                    key,
                    ttl,
                    &self.control.cancellation,
                    || ToolFailure::Rejected(ToolRejection::Cancelled),
                    || async {
                        self.run_workflow(cluster, move |session, _| {
                            Box::pin(async move {
                                let mut topics = session.topic_inventory().await?;
                                topics.sort();
                                topics.dedup();
                                Ok(QueryPayload::complete(Arc::new(topics)))
                            })
                        })
                        .await
                    },
                )
                .await
        })
    }

    /// The Consumer Group names of a cluster, kept like [`topic_names`](Self::topic_names) for
    /// as long as consumer data is current.
    fn consumer_group_names(&self, cluster: ResolvedCluster) -> WorkflowFuture<'_, QueryResult<Names>> {
        Box::pin(async move {
            let key = self.cache_key("consumer_group_names", &cluster.name, "");
            let ttl = Duration::from_millis(self.config.cache.consumer_lag_ttl_ms);
            self.cache
                .get_or_try_init_cancellable(
                    key,
                    ttl,
                    &self.control.cancellation,
                    || ToolFailure::Rejected(ToolRejection::Cancelled),
                    || async {
                        self.run_workflow(cluster, move |session, _| {
                            Box::pin(async move {
                                let groups = session.consumer_group_inventory().await?;
                                Ok(groups.map(|mut groups| {
                                    groups.sort();
                                    groups.dedup();
                                    Arc::new(groups)
                                }))
                            })
                        })
                        .await
                    },
                )
                .await
        })
    }
}

/// Selects the names of one snapshot from the names of a cluster, in their order.
///
/// The snapshot keeps the observation of the names: it is as old as they are, and it carries
/// their warnings, so a selection from stale names says that it is stale. At most one name more
/// than a snapshot holds is copied, which is all the store needs to refuse a selection as too
/// large.
fn select_names(
    names: QueryResult<Names>,
    snapshot_capacity: usize,
    selects: impl Fn(&str) -> bool,
) -> DerivedPayload<Vec<String>> {
    let selected = names
        .data
        .iter()
        .filter(|name| selects(name))
        .take(snapshot_capacity.saturating_add(1))
        .cloned()
        .collect();
    DerivedPayload {
        payload: QueryPayload::new(selected, names.partial, names.warnings, names.source_failures),
        origin: SnapshotOrigin {
            observed_at: names.observed_at,
            age: Duration::from_millis(names.freshness_ms),
            cache_status: names.cache_status,
        },
    }
}

fn counted(names: QueryResult<Names>) -> QueryPayload<usize> {
    QueryPayload::new(names.data.len(), names.partial, names.warnings, names.source_failures)
}

fn normalized_filter(filter: Option<&str>) -> Option<String> {
    filter
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
        .map(str::to_ascii_lowercase)
}
