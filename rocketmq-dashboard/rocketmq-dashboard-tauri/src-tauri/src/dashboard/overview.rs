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

use crate::{
    cluster::service::ClusterManager, consumer::ConsumerManager, producer::service::ProducerManager,
    topic::service::TopicManager,
};
use rocketmq_admin_core::core::consumer_workspace::{
    ConsumerInventoryResult, WorkspaceFailureStage, WorkspaceObservationState,
};
use rocketmq_dashboard_common::{ClusterHomePageRequest, TopicListRequest};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Quality {
    Complete,
    Reported,
    Partial,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Metric {
    value: Option<i64>,
    quality: Quality,
}
impl Metric {
    fn unknown() -> Self {
        Self {
            value: None,
            quality: Quality::Unknown,
        }
    }
    fn count(value: usize, quality: Quality) -> Self {
        Self {
            value: i64::try_from(value).ok(),
            quality,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OverviewStatus {
    Unconfigured,
    Down,
    Partial,
    Ready,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Overview {
    status: OverviewStatus,
    observed_at_ms: i64,
    brokers: Metric,
    topics: Metric,
    consumer_groups: Metric,
    producer_groups: Metric,
    total_lag: Metric,
    lagging_groups: Vec<String>,
}

fn assemble(configured: bool, metrics: [Metric; 5], lagging_groups: Vec<String>) -> Overview {
    let status = if !configured {
        OverviewStatus::Unconfigured
    } else if metrics.iter().all(|metric| metric.value.is_none()) {
        OverviewStatus::Down
    } else if metrics
        .iter()
        .any(|metric| matches!(metric.quality, Quality::Partial | Quality::Unknown))
    {
        OverviewStatus::Partial
    } else {
        OverviewStatus::Ready
    };
    let [brokers, topics, consumer_groups, producer_groups, total_lag] = metrics;
    Overview {
        status,
        observed_at_ms: chrono::Utc::now().timestamp_millis(),
        brokers,
        topics,
        consumer_groups,
        producer_groups,
        total_lag,
        lagging_groups,
    }
}

pub(super) async fn query(
    configured: bool,
    cluster: &ClusterManager,
    topic: &TopicManager,
    consumer: &ConsumerManager,
    producer: &ProducerManager,
) -> Overview {
    if !configured {
        return assemble(false, std::array::from_fn(|_| Metric::unknown()), vec![]);
    }
    let (brokers, topics, consumers, producers) = tokio::join!(
        cluster.get_cluster_home_page(ClusterHomePageRequest { force_refresh: false }),
        topic.get_topic_list(TopicListRequest {
            skip_sys_process: false,
            skip_retry_and_dlq: false
        }),
        consumer.workspace_inventory(),
        producer.workspace_inventory(),
    );
    let brokers = brokers
        .map(|value| {
            Metric::count(
                value.summary.total_brokers,
                if value.summary.brokers_with_status_errors > 0 {
                    Quality::Partial
                } else {
                    Quality::Complete
                },
            )
        })
        .unwrap_or_else(|_| Metric::unknown());
    // The existing Topic catalog does not expose per-source coverage; keep that qualification visible.
    let topics = topics
        .map(|value| Metric::count(value.total, Quality::Reported))
        .unwrap_or_else(|_| Metric::unknown());
    let (consumer_groups, total_lag, lagging_groups) = consumers
        .map(consumer_metrics)
        .unwrap_or_else(|_| (Metric::unknown(), Metric::unknown(), vec![]));
    let producer_groups = producers
        .map(|value| match value.observation {
            WorkspaceObservationState::Complete => Metric::count(value.items.len(), Quality::Complete),
            WorkspaceObservationState::Partial => Metric::count(value.items.len(), Quality::Partial),
            WorkspaceObservationState::Unknown => Metric::unknown(),
        })
        .unwrap_or_else(|_| Metric::unknown());
    assemble(
        true,
        [brokers, topics, consumer_groups, producer_groups, total_lag],
        lagging_groups,
    )
}

fn consumer_metrics(inventory: ConsumerInventoryResult) -> (Metric, Metric, Vec<String>) {
    let discovery_complete = !inventory.targets.is_empty()
        && !inventory
            .failures
            .iter()
            .any(|failure| failure.stage == WorkspaceFailureStage::Inventory);
    let groups = if discovery_complete {
        Metric::count(inventory.items.len(), Quality::Complete)
    } else if !inventory.items.is_empty() {
        Metric::count(inventory.items.len(), Quality::Partial)
    } else {
        Metric::unknown()
    };
    let mut lag = 0i64;
    let mut observed = 0;
    let mut complete = discovery_complete;
    let mut lagging = Vec::new();
    for group in &inventory.items {
        match group.diff_total.value().copied() {
            Some(value) if value >= 0 => {
                let Some(sum) = lag.checked_add(value) else {
                    return (groups, Metric::unknown(), lagging);
                };
                lag = sum;
                observed += 1;
                complete &= group.diff_total.state() == WorkspaceObservationState::Complete;
                if value > 0 && lagging.len() < 10 {
                    lagging.push(group.group.clone());
                }
            }
            _ => complete = false,
        }
    }
    let lag = if complete {
        Metric {
            value: Some(lag),
            quality: Quality::Complete,
        }
    } else if observed > 0 {
        Metric {
            value: Some(lag),
            quality: Quality::Partial,
        }
    } else {
        Metric::unknown()
    };
    (groups, lag, lagging)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overview_distinguishes_unconfigured_down_ready_and_partial_without_zero_fallback() {
        let unknown = || std::array::from_fn(|_| Metric::unknown());
        assert_eq!(assemble(false, unknown(), vec![]).status, OverviewStatus::Unconfigured);
        let down = assemble(true, unknown(), vec![]);
        assert_eq!(down.status, OverviewStatus::Down);
        assert!(down.brokers.value.is_none());
        let metrics = std::array::from_fn(|_| Metric::count(2, Quality::Complete));
        assert_eq!(assemble(true, metrics.clone(), vec![]).status, OverviewStatus::Ready);
        let mut partial = metrics;
        partial[4] = Metric::unknown();
        assert_eq!(assemble(true, partial, vec![]).status, OverviewStatus::Partial);
    }
    #[test]
    fn partial_consumer_lag_preserves_observed_values_without_claiming_complete() {
        use rocketmq_admin_core::core::consumer_workspace::{
            ConsumerInventoryItem, ConsumerWorkspaceTarget, WorkspaceObservation, WorkspaceUnknownReason,
        };
        let group = |name: &str, lag| ConsumerInventoryItem {
            group: name.into(),
            category: "NORMAL".into(),
            client_count: WorkspaceObservation::Unknown {
                reason: WorkspaceUnknownReason::Unavailable,
            },
            diff_total: lag,
            consume_type: WorkspaceObservation::Unknown {
                reason: WorkspaceUnknownReason::Unavailable,
            },
            message_model: WorkspaceObservation::Unknown {
                reason: WorkspaceUnknownReason::Unavailable,
            },
            targets: vec![],
        };
        let inventory = ConsumerInventoryResult {
            items: vec![
                group("a", WorkspaceObservation::Complete { value: 10 }),
                group(
                    "b",
                    WorkspaceObservation::Unknown {
                        reason: WorkspaceUnknownReason::Unavailable,
                    },
                ),
            ],
            targets: vec![ConsumerWorkspaceTarget {
                cluster_name: "c".into(),
                broker_name: "b".into(),
                broker_address: "addr".into(),
            }],
            observation: WorkspaceObservationState::Partial,
            failures: vec![],
        };
        let (groups, lag, lagging) = consumer_metrics(inventory);
        assert_eq!(groups.value, Some(2));
        assert_eq!(groups.quality, Quality::Complete);
        assert_eq!(lag.value, Some(10));
        assert_eq!(lag.quality, Quality::Partial);
        assert_eq!(lagging, ["a"]);
    }

    #[test]
    fn missing_consumer_inventory_cannot_claim_zero_lag() {
        let inventory = ConsumerInventoryResult {
            items: vec![],
            targets: vec![],
            observation: WorkspaceObservationState::Unknown,
            failures: vec![],
        };
        let (groups, lag, _) = consumer_metrics(inventory);
        assert!(groups.value.is_none());
        assert!(lag.value.is_none());
    }
}
