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

use crate::tools::executor::ToolRejection;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::model::contract::observed_at;
use crate::model::diagnosis::EvidenceItem;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidencePayload;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::EvidenceStatus;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;

pub(crate) const CONSUMER_LAG_EVIDENCE_VERSION_V2: &str = "rocketmq-mcp.evidence.consumer-lag.v2";

/// How many of the most lagging queues consumer lag evidence lists.
const WORST_QUEUE_LIMIT: usize = 10;

/// The evidence one consumer lag diagnosis is evaluated on.
///
/// Every part summarizes its complete source result. A report has no way to continue a cursor,
/// and a conclusion must not depend on which rows a first page happens to hold.
pub(crate) struct ConsumerLagEvidence {
    pub lag: Result<ConsumerLagSummary, ToolFailure>,
    pub topic: Result<TopicSummary, ToolFailure>,
    pub route: Result<TopicRouteSummary, ToolFailure>,
    pub broker: Option<Result<DescribeBrokerOutput, ToolFailure>>,
}

/// Lag of one Consumer Group on one Topic, measured over every queue.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct ConsumerLagSummary {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
    #[serde(flatten)]
    pub queues: QueueLagBreakdown,
    pub generated_at: String,
}

impl ConsumerLagSummary {
    /// The Broker that holds the most lag, or `None` when no queue was observed.
    pub(crate) fn top_lag_broker(&self) -> Option<&str> {
        self.queues.brokers.first().map(|broker| broker.broker_name.as_str())
    }
}

/// Where the lag of a complete queue set sits.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct QueueLagBreakdown {
    pub queue_count: usize,
    pub max_queue_lag: i64,
    /// Lag per Broker, largest first.
    pub brokers: Vec<BrokerLag>,
    /// The most lagging queues, largest first, at most [`WORST_QUEUE_LIMIT`].
    pub worst_queues: Vec<QueueLag>,
}

impl QueueLagBreakdown {
    /// Aggregates a complete queue set; the rows may arrive in any order.
    ///
    /// Equal lag is ordered by Broker name and then queue id, so the same queue set always
    /// yields the same breakdown.
    pub(crate) fn from_queues(mut queues: Vec<QueueLag>) -> Self {
        let mut brokers = Vec::<BrokerLag>::new();
        queues.sort_by(|left, right| {
            left.broker_name
                .cmp(&right.broker_name)
                .then(left.queue_id.cmp(&right.queue_id))
        });
        for queue in &queues {
            match brokers.last_mut() {
                Some(broker) if broker.broker_name == queue.broker_name => {
                    broker.queue_count += 1;
                    broker.lag = broker.lag.saturating_add(queue.lag);
                    broker.max_queue_lag = broker.max_queue_lag.max(queue.lag);
                }
                _ => brokers.push(BrokerLag {
                    broker_name: queue.broker_name.clone(),
                    queue_count: 1,
                    lag: queue.lag,
                    max_queue_lag: queue.lag,
                }),
            }
        }
        // Both sorts are stable, so the name and queue order above breaks ties.
        brokers.sort_by_key(|broker| std::cmp::Reverse(broker.lag));
        let queue_count = queues.len();
        let max_queue_lag = queues.iter().map(|queue| queue.lag).max().unwrap_or_default();
        queues.sort_by_key(|queue| std::cmp::Reverse(queue.lag));
        queues.truncate(WORST_QUEUE_LIMIT);
        Self {
            queue_count,
            max_queue_lag,
            brokers,
            worst_queues: queues,
        }
    }
}

/// Lag of the queues one Broker hosts.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct BrokerLag {
    pub broker_name: String,
    pub queue_count: usize,
    pub lag: i64,
    pub max_queue_lag: i64,
}

/// Every route row of a Topic: one per Broker that hosts it.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TopicRouteSummary {
    pub cluster: String,
    pub topic: String,
    pub brokers: Vec<TopicRouteBroker>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub queues: Vec<TopicRouteQueue>,
    pub generated_at: String,
}

/// The Brokers and queue totals of a Topic.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TopicSummary {
    pub cluster: String,
    pub topic: String,
    pub broker_names: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub generated_at: String,
}

impl TopicSummary {
    pub(crate) fn from_route(route: &TopicRouteSummary) -> Self {
        let mut broker_names = route
            .brokers
            .iter()
            .map(|broker| broker.broker_name.clone())
            .collect::<Vec<_>>();
        broker_names.sort();
        broker_names.dedup();
        Self {
            cluster: route.cluster.clone(),
            topic: route.topic.clone(),
            broker_names,
            read_queue_count: route.read_queue_count,
            write_queue_count: route.write_queue_count,
            generated_at: route.generated_at.clone(),
        }
    }
}

impl ConsumerLagEvidence {
    pub(crate) fn snapshot(&self, args: &DiagnoseConsumerLagArgs) -> EvidenceSnapshot {
        let query_hash = query_hash(args);
        let mut items = vec![
            item(
                "consumer_lag",
                "rocketmq_get_consumer_lag",
                EvidenceKind::ConsumerLag,
                &query_hash,
                &self.lag,
            ),
            item(
                "topic_description",
                "rocketmq_describe_topic",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.topic,
            ),
            item(
                "topic_route",
                "rocketmq_get_topic_route",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.route,
            ),
        ];
        items.push(match &self.broker {
            Some(result) => item(
                "broker_summary",
                "rocketmq_describe_broker",
                EvidenceKind::BrokerSummary,
                &query_hash,
                result,
            ),
            None => missing_broker_item(&query_hash),
        });
        EvidenceSnapshot {
            evidence_version: CONSUMER_LAG_EVIDENCE_VERSION_V2.to_string(),
            query_hash,
            cluster: args.cluster.clone(),
            target: json!({ "topic": args.topic, "consumer_group": args.consumer_group }),
            observed_at: observed_at(),
            items,
        }
    }
}

fn query_hash(args: &DiagnoseConsumerLagArgs) -> String {
    let mut digest = Sha256::new();
    digest.update(args.cluster.as_bytes());
    digest.update([0]);
    digest.update(args.topic.as_bytes());
    digest.update([0]);
    digest.update(args.consumer_group.as_bytes());
    digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn item<T>(
    id: &str,
    source_tool: &str,
    kind: EvidenceKind,
    query_hash: &str,
    result: &Result<T, ToolFailure>,
) -> EvidenceItem
where
    T: Serialize,
{
    match result {
        Ok(value) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: EvidenceStatus::Present,
            query_hash: query_hash.to_string(),
            payload: Some(payload(kind, value)),
            error_code: None,
            summary: format!("{source_tool} returned evidence."),
        },
        Err(error) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: error_status(error),
            query_hash: query_hash.to_string(),
            payload: None,
            error_code: Some(error.code().to_string()),
            summary: error.to_string(),
        },
    }
}

fn payload<T>(kind: EvidenceKind, value: &T) -> EvidencePayload
where
    T: Serialize,
{
    let value = serde_json::to_value(value).unwrap_or(Value::Null);
    match kind {
        EvidenceKind::ConsumerLag => EvidencePayload::ConsumerLag(value),
        EvidenceKind::TopicRoute => EvidencePayload::TopicRoute(value),
        EvidenceKind::BrokerSummary => EvidencePayload::BrokerSummary(value),
    }
}

fn error_status(error: &ToolFailure) -> EvidenceStatus {
    match error {
        ToolFailure::Rejected(ToolRejection::TimedOut { .. }) => EvidenceStatus::Timeout,
        ToolFailure::Rejected(ToolRejection::PermissionDenied) => EvidenceStatus::Unauthorized,
        ToolFailure::Rejected(ToolRejection::InvalidArguments { .. }) => EvidenceStatus::Invalid,
        // The lookup completed and established that the target does not exist.
        ToolFailure::Rejected(ToolRejection::NotFound { .. }) => EvidenceStatus::Missing,
        _ => EvidenceStatus::Unavailable,
    }
}

fn missing_broker_item(query_hash: &str) -> EvidenceItem {
    EvidenceItem {
        id: "broker_summary".to_string(),
        kind: EvidenceKind::BrokerSummary,
        source_tool: "rocketmq_describe_broker".to_string(),
        observed_at: observed_at(),
        freshness_ms: 0,
        status: EvidenceStatus::Missing,
        query_hash: query_hash.to_string(),
        payload: None,
        error_code: None,
        summary: "No broker was selected because consumer lag evidence was unavailable.".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_replayable_and_marks_missing_evidence() {
        let args = DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
        };
        let evidence = ConsumerLagEvidence {
            lag: Err(ToolFailure::Operational(
                crate::tools::executor::ToolExecutionError::Backend(None),
            )),
            topic: Err(ToolFailure::Operational(
                crate::tools::executor::ToolExecutionError::Backend(None),
            )),
            route: Err(ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut {
                timeout_ms: 5_000,
            })),
            broker: None,
        };

        let first = evidence.snapshot(&args);
        let second = evidence.snapshot(&args);

        assert_eq!(first.query_hash, second.query_hash);
        assert_eq!(first.evidence_version, CONSUMER_LAG_EVIDENCE_VERSION_V2);
        assert_eq!(first.items[0].status, EvidenceStatus::Unavailable);
        assert_eq!(first.items[2].status, EvidenceStatus::Timeout);
        assert_eq!(first.items[3].status, EvidenceStatus::Missing);
    }

    fn queue(broker_name: &str, queue_id: i32, lag: i64) -> QueueLag {
        QueueLag {
            topic: "orders".to_string(),
            broker_name: broker_name.to_string(),
            queue_id,
            broker_offset: lag,
            consumer_offset: 0,
            lag,
            inflight: 0,
            last_observed_at: None,
            client_ip: None,
        }
    }

    #[test]
    fn breakdown_aggregates_every_queue_and_orders_ties_deterministically() {
        let breakdown = QueueLagBreakdown::from_queues(vec![
            queue("broker-b", 1, 40),
            queue("broker-a", 1, 30),
            queue("broker-c", 0, 70),
            queue("broker-b", 0, 40),
            queue("broker-a", 0, 40),
        ]);

        assert_eq!(breakdown.queue_count, 5);
        assert_eq!(breakdown.max_queue_lag, 70);
        let brokers = breakdown
            .brokers
            .iter()
            .map(|broker| {
                (
                    broker.broker_name.as_str(),
                    broker.queue_count,
                    broker.lag,
                    broker.max_queue_lag,
                )
            })
            .collect::<Vec<_>>();
        // broker-a and broker-c both hold 70; the name decides.
        assert_eq!(
            brokers,
            [
                ("broker-b", 2, 80, 40),
                ("broker-a", 2, 70, 40),
                ("broker-c", 1, 70, 70)
            ]
        );
        let worst = breakdown
            .worst_queues
            .iter()
            .map(|queue| (queue.broker_name.as_str(), queue.queue_id, queue.lag))
            .collect::<Vec<_>>();
        assert_eq!(
            worst,
            [
                ("broker-c", 0, 70),
                ("broker-a", 0, 40),
                ("broker-b", 0, 40),
                ("broker-b", 1, 40),
                ("broker-a", 1, 30),
            ]
        );
    }

    #[test]
    fn breakdown_lists_only_the_worst_queues_and_tolerates_an_empty_set() {
        let breakdown =
            QueueLagBreakdown::from_queues((0..25).map(|id| queue("broker-a", id, i64::from(id))).collect());
        assert_eq!(breakdown.queue_count, 25);
        assert_eq!(breakdown.worst_queues.len(), WORST_QUEUE_LIMIT);
        assert_eq!(breakdown.worst_queues[0].lag, 24);
        assert_eq!(breakdown.worst_queues[WORST_QUEUE_LIMIT - 1].lag, 15);
        assert_eq!(breakdown.brokers[0].lag, (0..25).sum::<i64>());

        let empty = QueueLagBreakdown::from_queues(Vec::new());
        assert_eq!(empty.queue_count, 0);
        assert_eq!(empty.max_queue_lag, 0);
        assert!(empty.brokers.is_empty());
        assert!(empty.worst_queues.is_empty());
    }
}
