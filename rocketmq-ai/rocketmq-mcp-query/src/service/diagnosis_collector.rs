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

use serde::Serialize;
use serde_json::json;

use crate::model::contract::millis_from_observed_at;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::service::diagnosis_evidence::elapsed_millis;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::service::diagnosis_evidence::TopicConfigSummary;
use crate::service::diagnosis_snapshot;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;

pub(crate) const CONSUMER_LAG_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.consumer-lag.v3";

/// Identifiers of the evidence items of a consumer lag diagnosis. A report names an item by its
/// identifier wherever it refers to evidence.
pub(crate) mod evidence_id {
    pub(crate) const CONSUMER_LAG: &str = "consumer_lag";
    pub(crate) const TOPIC_ROUTE: &str = "topic_route";
    pub(crate) const BROKER_SUMMARY: &str = "broker_summary";
    pub(crate) const CONSUMER_GROUP: &str = "consumer_group";
    pub(crate) const TOPIC_CONFIG: &str = "topic_config";
    pub(crate) const RETRY_TOPIC_STATS: &str = "retry_topic_stats";
    pub(crate) const DEAD_LETTER_TOPIC_STATS: &str = "dead_letter_topic_stats";
}

/// How many of the most lagging queues consumer lag evidence lists.
const WORST_QUEUE_LIMIT: usize = 10;

/// The evidence one consumer lag diagnosis is evaluated on.
///
/// Every part summarizes its complete source result. A report has no way to continue a cursor,
/// and a conclusion must not depend on which rows a first page happens to hold.
///
/// `lag` and `route` say whether there is a backlog and where; the other parts explain it. A part
/// that could not be read is an error here and missing evidence in the report, never a reason to
/// fail the diagnosis.
pub(crate) struct ConsumerLagEvidence {
    pub lag: Result<ConsumerLagSummary, ToolFailure>,
    pub route: Result<TopicRouteSummary, ToolFailure>,
    /// The Broker that holds the most lag, or `None` when no lag was observed to pick it by.
    pub broker: Option<Result<DescribeBrokerOutput, ToolFailure>>,
    pub group: Result<ConsumerGroupState, ToolFailure>,
    pub topic_config: Result<TopicConfigSummary, ToolFailure>,
    pub retry: Result<TopicActivity, ToolFailure>,
    pub dead_letter: Result<TopicActivity, ToolFailure>,
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
    pub delay: ConsumeDelay,
    #[serde(flatten)]
    pub queues: QueueLagBreakdown,
    pub generated_at: String,
}

impl ConsumerLagSummary {
    /// The Broker that holds the most lag, or `None` when no queue was observed.
    pub(crate) fn top_lag_broker(&self) -> Option<&str> {
        self.queues.brokers.first().map(|broker| broker.broker_name.as_str())
    }

    pub(crate) fn figures(&self) -> LagFigures<'_> {
        LagFigures {
            total_lag: self.total_lag,
            consume_tps: self.consume_tps,
            inflight_total: self.inflight_total,
            delay: self.delay,
            queues: &self.queues,
        }
    }
}

/// The figures of a backlog that the rules and the severity grade read. They are the same whether
/// the backlog is that of one Topic or of every Topic a group consumes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LagFigures<'a> {
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
    pub delay: ConsumeDelay,
    pub queues: &'a QueueLagBreakdown,
}

impl LagFigures<'static> {
    /// The figures of a backlog that could not be read: nothing lags and nothing was observed.
    pub(crate) const UNOBSERVED: Self = Self {
        total_lag: 0,
        consume_tps: 0.0,
        inflight_total: 0,
        delay: ConsumeDelay {
            consume_delay_ms: None,
            estimated_produce_tps: None,
        },
        queues: &QueueLagBreakdown::EMPTY,
    };
}

/// How far behind production a Consumer Group is.
///
/// A queue reports the store time of the last message the group consumed from it. On a queue with
/// lag, everything stored since then is waiting, so the time since then bounds how long the oldest
/// waiting message has waited, and the lag divided by it is the rate at which the backlog built up.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq)]
pub(crate) struct ConsumeDelay {
    /// At least half of the measured backlog sits on queues that are this far behind, or further.
    /// `None` when the queues that hold most of the backlog report no consumed message.
    pub consume_delay_ms: Option<u64>,
    /// Messages per second that arrived on the lagging queues while the backlog built up.
    pub estimated_produce_tps: Option<f64>,
}

impl ConsumeDelay {
    /// Measures the delay of a complete queue set at `now_millis`.
    ///
    /// The delay is weighted by lag: one quiet queue that holds a single fresh message must not
    /// make a busy Topic look hours behind.
    pub(crate) fn measure(queues: &[QueueLag], now_millis: i64) -> Self {
        let lagging = queues.iter().filter(|queue| queue.lag > 0);
        let backlog = lagging
            .clone()
            .fold(0_i64, |total, queue| total.saturating_add(queue.lag));
        // A queue the group never consumed from reports the Unix epoch, which measures nothing.
        let mut measured = lagging
            .filter_map(|queue| {
                let consumed_at = millis_from_observed_at(queue.last_observed_at.as_deref()?)?;
                (consumed_at > 0).then(|| (elapsed_millis(consumed_at, now_millis), queue.lag))
            })
            .collect::<Vec<_>>();
        let measured_backlog = measured
            .iter()
            .fold(0_i64, |total, (_, lag)| total.saturating_add(*lag));
        if measured_backlog == 0 || measured_backlog.saturating_mul(2) < backlog {
            return Self::default();
        }
        let estimated_produce_tps = measured
            .iter()
            .map(|(delay_ms, lag)| *lag as f64 * 1_000.0 / (*delay_ms).max(1) as f64)
            .sum::<f64>();
        measured.sort_by_key(|(delay_ms, _)| std::cmp::Reverse(*delay_ms));
        let mut covered = 0_i64;
        let consume_delay_ms = measured.iter().find_map(|(delay_ms, lag)| {
            covered = covered.saturating_add(*lag);
            (covered.saturating_mul(2) >= measured_backlog).then_some(*delay_ms)
        });
        Self {
            consume_delay_ms,
            // Six decimals keep the rate of a Topic that receives one message an hour visible.
            estimated_produce_tps: Some((estimated_produce_tps * 1_000_000.0).round() / 1_000_000.0),
        }
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
    pub(crate) const EMPTY: Self = Self {
        queue_count: 0,
        max_queue_lag: 0,
        brokers: Vec::new(),
        worst_queues: Vec::new(),
    };

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

impl ConsumerLagEvidence {
    pub(crate) fn snapshot(&self, args: &DiagnoseConsumerLagArgs) -> EvidenceSnapshot {
        let query_hash = diagnosis_snapshot::query_hash(&[&args.cluster, &args.topic, &args.consumer_group]);
        let items = vec![
            diagnosis_snapshot::item(
                evidence_id::CONSUMER_LAG,
                "rocketmq_get_consumer_lag",
                EvidenceKind::ConsumerLag,
                &query_hash,
                &self.lag,
            ),
            diagnosis_snapshot::item(
                evidence_id::TOPIC_ROUTE,
                "rocketmq_get_topic_route",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.route,
            ),
            match &self.broker {
                Some(result) => diagnosis_snapshot::item(
                    evidence_id::BROKER_SUMMARY,
                    "rocketmq_describe_broker",
                    EvidenceKind::BrokerSummary,
                    &query_hash,
                    result,
                ),
                None => diagnosis_snapshot::not_collected(
                    evidence_id::BROKER_SUMMARY,
                    "rocketmq_describe_broker",
                    EvidenceKind::BrokerSummary,
                    &query_hash,
                    "No Broker was selected because no lag was observed to select it by.",
                ),
            },
            diagnosis_snapshot::item(
                evidence_id::CONSUMER_GROUP,
                "rocketmq_get_consumer_group_details",
                EvidenceKind::ConsumerGroup,
                &query_hash,
                &self.group,
            ),
            diagnosis_snapshot::item(
                evidence_id::TOPIC_CONFIG,
                "rocketmq_get_topic_config",
                EvidenceKind::TopicConfig,
                &query_hash,
                &self.topic_config,
            ),
            diagnosis_snapshot::system_topic_item(evidence_id::RETRY_TOPIC_STATS, &query_hash, &self.retry),
            diagnosis_snapshot::system_topic_item(evidence_id::DEAD_LETTER_TOPIC_STATS, &query_hash, &self.dead_letter),
        ];
        diagnosis_snapshot::snapshot(
            CONSUMER_LAG_EVIDENCE_VERSION,
            query_hash,
            &args.cluster,
            json!({ "topic": args.topic, "consumer_group": args.consumer_group }),
            items,
        )
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Evidence fixtures shared by the collector, rule and report tests.

    use super::*;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::config_tools::GetTopicConfigOutput;
    use crate::tools::config_tools::TopicConfigObservationRow;
    use crate::tools::consumer_tools::ConsumerConnectionState;
    use crate::tools::consumer_tools::ConsumerConsumeType;
    use crate::tools::consumer_tools::ConsumerGroupConfigPresence;
    use crate::tools::consumer_tools::ConsumerGroupDetailsBrokerRow;
    use crate::tools::consumer_tools::ConsumerMessageModel;
    use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
    use crate::tools::executor::ToolExecutionError;

    /// The instant every fixture is observed at.
    pub(crate) const NOW: &str = "2026-10-10T12:00:00.000Z";

    pub(crate) fn now_millis() -> i64 {
        millis_from_observed_at(NOW).unwrap()
    }

    /// The timestamp `seconds` before [`NOW`].
    pub(crate) fn seconds_ago(seconds: i64) -> Option<String> {
        crate::model::contract::observed_at_from_millis(now_millis() - seconds * 1_000)
    }

    pub(crate) fn args() -> DiagnoseConsumerLagArgs {
        DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
        }
    }

    pub(crate) fn queue(broker_name: &str, queue_id: i32, lag: i64) -> QueueLag {
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

    /// A queue whose last consumed message was stored `seconds` before [`NOW`].
    pub(crate) fn queue_behind(broker_name: &str, queue_id: i32, lag: i64, seconds: i64) -> QueueLag {
        QueueLag {
            last_observed_at: seconds_ago(seconds),
            ..queue(broker_name, queue_id, lag)
        }
    }

    pub(crate) fn lag_summary(queues: Vec<QueueLag>, consume_tps: f64) -> ConsumerLagSummary {
        ConsumerLagSummary {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
            total_lag: queues.iter().map(|queue| queue.lag).sum(),
            consume_tps,
            inflight_total: queues.iter().map(|queue| queue.inflight).sum(),
            delay: ConsumeDelay::measure(&queues, now_millis()),
            queues: QueueLagBreakdown::from_queues(queues),
            generated_at: NOW.to_string(),
        }
    }

    pub(crate) fn route(rows: &[(&str, u32)]) -> TopicRouteSummary {
        TopicRouteSummary {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            brokers: Vec::new(),
            read_queue_count: 4 * rows.len() as u32,
            write_queue_count: 4 * rows.len() as u32,
            queues: rows
                .iter()
                .map(|(broker_name, perm)| TopicRouteQueue {
                    broker_name: broker_name.to_string(),
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: *perm,
                    topic_sys_flag: 0,
                })
                .collect(),
            generated_at: NOW.to_string(),
        }
    }

    /// A group with one Broker row per entry: the Broker name and whether a consumer is connected.
    pub(crate) fn group(rows: &[(&str, bool)]) -> ConsumerGroupState {
        group_with(rows, |_| {})
    }

    pub(crate) fn group_with(
        rows: &[(&str, bool)],
        adjust: impl Fn(&mut ConsumerGroupDetailsBrokerRow),
    ) -> ConsumerGroupState {
        let brokers = rows
            .iter()
            .map(|(broker_name, online)| {
                let mut row = ConsumerGroupDetailsBrokerRow {
                    broker_name: broker_name.to_string(),
                    config_state: ConsumerGroupConfigPresence::Present,
                    config_version: Some(1),
                    consume_enable: Some(true),
                    consume_from_min_enable: Some(true),
                    consume_broadcast_enable: Some(false),
                    consume_message_orderly: Some(false),
                    retry_queue_nums: Some(1),
                    retry_max_times: Some(16),
                    notify_consumer_ids_changed_enable: Some(true),
                    consume_timeout_minutes: Some(15),
                    connection_state: Some(if *online {
                        ConsumerConnectionState::Online
                    } else {
                        ConsumerConnectionState::Offline
                    }),
                    connection_count: u64::from(*online) * 2,
                    consume_type: online.then_some(ConsumerConsumeType::Push),
                    message_model: online.then_some(ConsumerMessageModel::Clustering),
                    consume_from_where: None,
                };
                adjust(&mut row);
                row
            })
            .collect::<Vec<_>>();
        ConsumerGroupState::from_details(
            GetConsumerGroupDetailsOutput {
                cluster: "local-dev".to_string(),
                consumer_group: "order-service".to_string(),
                total_connection_count: brokers.iter().map(|row| row.connection_count).sum(),
                brokers,
                generated_at: NOW.to_string(),
            },
            true,
        )
    }

    pub(crate) fn topic_config(rows: &[(&str, u32)]) -> TopicConfigSummary {
        TopicConfigSummary::from_config(GetTopicConfigOutput {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            brokers: rows
                .iter()
                .map(|(broker_name, perm)| TopicConfigObservationRow {
                    broker_name: broker_name.to_string(),
                    version: 1,
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: *perm,
                    order: false,
                    message_type: "NORMAL".to_string(),
                })
                .collect(),
            inconsistent_fields: Vec::new(),
            generated_at: NOW.to_string(),
        })
    }

    /// A system Topic that holds `message_count` messages, the newest stored `seconds` ago.
    pub(crate) fn system_topic(topic: &str, message_count: u64, seconds: i64) -> TopicActivity {
        TopicActivity {
            cluster: "local-dev".to_string(),
            topic: topic.to_string(),
            exists: true,
            queue_count: 1,
            message_count,
            last_message_at: seconds_ago(seconds),
            last_message_age_ms: Some(u64::try_from(seconds).unwrap() * 1_000),
            generated_at: NOW.to_string(),
        }
    }

    pub(crate) fn unavailable<T>() -> Result<T, ToolFailure> {
        Err(ToolFailure::Operational(ToolExecutionError::SourceUnavailable(None)))
    }

    /// The description of the Broker that holds the most lag, as the workflow selects it.
    fn top_lag_broker(lag: &ConsumerLagSummary) -> Option<Result<DescribeBrokerOutput, ToolFailure>> {
        let broker_name = lag.top_lag_broker()?.to_string();
        Some(Ok(DescribeBrokerOutput {
            cluster: "local-dev".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
            brokers: vec![BrokerSummary {
                cluster: "local-dev".to_string(),
                broker_name: broker_name.clone(),
                broker_id: 0,
                broker_addr: "127.0.0.1:10911".to_string(),
                version: "V5_3_0".to_string(),
                in_tps: "10.0".to_string(),
                out_tps: "10.0".to_string(),
                timer_progress: "0".to_string(),
                page_cache_lock_time_millis: "0".to_string(),
                hour: "0".to_string(),
                space: "0".to_string(),
                broker_active: true,
            }],
            broker_name,
            generated_at: NOW.to_string(),
        }))
    }

    /// Complete evidence of a healthy group around `lag`: consumers online on every Broker that
    /// holds lag, reads allowed, and no retry or dead-letter Topic.
    pub(crate) fn evidence(lag: ConsumerLagSummary) -> ConsumerLagEvidence {
        let brokers = lag
            .queues
            .brokers
            .iter()
            .map(|broker| broker.broker_name.clone())
            .collect::<Vec<_>>();
        let online = brokers.iter().map(|name| (name.as_str(), true)).collect::<Vec<_>>();
        let readable = brokers.iter().map(|name| (name.as_str(), 6)).collect::<Vec<_>>();
        ConsumerLagEvidence {
            route: Ok(route(&readable)),
            broker: top_lag_broker(&lag),
            group: Ok(group(&online)),
            topic_config: Ok(topic_config(&readable)),
            retry: Ok(TopicActivity::absent("local-dev", "%RETRY%order-service".to_string())),
            dead_letter: Ok(TopicActivity::absent("local-dev", "%DLQ%order-service".to_string())),
            lag: Ok(lag),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::queue;
    use super::fixtures::queue_behind;
    use super::*;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::tools::executor::ToolRejection;

    #[test]
    fn snapshot_is_replayable_and_marks_missing_evidence() {
        let args = fixtures::args();
        let evidence = ConsumerLagEvidence {
            lag: fixtures::unavailable(),
            route: Err(ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 5_000 })),
            broker: None,
            group: fixtures::unavailable(),
            topic_config: fixtures::unavailable(),
            retry: Ok(TopicActivity::absent("local-dev", "%RETRY%order-service".to_string())),
            dead_letter: fixtures::unavailable(),
        };

        let first = evidence.snapshot(&args);
        let second = evidence.snapshot(&args);

        assert_eq!(first.query_hash, second.query_hash);
        assert_eq!(first.evidence_version, CONSUMER_LAG_EVIDENCE_VERSION);
        let status = |id: &str| first.items.iter().find(|item| item.id == id).unwrap().status;
        assert_eq!(status(evidence_id::CONSUMER_LAG), EvidenceStatus::Unavailable);
        assert_eq!(status(evidence_id::TOPIC_ROUTE), EvidenceStatus::Timeout);
        assert_eq!(status(evidence_id::BROKER_SUMMARY), EvidenceStatus::Missing);
        assert_eq!(
            status(evidence_id::DEAD_LETTER_TOPIC_STATS),
            EvidenceStatus::Unavailable
        );
        // A retry Topic that does not exist is a completed observation.
        assert_eq!(status(evidence_id::RETRY_TOPIC_STATS), EvidenceStatus::Present);
    }

    #[test]
    fn every_evidence_item_has_a_distinct_identifier() {
        let snapshot =
            fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 1)], 1.0)).snapshot(&fixtures::args());

        let mut ids = snapshot.items.iter().map(|item| item.id.as_str()).collect::<Vec<_>>();
        assert_eq!(ids.len(), 7);
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 7, "an identifier names exactly one piece of evidence");
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

    #[test]
    fn consume_delay_is_the_delay_of_most_of_the_backlog() {
        let now = fixtures::now_millis();
        // 900 of 1,000 lagging messages are ten minutes behind; one quiet queue is a day behind.
        let delay = ConsumeDelay::measure(
            &[
                queue_behind("broker-a", 0, 500, 600),
                queue_behind("broker-a", 1, 400, 600),
                queue_behind("broker-b", 0, 100, 86_400),
                queue_behind("broker-b", 1, 0, 172_800),
            ],
            now,
        );

        assert_eq!(delay.consume_delay_ms, Some(600_000));
        // 900 messages in ten minutes plus 100 in a day.
        assert_eq!(delay.estimated_produce_tps, Some(1.501157));
    }

    #[test]
    fn consume_delay_needs_a_consumed_message_on_most_of_the_backlog() {
        let now = fixtures::now_millis();
        let never_consumed = QueueLag {
            last_observed_at: Some("1970-01-01T00:00:00.000Z".to_string()),
            ..queue("broker-a", 0, 600)
        };

        assert_eq!(
            ConsumeDelay::measure(&[never_consumed.clone(), queue_behind("broker-a", 1, 400, 60)], now),
            ConsumeDelay::default(),
            "the measured queues hold less than half of the backlog"
        );
        assert_eq!(
            ConsumeDelay::measure(&[never_consumed, queue_behind("broker-a", 1, 600, 60)], now).consume_delay_ms,
            Some(60_000)
        );
        assert_eq!(
            ConsumeDelay::measure(&[queue("broker-a", 0, 0)], now),
            ConsumeDelay::default()
        );
        assert_eq!(ConsumeDelay::measure(&[], now), ConsumeDelay::default());
    }

    #[test]
    fn consume_delay_tolerates_a_broker_clock_ahead_of_this_server() {
        let delay = ConsumeDelay::measure(&[queue_behind("broker-a", 0, 10, -30)], fixtures::now_millis());

        assert_eq!(delay.consume_delay_ms, Some(0));
        assert_eq!(delay.estimated_produce_tps, Some(10_000.0));
    }
}
