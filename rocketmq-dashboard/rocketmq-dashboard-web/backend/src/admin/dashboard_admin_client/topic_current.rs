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

use std::collections::HashMap;

use rocketmq_admin_core::core::topic::TopicAdmin;
use rocketmq_admin_core::core::topic::TopicCurrentStatsItem;

use super::*;
use crate::model::DashboardTopicCurrent;
use crate::model::TopicCurrentMetric;

/// Most topics ranked per refresh. Each one costs a statistics query to every broker serving it, and
/// the history collector stores a total-message sample for each.
const TOPIC_CURRENT_LIMIT: usize = 20;

impl DashboardAdminClient {
    /// Stored messages and message rates of the busiest application topics.
    ///
    /// `total_topics` counts every topic. The ranking leaves retry, DLQ and system topics out and
    /// keeps at most [`TOPIC_CURRENT_LIMIT`] topics, those that received the most messages in the
    /// last 24 hours first. The rates are last-minute averages from the topic's master brokers:
    /// `in_tps` counts the messages written, `out_tps` the messages pulled by all consumer groups.
    /// Rates that cannot be read are reported as zero.
    pub async fn topic_current(&self) -> Result<DashboardTopicCurrent, DashboardError> {
        let topics = match self.list_topics().await {
            Ok(topics) => topics,
            Err(DashboardError::Admin(error)) => {
                tracing::warn!(error = %error, "RocketMQ cluster is not reachable while building topic-current metrics");
                return Ok(DashboardTopicCurrent {
                    total_topics: 0,
                    top_topics: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        let rates = match run_topic_admin_rpc!(self, |admin| admin.get_topic_current_stats()) {
            Ok(stats) => {
                if !stats.failures.is_empty() {
                    tracing::warn!(
                        failed_topics = stats.failures.len(),
                        "Some topic message rates could not be read while building topic-current metrics"
                    );
                }
                stats.items
            }
            Err(error) => {
                tracing::warn!(error = %error, "Topic message rates are unavailable while building topic-current metrics");
                Vec::new()
            }
        };
        let sampled = sample_topics(&topics.items, rates);

        let mut top_topics = run_admin_rpc!(self, |admin| async {
            let mut top_topics = Vec::with_capacity(sampled.len());
            for topic in sampled {
                if let Ok(stats) = admin.dashboard_topic_stats(&topic.topic).await {
                    top_topics.push(TopicCurrentMetric {
                        topic: topic.topic,
                        total_msg: stats.total_max_offset.saturating_sub(stats.total_min_offset),
                        in_tps: topic.in_tps,
                        out_tps: topic.out_tps,
                    });
                }
            }
            Ok::<_, DashboardError>(top_topics)
        })?;
        top_topics.sort_by_key(|topic| Reverse(topic.total_msg));
        Ok(DashboardTopicCurrent {
            total_topics: topics.total,
            top_topics,
        })
    }
}

/// A topic picked for the ranking, with the rates its master brokers report.
#[derive(Debug, Clone, PartialEq)]
struct SampledTopic {
    topic: String,
    in_tps: f64,
    out_tps: f64,
}

/// Picks the application topics to rank: the most messages received in the last 24 hours first,
/// then the highest current write rate, then by name. Topics without reported rates count as idle.
fn sample_topics(topics: &[TopicInfo], rates: Vec<TopicCurrentStatsItem>) -> Vec<SampledTopic> {
    let rates = rates
        .into_iter()
        .map(|rate| (rate.topic.clone(), rate))
        .collect::<HashMap<_, _>>();
    let mut candidates = topics
        .iter()
        .filter(|topic| is_application_topic(topic))
        .map(|topic| (topic.topic.as_str(), rates.get(&topic.topic)))
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_topic, left), (right_topic, right)| {
        let (left_count, left_tps) = activity(*left);
        let (right_count, right_tps) = activity(*right);
        right_count
            .cmp(&left_count)
            .then_with(|| right_tps.total_cmp(&left_tps))
            .then_with(|| left_topic.cmp(right_topic))
    });
    candidates
        .into_iter()
        .take(TOPIC_CURRENT_LIMIT)
        .map(|(topic, rate)| SampledTopic {
            topic: topic.to_string(),
            in_tps: rate.map_or(0.0, |rate| rate.in_tps),
            out_tps: rate.map_or(0.0, |rate| rate.out_tps),
        })
        .collect()
}

fn activity(rate: Option<&TopicCurrentStatsItem>) -> (u64, f64) {
    rate.map_or((0, 0.0), |rate| (rate.produced_msg_count_24h, rate.in_tps))
}

fn is_application_topic(topic: &TopicInfo) -> bool {
    !topic.system_topic && !matches!(topic.category.as_str(), "RETRY" | "DLQ")
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::topic::TopicCurrentStatsItem;

    use super::SampledTopic;
    use super::TOPIC_CURRENT_LIMIT;
    use super::sample_topics;
    use crate::model::TopicInfo;

    fn topic(name: &str, category: &str) -> TopicInfo {
        TopicInfo {
            topic: name.to_string(),
            broker_name: Some("broker-a".to_string()),
            brokers: vec!["broker-a".to_string()],
            clusters: vec!["DefaultCluster".to_string()],
            read_queue_count: 4,
            write_queue_count: 4,
            perm: 6,
            category: category.to_string(),
            message_type: "NORMAL".to_string(),
            order: false,
            system_topic: category == "SYSTEM",
        }
    }

    fn rate(topic: &str, produced_msg_count_24h: u64, in_tps: f64, out_tps: f64) -> TopicCurrentStatsItem {
        TopicCurrentStatsItem {
            topic: topic.to_string(),
            total_msg: 0,
            produced_msg_count_24h,
            consumed_msg_count_24h: 0,
            in_tps,
            out_tps,
            consumer_group_count: 1,
        }
    }

    fn names(sampled: &[SampledTopic]) -> Vec<&str> {
        sampled.iter().map(|topic| topic.topic.as_str()).collect()
    }

    #[test]
    fn ranks_application_topics_with_the_rates_their_brokers_report() {
        let topics = vec![
            topic("%DLQ%order-consumer", "DLQ"),
            topic("%RETRY%order-consumer", "RETRY"),
            topic("SCHEDULE_TOPIC_XXXX", "SYSTEM"),
            topic("audit-events", "NORMAL"),
            topic("idle-events", "NORMAL"),
            topic("order-events", "FIFO"),
            topic("payment-events", "NORMAL"),
        ];
        let rates = vec![
            rate("%RETRY%order-consumer", 50_000, 40.0, 40.0),
            rate("SCHEDULE_TOPIC_XXXX", 90_000, 80.0, 0.0),
            rate("audit-events", 2_400, 4.0, 8.0),
            rate("order-events", 10_226, 16.98, 33.96),
            rate("payment-events", 3_600, 6.0, 8.13),
        ];

        let sampled = sample_topics(&topics, rates);

        assert_eq!(
            names(&sampled),
            vec!["order-events", "payment-events", "audit-events", "idle-events"]
        );
        assert_eq!((sampled[0].in_tps, sampled[0].out_tps), (16.98, 33.96));
        assert_eq!((sampled[2].in_tps, sampled[2].out_tps), (4.0, 8.0));
        assert_eq!((sampled[3].in_tps, sampled[3].out_tps), (0.0, 0.0));
    }

    #[test]
    fn keeps_the_busiest_topics_when_more_topics_than_the_limit_exist() {
        let mut topics = (0..TOPIC_CURRENT_LIMIT)
            .map(|index| topic(&format!("a-quiet-{index:02}"), "NORMAL"))
            .collect::<Vec<_>>();
        topics.push(topic("z-busy", "NORMAL"));
        topics.push(topic("z-starting", "NORMAL"));
        // A topic that only started receiving messages ranks after one with a 24-hour history.
        let rates = vec![rate("z-starting", 0, 2.5, 0.0), rate("z-busy", 7_200, 1.0, 1.0)];

        let sampled = sample_topics(&topics, rates);

        assert_eq!(sampled.len(), TOPIC_CURRENT_LIMIT);
        assert_eq!(names(&sampled[..3]), vec!["z-busy", "z-starting", "a-quiet-00"]);
        assert_eq!(sampled[TOPIC_CURRENT_LIMIT - 1].topic, "a-quiet-17");
    }
}
