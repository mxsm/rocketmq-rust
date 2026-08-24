// Copyright 2025 The RocketMQ Rust Authors
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

//! Protocol-independent Dashboard requests, projections, and rankings.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{BrokerCurrentMetric, BrokerIdentity, EndpointAvailability};

/// Request for broker overview data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBrokerOverviewRequest {
    /// Forces the backend to bypass a cached overview response.
    pub force_refresh: bool,
}

/// Request for broker history data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBrokerHistoryRequest {
    /// Existing Java Dashboard-compatible history date.
    pub date: String,
}

/// Request for topic history data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTopicHistoryRequest {
    /// Existing Java Dashboard-compatible history date.
    pub date: String,
    /// Topic selected for the history query.
    pub topic_name: String,
}

/// A value explicitly distinguished from missing or unproven data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "value")]
pub enum Observed<T> {
    /// The provider did not return enough evidence to make a claim.
    #[default]
    Unknown,
    /// The value was present in a successful provider response.
    Observed(T),
}

impl<T> Observed<T> {
    /// Returns a reference to the observed value, if one exists.
    pub const fn as_ref(&self) -> Observed<&T> {
        match self {
            Self::Unknown => Observed::Unknown,
            Self::Observed(value) => Observed::Observed(value),
        }
    }

    /// Transforms an observed value without changing `Unknown` semantics.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Observed<U> {
        match self {
            Self::Unknown => Observed::Unknown,
            Self::Observed(value) => Observed::Observed(map(value)),
        }
    }

    /// Returns whether the provider supplied a value.
    pub const fn is_observed(&self) -> bool {
        matches!(self, Self::Observed(_))
    }
}

/// Dashboard overview values projected only from successful Admin responses.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardOverview {
    /// Number of inventory brokers returned by the provider.
    pub broker_count: Observed<u64>,
    /// Number of topics returned by the provider.
    pub topic_count: Observed<u64>,
    /// Number of consumer groups returned by the provider.
    pub consumer_group_count: Observed<u64>,
    /// Number of producer groups observed by the provider.
    pub producer_group_count: Observed<u64>,
    /// Backlog is unknown unless a complete successful aggregation proves it.
    pub consumer_backlog: Observed<i64>,
    /// NameServer availability comes from a real health request.
    pub nameserver_availability: EndpointAvailability,
    /// Broker availability is classified from inventory runtime evidence.
    pub broker_availability: EndpointAvailability,
}

/// Inputs that may independently succeed or fail while building an overview.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DashboardOverviewEvidence {
    /// Successful broker count, if inventory completed.
    pub broker_count: Option<u64>,
    /// Successful topic count, if topic listing completed.
    pub topic_count: Option<u64>,
    /// Successful consumer-group count, if consumer listing completed.
    pub consumer_group_count: Option<u64>,
    /// Successful observed producer-group count, if producer listing completed.
    pub producer_group_count: Option<u64>,
    /// Complete backlog sum. Partial or failed aggregation remains `None`.
    pub complete_consumer_backlog: Option<i64>,
    /// Real NameServer health result, if checked.
    pub nameserver_availability: Option<EndpointAvailability>,
    /// Real per-broker availability results.
    pub broker_availability: Vec<EndpointAvailability>,
}

/// Projects independently obtained evidence without substituting zero or healthy states.
pub fn project_dashboard_overview(evidence: DashboardOverviewEvidence) -> DashboardOverview {
    DashboardOverview {
        broker_count: evidence.broker_count.map_or(Observed::Unknown, Observed::Observed),
        topic_count: evidence.topic_count.map_or(Observed::Unknown, Observed::Observed),
        consumer_group_count: evidence
            .consumer_group_count
            .map_or(Observed::Unknown, Observed::Observed),
        producer_group_count: evidence
            .producer_group_count
            .map_or(Observed::Unknown, Observed::Observed),
        consumer_backlog: evidence
            .complete_consumer_backlog
            .map_or(Observed::Unknown, Observed::Observed),
        nameserver_availability: evidence.nameserver_availability.unwrap_or_default(),
        broker_availability: aggregate_availability(&evidence.broker_availability),
    }
}

/// A current metric backed by real Topic offset statistics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentMetric {
    /// Topic name returned by the Admin API.
    pub topic: String,
    /// Sum of max-minus-min offsets when all returned offsets were valid.
    pub total_messages: Observed<u64>,
    /// Produce TPS is unknown until a provider contract returns it.
    pub produce_tps: Observed<u64>,
    /// Consume TPS is unknown until a provider contract returns it.
    pub consume_tps: Observed<u64>,
}

/// Creates a current Topic metric from a successful offset range.
pub fn topic_current_from_offsets(topic: String, total_min_offset: i64, total_max_offset: i64) -> TopicCurrentMetric {
    let total_messages = total_max_offset
        .checked_sub(total_min_offset)
        .and_then(|value| u64::try_from(value).ok())
        .map_or(Observed::Unknown, Observed::Observed);
    TopicCurrentMetric {
        topic,
        total_messages,
        produce_tps: Observed::Unknown,
        consume_tps: Observed::Unknown,
    }
}

/// A reachable action whose destination is implemented in the current product slice.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardAction {
    /// Open connection and NameServer settings.
    OpenOperations,
    /// Open the Broker inventory.
    OpenBrokers,
}

/// Builds only actionable entries backed by current real state.
pub fn dashboard_actions(overview: &DashboardOverview) -> Vec<DashboardAction> {
    let mut actions = Vec::new();
    if overview.nameserver_availability != EndpointAvailability::Available {
        actions.push(DashboardAction::OpenOperations);
    }
    if matches!(overview.broker_count, Observed::Observed(0))
        || overview.broker_availability == EndpointAvailability::Unavailable
    {
        actions.push(DashboardAction::OpenBrokers);
    }
    actions
}

/// Returns a stable descending Topic ranking. Unknown values follow observed values.
pub fn rank_topics(mut metrics: Vec<TopicCurrentMetric>) -> Vec<TopicCurrentMetric> {
    metrics.sort_by(|left, right| {
        compare_observed_desc(&left.total_messages, &right.total_messages).then(left.topic.cmp(&right.topic))
    });
    metrics
}

/// Returns a stable descending Broker ranking using only observed combined TPS.
pub fn rank_brokers(mut metrics: Vec<BrokerCurrentMetric>) -> Vec<BrokerCurrentMetric> {
    metrics.sort_by(|left, right| {
        compare_observed_f64_desc(&left.combined_tps, &right.combined_tps)
            .then(compare_broker_identity(&left.identity, &right.identity))
    });
    metrics
}

fn aggregate_availability(values: &[EndpointAvailability]) -> EndpointAvailability {
    if values.contains(&EndpointAvailability::Unavailable) {
        EndpointAvailability::Unavailable
    } else if !values.is_empty() && values.iter().all(|value| *value == EndpointAvailability::Available) {
        EndpointAvailability::Available
    } else {
        EndpointAvailability::Unknown
    }
}

fn compare_observed_desc<T: Ord>(left: &Observed<T>, right: &Observed<T>) -> Ordering {
    match (left, right) {
        (Observed::Observed(left), Observed::Observed(right)) => right.cmp(left),
        (Observed::Observed(_), Observed::Unknown) => Ordering::Less,
        (Observed::Unknown, Observed::Observed(_)) => Ordering::Greater,
        (Observed::Unknown, Observed::Unknown) => Ordering::Equal,
    }
}

fn compare_observed_f64_desc(left: &Observed<f64>, right: &Observed<f64>) -> Ordering {
    match (left, right) {
        (Observed::Observed(left), Observed::Observed(right)) => right.total_cmp(left),
        (Observed::Observed(_), Observed::Unknown) => Ordering::Less,
        (Observed::Unknown, Observed::Observed(_)) => Ordering::Greater,
        (Observed::Unknown, Observed::Unknown) => Ordering::Equal,
    }
}

fn compare_broker_identity(left: &BrokerIdentity, right: &BrokerIdentity) -> Ordering {
    left.cluster
        .cmp(&right.cluster)
        .then(left.broker_name.cmp(&right.broker_name))
        .then(left.broker_id.cmp(&right.broker_id))
        .then(left.address.cmp(&right.address))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BrokerCurrentMetric, BrokerIdentity};

    fn identity(name: &str) -> BrokerIdentity {
        BrokerIdentity {
            cluster: "cluster-a".into(),
            broker_name: name.into(),
            broker_id: 0,
            address: format!("{name}:10911"),
        }
    }

    #[test]
    fn existing_request_fields_remain_wire_compatible() {
        assert_eq!(
            serde_json::to_value(DashboardBrokerOverviewRequest { force_refresh: true }).expect("serialize"),
            serde_json::json!({ "forceRefresh": true })
        );
        assert_eq!(
            serde_json::to_value(DashboardBrokerHistoryRequest {
                date: "2026-05-04".into(),
            })
            .expect("serialize"),
            serde_json::json!({ "date": "2026-05-04" })
        );
        assert_eq!(
            serde_json::to_value(DashboardTopicHistoryRequest {
                date: "2026-05-04".into(),
                topic_name: "TopicTest".into(),
            })
            .expect("serialize"),
            serde_json::json!({ "date": "2026-05-04", "topicName": "TopicTest" })
        );
    }

    #[test]
    fn missing_overview_evidence_never_becomes_zero_or_healthy() {
        let overview = project_dashboard_overview(DashboardOverviewEvidence {
            broker_count: Some(0),
            ..Default::default()
        });

        assert_eq!(overview.broker_count, Observed::Observed(0));
        assert_eq!(overview.topic_count, Observed::Unknown);
        assert_eq!(overview.consumer_backlog, Observed::Unknown);
        assert_eq!(overview.nameserver_availability, EndpointAvailability::Unknown);
        assert_eq!(overview.broker_availability, EndpointAvailability::Unknown);
        assert_eq!(
            dashboard_actions(&overview),
            vec![DashboardAction::OpenOperations, DashboardAction::OpenBrokers]
        );
    }

    #[test]
    fn invalid_offset_range_is_unknown_and_tps_is_not_fabricated() {
        let metric = topic_current_from_offsets("orders".into(), 20, 10);
        assert_eq!(metric.total_messages, Observed::Unknown);
        assert_eq!(metric.produce_tps, Observed::Unknown);
        assert_eq!(metric.consume_tps, Observed::Unknown);
    }

    #[test]
    fn rankings_are_stable_and_put_unknown_values_last() {
        let topics = rank_topics(vec![
            TopicCurrentMetric {
                topic: "unknown".into(),
                total_messages: Observed::Unknown,
                produce_tps: Observed::Unknown,
                consume_tps: Observed::Unknown,
            },
            topic_current_from_offsets("b".into(), 0, 7),
            topic_current_from_offsets("a".into(), 0, 7),
        ]);
        assert_eq!(
            topics.iter().map(|item| item.topic.as_str()).collect::<Vec<_>>(),
            ["a", "b", "unknown"]
        );

        let brokers = rank_brokers(vec![
            BrokerCurrentMetric::unknown(identity("unknown"), EndpointAvailability::Unknown),
            BrokerCurrentMetric::observed(identity("b"), "1".into(), 2.0, 3.0),
            BrokerCurrentMetric::observed(identity("a"), "1".into(), 2.0, 3.0),
        ]);
        assert_eq!(
            brokers
                .iter()
                .map(|item| item.identity.broker_name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "unknown"]
        );
    }
}
