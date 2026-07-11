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
use serde_json::Value;

#[cfg(test)]
use crate::adapter::query_facade::ReadOnlyQuery;
use crate::model::contract::observed_at;
#[cfg(test)]
use crate::model::contract::PageRequest;
#[cfg(test)]
use crate::model::contract::QueryResult;
use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::Evidence;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::ImpactItem;
use crate::model::diagnosis::MetricWatchItem;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::RootCauseCandidate;
use crate::model::diagnosis::Severity;
#[cfg(test)]
use crate::tools::broker_tools::DescribeBrokerArgs;
#[cfg(test)]
use crate::tools::consumer_tools::QueryConsumerLagArgs;
use crate::tools::consumer_tools::QueryConsumerLagOutput;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
#[cfg(test)]
use crate::tools::topic_tools::DescribeTopicArgs;
#[cfg(test)]
use crate::tools::topic_tools::QueryTopicRouteArgs;

#[cfg(test)]
const DEFAULT_LAG_THRESHOLD: i64 = 1_000;
const SKEW_RATIO_THRESHOLD: f64 = 0.70;
pub(crate) const CONSUMER_LAG_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.consumer-lag.v1";
pub(crate) const CONSUMER_LAG_RULES_VERSION: &str = "rocketmq-mcp.rules.consumer-lag.v1";

#[cfg(test)]
pub(crate) async fn diagnose_consumer_lag<A>(
    adapter: &A,
    args: DiagnoseConsumerLagArgs,
) -> Result<DiagnosisReport, ToolExecutionError>
where
    A: ReadOnlyQuery,
{
    let lag_result = adapter
        .query_consumer_lag(QueryConsumerLagArgs {
            cluster: args.cluster.clone(),
            topic: args.topic.clone(),
            consumer_group: args.consumer_group.clone(),
            page: PageRequest::default(),
        })
        .await
        .map(|result| result.data);
    let topic_result = adapter
        .describe_topic(DescribeTopicArgs {
            cluster: args.cluster.clone(),
            topic: args.topic.clone(),
            page: PageRequest::default(),
        })
        .await
        .map(|result| result.data);
    let route_result = adapter
        .query_topic_route(QueryTopicRouteArgs {
            cluster: args.cluster.clone(),
            topic: args.topic.clone(),
            page: PageRequest::default(),
        })
        .await
        .map(|result| result.data);
    let broker_result = match top_lag_broker(lag_result.as_ref().ok()) {
        Some(broker_name) => Some(
            adapter
                .describe_broker(DescribeBrokerArgs {
                    cluster: args.cluster.clone(),
                    broker_name,
                })
                .await
                .map(|result| result.data),
        ),
        None => None,
    };

    Ok(build_consumer_lag_report(
        args,
        lag_result,
        topic_result,
        route_result,
        broker_result,
    ))
}

#[cfg(test)]
pub(crate) fn build_consumer_lag_report<Topic, Route, Broker>(
    args: DiagnoseConsumerLagArgs,
    lag_result: Result<QueryConsumerLagOutput, ToolExecutionError>,
    topic_result: Result<Topic, ToolExecutionError>,
    route_result: Result<Route, ToolExecutionError>,
    broker_result: Option<Result<Broker, ToolExecutionError>>,
) -> DiagnosisReport
where
    Topic: Serialize,
    Route: Serialize,
    Broker: Serialize,
{
    build_consumer_lag_report_with_threshold(
        args,
        DEFAULT_LAG_THRESHOLD,
        lag_result,
        topic_result,
        route_result,
        broker_result,
    )
}

pub(crate) fn build_consumer_lag_report_with_threshold<Topic, Route, Broker>(
    args: DiagnoseConsumerLagArgs,
    threshold: i64,
    lag_result: Result<QueryConsumerLagOutput, ToolExecutionError>,
    topic_result: Result<Topic, ToolExecutionError>,
    route_result: Result<Route, ToolExecutionError>,
    broker_result: Option<Result<Broker, ToolExecutionError>>,
) -> DiagnosisReport
where
    Topic: Serialize,
    Route: Serialize,
    Broker: Serialize,
{
    let threshold = threshold.max(0);
    let lag = lag_result.as_ref().ok();
    let severity = severity_for_lag(lag, threshold);
    let confidence = confidence_for_evidence(lag, topic_result.as_ref().ok(), route_result.as_ref().ok());
    let summary = summary_for_lag(&args, lag, threshold, severity);
    let mut evidences = Vec::new();
    evidences.push(evidence_from_result(
        "consumer_lag",
        "rocketmq_get_consumer_lag",
        &lag_result,
    ));
    evidences.push(evidence_from_result(
        "topic_description",
        "rocketmq_describe_topic",
        &topic_result,
    ));
    evidences.push(evidence_from_result(
        "topic_route",
        "rocketmq_get_topic_route",
        &route_result,
    ));
    if let Some(result) = &broker_result {
        evidences.push(evidence_from_result(
            "broker_description",
            "rocketmq_describe_broker",
            result,
        ));
    } else {
        evidences.push(Evidence {
            id: "broker_description".to_string(),
            source_tool: "rocketmq_describe_broker".to_string(),
            status: EvidenceStatus::Missing,
            summary: "No broker was selected because consumer lag evidence was unavailable.".to_string(),
            data: Value::Null,
        });
    }
    let partial = evidences
        .iter()
        .any(|evidence| evidence.status != EvidenceStatus::Present);
    let missing_evidence = evidences
        .iter()
        .filter(|evidence| evidence.status != EvidenceStatus::Present)
        .map(|evidence| evidence.id.clone())
        .collect();
    let evidence_refs = evidences
        .iter()
        .filter(|evidence| evidence.status == EvidenceStatus::Present)
        .map(|evidence| evidence.id.clone())
        .collect();

    DiagnosisReport {
        report_type: "consumer_lag".to_string(),
        evidence_version: CONSUMER_LAG_EVIDENCE_VERSION.to_string(),
        rules_version: CONSUMER_LAG_RULES_VERSION.to_string(),
        cluster: args.cluster.clone(),
        target: json!({
            "topic": args.topic,
            "consumer_group": args.consumer_group,
            "lag_threshold": threshold,
        }),
        severity,
        confidence,
        confidence_band: confidence_band(confidence),
        policy_profile: "default".to_string(),
        partial,
        missing_evidence,
        evidence_refs,
        evidence_snapshot: None,
        summary,
        impacts: impacts_for_lag(lag, severity),
        evidences,
        root_causes: root_causes_for_lag(lag, threshold),
        recommendations: recommendations_for_lag(lag, severity),
        risks: risks_for_lag(lag, severity),
        follow_up_metrics: follow_up_metrics(),
        generated_at: observed_at(),
    }
}

fn confidence_band(confidence: f32) -> ConfidenceBand {
    if confidence >= 0.8 {
        ConfidenceBand::High
    } else if confidence >= 0.5 {
        ConfidenceBand::Medium
    } else {
        ConfidenceBand::Low
    }
}

fn severity_for_lag(lag: Option<&QueryConsumerLagOutput>, threshold: i64) -> Severity {
    let Some(lag) = lag else {
        return Severity::Unknown;
    };
    if lag.page.total_count == 0 {
        return Severity::Unknown;
    }
    if lag.total_lag <= threshold {
        return Severity::Healthy;
    }
    if lag.total_lag >= threshold.saturating_mul(20) {
        return Severity::Critical;
    }
    if lag.total_lag >= threshold.saturating_mul(10) {
        return Severity::High;
    }
    Severity::Medium
}

fn confidence_for_evidence<Topic, Route>(
    lag: Option<&QueryConsumerLagOutput>,
    topic: Option<&Topic>,
    route: Option<&Route>,
) -> f32 {
    if lag.is_none() {
        return 0.20;
    }
    let mut confidence = 0.55;
    if topic.is_some() {
        confidence += 0.15;
    }
    if route.is_some() {
        confidence += 0.15;
    }
    if lag.is_some_and(|lag| lag.page.total_count > 0) {
        confidence += 0.10;
    }
    confidence
}

fn summary_for_lag(
    args: &DiagnoseConsumerLagArgs,
    lag: Option<&QueryConsumerLagOutput>,
    threshold: i64,
    severity: Severity,
) -> String {
    match lag {
        Some(lag) if severity == Severity::Healthy => format!(
            "Consumer group {} on topic {} is within the lag threshold: total_lag={} threshold={}.",
            args.consumer_group, args.topic, lag.total_lag, threshold
        ),
        Some(lag) => format!(
            "Consumer group {} on topic {} has {} total lag across {} queues; severity is {:?}.",
            args.consumer_group, args.topic, lag.total_lag, lag.page.total_count, severity
        ),
        None => format!(
            "Consumer lag diagnosis for group {} on topic {} is Unknown because primary lag evidence is missing.",
            args.consumer_group, args.topic
        ),
    }
}

fn impacts_for_lag(lag: Option<&QueryConsumerLagOutput>, severity: Severity) -> Vec<ImpactItem> {
    let Some(lag) = lag else {
        return vec![ImpactItem {
            area: "consumer_lag".to_string(),
            description: "Primary lag evidence is unavailable, so impact cannot be quantified.".to_string(),
            severity,
        }];
    };

    vec![ImpactItem {
        area: format!("{}:{}", lag.topic, lag.consumer_group),
        description: format!(
            "Total lag is {} across {} queues, with max queue lag {}.",
            lag.total_lag, lag.page.total_count, lag.max_queue_lag
        ),
        severity,
    }]
}

fn root_causes_for_lag(lag: Option<&QueryConsumerLagOutput>, threshold: i64) -> Vec<RootCauseCandidate> {
    let Some(lag) = lag else {
        return vec![RootCauseCandidate {
            cause: "Unknown".to_string(),
            confidence: 0.20,
            evidence_refs: vec!["consumer_lag".to_string()],
            reasoning: "Primary consumer lag evidence is unavailable.".to_string(),
        }];
    };
    if lag.page.total_count == 0 {
        return vec![RootCauseCandidate {
            cause: "Unknown".to_string(),
            confidence: 0.30,
            evidence_refs: vec!["consumer_lag".to_string()],
            reasoning: "Consumer lag evidence contains no queue rows.".to_string(),
        }];
    }
    if lag.total_lag <= threshold {
        return vec![RootCauseCandidate {
            cause: "No active backlog above threshold".to_string(),
            confidence: 0.80,
            evidence_refs: vec!["consumer_lag".to_string()],
            reasoning: "Observed total lag is at or below the configured threshold.".to_string(),
        }];
    }

    let mut causes = Vec::new();
    if lag.consume_tps < 1.0 {
        causes.push(RootCauseCandidate {
            cause: "Consumer throughput is too low for the backlog".to_string(),
            confidence: 0.72,
            evidence_refs: vec!["consumer_lag".to_string()],
            reasoning: format!("consume_tps={} while total_lag={}.", lag.consume_tps, lag.total_lag),
        });
    }
    if skew_ratio(lag) >= SKEW_RATIO_THRESHOLD && lag.page.total_count > 1 {
        causes.push(RootCauseCandidate {
            cause: "Lag is concentrated on a small subset of queues".to_string(),
            confidence: 0.68,
            evidence_refs: vec!["consumer_lag".to_string(), "topic_route".to_string()],
            reasoning: format!(
                "max_queue_lag={} accounts for {:.0}% of total_lag={}.",
                lag.max_queue_lag,
                skew_ratio(lag) * 100.0,
                lag.total_lag
            ),
        });
    }
    if causes.is_empty() {
        causes.push(RootCauseCandidate {
            cause: "Backlog exists but available evidence is insufficient for a specific cause".to_string(),
            confidence: 0.45,
            evidence_refs: vec!["consumer_lag".to_string()],
            reasoning: "Lag is above threshold, but throughput and queue distribution do not isolate a cause."
                .to_string(),
        });
    }
    causes
}

fn recommendations_for_lag(lag: Option<&QueryConsumerLagOutput>, severity: Severity) -> Vec<Recommendation> {
    match (lag, severity) {
        (None, _) | (_, Severity::Unknown) => vec![Recommendation {
            action: "Collect consumer lag, topic route, and broker evidence again.".to_string(),
            priority: "high".to_string(),
            rationale: "The report is Unknown because required evidence is missing or empty.".to_string(),
            risk: "Do not change offsets or topic configuration without evidence.".to_string(),
            verification: "Re-run the diagnosis after all required evidence is available.".to_string(),
        }],
        (Some(_), Severity::Healthy) => vec![Recommendation {
            action: "Continue monitoring consumer lag.".to_string(),
            priority: "low".to_string(),
            rationale: "Lag is within the configured threshold.".to_string(),
            risk: "No immediate operational change is recommended.".to_string(),
            verification: "Confirm total_lag remains within the policy threshold.".to_string(),
        }],
        (Some(lag), _) => vec![
            Recommendation {
                action: "Scale or speed up the consumer group before changing offsets.".to_string(),
                priority: "high".to_string(),
                rationale: format!("total_lag={} is above the configured threshold.", lag.total_lag),
                risk: "Scaling consumers can increase broker fetch pressure; watch broker latency.".to_string(),
                verification: "Confirm consume_tps rises and total_lag declines after scaling.".to_string(),
            },
            Recommendation {
                action: "Inspect queues with the highest lag and their broker placement.".to_string(),
                priority: "medium".to_string(),
                rationale: format!(
                    "max_queue_lag={} may identify a localized queue or broker issue.",
                    lag.max_queue_lag
                ),
                risk: "Queue-level findings require route evidence before rebalancing decisions.".to_string(),
                verification: "Confirm max_queue_lag converges after the placement review.".to_string(),
            },
        ],
    }
}

fn risks_for_lag(lag: Option<&QueryConsumerLagOutput>, severity: Severity) -> Vec<String> {
    match (lag, severity) {
        (None, _) | (_, Severity::Unknown) => {
            vec!["Evidence is incomplete; automated remediation is not safe.".to_string()]
        }
        (_, Severity::Healthy) => vec!["No backlog risk above the configured threshold was observed.".to_string()],
        (Some(lag), _) => vec![
            format!(
                "Delayed consumption may affect downstream processing for topic {}.",
                lag.topic
            ),
            "Resetting offsets would hide evidence and may cause message loss or replay.".to_string(),
        ],
    }
}

fn follow_up_metrics() -> Vec<MetricWatchItem> {
    vec![
        MetricWatchItem {
            name: "total_lag".to_string(),
            target: "consumer_group/topic".to_string(),
            reason: "Confirms whether backlog is draining.".to_string(),
        },
        MetricWatchItem {
            name: "max_queue_lag".to_string(),
            target: "message_queue".to_string(),
            reason: "Detects queue-level lag skew.".to_string(),
        },
        MetricWatchItem {
            name: "consume_tps".to_string(),
            target: "consumer_group".to_string(),
            reason: "Shows whether consumer throughput recovers.".to_string(),
        },
    ]
}

fn evidence_from_result<T>(id: &str, source_tool: &str, result: &Result<T, ToolExecutionError>) -> Evidence
where
    T: Serialize,
{
    match result {
        Ok(value) => Evidence {
            id: id.to_string(),
            source_tool: source_tool.to_string(),
            status: EvidenceStatus::Present,
            summary: format!("{source_tool} returned evidence."),
            data: serde_json::to_value(value).unwrap_or(Value::Null),
        },
        Err(error) => Evidence {
            id: id.to_string(),
            source_tool: source_tool.to_string(),
            status: EvidenceStatus::Unavailable,
            summary: error.to_string(),
            data: Value::Null,
        },
    }
}

#[cfg(test)]
fn top_lag_broker(lag: Option<&QueryConsumerLagOutput>) -> Option<String> {
    lag.and_then(|lag| {
        lag.page
            .items
            .iter()
            .max_by_key(|queue| queue.lag)
            .map(|queue| queue.broker_name.clone())
    })
}

fn skew_ratio(lag: &QueryConsumerLagOutput) -> f64 {
    if lag.total_lag <= 0 {
        return 0.0;
    }
    lag.max_queue_lag as f64 / lag.total_lag as f64
}

#[cfg(test)]
mod tests {
    use crate::adapter::query_facade::ReadOnlyQuery;
    use crate::model::contract::Page;
    use crate::tools::broker_tools::DescribeBrokerOutput;
    use crate::tools::cluster_tools::BrokerSummary;
    use crate::tools::cluster_tools::ClusterOverviewArgs;
    use crate::tools::cluster_tools::ClusterOverviewOutput;
    use crate::tools::consumer_tools::ListConsumerGroupsArgs;
    use crate::tools::consumer_tools::ListConsumerGroupsOutput;
    use crate::tools::consumer_tools::QueueLag;
    use crate::tools::topic_tools::DescribeTopicOutput;
    use crate::tools::topic_tools::ListTopicsArgs;
    use crate::tools::topic_tools::ListTopicsOutput;
    use crate::tools::topic_tools::QueryTopicRouteOutput;
    use crate::tools::topic_tools::TopicRouteBroker;
    use crate::tools::topic_tools::TopicRouteQueue;

    use super::*;

    #[derive(Clone)]
    struct FakeDiagnosisAdapter {
        missing_lag: bool,
    }

    #[async_trait::async_trait]
    impl ReadOnlyQuery for FakeDiagnosisAdapter {
        async fn cluster_overview(
            &self,
            _args: ClusterOverviewArgs,
        ) -> Result<QueryResult<ClusterOverviewOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn list_topics(
            &self,
            _args: ListTopicsArgs,
        ) -> Result<QueryResult<ListTopicsOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn describe_topic(
            &self,
            args: DescribeTopicArgs,
        ) -> Result<QueryResult<DescribeTopicOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(DescribeTopicOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                topic: args.topic,
                broker_names: vec!["broker-a".to_string()],
                read_queue_count: 4,
                write_queue_count: 4,
                brokers: vec![route_broker()],
                page: Page {
                    items: vec![route_queue()],
                    count: 1,
                    total_count: 1,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "1".to_string(),
            }))
        }

        async fn query_topic_route(
            &self,
            args: QueryTopicRouteArgs,
        ) -> Result<QueryResult<QueryTopicRouteOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(QueryTopicRouteOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                topic: args.topic,
                brokers: vec![route_broker()],
                read_queue_count: 4,
                write_queue_count: 4,
                page: Page {
                    items: vec![route_queue()],
                    count: 1,
                    total_count: 1,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "1".to_string(),
            }))
        }

        async fn list_consumer_groups(
            &self,
            _args: ListConsumerGroupsArgs,
        ) -> Result<QueryResult<ListConsumerGroupsOutput>, ToolExecutionError> {
            unimplemented!("not needed by this test")
        }

        async fn query_consumer_lag(
            &self,
            args: QueryConsumerLagArgs,
        ) -> Result<QueryResult<QueryConsumerLagOutput>, ToolExecutionError> {
            if self.missing_lag {
                return Err(ToolExecutionError::backend("lag evidence unavailable"));
            }
            Ok(QueryResult::bypass(QueryConsumerLagOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                topic: args.topic,
                consumer_group: args.consumer_group,
                total_lag: 10_000,
                max_queue_lag: 8_500,
                consume_tps: 0.0,
                inflight_total: 50,
                page: Page {
                    items: vec![
                        QueueLag {
                            topic: "orders".to_string(),
                            broker_name: "broker-a".to_string(),
                            queue_id: 0,
                            broker_offset: 10_000,
                            consumer_offset: 1_500,
                            lag: 8_500,
                            inflight: 50,
                            last_observed_at: Some("1970-01-01T00:00:00.001Z".to_string()),
                            client_ip: None,
                        },
                        QueueLag {
                            topic: "orders".to_string(),
                            broker_name: "broker-b".to_string(),
                            queue_id: 1,
                            broker_offset: 2_000,
                            consumer_offset: 500,
                            lag: 1_500,
                            inflight: 0,
                            last_observed_at: Some("1970-01-01T00:00:00.001Z".to_string()),
                            client_ip: None,
                        },
                    ],
                    count: 2,
                    total_count: 2,
                    has_more: false,
                    next_cursor: None,
                },
                generated_at: "1".to_string(),
            }))
        }

        async fn describe_broker(
            &self,
            args: DescribeBrokerArgs,
        ) -> Result<QueryResult<DescribeBrokerOutput>, ToolExecutionError> {
            Ok(QueryResult::bypass(DescribeBrokerOutput {
                cluster: args.cluster,
                namesrv_addr: "127.0.0.1:9876".to_string(),
                broker_name: args.broker_name,
                brokers: vec![broker_summary()],
                generated_at: "1".to_string(),
            }))
        }

        async fn diagnose_consumer_lag(
            &self,
            _args: DiagnoseConsumerLagArgs,
        ) -> Result<QueryResult<DiagnosisReport>, ToolExecutionError> {
            unimplemented!("the rule tests invoke the test-only composition helper")
        }
    }

    #[tokio::test]
    async fn high_lag_report_contains_ranked_evidence() {
        let report = diagnose_consumer_lag(&FakeDiagnosisAdapter { missing_lag: false }, request())
            .await
            .unwrap();

        assert_eq!(report.severity, Severity::High);
        assert!(report.confidence > 0.8);
        assert!(report
            .evidences
            .iter()
            .any(|evidence| evidence.status == EvidenceStatus::Present));
        assert!(report
            .root_causes
            .iter()
            .any(|cause| cause.cause.contains("throughput")));
        assert!(report
            .root_causes
            .iter()
            .any(|cause| cause.cause.contains("concentrated")));
        assert!(!report.recommendations.is_empty());
    }

    #[tokio::test]
    async fn missing_lag_evidence_returns_unknown_report() {
        let report = diagnose_consumer_lag(&FakeDiagnosisAdapter { missing_lag: true }, request())
            .await
            .unwrap();

        assert_eq!(report.severity, Severity::Unknown);
        assert!(report.confidence <= 0.2);
        assert!(report.root_causes.iter().any(|cause| cause.cause == "Unknown"));
        assert!(report
            .evidences
            .iter()
            .any(|evidence| evidence.id == "consumer_lag" && evidence.status == EvidenceStatus::Unavailable));
    }

    fn request() -> DiagnoseConsumerLagArgs {
        DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
        }
    }

    fn route_broker() -> TopicRouteBroker {
        TopicRouteBroker {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
            broker_addrs: [("0".to_string(), "127.0.0.1:10911".to_string())].into(),
            zone_name: None,
            enable_acting_master: false,
        }
    }

    fn route_queue() -> TopicRouteQueue {
        TopicRouteQueue {
            broker_name: "broker-a".to_string(),
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm: 6,
            topic_sys_flag: 0,
        }
    }

    fn broker_summary() -> BrokerSummary {
        BrokerSummary {
            cluster: "local-dev".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id: 0,
            broker_addr: "127.0.0.1:10911".to_string(),
            version: "5.3.0".to_string(),
            in_tps: "1.0".to_string(),
            out_tps: "1.0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "0".to_string(),
            broker_active: true,
        }
    }
}
