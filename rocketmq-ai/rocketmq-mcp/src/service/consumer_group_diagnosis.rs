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

//! The diagnosis of one Consumer Group across every Topic it consumes.
//!
//! It asks the same questions as the consumer lag diagnosis of one Topic, and answers them with
//! the same rules and the same severity grade, over the backlog of the whole group. What it cannot
//! see is the route and the permissions of a Topic; when one Topic holds the backlog, the report
//! sends the reader to the diagnosis of that Topic.

use serde::Serialize;
use serde_json::json;

use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_collector::evidence_id;
use crate::service::diagnosis_collector::ConsumeDelay;
use crate::service::diagnosis_collector::LagFigures;
use crate::service::diagnosis_collector::QueueLagBreakdown;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::service::diagnosis_rules::conclusion;
use crate::service::diagnosis_rules::severity;
use crate::service::diagnosis_rules::table;
use crate::service::diagnosis_rules::ConsumerLagPolicy;
use crate::service::diagnosis_rules::Finding;
use crate::service::diagnosis_service;
use crate::service::diagnosis_service::Assessment;
use crate::service::diagnosis_service::LagReport;
use crate::service::diagnosis_service::LagSubject;
use crate::service::diagnosis_snapshot;
use crate::tools::consumer_tools::ConsumerProgressState;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::diagnosis_tools::DiagnoseConsumerGroupArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;

pub(crate) const CONSUMER_GROUP_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.consumer-group.v1";
pub(crate) const CONSUMER_GROUP_RULES_VERSION: &str = "rocketmq-mcp.rules.consumer-group.v1";

/// How many of the most lagging Topics the progress evidence lists.
const WORST_TOPIC_LIMIT: usize = 10;

/// What each evidence item other than the backlog adds to the confidence of a report, in thousandths.
const CONFIDENCE_WEIGHTS: &[(&str, u16)] = &[
    (evidence_id::CONSUMER_GROUP, 250),
    (evidence_id::RETRY_TOPIC_STATS, 50),
    (evidence_id::DEAD_LETTER_TOPIC_STATS, 50),
];

/// The backlog of one Consumer Group over every Topic it consumes.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct ConsumerProgressSummary {
    pub cluster: String,
    pub consumer_group: String,
    pub state: ConsumerProgressState,
    pub topic_count: usize,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
    #[serde(flatten)]
    pub delay: ConsumeDelay,
    #[serde(flatten)]
    pub queues: QueueLagBreakdown,
    /// Lag per Topic, largest first, at most [`WORST_TOPIC_LIMIT`].
    pub topics: Vec<TopicLag>,
    /// Whether the source holds more queue rows than it returned. The totals cover every queue;
    /// the per-Topic, per-Broker and per-queue figures then cover the returned rows only.
    pub truncated: bool,
    pub generated_at: String,
}

impl ConsumerProgressSummary {
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

/// Lag of the queues of one Topic.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TopicLag {
    pub topic: String,
    pub queue_count: usize,
    pub lag: i64,
    pub max_queue_lag: i64,
}

impl TopicLag {
    /// Aggregates queue rows by Topic and keeps the most lagging Topics, largest first. Equal lag
    /// is ordered by Topic name, so the same rows always yield the same list.
    pub(crate) fn worst(queues: &[QueueLag]) -> Vec<Self> {
        let mut topics = std::collections::BTreeMap::<&str, Self>::new();
        for queue in queues {
            let topic = topics.entry(queue.topic.as_str()).or_insert_with(|| Self {
                topic: queue.topic.clone(),
                queue_count: 0,
                lag: 0,
                max_queue_lag: 0,
            });
            topic.queue_count += 1;
            topic.lag = topic.lag.saturating_add(queue.lag);
            topic.max_queue_lag = topic.max_queue_lag.max(queue.lag);
        }
        let mut topics = topics.into_values().collect::<Vec<_>>();
        // The sort is stable, so the name order of the map breaks ties.
        topics.sort_by_key(|topic| std::cmp::Reverse(topic.lag));
        topics.truncate(WORST_TOPIC_LIMIT);
        topics
    }
}

/// The evidence one Consumer Group diagnosis is evaluated on.
///
/// `group` and `progress` say whether the group can consume and how far behind it is; the retry
/// and dead-letter Topics say whether what it consumes fails. A part that could not be read is an
/// error here and missing evidence in the report.
pub(crate) struct ConsumerGroupEvidence {
    pub group: Result<ConsumerGroupState, ToolFailure>,
    pub progress: Result<ConsumerProgressSummary, ToolFailure>,
    pub retry: Result<TopicActivity, ToolFailure>,
    pub dead_letter: Result<TopicActivity, ToolFailure>,
}

impl ConsumerGroupEvidence {
    /// The backlog item is named `consumer_lag`, as in the diagnosis of one Topic, because the
    /// shared rules refer to the backlog by that identifier.
    fn snapshot(&self, args: &DiagnoseConsumerGroupArgs) -> EvidenceSnapshot {
        let query_hash = diagnosis_snapshot::query_hash(&[&args.cluster, &args.consumer_group]);
        let items = vec![
            diagnosis_snapshot::item(
                evidence_id::CONSUMER_GROUP,
                "rocketmq_get_consumer_group_details",
                EvidenceKind::ConsumerGroup,
                &query_hash,
                &self.group,
            ),
            diagnosis_snapshot::item(
                evidence_id::CONSUMER_LAG,
                "rocketmq_get_consumer_progress",
                EvidenceKind::ConsumerProgress,
                &query_hash,
                &self.progress,
            ),
            diagnosis_snapshot::system_topic_item(evidence_id::RETRY_TOPIC_STATS, &query_hash, &self.retry),
            diagnosis_snapshot::system_topic_item(evidence_id::DEAD_LETTER_TOPIC_STATS, &query_hash, &self.dead_letter),
        ];
        diagnosis_snapshot::snapshot(
            CONSUMER_GROUP_EVIDENCE_VERSION,
            query_hash,
            &args.cluster,
            json!({ "consumer_group": args.consumer_group }),
            items,
        )
    }

    /// Whether a completed lookup established that the group does not exist.
    fn group_is_absent(&self) -> bool {
        let absent = |error: Option<&ToolFailure>| {
            error.and_then(ToolFailure::not_found_entity) == Some(NotFoundEntity::ConsumerGroup)
        };
        absent(self.group.as_ref().err()) || absent(self.progress.as_ref().err())
    }
}

/// Evaluates the rules on `evidence` and writes the report.
///
/// The thresholds are those of the group across Topics: an override that names a Topic does not
/// apply. Without the backlog the severity is unknown, but what the group evidence establishes
/// about the group being unable to consume is still reported.
pub(crate) fn evaluate(
    args: &DiagnoseConsumerGroupArgs,
    evidence: ConsumerGroupEvidence,
    policy: &ConsumerLagPolicy,
) -> DiagnosisReport {
    let thresholds = policy.thresholds_for(None, &args.consumer_group);
    let snapshot = evidence.snapshot(args);
    let progress = evidence.progress.as_ref().ok();
    let figures = progress.map(ConsumerProgressSummary::figures);
    let input = table::RuleInput {
        lag: figures.unwrap_or(LagFigures::UNOBSERVED),
        route: None,
        group: evidence.group.as_ref().ok(),
        topic_config: None,
        retry: evidence.retry.as_ref().ok(),
        dead_letter: evidence.dead_letter.as_ref().ok(),
        thresholds: &thresholds,
    };
    let mut findings = table::evaluate(&input);
    if input.has_backlog() {
        findings.extend(
            progress
                .and_then(lag_concentrated_on_topic)
                .map(|finding| ("lag_concentrated_on_topic", finding)),
        );
        // The sort is stable, so equally confident conclusions keep the order they fired in.
        findings.sort_by(|left, right| right.1.confidence.total_cmp(&left.1.confidence));
    }
    let assessment = Assessment {
        severity: figures.map_or(Severity::Unknown, |figures| {
            severity::grade(figures, &thresholds, input.consumption_stopped())
        }),
        backlog: input.has_backlog(),
        findings,
    };
    let mut report = diagnosis_service::build_lag_report(LagReport {
        report_type: "consumer_group",
        rules_version: CONSUMER_GROUP_RULES_VERSION,
        target: json!({ "consumer_group": args.consumer_group }),
        subject: LagSubject {
            phrase: format!("group {}", args.consumer_group),
            area: args.consumer_group.clone(),
            affected: format!("the topics of group {}", args.consumer_group),
            progress_scope: "",
        },
        policy_profile: &policy.profile,
        thresholds: &thresholds,
        lag_evidence: evidence_id::CONSUMER_LAG,
        confidence_weights: CONFIDENCE_WEIGHTS,
        snapshot,
        lag: figures,
        assessment,
    });
    if evidence.group_is_absent() {
        report_absent_group(args, &evidence, &mut report);
    }
    report
}

/// One Topic holds most of the backlog of a group that consumes several.
fn lag_concentrated_on_topic(progress: &ConsumerProgressSummary) -> Option<Finding> {
    let top = progress.topics.first()?;
    let share = table::share(top.lag, progress.total_lag);
    if progress.topic_count < 2 || share < table::CONCENTRATION_RATIO {
        return None;
    }
    Some(Finding {
        cause: format!("Lag is concentrated on topic {}", top.topic),
        confidence: 0.66,
        reasoning: format!(
            "{} holds lag={} on {} queues, {:.0}% of total_lag={} across {} topics.",
            top.topic,
            top.lag,
            top.queue_count,
            share * 100.0,
            progress.total_lag,
            progress.topic_count
        ),
        evidence_refs: vec![evidence_id::CONSUMER_LAG],
        stops_consumption: false,
        recommendation: Recommendation {
            action: format!(
                "Run rocketmq_diagnose_consumer_lag for topic {} and this group; it adds the route and the \
                 permissions of the topic.",
                top.topic
            ),
            priority: "medium".to_string(),
            rationale: "A backlog on one topic usually has its cause on that topic: its producers, its route, or the \
                        consumers subscribed to it."
                .to_string(),
            risk: "None. The diagnosis is read-only.".to_string(),
            verification: "The topic diagnosis names a cause or reports a backlog within the threshold.".to_string(),
        },
    })
}

/// Replaces "evidence is missing" with the definite conclusion: there is no such group.
fn report_absent_group(
    args: &DiagnoseConsumerGroupArgs,
    evidence: &ConsumerGroupEvidence,
    report: &mut DiagnosisReport,
) {
    const ABSENT_GROUP_CONFIDENCE: f32 = 0.90;

    let entity = NotFoundEntity::ConsumerGroup;
    let (evidence_id, source_tool) = if evidence.group.is_err() {
        (evidence_id::CONSUMER_GROUP, "rocketmq_get_consumer_group_details")
    } else {
        (evidence_id::CONSUMER_LAG, "rocketmq_get_consumer_progress")
    };
    report.severity = Severity::Unknown;
    report.summary = format!(
        "Consumer group diagnosis for group {} is Unknown because the {} does not exist in the selected cluster.",
        args.consumer_group,
        entity.label()
    );
    report.confidence = ABSENT_GROUP_CONFIDENCE;
    report.confidence_band = ConfidenceBand::High;
    report.root_causes = vec![diagnosis_service::conclude(
        conclusion::TARGET_NOT_FOUND,
        evidence_id,
        &format!("The {} does not exist in the selected cluster", entity.label()),
        ABSENT_GROUP_CONFIDENCE,
        &format!(
            "{source_tool} reported that {} {} was not found.",
            entity.label(),
            args.consumer_group
        ),
    )];
    report.recommendations = vec![Recommendation {
        action: entity.suggestion().to_string(),
        priority: "high".to_string(),
        rationale: format!("The {} was not found, so there is nothing to diagnose.", entity.label()),
        risk: "None. Listing names is read-only.".to_string(),
        verification: "Run the diagnosis again with the confirmed name.".to_string(),
    }];
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use crate::service::diagnosis_collector::fixtures as lag;

    pub(crate) fn args() -> DiagnoseConsumerGroupArgs {
        DiagnoseConsumerGroupArgs {
            cluster: "local-dev".to_string(),
            consumer_group: "order-service".to_string(),
        }
    }

    /// A queue of `topic` on `broker-a`.
    pub(crate) fn queue_of(topic: &str, queue_id: i32, lag: i64) -> QueueLag {
        QueueLag {
            topic: topic.to_string(),
            ..lag::queue("broker-a", queue_id, lag)
        }
    }

    pub(crate) fn progress(queues: Vec<QueueLag>, consume_tps: f64) -> ConsumerProgressSummary {
        let mut topics = queues.iter().map(|queue| queue.topic.as_str()).collect::<Vec<_>>();
        topics.sort_unstable();
        topics.dedup();
        ConsumerProgressSummary {
            cluster: "local-dev".to_string(),
            consumer_group: "order-service".to_string(),
            state: if queues.is_empty() {
                ConsumerProgressState::NoConsumption
            } else {
                ConsumerProgressState::Observed
            },
            topic_count: topics.len(),
            total_lag: queues.iter().map(|queue| queue.lag).sum(),
            consume_tps,
            inflight_total: queues.iter().map(|queue| queue.inflight).sum(),
            delay: ConsumeDelay::measure(&queues, lag::now_millis()),
            topics: TopicLag::worst(&queues),
            queues: QueueLagBreakdown::from_queues(queues),
            truncated: false,
            generated_at: lag::NOW.to_string(),
        }
    }

    /// Complete evidence of a group whose consumers are online on `broker-a`.
    pub(crate) fn evidence(progress: ConsumerProgressSummary) -> ConsumerGroupEvidence {
        ConsumerGroupEvidence {
            group: Ok(lag::group(&[("broker-a", true)])),
            progress: Ok(progress),
            retry: Ok(TopicActivity::absent("local-dev", "%RETRY%order-service".to_string())),
            dead_letter: Ok(TopicActivity::absent("local-dev", "%DLQ%order-service".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::queue_of;
    use super::*;
    use crate::config::DiagnosisConfig;
    use crate::config::DiagnosisOverride;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::service::diagnosis_collector::fixtures as lag;

    fn diagnose(evidence: ConsumerGroupEvidence) -> DiagnosisReport {
        evaluate(&fixtures::args(), evidence, &ConsumerLagPolicy::default())
    }

    fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
        report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
    }

    #[test]
    fn a_group_within_the_threshold_is_healthy_and_complete() {
        let report = diagnose(fixtures::evidence(fixtures::progress(
            vec![queue_of("orders", 0, 100), queue_of("payments", 0, 200)],
            50.0,
        )));

        assert_eq!(report.report_type, "consumer_group");
        assert_eq!(report.rules_version, CONSUMER_GROUP_RULES_VERSION);
        assert_eq!(report.evidence_version, CONSUMER_GROUP_EVIDENCE_VERSION);
        assert_eq!(report.severity, Severity::Healthy);
        assert!(!report.partial);
        assert_eq!(report.confidence, 0.95);
        assert_eq!(rule_ids(&report), ["backlog_within_threshold"]);
        assert_eq!(
            report.evidence_refs,
            [
                "consumer_group",
                "consumer_lag",
                "retry_topic_stats",
                "dead_letter_topic_stats"
            ]
        );
        assert_eq!(
            report.summary,
            "Consumer group order-service is within the lag threshold: total_lag=300 threshold=1000."
        );
        assert_eq!(report.target["consumer_group"], "order-service");
        assert_eq!(report.target["lag_threshold"], 1_000);
    }

    #[test]
    fn the_progress_evidence_lists_the_topics_that_hold_the_lag() {
        let report = diagnose(fixtures::evidence(fixtures::progress(
            vec![
                queue_of("orders", 0, 100),
                queue_of("orders", 1, 300),
                queue_of("payments", 0, 900),
                queue_of("refunds", 0, 0),
            ],
            50.0,
        )));

        let item = report
            .evidence_snapshot
            .items
            .iter()
            .find(|item| item.id == "consumer_lag")
            .unwrap();
        assert_eq!(item.kind, EvidenceKind::ConsumerProgress);
        assert_eq!(item.source_tool, "rocketmq_get_consumer_progress");
        let data = serde_json::to_value(item.payload.as_ref().unwrap()).unwrap()["data"].clone();
        assert_eq!(data["topic_count"], 3);
        assert_eq!(data["total_lag"], 1_300);
        assert_eq!(data["queue_count"], 4);
        assert_eq!(
            data["topics"],
            json!([
                { "topic": "payments", "queue_count": 1, "lag": 900, "max_queue_lag": 900 },
                { "topic": "orders", "queue_count": 2, "lag": 400, "max_queue_lag": 300 },
                { "topic": "refunds", "queue_count": 1, "lag": 0, "max_queue_lag": 0 },
            ])
        );
    }

    #[test]
    fn only_the_most_lagging_topics_are_listed() {
        let queues = (0..25)
            .map(|index| queue_of(&format!("topic-{index:02}"), 0, i64::from(index)))
            .collect::<Vec<_>>();

        let topics = TopicLag::worst(&queues);

        assert_eq!(topics.len(), WORST_TOPIC_LIMIT);
        assert_eq!(topics[0].topic, "topic-24");
        assert_eq!(topics[WORST_TOPIC_LIMIT - 1].topic, "topic-15");
    }

    #[test]
    fn the_lag_rules_of_one_topic_apply_to_the_whole_group() {
        // The same stalled backlog, once with consumers and once without.
        let stalled = || fixtures::progress(vec![queue_of("orders", 0, 6_000), queue_of("payments", 0, 6_000)], 0.0);

        let slow = diagnose(fixtures::evidence(stalled()));
        let mut offline = fixtures::evidence(stalled());
        offline.group = Ok(lag::group(&[("broker-a", false)]));
        let offline = diagnose(offline);

        assert_eq!(slow.severity, Severity::High);
        assert_eq!(rule_ids(&slow), ["consume_throughput_low"]);
        assert_eq!(slow.root_causes[0].evidence_refs, ["consumer_lag"]);
        assert_eq!(rule_ids(&offline), ["no_online_consumer"]);
        assert_eq!(
            slow.summary,
            "Consumer group order-service has 12000 total lag across 2 queues; severity is High."
        );
    }

    #[test]
    fn lag_concentrated_on_topic_points_at_the_topic_diagnosis() {
        let report = diagnose(fixtures::evidence(fixtures::progress(
            vec![
                queue_of("orders", 0, 2_000),
                queue_of("orders", 1, 2_000),
                queue_of("orders", 2, 2_000),
                queue_of("payments", 0, 500),
            ],
            500.0,
        )));

        assert_eq!(rule_ids(&report), ["lag_concentrated_on_topic"]);
        let cause = &report.root_causes[0];
        assert_eq!(cause.cause, "Lag is concentrated on topic orders");
        assert!(cause.reasoning.contains("92% of total_lag=6500 across 2 topics"));
        assert!(report.recommendations[0]
            .action
            .starts_with("Run rocketmq_diagnose_consumer_lag for topic orders"));

        // A group that consumes one Topic has nothing to compare it with.
        let single = diagnose(fixtures::evidence(fixtures::progress(
            vec![queue_of("orders", 0, 3_000), queue_of("orders", 1, 3_000)],
            500.0,
        )));
        assert_eq!(rule_ids(&single), ["cause_not_isolated"]);
    }

    #[test]
    fn a_group_that_cannot_consume_is_named_even_when_its_backlog_cannot_be_read() {
        let mut evidence = fixtures::evidence(fixtures::progress(Vec::new(), 0.0));
        evidence.progress = lag::unavailable();
        evidence.group = Ok(lag::group_with(&[("broker-a", false)], |row| {
            row.consume_enable = Some(false);
        }));

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Unknown);
        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["consumer_lag"]);
        assert_eq!(rule_ids(&report), ["consumption_disabled", "no_online_consumer"]);
        assert!(report
            .root_causes
            .iter()
            .all(|cause| cause.evidence_refs == ["consumer_group"]));
        assert!(report.summary.contains("primary lag evidence is missing"));
        assert!(report.recommendations[0]
            .action
            .starts_with("Find out why consumption was disabled"));
    }

    #[test]
    fn a_group_without_consume_progress_is_unknown() {
        let report = diagnose(fixtures::evidence(fixtures::progress(Vec::new(), 0.0)));

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(rule_ids(&report), ["no_queue_rows"]);
        assert_eq!(
            report.summary,
            "Consumer lag diagnosis for group order-service is Unknown because the group reports no consume progress."
        );
    }

    #[test]
    fn a_group_that_does_not_exist_is_reported_as_such() {
        let mut evidence = fixtures::evidence(fixtures::progress(Vec::new(), 0.0));
        evidence.group = Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup));
        evidence.progress = Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup));

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(rule_ids(&report), ["target_not_found"]);
        assert_eq!(report.root_causes[0].evidence_refs, ["consumer_group"]);
        assert!(report.summary.contains("the consumer group does not exist"));
        assert!(report.recommendations[0]
            .action
            .contains("rocketmq_list_consumer_groups"));
        let status = |id: &str| {
            report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == id)
                .unwrap()
                .status
        };
        assert_eq!(status("consumer_group"), EvidenceStatus::Missing);
        assert_eq!(status("consumer_lag"), EvidenceStatus::Missing);
    }

    #[test]
    fn only_an_override_that_names_the_group_alone_applies() {
        let policy = ConsumerLagPolicy::from(&DiagnosisConfig {
            overrides: vec![
                DiagnosisOverride {
                    topic: Some("orders".to_string()),
                    consumer_lag_threshold: Some(1),
                    ..DiagnosisOverride::default()
                },
                DiagnosisOverride {
                    consumer_group: Some("order-service".to_string()),
                    consumer_lag_threshold: Some(50_000),
                    ..DiagnosisOverride::default()
                },
            ],
            ..DiagnosisConfig::default()
        });

        let report = evaluate(
            &fixtures::args(),
            fixtures::evidence(fixtures::progress(vec![queue_of("orders", 0, 40_000)], 0.0)),
            &policy,
        );

        assert_eq!(report.severity, Severity::Healthy);
        assert_eq!(report.target["lag_threshold"], 50_000);
    }
}
