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

use crate::config::DiagnosisConfig;
use crate::config::DiagnosisOverride;
use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::RootCauseCandidate;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_collector::evidence_id;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::service::diagnosis_collector::LagFigures;
use crate::service::diagnosis_service;
use crate::service::diagnosis_service::Assessment;
use crate::service::diagnosis_service::LagReport;
use crate::service::diagnosis_service::LagSubject;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;

pub(crate) mod severity;
pub(crate) mod table;

pub(crate) use table::conclusion;
pub(crate) use table::Finding;

pub(crate) const CONSUMER_LAG_RULES_VERSION: &str = "rocketmq-mcp.rules.consumer-lag.v3";

/// The thresholds one diagnosis is evaluated against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Thresholds {
    /// Backlog, in messages, up to which the group is healthy.
    pub lag: i64,
    /// Consume delay from which a backlog counts as delayed.
    pub consume_delay_ms: u64,
    /// In-flight messages from which a POP group holds too many unacknowledged.
    pub pop_inflight: i64,
    /// How recently a retry or dead-letter Topic must have received a message to count as growing.
    pub retry_activity_window_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerLagPolicy {
    pub profile: String,
    defaults: Thresholds,
    overrides: Vec<DiagnosisOverride>,
}

impl Default for ConsumerLagPolicy {
    fn default() -> Self {
        Self::from(&DiagnosisConfig::default())
    }
}

impl From<&DiagnosisConfig> for ConsumerLagPolicy {
    fn from(config: &DiagnosisConfig) -> Self {
        Self {
            profile: config.consumer_lag_policy_profile.clone(),
            defaults: Thresholds {
                lag: config.consumer_lag_threshold,
                consume_delay_ms: config.consume_delay_threshold_ms,
                pop_inflight: config.pop_inflight_threshold,
                retry_activity_window_ms: config.retry_activity_window_ms,
            },
            overrides: config.overrides.clone(),
        }
    }
}

impl ConsumerLagPolicy {
    /// The thresholds for one Consumer Group, on one Topic or, with `topic` set to `None`, across
    /// every Topic it consumes.
    ///
    /// Every override that selects the target applies, the least specific first: one that names
    /// only the Topic, then one that names only the group, then one that names both. A later
    /// override replaces only the thresholds it sets. An override that names a Topic never
    /// applies to a diagnosis across Topics.
    pub(crate) fn thresholds_for(&self, topic: Option<&str>, consumer_group: &str) -> Thresholds {
        let mut matching = self
            .overrides
            .iter()
            .filter(|entry| {
                entry.topic.as_deref().is_none_or(|name| Some(name) == topic)
                    && entry
                        .consumer_group
                        .as_deref()
                        .is_none_or(|name| name == consumer_group)
            })
            .collect::<Vec<_>>();
        matching.sort_by_key(|entry| (entry.consumer_group.is_some(), entry.topic.is_some()));
        matching
            .into_iter()
            .fold(self.defaults, |thresholds, entry| Thresholds {
                lag: entry.consumer_lag_threshold.unwrap_or(thresholds.lag),
                consume_delay_ms: entry.consume_delay_threshold_ms.unwrap_or(thresholds.consume_delay_ms),
                pop_inflight: entry.pop_inflight_threshold.unwrap_or(thresholds.pop_inflight),
                retry_activity_window_ms: entry
                    .retry_activity_window_ms
                    .unwrap_or(thresholds.retry_activity_window_ms),
            })
    }
}

/// Evaluates the rules on `evidence` and writes the report.
///
/// Missing evidence never fails a diagnosis. Without consumer lag the severity is unknown and no
/// cause is named; without any other part, the rules that need it stay silent and the report says
/// which evidence is missing.
pub(crate) fn evaluate(
    args: &DiagnoseConsumerLagArgs,
    evidence: ConsumerLagEvidence,
    policy: &ConsumerLagPolicy,
) -> DiagnosisReport {
    let thresholds = policy.thresholds_for(Some(&args.topic), &args.consumer_group);
    let snapshot = evidence.snapshot(args);
    let assessment = assess(&evidence, &thresholds);
    let mut report = consumer_lag_report(
        args,
        &policy.profile,
        &thresholds,
        snapshot,
        evidence.lag.as_ref().ok().map(|lag| lag.figures()),
        assessment,
    );
    let absent_targets = absent_targets(&evidence);
    if !absent_targets.is_empty() {
        report_absent_targets(args, &absent_targets, &mut report);
    }
    report
}

/// What each evidence item other than the lag adds to the confidence of a report, in thousandths.
const CONFIDENCE_WEIGHTS: &[(&str, u16)] = &[
    (evidence_id::TOPIC_ROUTE, 100),
    (evidence_id::CONSUMER_GROUP, 100),
    (evidence_id::TOPIC_CONFIG, 50),
    (evidence_id::BROKER_SUMMARY, 50),
    (evidence_id::RETRY_TOPIC_STATS, 25),
    (evidence_id::DEAD_LETTER_TOPIC_STATS, 25),
];

/// Writes the report of the backlog of one Consumer Group on one Topic.
pub(crate) fn consumer_lag_report(
    args: &DiagnoseConsumerLagArgs,
    policy_profile: &str,
    thresholds: &Thresholds,
    snapshot: EvidenceSnapshot,
    lag: Option<LagFigures<'_>>,
    assessment: Assessment,
) -> DiagnosisReport {
    diagnosis_service::build_lag_report(LagReport {
        report_type: "consumer_lag",
        rules_version: CONSUMER_LAG_RULES_VERSION,
        target: serde_json::json!({ "topic": args.topic, "consumer_group": args.consumer_group }),
        subject: LagSubject {
            phrase: format!("group {} on topic {}", args.consumer_group, args.topic),
            area: format!("{}:{}", args.topic, args.consumer_group),
            affected: format!("topic {}", args.topic),
            progress_scope: " on the topic",
        },
        policy_profile,
        thresholds,
        lag_evidence: evidence_id::CONSUMER_LAG,
        confidence_weights: CONFIDENCE_WEIGHTS,
        snapshot,
        lag,
        assessment,
    })
}

/// Grades the backlog and collects what the rules conclude about it.
fn assess(evidence: &ConsumerLagEvidence, thresholds: &Thresholds) -> Assessment {
    let Some(input) = table::RuleInput::new(evidence, thresholds) else {
        return Assessment {
            severity: Severity::Unknown,
            backlog: false,
            findings: Vec::new(),
        };
    };
    Assessment {
        severity: severity::grade(input.lag, thresholds, input.consumption_stopped()),
        backlog: input.has_backlog(),
        findings: table::evaluate(&input),
    }
}

/// A diagnosis target that a completed lookup established does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AbsentTarget {
    entity: NotFoundEntity,
    /// The evidence item that recorded the missing target.
    evidence_id: &'static str,
    source_tool: &'static str,
}

/// Collects the Topic and Consumer Group lookups that reported `not_found`.
///
/// A missing Topic is listed first because it also explains a missing lag result.
fn absent_targets(evidence: &ConsumerLagEvidence) -> Vec<AbsentTarget> {
    let lookups = [
        (
            evidence_id::TOPIC_ROUTE,
            "rocketmq_get_topic_route",
            evidence.route.as_ref().err(),
        ),
        (
            evidence_id::CONSUMER_LAG,
            "rocketmq_get_consumer_lag",
            evidence.lag.as_ref().err(),
        ),
    ];
    let mut targets = Vec::<AbsentTarget>::new();
    for (evidence_id, source_tool, error) in lookups {
        let Some(entity @ (NotFoundEntity::Topic | NotFoundEntity::ConsumerGroup)) =
            error.and_then(ToolFailure::not_found_entity)
        else {
            continue;
        };
        if targets.iter().all(|target| target.entity != entity) {
            targets.push(AbsentTarget {
                entity,
                evidence_id,
                source_tool,
            });
        }
    }
    targets
}

/// Replaces the "evidence is missing, collect it again" conclusion with the definite one:
/// the target does not exist, so there is no backlog to analyze and retrying cannot help.
fn report_absent_targets(args: &DiagnoseConsumerLagArgs, targets: &[AbsentTarget], report: &mut DiagnosisReport) {
    const ABSENT_TARGET_CONFIDENCE: f32 = 0.90;

    let name = |entity: NotFoundEntity| match entity {
        NotFoundEntity::Topic => args.topic.as_str(),
        _ => args.consumer_group.as_str(),
    };
    report.summary = format!(
        "Consumer lag diagnosis for group {} on topic {} is Unknown because the {} does not exist in the selected \
         cluster.",
        args.consumer_group,
        args.topic,
        targets[0].entity.label()
    );
    report.confidence = ABSENT_TARGET_CONFIDENCE;
    report.confidence_band = ConfidenceBand::High;
    report.root_causes = targets
        .iter()
        .map(|target| RootCauseCandidate {
            rule_id: conclusion::TARGET_NOT_FOUND.to_string(),
            cause: format!("The {} does not exist in the selected cluster", target.entity.label()),
            confidence: ABSENT_TARGET_CONFIDENCE,
            evidence_refs: vec![target.evidence_id.to_string()],
            reasoning: format!(
                "{} reported that {} {} was not found.",
                target.source_tool,
                target.entity.label(),
                name(target.entity)
            ),
        })
        .collect();
    report.recommendations = targets
        .iter()
        .map(|target| Recommendation {
            action: target.entity.suggestion().to_string(),
            priority: "high".to_string(),
            rationale: format!(
                "The {} was not found, so there is no backlog to analyze.",
                target.entity.label()
            ),
            risk: "None. Listing names is read-only.".to_string(),
            verification: "Run the diagnosis again with the confirmed name.".to_string(),
        })
        .collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::service::diagnosis_collector::fixtures;
    use crate::service::diagnosis_collector::fixtures::queue;
    use crate::service::diagnosis_collector::fixtures::queue_behind;
    use crate::tools::executor::ToolRejection;

    fn diagnose(evidence: ConsumerLagEvidence) -> DiagnosisReport {
        evaluate(&fixtures::args(), evidence, &ConsumerLagPolicy::default())
    }

    fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
        report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
    }

    fn override_for(topic: Option<&str>, consumer_group: Option<&str>) -> DiagnosisOverride {
        DiagnosisOverride {
            topic: topic.map(str::to_string),
            consumer_group: consumer_group.map(str::to_string),
            ..DiagnosisOverride::default()
        }
    }

    #[test]
    fn unavailable_evidence_produces_partial_unknown_without_root_causes() {
        let report = diagnose(ConsumerLagEvidence {
            lag: Err(ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 5_000 })),
            route: fixtures::unavailable(),
            broker: None,
            group: fixtures::unavailable(),
            topic_config: fixtures::unavailable(),
            retry: fixtures::unavailable(),
            dead_letter: fixtures::unavailable(),
        });

        assert!(report.partial);
        assert_eq!(report.severity, Severity::Unknown);
        assert!(report.root_causes.is_empty());
        assert!(report.missing_evidence.contains(&"consumer_lag".to_string()));
        assert!(report.evidence_refs.is_empty());
        assert_eq!(report.rules_version, CONSUMER_LAG_RULES_VERSION);
    }

    #[test]
    fn diagnosis_distinguishes_offline_consumers_from_slow_consumers() {
        // The same backlog and the same consumption rate of zero, read with two groups behind it.
        let stalled = || fixtures::lag_summary(vec![queue("broker-a", 0, 6_000), queue("broker-b", 0, 6_000)], 0.0);

        let mut offline = fixtures::evidence(stalled());
        offline.group = Ok(fixtures::group(&[("broker-a", false), ("broker-b", false)]));
        let offline = diagnose(offline);

        let slow = diagnose(fixtures::evidence(stalled()));

        assert_eq!(rule_ids(&offline), ["no_online_consumer"]);
        assert_eq!(offline.root_causes[0].cause, "No consumer of the group is online");
        assert_eq!(offline.root_causes[0].evidence_refs, ["consumer_group"]);
        assert!(offline.recommendations[0].action.starts_with("Start the consumers"));

        assert_eq!(rule_ids(&slow), ["consume_throughput_low"]);
        assert_eq!(slow.root_causes[0].evidence_refs, ["consumer_lag"]);
        assert!(slow.recommendations[0].action.starts_with("Scale or speed up"));
    }

    #[test]
    fn diagnosis_severity_accounts_for_produce_rate() {
        // 1,001 waiting messages on each Topic. The busy Topic stored the last consumed message
        // 10 ms ago, the quiet one 1,001 hours ago.
        let busy = QueueBehind { lag: 1_001, millis: 10 };
        let quiet = QueueBehind {
            lag: 1_001,
            millis: 1_001 * 3_600_000,
        };
        let diagnose = |queue: QueueBehind| {
            let mut row = fixtures::queue("broker-a", 0, queue.lag);
            row.last_observed_at =
                crate::model::contract::observed_at_from_millis(fixtures::now_millis() - queue.millis);
            diagnose(fixtures::evidence(fixtures::lag_summary(vec![row], 2_000.0)))
        };

        let busy = diagnose(busy);
        let quiet = diagnose(quiet);

        let lag = |report: &DiagnosisReport| {
            let item = report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == "consumer_lag")
                .unwrap();
            serde_json::to_value(item.payload.as_ref().unwrap()).unwrap()["data"].clone()
        };
        assert_eq!(lag(&busy)["total_lag"], lag(&quiet)["total_lag"]);
        assert_eq!(lag(&busy)["estimated_produce_tps"], 100_100.0);
        assert_eq!(lag(&quiet)["estimated_produce_tps"], 0.000_278);
        assert_eq!(busy.severity, Severity::Low);
        assert_eq!(quiet.severity, Severity::Critical);
        assert!(busy.summary.contains("10 ms behind production"));
        assert!(quiet.summary.contains("41.7 d behind production"));
        assert_eq!(rule_ids(&busy), ["cause_not_isolated"]);
        assert_eq!(rule_ids(&quiet), ["consume_delay"]);
    }

    struct QueueBehind {
        lag: i64,
        millis: i64,
    }

    #[test]
    fn diagnosis_survives_a_failed_evidence_source() {
        let mut evidence = fixtures::evidence(fixtures::lag_summary(
            vec![queue("broker-a", 0, 6_000), queue("broker-b", 0, 6_000)],
            0.0,
        ));
        evidence.group = Err(ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 5_000 }));
        evidence.retry = fixtures::unavailable();

        let report = diagnose(evidence);

        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["consumer_group", "retry_topic_stats"]);
        let status = |id: &str| {
            report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == id)
                .unwrap()
                .status
        };
        assert_eq!(status("consumer_group"), EvidenceStatus::Timeout);
        assert_eq!(status("retry_topic_stats"), EvidenceStatus::Unavailable);
        // The backlog is still graded and explained from what was collected, and no conclusion
        // rests on the evidence that is missing.
        assert_eq!(report.severity, Severity::High);
        assert_eq!(rule_ids(&report), ["consume_throughput_low"]);
        assert!(report
            .root_causes
            .iter()
            .flat_map(|cause| &cause.evidence_refs)
            .all(|reference| report.evidence_refs.contains(reference)));
        assert!(report.confidence < 0.95);
    }

    #[test]
    fn a_backlog_without_a_fired_rule_is_reported_as_not_isolated() {
        let report = diagnose(fixtures::evidence(fixtures::lag_summary(
            vec![queue("broker-a", 0, 3_000), queue("broker-b", 0, 3_000)],
            500.0,
        )));

        assert_eq!(report.severity, Severity::Medium);
        assert_eq!(rule_ids(&report), ["cause_not_isolated"]);
        assert!(!report.partial);
        assert_eq!(report.confidence, 0.95);
        assert_eq!(report.evidence_refs.len(), 7, "every evidence item was collected");
        assert!(report.missing_evidence.is_empty());
    }

    #[test]
    fn a_healthy_group_that_cannot_consume_is_reported_as_a_risk() {
        let mut evidence = fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 10)], 0.0));
        evidence.group = Ok(fixtures::group(&[("broker-a", false)]));

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Healthy);
        assert_eq!(rule_ids(&report), ["backlog_within_threshold"]);
        assert_eq!(report.risks.len(), 1);
        assert!(report.risks[0].starts_with("No consumer of the group is online."));
        assert!(report.recommendations[0].action.starts_with("Start the consumers"));
    }

    #[test]
    fn root_causes_are_ranked_and_recommendations_follow_them() {
        let mut evidence = fixtures::evidence(fixtures::lag_summary(
            vec![queue_behind("broker-a", 0, 6_000, 7_200), queue("broker-b", 0, 100)],
            500.0,
        ));
        evidence.dead_letter = Ok(fixtures::system_topic("%DLQ%order-service", 3, 30));

        let report = diagnose(evidence);

        assert_eq!(
            rule_ids(&report),
            [
                "lag_concentrated_on_queue",
                "dead_letter_queue_growing",
                "consume_delay"
            ]
        );
        assert!(report
            .root_causes
            .windows(2)
            .all(|pair| pair[0].confidence >= pair[1].confidence));
        assert_eq!(report.recommendations.len(), 3);
        assert_eq!(
            report.recommendations[0].action,
            "Inspect queues with the highest lag and their broker placement."
        );
        assert_eq!(
            report.severity,
            Severity::Critical,
            "two hours is 24 times the delay threshold"
        );
    }

    #[test]
    fn the_report_states_the_thresholds_it_was_evaluated_against() {
        let policy = ConsumerLagPolicy::from(&DiagnosisConfig {
            overrides: vec![DiagnosisOverride {
                consumer_lag_threshold: Some(50_000),
                ..override_for(Some("orders"), None)
            }],
            ..DiagnosisConfig::default()
        });

        let report = evaluate(
            &fixtures::args(),
            fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 40_000)], 0.0)),
            &policy,
        );

        assert_eq!(
            report.severity,
            Severity::Healthy,
            "the Topic override raises the lag threshold"
        );
        assert_eq!(report.target["lag_threshold"], 50_000);
        assert_eq!(report.target["consume_delay_threshold_ms"], 300_000);
        assert_eq!(report.target["pop_inflight_threshold"], 1_000);
        assert_eq!(report.target["retry_activity_window_ms"], 600_000);
        assert_eq!(report.policy_profile, "production-default");
    }

    #[test]
    fn overrides_apply_from_the_least_to_the_most_specific() {
        let policy = ConsumerLagPolicy::from(&DiagnosisConfig {
            // Listed most specific first: the order in the configuration does not matter.
            overrides: vec![
                DiagnosisOverride {
                    pop_inflight_threshold: Some(7),
                    ..override_for(Some("orders"), Some("order-service"))
                },
                DiagnosisOverride {
                    consume_delay_threshold_ms: Some(60_000),
                    pop_inflight_threshold: Some(6),
                    ..override_for(None, Some("order-service"))
                },
                DiagnosisOverride {
                    consumer_lag_threshold: Some(50_000),
                    consume_delay_threshold_ms: Some(30_000),
                    pop_inflight_threshold: Some(5),
                    ..override_for(Some("orders"), None)
                },
            ],
            ..DiagnosisConfig::default()
        });

        assert_eq!(
            policy.thresholds_for(Some("orders"), "order-service"),
            Thresholds {
                lag: 50_000,
                consume_delay_ms: 60_000,
                pop_inflight: 7,
                retry_activity_window_ms: 600_000,
            }
        );
        assert_eq!(
            policy.thresholds_for(Some("orders"), "billing").consume_delay_ms,
            30_000
        );
        // Across the Topics of the group, only the override that names the group alone applies.
        assert_eq!(
            policy.thresholds_for(None, "order-service"),
            Thresholds {
                lag: 1_000,
                consume_delay_ms: 60_000,
                pop_inflight: 6,
                retry_activity_window_ms: 600_000,
            }
        );
        assert_eq!(policy.thresholds_for(Some("payments"), "order-service").lag, 1_000);
        assert_eq!(policy.thresholds_for(Some("payments"), "order-service").pop_inflight, 6);
        assert_eq!(
            policy.thresholds_for(Some("payments"), "billing"),
            ConsumerLagPolicy::default().thresholds_for(Some("payments"), "billing")
        );
        // Names are matched exactly.
        assert_eq!(policy.thresholds_for(Some("Orders"), "billing").lag, 1_000);
    }
}
