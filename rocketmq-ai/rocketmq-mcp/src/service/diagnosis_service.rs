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

//! Writes diagnosis reports from an evidence snapshot and what the rules concluded.
//!
//! [`finish`] completes any report. [`build_lag_report`] writes the report of a backlog, which the
//! diagnosis of one Topic and the diagnosis of a whole Consumer Group share.

use serde_json::json;
use serde_json::Value;

use crate::model::contract::observed_at;
use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceItem;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::ImpactItem;
use crate::model::diagnosis::MetricWatchItem;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::RootCauseCandidate;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_collector::LagFigures;
use crate::service::diagnosis_evidence::duration_text;
use crate::service::diagnosis_rules::conclusion;
use crate::service::diagnosis_rules::Finding;
use crate::service::diagnosis_rules::Thresholds;

/// The parts of a report that a diagnosis writes itself.
pub(crate) struct ReportBody {
    pub report_type: &'static str,
    pub rules_version: &'static str,
    pub target: Value,
    pub policy_profile: String,
    pub severity: Severity,
    pub confidence: f32,
    pub summary: String,
    pub impacts: Vec<ImpactItem>,
    pub root_causes: Vec<RootCauseCandidate>,
    pub recommendations: Vec<Recommendation>,
    pub risks: Vec<String>,
    pub follow_up_metrics: Vec<MetricWatchItem>,
}

/// Completes a report from its body and its evidence.
///
/// The snapshot is the only place a report holds evidence. Which items were collected, which are
/// missing and whether the report is partial follow from it, so they cannot disagree with it.
pub(crate) fn finish(body: ReportBody, snapshot: EvidenceSnapshot) -> DiagnosisReport {
    let ids = |present: bool| {
        snapshot
            .items
            .iter()
            .filter(|item| is_present(item) == present)
            .map(|item| item.id.clone())
            .collect::<Vec<_>>()
    };
    let missing_evidence = ids(false);
    DiagnosisReport {
        report_type: body.report_type.to_string(),
        evidence_version: snapshot.evidence_version.clone(),
        rules_version: body.rules_version.to_string(),
        cluster: snapshot.cluster.clone(),
        target: body.target,
        severity: body.severity,
        confidence: body.confidence,
        confidence_band: confidence_band(body.confidence),
        policy_profile: body.policy_profile,
        partial: !missing_evidence.is_empty(),
        missing_evidence,
        evidence_refs: ids(true),
        summary: body.summary,
        impacts: body.impacts,
        root_causes: body.root_causes,
        recommendations: body.recommendations,
        risks: body.risks,
        follow_up_metrics: body.follow_up_metrics,
        evidence_snapshot: snapshot,
        generated_at: observed_at(),
    }
}

fn is_present(item: &EvidenceItem) -> bool {
    item.status == EvidenceStatus::Present
}

/// How much of the evidence a complete diagnosis rests on was collected.
///
/// `base` is what the evidence the diagnosis cannot do without is worth, and `weights` what each
/// other item adds once it is present. All are thousandths, so that the sum is exact.
pub(crate) fn weighted_confidence(snapshot: &EvidenceSnapshot, base: u16, weights: &[(&str, u16)]) -> f32 {
    let collected = snapshot
        .items
        .iter()
        .filter(|item| is_present(item))
        .filter_map(|item| weights.iter().find(|(id, _)| *id == item.id).map(|(_, weight)| *weight))
        .sum::<u16>();
    f32::from(base + collected) / 1_000.0
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

/// The root causes that fired rules stand for, in the order given.
pub(crate) fn fired_root_causes<'a>(
    findings: &'a [(&'static str, Finding)],
) -> impl Iterator<Item = RootCauseCandidate> + 'a {
    findings.iter().map(|(rule_id, finding)| RootCauseCandidate {
        rule_id: rule_id.to_string(),
        cause: finding.cause.clone(),
        confidence: finding.confidence,
        evidence_refs: finding.evidence_refs.iter().map(|id| id.to_string()).collect(),
        reasoning: finding.reasoning.clone(),
    })
}

/// A conclusion that no rule drew, resting on one evidence item.
pub(crate) fn conclude(
    rule_id: &str,
    evidence: &str,
    cause: &str,
    confidence: f32,
    reasoning: &str,
) -> RootCauseCandidate {
    RootCauseCandidate {
        rule_id: rule_id.to_string(),
        cause: cause.to_string(),
        confidence,
        evidence_refs: vec![evidence.to_string()],
        reasoning: reasoning.to_string(),
    }
}

/// A condition that a health diagnosis found, and how bad it is on its own.
pub(crate) struct HealthFinding {
    pub rule_id: &'static str,
    pub severity: Severity,
    /// What the condition affects, as the area of an impact.
    pub area: String,
    pub finding: Finding,
}

/// Everything the report of a health diagnosis is written from.
pub(crate) struct HealthReport<'a> {
    pub report_type: &'static str,
    pub rules_version: &'static str,
    pub target: Value,
    /// Who the report is about, as the summary names it: `Broker broker-a`.
    pub subject: String,
    pub policy_profile: &'a str,
    /// The evidence the diagnosis cannot do without, or `None` when it was collected.
    pub missing_primary: Option<&'static str>,
    /// What a report without a finding says after "is Healthy: ".
    pub healthy_summary: String,
    pub confidence: f32,
    pub findings: Vec<HealthFinding>,
    /// What to keep in mind before acting on a finding.
    pub risks: Vec<String>,
    pub follow_up_metrics: Vec<MetricWatchItem>,
    pub snapshot: EvidenceSnapshot,
}

/// How severities order when the worst of several is wanted. `Unknown` never wins: it is what a
/// report is when nothing could be judged, not a grade of a finding.
fn rank(severity: Severity) -> u8 {
    match severity {
        Severity::Unknown | Severity::Healthy => 0,
        Severity::Low => 1,
        Severity::Medium => 2,
        Severity::High => 3,
        Severity::Critical => 4,
    }
}

/// Writes the report of a health diagnosis.
///
/// The report is as severe as its worst finding, `healthy` without one, and `unknown` when the
/// evidence it cannot do without is missing. Findings are listed worst first.
pub(crate) fn build_health_report(report: HealthReport<'_>) -> DiagnosisReport {
    let HealthReport {
        report_type,
        rules_version,
        target,
        subject,
        policy_profile,
        missing_primary,
        healthy_summary,
        confidence,
        mut findings,
        risks,
        follow_up_metrics,
        snapshot,
    } = report;
    // Both sorts are stable, so equal findings keep the order of the rule table.
    findings.sort_by(|left, right| right.finding.confidence.total_cmp(&left.finding.confidence));
    findings.sort_by_key(|finding| std::cmp::Reverse(rank(finding.severity)));
    let severity = match (missing_primary, findings.first()) {
        (Some(_), _) => Severity::Unknown,
        (None, None) => Severity::Healthy,
        (None, Some(worst)) => worst.severity,
    };
    let summary = match (missing_primary, findings.as_slice()) {
        (Some(missing), _) => format!("{subject} health is Unknown because {missing} evidence is missing."),
        (None, []) => format!("{subject} is Healthy: {healthy_summary}"),
        (None, [worst]) => format!("{subject} is {severity:?}: {}.", worst.finding.cause),
        (None, [worst, others @ ..]) => format!(
            "{subject} is {severity:?}: {}, and {} more findings.",
            worst.finding.cause,
            others.len()
        ),
    };
    let mut recommendations = Vec::<Recommendation>::new();
    for finding in &findings {
        // Two findings often call for the same step; it is recommended once.
        if recommendations
            .iter()
            .all(|listed| listed.action != finding.finding.recommendation.action)
        {
            recommendations.push(finding.finding.recommendation.clone());
        }
    }
    if recommendations.is_empty() {
        recommendations.push(match missing_primary {
            Some(_) => Recommendation {
                action: "Collect the evidence again.".to_string(),
                priority: "high".to_string(),
                rationale: "The report is Unknown because required evidence is missing.".to_string(),
                risk: "Do not change configuration without evidence.".to_string(),
                verification: "Re-run the diagnosis after the evidence is available.".to_string(),
            },
            None => Recommendation {
                action: "Continue monitoring.".to_string(),
                priority: "low".to_string(),
                rationale: "No rule found a problem in the collected evidence.".to_string(),
                risk: "No immediate operational change is recommended.".to_string(),
                verification: "Confirm the next diagnosis is healthy too.".to_string(),
            },
        });
    }
    let risks = match (missing_primary, findings.is_empty()) {
        (Some(_), _) => vec!["Evidence is incomplete; automated remediation is not safe.".to_string()],
        (None, true) => vec!["No health risk was observed in the collected evidence.".to_string()],
        (None, false) => risks,
    };
    finish(
        ReportBody {
            report_type,
            rules_version,
            target,
            policy_profile: policy_profile.to_string(),
            severity,
            confidence,
            summary,
            // A finding that is not a problem has no impact.
            impacts: findings
                .iter()
                .filter(|finding| rank(finding.severity) > 0)
                .map(|finding| ImpactItem {
                    area: finding.area.clone(),
                    description: finding.finding.cause.clone(),
                    severity: finding.severity,
                })
                .collect(),
            root_causes: findings
                .iter()
                .map(|finding| RootCauseCandidate {
                    rule_id: finding.rule_id.to_string(),
                    cause: finding.finding.cause.clone(),
                    confidence: finding.finding.confidence,
                    evidence_refs: finding.finding.evidence_refs.iter().map(|id| id.to_string()).collect(),
                    reasoning: finding.finding.reasoning.clone(),
                })
                .collect(),
            recommendations,
            risks,
            follow_up_metrics,
        },
        snapshot,
    )
}

/// What the rules made of a backlog.
pub(crate) struct Assessment {
    pub severity: Severity,
    /// Whether the backlog is above the lag threshold.
    pub backlog: bool,
    /// The rules that fired, by identifier, most confident first. Without a backlog these are only
    /// conditions that stop consumption.
    pub findings: Vec<(&'static str, Finding)>,
}

/// Whose backlog a report is about, in the words the report uses.
pub(crate) struct LagSubject {
    /// Completes "Consumer ...": `group orders-consumer on topic orders`.
    pub phrase: String,
    /// The area an impact names: `orders:orders-consumer`.
    pub area: String,
    /// What delayed consumption affects: `topic orders`.
    pub affected: String,
    /// Completes "the group reports no consume progress...": ` on the topic`, or nothing.
    pub progress_scope: &'static str,
}

/// Everything the report of a backlog is written from.
pub(crate) struct LagReport<'a> {
    pub report_type: &'static str,
    pub rules_version: &'static str,
    /// Who the report is about; the thresholds are added to it.
    pub target: Value,
    pub subject: LagSubject,
    pub policy_profile: &'a str,
    pub thresholds: &'a Thresholds,
    /// The evidence item that holds the backlog, which a conclusion without a rule rests on.
    pub lag_evidence: &'static str,
    /// What each evidence item other than the backlog adds to the confidence, in thousandths.
    pub confidence_weights: &'static [(&'static str, u16)],
    pub snapshot: EvidenceSnapshot,
    /// The backlog, when it was read.
    pub lag: Option<LagFigures<'a>>,
    pub assessment: Assessment,
}

/// Writes the report of a backlog.
pub(crate) fn build_lag_report(report: LagReport<'_>) -> DiagnosisReport {
    let LagReport {
        report_type,
        rules_version,
        mut target,
        subject,
        policy_profile,
        thresholds,
        lag_evidence,
        confidence_weights,
        snapshot,
        lag,
        assessment,
    } = report;
    if let Value::Object(target) = &mut target {
        // The values in effect after overrides, so that a reader can tell what "healthy" meant.
        target.insert("lag_threshold".to_string(), json!(thresholds.lag));
        target.insert(
            "consume_delay_threshold_ms".to_string(),
            json!(thresholds.consume_delay_ms),
        );
        target.insert("pop_inflight_threshold".to_string(), json!(thresholds.pop_inflight));
        target.insert(
            "retry_activity_window_ms".to_string(),
            json!(thresholds.retry_activity_window_ms),
        );
    }
    let severity = assessment.severity;
    finish(
        ReportBody {
            report_type,
            rules_version,
            target,
            policy_profile: policy_profile.to_string(),
            severity,
            confidence: lag_confidence(&snapshot, lag, confidence_weights),
            summary: summary(&subject, lag, thresholds, severity),
            impacts: impacts(&subject, lag, severity),
            root_causes: root_causes(lag, lag_evidence, &assessment),
            recommendations: recommendations(lag, &assessment),
            risks: risks(&subject, lag, &assessment),
            follow_up_metrics: follow_up_metrics(),
        },
        snapshot,
    )
}

/// The backlog carries most of the weight, because nothing about it can be concluded without it.
fn lag_confidence(snapshot: &EvidenceSnapshot, lag: Option<LagFigures<'_>>, weights: &[(&str, u16)]) -> f32 {
    match lag {
        None => 0.20,
        Some(lag) if lag.queues.queue_count == 0 => weighted_confidence(snapshot, 500, weights),
        Some(_) => weighted_confidence(snapshot, 600, weights),
    }
}

fn summary(subject: &LagSubject, lag: Option<LagFigures<'_>>, thresholds: &Thresholds, severity: Severity) -> String {
    let Some(lag) = lag else {
        return format!(
            "Consumer lag diagnosis for {} is Unknown because primary lag evidence is missing.",
            subject.phrase
        );
    };
    if lag.queues.queue_count == 0 {
        return format!(
            "Consumer lag diagnosis for {} is Unknown because the group reports no consume progress{}.",
            subject.phrase, subject.progress_scope
        );
    }
    if severity == Severity::Healthy {
        return format!(
            "Consumer {} is within the lag threshold: total_lag={} threshold={}.",
            subject.phrase, lag.total_lag, thresholds.lag
        );
    }
    let delay = lag
        .delay
        .consume_delay_ms
        .map(|delay| format!(" and is {} behind production", duration_text(delay)))
        .unwrap_or_default();
    format!(
        "Consumer {} has {} total lag across {} queues{delay}; severity is {:?}.",
        subject.phrase, lag.total_lag, lag.queues.queue_count, severity
    )
}

fn impacts(subject: &LagSubject, lag: Option<LagFigures<'_>>, severity: Severity) -> Vec<ImpactItem> {
    let Some(lag) = lag else {
        return vec![ImpactItem {
            area: "consumer_lag".to_string(),
            description: "Primary lag evidence is unavailable, so impact cannot be quantified.".to_string(),
            severity,
        }];
    };
    let delay = lag
        .delay
        .consume_delay_ms
        .map(|delay| format!(" Consumption is {} behind production.", duration_text(delay)))
        .unwrap_or_default();
    vec![ImpactItem {
        area: subject.area.clone(),
        description: format!(
            "Total lag is {} across {} queues, with max queue lag {}.{delay}",
            lag.total_lag, lag.queues.queue_count, lag.queues.max_queue_lag
        ),
        severity,
    }]
}

/// The conclusions of the report, most confident first.
///
/// Without the backlog nothing is concluded about it; only what the rules established from the
/// other evidence remains. A backlog within the threshold has nothing to explain, so what the
/// rules found there is a risk, not a cause.
fn root_causes(lag: Option<LagFigures<'_>>, lag_evidence: &str, assessment: &Assessment) -> Vec<RootCauseCandidate> {
    let Some(lag) = lag else {
        return fired_root_causes(&assessment.findings).collect();
    };
    if lag.queues.queue_count == 0 {
        // A group that cannot consume is the likely reason it reports no progress.
        return fired_root_causes(&assessment.findings)
            .chain([conclude(
                conclusion::NO_QUEUE_ROWS,
                lag_evidence,
                "Unknown",
                0.30,
                "Consumer lag evidence contains no queue rows.",
            )])
            .collect();
    }
    if !assessment.backlog {
        return vec![conclude(
            conclusion::BACKLOG_WITHIN_THRESHOLD,
            lag_evidence,
            "No active backlog above threshold",
            0.80,
            "Observed total lag is at or below the configured threshold.",
        )];
    }
    if assessment.findings.is_empty() {
        return vec![conclude(
            conclusion::CAUSE_NOT_ISOLATED,
            lag_evidence,
            "Backlog exists but available evidence is insufficient for a specific cause",
            0.45,
            "Lag is above the threshold, but no rule isolated a cause from the collected evidence.",
        )];
    }
    fired_root_causes(&assessment.findings).collect()
}

fn recommendations(lag: Option<LagFigures<'_>>, assessment: &Assessment) -> Vec<Recommendation> {
    if !assessment.findings.is_empty() {
        return assessment
            .findings
            .iter()
            .map(|(_, finding)| finding.recommendation.clone())
            .collect();
    }
    match (lag, assessment.severity) {
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
                    lag.queues.max_queue_lag
                ),
                risk: "Queue-level findings require route evidence before rebalancing decisions.".to_string(),
                verification: "Confirm max_queue_lag converges after the placement review.".to_string(),
            },
        ],
    }
}

fn risks(subject: &LagSubject, lag: Option<LagFigures<'_>>, assessment: &Assessment) -> Vec<String> {
    match (lag, assessment.severity) {
        (None, _) | (_, Severity::Unknown) => {
            vec!["Evidence is incomplete; automated remediation is not safe.".to_string()]
        }
        (_, Severity::Healthy) if assessment.findings.is_empty() => {
            vec!["No backlog risk above the configured threshold was observed.".to_string()]
        }
        // The backlog is small today, and something stops the group from consuming.
        (_, Severity::Healthy) => assessment
            .findings
            .iter()
            .map(|(_, finding)| {
                format!(
                    "{}. Messages produced for this group accumulate while this lasts.",
                    finding.cause
                )
            })
            .collect(),
        (Some(_), _) => vec![
            format!(
                "Delayed consumption may affect downstream processing for {}.",
                subject.affected
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
        MetricWatchItem {
            name: "consume_delay_ms".to_string(),
            target: "consumer_group/topic".to_string(),
            reason: "Shows whether the group catches up with production.".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::diagnosis_collector::fixtures;
    use crate::service::diagnosis_collector::fixtures::queue;
    use crate::service::diagnosis_collector::ConsumerLagEvidence;
    use crate::service::diagnosis_rules::consumer_lag_report;

    const THRESHOLDS: Thresholds = Thresholds {
        lag: 1_000,
        consume_delay_ms: 300_000,
        pop_inflight: 1_000,
        retry_activity_window_ms: 600_000,
    };

    fn report(evidence: &ConsumerLagEvidence, assessment: Assessment) -> DiagnosisReport {
        consumer_lag_report(
            &fixtures::args(),
            "production-default",
            &THRESHOLDS,
            evidence.snapshot(&fixtures::args()),
            evidence.lag.as_ref().ok().map(|lag| lag.figures()),
            assessment,
        )
    }

    fn no_findings(severity: Severity, backlog: bool) -> Assessment {
        Assessment {
            severity,
            backlog,
            findings: Vec::new(),
        }
    }

    #[test]
    fn confidence_follows_how_much_evidence_was_collected() {
        let complete = || fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 5_000)], 500.0));

        let all = report(&complete(), no_findings(Severity::Medium, true));
        assert_eq!(all.confidence, 0.95);
        assert_eq!(all.confidence_band, ConfidenceBand::High);

        let mut evidence = complete();
        evidence.group = fixtures::unavailable();
        evidence.topic_config = fixtures::unavailable();
        evidence.retry = fixtures::unavailable();
        let some = report(&evidence, no_findings(Severity::Medium, true));
        assert_eq!(some.confidence, 0.775);
        assert_eq!(some.confidence_band, ConfidenceBand::Medium);
        assert_eq!(
            some.missing_evidence,
            ["consumer_group", "topic_config", "retry_topic_stats"]
        );

        evidence.lag = fixtures::unavailable();
        let none = report(&evidence, no_findings(Severity::Unknown, false));
        assert_eq!(none.confidence, 0.20);
        assert_eq!(none.confidence_band, ConfidenceBand::Low);
        assert!(none.root_causes.is_empty());
        assert!(none.summary.contains("primary lag evidence is missing"));
    }

    #[test]
    fn a_group_without_consume_progress_is_unknown_and_says_so() {
        let evidence = fixtures::evidence(fixtures::lag_summary(Vec::new(), 0.0));

        let report = report(&evidence, no_findings(Severity::Unknown, false));

        assert!(report.summary.contains("reports no consume progress on the topic"));
        assert_eq!(report.root_causes.len(), 1);
        assert_eq!(report.root_causes[0].rule_id, "no_queue_rows");
        assert_eq!(
            report.missing_evidence,
            ["broker_summary"],
            "there was no lag to pick a Broker by"
        );
        assert!(report.recommendations[0].action.starts_with("Collect consumer lag"));
    }

    #[test]
    fn the_evidence_is_held_once_and_referred_to_by_identifier() {
        let evidence = fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 5_000)], 500.0));

        let report = serde_json::to_value(report(&evidence, no_findings(Severity::Medium, true))).unwrap();

        assert!(report.get("evidences").is_none());
        let items = report["evidence_snapshot"]["items"].as_array().unwrap();
        let ids = items
            .iter()
            .map(|item| item["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                "consumer_lag",
                "topic_route",
                "broker_summary",
                "consumer_group",
                "topic_config",
                "retry_topic_stats",
                "dead_letter_topic_stats"
            ]
        );
        assert_eq!(report["evidence_refs"], serde_json::json!(ids));
        // The route rows of the Topic appear in exactly one item.
        let serialized = report.to_string();
        assert_eq!(serialized.matches("\"topic_sys_flag\"").count(), 1);
    }

    #[test]
    fn a_finished_report_derives_its_evidence_lists_from_the_snapshot() {
        let mut evidence = fixtures::evidence(fixtures::lag_summary(vec![queue("broker-a", 0, 5_000)], 500.0));
        evidence.route = fixtures::unavailable();
        let snapshot = evidence.snapshot(&fixtures::args());

        let report = finish(
            ReportBody {
                report_type: "example",
                rules_version: "rules.v1",
                target: json!({}),
                policy_profile: "profile".to_string(),
                severity: Severity::Low,
                confidence: 0.5,
                summary: String::new(),
                impacts: Vec::new(),
                root_causes: Vec::new(),
                recommendations: Vec::new(),
                risks: Vec::new(),
                follow_up_metrics: Vec::new(),
            },
            snapshot,
        );

        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["topic_route"]);
        assert_eq!(report.evidence_refs.len(), 6);
        assert_eq!(report.cluster, "local-dev");
        assert_eq!(report.evidence_version, report.evidence_snapshot.evidence_version);
        assert_eq!(report.confidence_band, ConfidenceBand::Medium);
    }
}
