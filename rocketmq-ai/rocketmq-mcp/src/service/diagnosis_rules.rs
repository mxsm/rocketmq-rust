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

use std::collections::HashSet;

use crate::config::DiagnosisConfig;
use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::RootCauseCandidate;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;

pub(crate) const CONSUMER_LAG_RULES_VERSION_V2: &str = "rocketmq-mcp.rules.consumer-lag.v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerLagPolicy {
    pub profile: String,
    pub lag_threshold: i64,
}

impl Default for ConsumerLagPolicy {
    fn default() -> Self {
        Self {
            profile: "production-default".to_string(),
            lag_threshold: 1_000,
        }
    }
}

impl From<&DiagnosisConfig> for ConsumerLagPolicy {
    fn from(config: &DiagnosisConfig) -> Self {
        Self {
            profile: config.consumer_lag_policy_profile.clone(),
            lag_threshold: config.consumer_lag_threshold,
        }
    }
}

pub(crate) fn evaluate(
    args: &DiagnoseConsumerLagArgs,
    evidence: ConsumerLagEvidence,
    policy: &ConsumerLagPolicy,
) -> DiagnosisReport {
    let snapshot = evidence.snapshot(args);
    let absent_targets = absent_targets(&evidence);
    let legacy_args = args.clone();
    let mut report = crate::service::diagnosis_service::build_consumer_lag_report_with_threshold(
        legacy_args,
        policy.lag_threshold,
        evidence.lag,
        evidence.topic,
        evidence.route,
        evidence.broker,
    );
    let present = snapshot
        .items
        .iter()
        .filter(|item| item.status == EvidenceStatus::Present)
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    report.evidence_version = snapshot.evidence_version.clone();
    report.rules_version = CONSUMER_LAG_RULES_VERSION_V2.to_string();
    report.policy_profile = policy.profile.clone();
    report.partial = snapshot.items.iter().any(|item| item.status != EvidenceStatus::Present);
    report.missing_evidence = snapshot
        .items
        .iter()
        .filter(|item| item.status != EvidenceStatus::Present)
        .map(|item| item.id.clone())
        .collect();
    report.evidence_refs = snapshot
        .items
        .iter()
        .filter(|item| item.status == EvidenceStatus::Present)
        .map(|item| item.id.clone())
        .collect();
    report.evidence_snapshot = Some(snapshot.clone());
    report.root_causes.retain(|cause| {
        cause
            .evidence_refs
            .iter()
            .all(|reference| present.contains(reference.as_str()))
    });
    if !absent_targets.is_empty() {
        report_absent_targets(args, &absent_targets, &mut report);
    }
    report
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
        ("topic_route", "rocketmq_get_topic_route", evidence.route.as_ref().err()),
        ("consumer_lag", "rocketmq_get_consumer_lag", evidence.lag.as_ref().err()),
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

    #[test]
    fn unavailable_evidence_produces_partial_unknown_without_root_causes() {
        let args = DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
        };
        let report = evaluate(
            &args,
            ConsumerLagEvidence {
                lag: Err(crate::tools::executor::ToolFailure::Rejected(
                    crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 5_000 },
                )),
                topic: Err(crate::tools::executor::ToolFailure::Operational(
                    crate::tools::executor::ToolExecutionError::Backend(None),
                )),
                route: Err(crate::tools::executor::ToolFailure::Operational(
                    crate::tools::executor::ToolExecutionError::Backend(None),
                )),
                broker: None,
            },
            &ConsumerLagPolicy::default(),
        );

        assert!(report.partial);
        assert_eq!(report.severity, crate::model::diagnosis::Severity::Unknown);
        assert!(report.root_causes.is_empty());
        assert!(report.missing_evidence.contains(&"consumer_lag".to_string()));
    }
}
