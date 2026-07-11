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
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceStatus;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;

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
    report
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
                lag: Err(crate::tools::executor::ToolExecutionError::TimedOut { timeout_ms: 5_000 }),
                topic: Err(crate::tools::executor::ToolExecutionError::backend(
                    "topic route unavailable",
                )),
                route: Err(crate::tools::executor::ToolExecutionError::backend(
                    "topic route unavailable",
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
