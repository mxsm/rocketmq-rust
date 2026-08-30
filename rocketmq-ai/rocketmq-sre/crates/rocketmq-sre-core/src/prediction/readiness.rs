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

use rocketmq_sre_contracts::ReadinessFinding;
use rocketmq_sre_contracts::ReadinessFindingSeverity;
use rocketmq_sre_contracts::ReadinessStatus;

/// Deterministic inputs shared by upgrade and disaster-recovery readiness.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadinessSignals {
    pub health_acceptable: Option<bool>,
    pub capacity_runway_acceptable: Option<bool>,
    pub quorum_ready: Option<bool>,
    pub recovery_verified: Option<bool>,
    pub telemetry_fresh: Option<bool>,
    pub rollback_or_failback_defined: Option<bool>,
}

/// Explainable readiness outcome before report identity and expiry are added.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadinessEvaluation {
    pub status: ReadinessStatus,
    pub findings: Vec<ReadinessFinding>,
}

/// Evaluates upgrade readiness from deterministic Pack summaries.
#[must_use]
pub fn evaluate_upgrade(signals: ReadinessSignals) -> ReadinessEvaluation {
    evaluate(
        signals,
        [
            ("health_acceptable", "cluster", "Cluster health must be acceptable"),
            (
                "capacity_runway_acceptable",
                "capacity",
                "Capacity runway must cover the upgrade window",
            ),
            (
                "quorum_ready",
                "controller",
                "Controller and Broker quorum must be ready",
            ),
            ("recovery_verified", "store", "Store recovery evidence must be current"),
            (
                "telemetry_fresh",
                "telemetry",
                "Telemetry must be fresh during the canary",
            ),
            (
                "rollback_or_failback_defined",
                "upgrade",
                "Canary rollback must be defined",
            ),
        ],
    )
}

/// Evaluates disaster-recovery readiness from deterministic Pack summaries.
#[must_use]
pub fn evaluate_dr(signals: ReadinessSignals) -> ReadinessEvaluation {
    evaluate(
        signals,
        [
            (
                "health_acceptable",
                "cluster",
                "Primary cluster health must be acceptable",
            ),
            (
                "capacity_runway_acceptable",
                "capacity",
                "Target capacity must cover failover demand",
            ),
            (
                "quorum_ready",
                "controller",
                "Controller snapshot and quorum must be ready",
            ),
            (
                "recovery_verified",
                "store",
                "Backup and restore verification must be current",
            ),
            ("telemetry_fresh", "telemetry", "DR telemetry must be fresh"),
            (
                "rollback_or_failback_defined",
                "dr",
                "Failback ownership and procedure must be defined",
            ),
        ],
    )
}

fn evaluate(signals: ReadinessSignals, definitions: [(&str, &str, &str); 6]) -> ReadinessEvaluation {
    let values = [
        signals.health_acceptable,
        signals.capacity_runway_acceptable,
        signals.quorum_ready,
        signals.recovery_verified,
        signals.telemetry_fresh,
        signals.rollback_or_failback_defined,
    ];
    let findings = definitions
        .into_iter()
        .zip(values)
        .filter_map(|((code, component, summary), value)| match value {
            Some(true) => None,
            Some(false) => Some(ReadinessFinding {
                code: format!("{code}_failed"),
                severity: ReadinessFindingSeverity::Blocker,
                component: component.to_owned(),
                summary: summary.to_owned(),
                evidence_ids: Vec::new(),
                remediation_hint: Some(format!("resolve_{code}")),
            }),
            None => Some(ReadinessFinding {
                code: format!("{code}_missing"),
                severity: ReadinessFindingSeverity::Warning,
                component: component.to_owned(),
                summary: format!("{summary}; evidence is missing"),
                evidence_ids: Vec::new(),
                remediation_hint: Some(format!("collect_{code}_evidence")),
            }),
        })
        .collect::<Vec<_>>();
    let status = if findings
        .iter()
        .any(|finding| finding.severity == ReadinessFindingSeverity::Blocker)
    {
        ReadinessStatus::Blocked
    } else if findings.len() == definitions.len() {
        ReadinessStatus::InsufficientData
    } else if findings.is_empty() {
        ReadinessStatus::Ready
    } else {
        ReadinessStatus::ReadyWithWarnings
    };
    ReadinessEvaluation { status, findings }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_evidence_is_never_reported_ready() {
        let evaluation = evaluate_upgrade(ReadinessSignals::default());
        assert_eq!(evaluation.status, ReadinessStatus::InsufficientData);
        assert_eq!(evaluation.findings.len(), 6);
    }

    #[test]
    fn blocker_overrides_other_ready_signals() {
        let evaluation = evaluate_dr(ReadinessSignals {
            health_acceptable: Some(true),
            capacity_runway_acceptable: Some(false),
            quorum_ready: Some(true),
            recovery_verified: Some(true),
            telemetry_fresh: Some(true),
            rollback_or_failback_defined: Some(true),
        });
        assert_eq!(evaluation.status, ReadinessStatus::Blocked);
        assert_eq!(evaluation.findings.len(), 1);
    }
}
