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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::ReleaseObservation;
use rocketmq_sre_contracts::ReleaseObservationPhase;
use rocketmq_sre_contracts::ReleaseReadinessSnapshot;
use rocketmq_sre_contracts::ReleaseReport;
use rocketmq_sre_contracts::ReleaseReportId;
use rocketmq_sre_contracts::ReleaseStatus;
use rocketmq_sre_contracts::ReleaseWorkflow;
use rocketmq_sre_contracts::SreTimestamp;
use rocketmq_sre_contracts::is_sha256_digest;

/// Deterministic release validation/state error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseError {
    InvalidWorkflow(String),
    InvalidTransition { from: ReleaseStatus, to: ReleaseStatus },
    ReadinessNotSatisfied,
    ReadinessExpired,
    RollbackUnavailable,
    ReportNotReady,
    SensitiveDataRejected,
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWorkflow(reason) => write!(formatter, "invalid release workflow: {reason}"),
            Self::InvalidTransition { from, to } => {
                write!(formatter, "invalid release transition from {from:?} to {to:?}")
            }
            Self::ReadinessNotSatisfied => formatter.write_str("release readiness gates are not satisfied"),
            Self::ReadinessExpired => formatter.write_str("release readiness snapshot is expired"),
            Self::RollbackUnavailable => formatter.write_str("approved typed rollback is unavailable"),
            Self::ReportNotReady => formatter.write_str("release report requires a terminal workflow and observations"),
            Self::SensitiveDataRejected => {
                formatter.write_str("release metadata contains prohibited sensitive material")
            }
        }
    }
}

impl Error for ReleaseError {}

/// Validates release identity, readiness, observations, and reports.
pub struct ReleaseValidator;

impl ReleaseValidator {
    /// Validates immutable release identity and plan bindings.
    ///
    /// # Errors
    ///
    /// Rejects malformed identities, hashes, target versions, and partial
    /// rollback bindings.
    pub fn validate_workflow(workflow: &ReleaseWorkflow) -> Result<(), ReleaseError> {
        if workflow.schema_version != "rocketmq-sre.release-workflow.v1"
            || workflow.id.as_uuid().is_nil()
            || workflow.tenant_id.as_uuid().is_nil()
            || workflow.cluster_id.as_uuid().is_nil()
            || workflow.incident_id.as_uuid().is_nil()
            || workflow.correlation_id.as_uuid().is_nil()
            || workflow.runbook_id.as_uuid().is_nil()
            || workflow.plan_id.as_uuid().is_nil()
            || !is_sha256_digest(&workflow.plan_hash)
            || workflow.change_id.trim().is_empty()
            || workflow.change_id.chars().count() > 256
            || workflow.release_ref.trim().is_empty()
            || workflow.release_ref.chars().count() > 256
            || workflow.target_version.trim().is_empty()
            || workflow.target_version.chars().count() > 128
            || workflow.runbook_version.trim().is_empty()
            || workflow.runbook_version.chars().count() > 64
            || workflow.created_by.trim().is_empty()
            || workflow.created_by.chars().count() > 256
            || workflow.updated_at < workflow.created_at
        {
            return Err(ReleaseError::InvalidWorkflow(
                "identity, plan hash, version, actor, or timestamps are invalid".to_owned(),
            ));
        }
        reject_sensitive(&workflow.change_id)?;
        reject_sensitive(&workflow.release_ref)?;
        reject_sensitive(&workflow.target_version)?;
        match (&workflow.rollback_plan_id, &workflow.rollback_plan_hash) {
            (None, None) => {}
            (Some(plan_id), Some(plan_hash)) if !plan_id.as_uuid().is_nil() && is_sha256_digest(plan_hash) => {}
            _ => {
                return Err(ReleaseError::InvalidWorkflow(
                    "rollback plan identity and hash must be present together".to_owned(),
                ));
            }
        }
        if let Some(readiness) = &workflow.readiness {
            validate_readiness_shape(readiness)?;
        }
        Ok(())
    }

    /// Requires a current, fully passing readiness snapshot.
    ///
    /// # Errors
    ///
    /// Rejects absent, failed, or expired readiness gates.
    pub fn require_ready(workflow: &ReleaseWorkflow, now: SreTimestamp) -> Result<(), ReleaseError> {
        let readiness = workflow.readiness.as_ref().ok_or(ReleaseError::ReadinessNotSatisfied)?;
        validate_readiness_shape(readiness)?;
        if readiness.valid_until <= now {
            return Err(ReleaseError::ReadinessExpired);
        }
        if !readiness.ready() {
            return Err(ReleaseError::ReadinessNotSatisfied);
        }
        Ok(())
    }

    /// Requires a complete approved rollback binding.
    ///
    /// # Errors
    ///
    /// Rejects release workflows without a typed rollback plan and digest.
    pub fn require_rollback(workflow: &ReleaseWorkflow) -> Result<(), ReleaseError> {
        match (&workflow.rollback_plan_id, &workflow.rollback_plan_hash) {
            (Some(plan_id), Some(plan_hash)) if !plan_id.as_uuid().is_nil() && is_sha256_digest(plan_hash) => Ok(()),
            _ => Err(ReleaseError::RollbackUnavailable),
        }
    }

    /// Validates a bounded SLO and synthetic-probe observation.
    ///
    /// # Errors
    ///
    /// Rejects unbounded evidence, inconsistent regression facts, or
    /// sensitive/unbounded summaries.
    pub fn validate_observation(observation: &ReleaseObservation) -> Result<(), ReleaseError> {
        if observation.evidence_ids.len() > 64
            || observation.sanitized_summary.trim().is_empty()
            || observation.sanitized_summary.chars().count() > 2_048
            || observation.sanitized_summary.chars().any(char::is_control)
            || (observation.regression_detected && observation.slo_healthy && observation.synthetic_probe_healthy)
        {
            return Err(ReleaseError::InvalidWorkflow(
                "release observation is unbounded or internally inconsistent".to_owned(),
            ));
        }
        reject_sensitive(&observation.sanitized_summary)
    }

    /// Builds an immutable before/during/after report.
    ///
    /// # Errors
    ///
    /// Rejects non-terminal workflows, missing phase coverage, or invalid
    /// observations.
    pub fn build_report(
        workflow: &ReleaseWorkflow,
        observations: &[ReleaseObservation],
        generated_at: SreTimestamp,
    ) -> Result<ReleaseReport, ReleaseError> {
        Self::validate_workflow(workflow)?;
        if !workflow.status.is_terminal() {
            return Err(ReleaseError::ReportNotReady);
        }
        for observation in observations {
            Self::validate_observation(observation)?;
        }
        let by_phase = |phase| {
            observations
                .iter()
                .filter(|observation| observation.phase == phase)
                .cloned()
                .collect::<Vec<_>>()
        };
        let before = by_phase(ReleaseObservationPhase::Before);
        let during = by_phase(ReleaseObservationPhase::During);
        let after = by_phase(ReleaseObservationPhase::After);
        if before.is_empty() || during.is_empty() || after.is_empty() {
            return Err(ReleaseError::ReportNotReady);
        }
        Ok(ReleaseReport {
            schema_version: "rocketmq-sre.release-report.v1".to_owned(),
            id: ReleaseReportId::new(),
            release_id: workflow.id,
            tenant_id: workflow.tenant_id,
            cluster_id: workflow.cluster_id,
            incident_id: workflow.incident_id,
            change_id: workflow.change_id.clone(),
            release_ref: workflow.release_ref.clone(),
            final_status: workflow.status,
            before,
            during,
            after,
            generated_at,
        })
    }
}

/// Closed release lifecycle transition validator.
pub struct ReleaseStateMachine;

impl ReleaseStateMachine {
    /// Validates one state transition.
    ///
    /// # Errors
    ///
    /// Rejects skips, terminal-state exits, and rollback paths that do not
    /// follow pause or active rollout states.
    pub fn transition(from: ReleaseStatus, to: ReleaseStatus) -> Result<(), ReleaseError> {
        let allowed = matches!(
            (from, to),
            (ReleaseStatus::Planned, ReleaseStatus::ReadinessChecking)
                | (ReleaseStatus::ReadinessChecking, ReleaseStatus::Ready)
                | (ReleaseStatus::ReadinessChecking, ReleaseStatus::Failed)
                | (ReleaseStatus::Ready, ReleaseStatus::CanaryRunning)
                | (ReleaseStatus::Ready, ReleaseStatus::Failed)
                | (ReleaseStatus::CanaryRunning, ReleaseStatus::Paused)
                | (ReleaseStatus::CanaryRunning, ReleaseStatus::Verifying)
                | (ReleaseStatus::CanaryRunning, ReleaseStatus::RollingBack)
                | (ReleaseStatus::CanaryRunning, ReleaseStatus::ManualTakeover)
                | (ReleaseStatus::Paused, ReleaseStatus::CanaryRunning)
                | (ReleaseStatus::Paused, ReleaseStatus::RollingBack)
                | (ReleaseStatus::Paused, ReleaseStatus::ManualTakeover)
                | (ReleaseStatus::Verifying, ReleaseStatus::Completed)
                | (ReleaseStatus::Verifying, ReleaseStatus::Paused)
                | (ReleaseStatus::Verifying, ReleaseStatus::RollingBack)
                | (ReleaseStatus::Verifying, ReleaseStatus::ManualTakeover)
                | (ReleaseStatus::Verifying, ReleaseStatus::Failed)
                | (ReleaseStatus::RollingBack, ReleaseStatus::RolledBack)
                | (ReleaseStatus::RollingBack, ReleaseStatus::ManualTakeover)
                | (ReleaseStatus::RollingBack, ReleaseStatus::Failed)
        );
        if allowed {
            Ok(())
        } else {
            Err(ReleaseError::InvalidTransition { from, to })
        }
    }

    /// Determines the fail-safe state after a during-release observation.
    ///
    /// Healthy observations preserve the current state. A regression pauses
    /// active rollout or verification before any further typed step.
    ///
    /// # Errors
    ///
    /// Rejects observations outside an active release state.
    pub fn observe(current: ReleaseStatus, observation: &ReleaseObservation) -> Result<ReleaseStatus, ReleaseError> {
        ReleaseValidator::validate_observation(observation)?;
        if !matches!(current, ReleaseStatus::CanaryRunning | ReleaseStatus::Verifying) {
            return Err(ReleaseError::InvalidTransition {
                from: current,
                to: ReleaseStatus::Paused,
            });
        }
        Ok(if observation.regression_detected {
            ReleaseStatus::Paused
        } else {
            current
        })
    }
}

fn validate_readiness_shape(readiness: &ReleaseReadinessSnapshot) -> Result<(), ReleaseError> {
    if readiness.upgrade_readiness_id.as_uuid().is_nil()
        || readiness.simulation_id.as_uuid().is_nil()
        || readiness.evidence_ids.len() > 64
        || readiness.valid_until <= readiness.observed_at
    {
        return Err(ReleaseError::InvalidWorkflow(
            "readiness identity, evidence bound, or validity interval is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn reject_sensitive(value: &str) -> Result<(), ReleaseError> {
    let normalized = value.to_ascii_lowercase();
    if [
        "token=",
        "secret=",
        "password=",
        "authorization:",
        "private key",
        "message body",
        "message_body",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Err(ReleaseError::SensitiveDataRejected);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use chrono::Utc;
    use rocketmq_sre_contracts::ActionPlanId;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::ReadinessReportId;
    use rocketmq_sre_contracts::ReleaseId;
    use rocketmq_sre_contracts::RunbookId;
    use rocketmq_sre_contracts::SimulationId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn workflow(status: ReleaseStatus) -> ReleaseWorkflow {
        let now = Utc::now();
        ReleaseWorkflow {
            schema_version: "rocketmq-sre.release-workflow.v1".to_owned(),
            id: ReleaseId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            incident_id: IncidentId::new(),
            correlation_id: CorrelationId::new(),
            change_id: "CHG-1001".to_owned(),
            release_ref: "release-2026.07.28".to_owned(),
            target_version: "5.3.0".to_owned(),
            runbook_id: RunbookId::new(),
            runbook_version: "1.0.0".to_owned(),
            plan_id: ActionPlanId::new(),
            plan_hash: format!("sha256:{}", "a".repeat(64)),
            rollback_plan_id: Some(ActionPlanId::new()),
            rollback_plan_hash: Some(format!("sha256:{}", "b".repeat(64))),
            readiness: Some(ReleaseReadinessSnapshot {
                upgrade_readiness_id: ReadinessReportId::new(),
                simulation_id: SimulationId::new(),
                pdb_ready: true,
                capacity_ready: true,
                quorum_ready: true,
                store_recovery_ready: true,
                synthetic_probe_ready: true,
                evidence_ids: vec![],
                observed_at: now,
                valid_until: now + Duration::hours(1),
            }),
            status,
            active_execution_id: None,
            regression_detected: false,
            pause_reason: None,
            created_by: "operator".to_owned(),
            created_at: now,
            updated_at: now,
        }
    }

    fn observation(phase: ReleaseObservationPhase, regression: bool) -> ReleaseObservation {
        ReleaseObservation {
            phase,
            slo_healthy: !regression,
            synthetic_probe_healthy: !regression,
            regression_detected: regression,
            evidence_ids: vec![],
            sanitized_summary: if regression {
                "Proxy canary error budget regressed".to_owned()
            } else {
                "SLO and synthetic probe remain healthy".to_owned()
            },
            observed_at: Utc::now(),
        }
    }

    #[test]
    fn readiness_and_regression_pause_are_fail_closed() {
        let workflow = workflow(ReleaseStatus::CanaryRunning);
        assert!(ReleaseValidator::require_ready(&workflow, Utc::now()).is_ok());
        assert_eq!(
            ReleaseStateMachine::observe(
                ReleaseStatus::CanaryRunning,
                &observation(ReleaseObservationPhase::During, true)
            ),
            Ok(ReleaseStatus::Paused)
        );
        assert!(ReleaseStateMachine::transition(ReleaseStatus::Paused, ReleaseStatus::RollingBack).is_ok());
        assert!(ReleaseStateMachine::transition(ReleaseStatus::Completed, ReleaseStatus::Ready).is_err());
    }

    #[test]
    fn release_report_requires_all_phases_and_terminal_state() {
        let workflow = workflow(ReleaseStatus::Completed);
        let observations = vec![
            observation(ReleaseObservationPhase::Before, false),
            observation(ReleaseObservationPhase::During, false),
            observation(ReleaseObservationPhase::After, false),
        ];
        let report = ReleaseValidator::build_report(&workflow, &observations, Utc::now()).expect("release report");
        assert_eq!(report.final_status, ReleaseStatus::Completed);
        assert_eq!(report.before.len(), 1);
        assert_eq!(report.during.len(), 1);
        assert_eq!(report.after.len(), 1);
    }
}
