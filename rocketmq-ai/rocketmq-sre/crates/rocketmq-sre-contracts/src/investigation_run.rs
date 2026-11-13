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

use chrono::{DateTime, Duration, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::investigation_run_error::{unique_run_ids, valid_run_key, valid_run_text};
use crate::{
    ClusterId, EvidenceId, HypothesisAssessment, InvestigationControlEpoch, InvestigationId, InvestigationRevision,
    InvestigationRunFailure, InvestigationRunId, InvestigationStep, ResourceKind, TenantId, TimeRange,
};

mod validation;

pub const INVESTIGATION_RUN_SCHEMA_VERSION: &str = "rocketmq-sre.investigation-run.v1";

/// Run control is separate from the saved Investigation and Incident lifecycles.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationRunStatus {
    Queued,
    Running,
    Pausing,
    Paused,
    NeedsEvidence,
    Completed,
    Inconclusive,
    Cancelling,
    Cancelled,
    Failed,
}

impl InvestigationRunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Inconclusive | Self::Cancelled | Self::Failed
        )
    }
}

/// An investigation subject, not a canonical Evidence source resource or URL.
#[derive(Clone, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationResource {
    pub kind: ResourceKind,
    pub key: String,
}

/// Immutable authority boundary and requested absolute observation window.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunScope {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub investigation_id: InvestigationId,
    pub resource: Option<InvestigationResource>,
    pub time_range: TimeRange,
}

impl InvestigationRunScope {
    /// Checks shape and the seven-day maximum requested window.
    ///
    /// # Errors
    /// Rejects nil identities, invalid resources, and unbounded or inverted windows.
    pub fn validate(&self) -> Result<(), InvestigationRunFailure> {
        if self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.investigation_id.as_uuid().is_nil()
            || self.time_range.start > self.time_range.end
            || self.time_range.end.signed_duration_since(self.time_range.start) > Duration::days(7)
            || self
                .resource
                .as_ref()
                .is_some_and(|item| !valid_run_text(&item.key, 512))
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        Ok(())
    }

    /// Uses exact subject identity; string prefixes never establish containment.
    #[must_use]
    pub fn narrows(&self, original: &Self) -> bool {
        self.tenant_id == original.tenant_id
            && self.cluster_id == original.cluster_id
            && self.investigation_id == original.investigation_id
            && (original.resource.is_none() || self.resource == original.resource)
            && self.time_range.start >= original.time_range.start
            && self.time_range.end <= original.time_range.end
    }
}

/// Exact compiled pack version; neither a catalog entry nor a directory grants authority.
#[derive(Clone, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationPackRef {
    pub pack_id: String,
    pub pack_version: String,
}

impl InvestigationPackRef {
    pub(crate) fn validate(&self) -> Result<(), InvestigationRunFailure> {
        if !valid_run_key(&self.pack_id)
            || !valid_run_text(&self.pack_version, 64)
            || semver::Version::parse(&self.pack_version).is_err()
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        Ok(())
    }
}

/// Fixed collector requirement resolved by the server from a compiled pack.
#[derive(Clone, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRequirementRef {
    pub pack: InvestigationPackRef,
    pub requirement_id: String,
}

/// Conservatively reserved or actually consumed resources, shared across all steps.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationBudgetCost {
    #[schemars(range(max = 12))]
    pub read_queries: u64,
    #[schemars(range(max = 8))]
    pub model_calls: u64,
    #[schemars(range(max = 256))]
    pub evidence_items: u64,
    #[schemars(range(max = 4_194_304))]
    pub output_bytes: u64,
}

impl InvestigationBudgetCost {
    pub(crate) fn add(self, other: Self) -> Result<Self, InvestigationRunFailure> {
        Ok(Self {
            read_queries: self
                .read_queries
                .checked_add(other.read_queries)
                .ok_or(InvestigationRunFailure::CounterExhausted)?,
            model_calls: self
                .model_calls
                .checked_add(other.model_calls)
                .ok_or(InvestigationRunFailure::CounterExhausted)?,
            evidence_items: self
                .evidence_items
                .checked_add(other.evidence_items)
                .ok_or(InvestigationRunFailure::CounterExhausted)?,
            output_bytes: self
                .output_bytes
                .checked_add(other.output_bytes)
                .ok_or(InvestigationRunFailure::CounterExhausted)?,
        })
    }

    #[must_use]
    pub fn fits_within(self, limit: Self) -> bool {
        self.read_queries <= limit.read_queries
            && self.model_calls <= limit.model_calls
            && self.evidence_items <= limit.evidence_items
            && self.output_bytes <= limit.output_bytes
    }
}

/// Hard limits: retries are admissions and cannot create a fresh budget.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationBudget {
    pub limit: InvestigationBudgetCost,
    pub max_steps: u32,
    pub max_in_flight: u8,
    pub max_attempts_per_step: u8,
    #[schemars(range(max = 2))]
    pub max_query_retries: u8,
}

impl InvestigationBudget {
    pub(crate) fn validate(&self) -> Result<(), InvestigationRunFailure> {
        if !(1..=64).contains(&self.max_steps)
            || !(1..=4).contains(&self.max_in_flight)
            || !(1..=3).contains(&self.max_attempts_per_step)
            || self.max_query_retries > 2
            || self.limit.read_queries > 12
            || self.limit.model_calls > 8
            || !(1..=256).contains(&self.limit.evidence_items)
            || !(1..=4_194_304).contains(&self.limit.output_bytes)
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        Ok(())
    }
}

/// Current permission supplied by the service, never accepted from a command body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationRunPermission {
    Read,
    Control,
}

/// A service-verified subject/window binding, not merely a cluster-readable ID.
///
/// The service validates actual Evidence metadata and related-resource provenance
/// before constructing this input. `scope` identifies the investigation subject
/// for which that reference was checked; it is not an Evidence canonical path.
pub struct InvestigationEvidenceContext {
    pub evidence_id: EvidenceId,
    pub scope: InvestigationRunScope,
}

/// Trusted service input after current authorization and Evidence visibility checks.
///
/// Deliberately not serializable: D14 must derive it from authenticated server
/// state, not deserialize it from the browser, model, or connector. This pure
/// contract does not replace token verification or saved-revision authorization.
pub struct InvestigationRunContext {
    pub actor_subject: String,
    pub permission: InvestigationRunPermission,
    pub authorized_scope: InvestigationRunScope,
    pub allowed_packs: Vec<InvestigationPackRef>,
    pub allowed_requirements: Vec<InvestigationRequirementRef>,
    pub visible_evidence: Vec<InvestigationEvidenceContext>,
}

impl InvestigationRunContext {
    pub(crate) fn authorize(&self, scope: &InvestigationRunScope) -> Result<(), InvestigationRunFailure> {
        self.authorized_scope.validate()?;
        if !unique_run_ids(&self.allowed_packs, 64)
            || !unique_run_ids(&self.allowed_requirements, 256)
            || self.visible_evidence.len() > 256
        {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        if self.permission != InvestigationRunPermission::Control || !valid_run_text(&self.actor_subject, 255) {
            return Err(InvestigationRunFailure::Unauthorized);
        }
        if !scope.narrows(&self.authorized_scope) {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        Ok(())
    }

    pub(crate) fn evidence_visible(
        &self,
        ids: &[EvidenceId],
        scope: &InvestigationRunScope,
    ) -> Result<(), InvestigationRunFailure> {
        if !unique_run_ids(ids, 256)
            || ids.iter().any(|id| {
                id.as_uuid().is_nil()
                    || !self.visible_evidence.iter().any(|proof| {
                        proof.evidence_id == *id && proof.scope.validate().is_ok() && proof.scope.narrows(scope)
                    })
            })
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        Ok(())
    }
}

/// Durable read model for bounded read-only work. No deployed run route exists in D13.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRun {
    pub schema_version: String,
    pub id: InvestigationRunId,
    pub initial_scope: InvestigationRunScope,
    pub effective_scope: InvestigationRunScope,
    pub initial_packs: Vec<InvestigationPackRef>,
    pub effective_packs: Vec<InvestigationPackRef>,
    pub status: InvestigationRunStatus,
    pub revision: InvestigationRevision,
    pub control_epoch: InvestigationControlEpoch,
    pub budget: InvestigationBudget,
    pub deadline: DateTime<Utc>,
    pub steps: Vec<InvestigationStep>,
    pub evidence_ids: Vec<EvidenceId>,
    /// Imported, already saved references consume the same distinct-Evidence budget.
    pub supplied_evidence_ids: Vec<EvidenceId>,
    pub assessments: Vec<HypothesisAssessment>,
    pub created_by_subject: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl InvestigationRun {
    /// Counts tool-query retries across all logical steps, including superseded attempts.
    ///
    /// # Errors
    /// Rejects counter overflow for malformed in-memory inputs.
    pub fn query_retry_count(&self) -> Result<u64, InvestigationRunFailure> {
        self.steps
            .iter()
            .filter(|step| matches!(step.spec.work, crate::InvestigationStepKind::CollectEvidence { .. }))
            .try_fold(0_u64, |count, step| {
                let retries = u64::try_from(step.attempts.len().saturating_sub(1))
                    .map_err(|_| InvestigationRunFailure::CounterExhausted)?;
                count
                    .checked_add(retries)
                    .ok_or(InvestigationRunFailure::CounterExhausted)
            })
    }

    /// Charged usage and outstanding reservations across all attempts.
    ///
    /// Every admitted read/model call remains charged after settlement, including
    /// failed and superseded work. Actual reported consumption stays in each
    /// result; Evidence and output settle to actual usage instead of their bound.
    /// Interrupted attempts retain their complete unknown reservation.
    ///
    /// # Errors
    /// Rejects arithmetic overflow instead of wrapping or resetting a budget.
    pub fn budget_usage(&self) -> Result<(InvestigationBudgetCost, InvestigationBudgetCost), InvestigationRunFailure> {
        let mut used = InvestigationBudgetCost {
            evidence_items: self.supplied_evidence_ids.len() as u64,
            ..InvestigationBudgetCost::default()
        };
        let mut reserved = InvestigationBudgetCost::default();
        for attempt in self.steps.iter().flat_map(|step| &step.attempts) {
            if let Some(result) = &attempt.result {
                used = used.add(InvestigationBudgetCost {
                    read_queries: attempt.reservation.read_queries,
                    model_calls: attempt.reservation.model_calls,
                    ..result.actual_cost
                })?;
            } else {
                reserved = reserved.add(attempt.reservation)?;
            }
        }
        Ok((used, reserved))
    }

    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.steps
            .iter()
            .flat_map(|step| &step.attempts)
            .filter(|attempt| attempt.result.is_none() && attempt.interruption.is_none())
            .count()
    }

    pub(crate) fn bump(&mut self, at: DateTime<Utc>) -> Result<(), InvestigationRunFailure> {
        if at < self.updated_at {
            return Err(InvestigationRunFailure::InvalidInput);
        }
        self.revision = self.revision.next()?;
        self.updated_at = at;
        Ok(())
    }

    pub(crate) fn next_epoch(&mut self) -> Result<(), InvestigationRunFailure> {
        self.control_epoch = self.control_epoch.next()?;
        Ok(())
    }

    pub(crate) fn expect_revision(&self, expected: InvestigationRevision) -> Result<(), InvestigationRunFailure> {
        if expected != self.revision {
            return Err(InvestigationRunFailure::RevisionConflict {
                expected,
                actual: self.revision,
            });
        }
        Ok(())
    }

    pub(crate) fn check_live(&self, at: DateTime<Utc>) -> Result<(), InvestigationRunFailure> {
        if at >= self.deadline {
            return Err(InvestigationRunFailure::DeadlineExceeded);
        }
        Ok(())
    }

    /// Advances queued work without changing the immutable scope or budget.
    ///
    /// # Errors
    /// Rejects lost authority, stale revisions, expired deadlines, and non-queued runs.
    pub fn start(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(expected_revision)?;
        self.check_live(at)?;
        if self.status != InvestigationRunStatus::Queued {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        let mut next = self.clone();
        next.status = InvestigationRunStatus::Running;
        next.bump(at)?;
        *self = next;
        Ok(())
    }
}

pub(crate) fn validate_packs(packs: &[InvestigationPackRef]) -> Result<(), InvestigationRunFailure> {
    if packs.is_empty() || !unique_run_ids(packs, 16) {
        return Err(InvestigationRunFailure::InvalidInput);
    }
    for pack in packs {
        pack.validate()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_addition_rejects_u64_overflow_for_every_meter() {
        for index in 0..4 {
            let mut maximum = InvestigationBudgetCost::default();
            let mut increment = InvestigationBudgetCost::default();
            match index {
                0 => {
                    maximum.read_queries = u64::MAX;
                    increment.read_queries = 1;
                }
                1 => {
                    maximum.model_calls = u64::MAX;
                    increment.model_calls = 1;
                }
                2 => {
                    maximum.evidence_items = u64::MAX;
                    increment.evidence_items = 1;
                }
                _ => {
                    maximum.output_bytes = u64::MAX;
                    increment.output_bytes = 1;
                }
            }
            assert_eq!(
                maximum.add(increment).unwrap_err(),
                InvestigationRunFailure::CounterExhausted
            );
        }
    }
}
