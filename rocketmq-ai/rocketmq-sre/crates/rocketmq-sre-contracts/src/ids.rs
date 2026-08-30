// Copyright 2023 The RocketMQ Rust Authors
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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

macro_rules! uuid_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a collision-resistant identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the underlying UUID.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

uuid_id!(IncidentId, "Stable identifier for an SRE incident.");
uuid_id!(EvidenceId, "Stable identifier for an evidence snapshot.");
uuid_id!(QueryId, "Stable identifier for an evidence query.");
uuid_id!(CorrelationId, "Identifier propagated across one logical SRE operation.");
uuid_id!(ClusterId, "Internal identifier for an onboarded cluster.");
uuid_id!(TenantId, "Stable tenant boundary identifier.");
uuid_id!(FleetId, "Stable identifier for an enterprise RocketMQ fleet.");
uuid_id!(FleetReleaseId, "Stable identifier for one multi-cluster Fleet release.");
uuid_id!(RegionId, "Stable identifier for a data-residency region.");
uuid_id!(QuotaPolicyId, "Stable identifier for a versioned Fleet quota policy.");
uuid_id!(
    ComplianceFindingId,
    "Stable identifier for an immutable Fleet compliance finding."
);
uuid_id!(
    FleetInspectionRunId,
    "Stable identifier for a bounded multi-cluster inspection run."
);
uuid_id!(
    EnterpriseIntegrationEventId,
    "Stable identifier for a signed enterprise integration event."
);
uuid_id!(
    FleetOnboardingAssessmentId,
    "Stable identifier for an immutable Fleet onboarding assessment."
);
uuid_id!(
    FleetQuotaDecisionId,
    "Stable identifier for one persisted Fleet quota decision."
);
uuid_id!(DrPlanId, "Stable identifier for a versioned disaster-recovery plan.");
uuid_id!(
    DrBackupAssetId,
    "Stable identifier for one disaster-recovery backup asset."
);
uuid_id!(DrExerciseId, "Stable identifier for a disaster-recovery exercise.");
uuid_id!(
    RecoveryCheckpointId,
    "Stable identifier for one immutable disaster-recovery checkpoint observation."
);
uuid_id!(DrFindingId, "Stable identifier for a disaster-recovery finding.");
uuid_id!(
    DrActionItemId,
    "Stable identifier for an action item created from a disaster-recovery finding."
);
uuid_id!(
    GovernanceArtifactId,
    "Stable identifier for one governed logical artifact."
);
uuid_id!(
    GovernanceVersionId,
    "Stable identifier for an immutable governed artifact version."
);
uuid_id!(
    GovernanceEventId,
    "Stable identifier for an append-only governance lifecycle event."
);
uuid_id!(
    GovernanceAdmissionId,
    "Stable identifier for one immutable governance admission decision."
);
uuid_id!(
    FinOpsCostEntryId,
    "Stable identifier for one append-only FinOps cost ledger entry."
);
uuid_id!(FinOpsBudgetId, "Stable identifier for a versioned FinOps budget.");
uuid_id!(
    FinOpsDecisionId,
    "Stable identifier for one immutable FinOps budget decision."
);
uuid_id!(
    FinOpsAllocationPolicyId,
    "Stable identifier for a showback or confirmed chargeback allocation policy."
);
uuid_id!(ConversationId, "Stable identifier for an operator conversation.");
uuid_id!(
    ConversationTurnId,
    "Stable identifier for an operator conversation turn."
);
uuid_id!(
    ConversationAnswerRevisionId,
    "Stable identifier for an immutable conversation answer revision."
);
uuid_id!(InvestigationId, "Stable identifier for a multi-step investigation.");
uuid_id!(TimelineEventId, "Stable identifier for a workflow timeline event.");
uuid_id!(
    DiagnosisRevisionId,
    "Stable identifier for an immutable diagnosis revision."
);
uuid_id!(InspectionRunId, "Stable identifier for an inspection run.");
uuid_id!(RecommendationId, "Stable identifier for a read-only recommendation.");
uuid_id!(AssetSnapshotId, "Stable identifier for an asset snapshot.");
uuid_id!(TopologyEdgeId, "Stable identifier for a versioned topology edge.");
uuid_id!(KnowledgeItemId, "Stable identifier for a knowledge item.");
uuid_id!(KnowledgeChunkId, "Stable identifier for a retrievable knowledge chunk.");
uuid_id!(ModelProfileId, "Stable identifier for a configured model profile.");
uuid_id!(ModelInvocationId, "Stable identifier for a model invocation.");
uuid_id!(ConnectorSessionId, "Stable identifier for a connector session.");
uuid_id!(AlertEventId, "Stable identifier for an ingested alert event.");
uuid_id!(
    IncidentRelationId,
    "Stable identifier for a relation between incidents."
);
uuid_id!(
    TopologySnapshotId,
    "Stable identifier for an immutable topology snapshot."
);
uuid_id!(ForecastId, "Stable identifier for a capacity or backlog forecast.");
uuid_id!(BaselineId, "Stable identifier for an anomaly baseline.");
uuid_id!(ChangePointId, "Stable identifier for a detected change point.");
uuid_id!(SimulationId, "Stable identifier for a read-only what-if simulation.");
uuid_id!(
    ReadinessReportId,
    "Stable identifier for an immutable readiness report."
);
uuid_id!(NotificationTargetId, "Stable identifier for a notification target.");
uuid_id!(
    NotificationDeliveryId,
    "Stable identifier for one notification delivery."
);
uuid_id!(OnCallOwnerId, "Stable identifier for an on-call owner mapping.");
uuid_id!(PostmortemId, "Stable identifier for a postmortem.");
uuid_id!(
    PostmortemRevisionId,
    "Stable identifier for an immutable postmortem revision."
);
uuid_id!(ActionItemId, "Stable identifier for a postmortem action item.");
uuid_id!(
    HealthSnapshotId,
    "Stable identifier for an immutable cluster health evaluation."
);
uuid_id!(ActionPlanId, "Stable identifier for an immutable action plan.");
uuid_id!(PlanStepId, "Stable identifier for an immutable action plan step.");
uuid_id!(PolicyDecisionId, "Stable identifier for an immutable policy decision.");
uuid_id!(CriticReviewId, "Stable identifier for an immutable critic review.");
uuid_id!(ApprovalId, "Stable identifier for an approval decision.");
uuid_id!(ExecutionId, "Stable identifier for a supervised execution.");
uuid_id!(ExecutionStepId, "Stable identifier for an execution step.");
uuid_id!(AuditEventId, "Stable identifier for an append-only audit event.");
uuid_id!(ResourceLockId, "Stable identifier for a temporary resource lock.");
uuid_id!(LeaseId, "Stable identifier for an executor lease generation.");
uuid_id!(
    ResourceQuarantineId,
    "Stable identifier for a persistent resource quarantine."
);
uuid_id!(RunbookId, "Stable identifier for a versioned change runbook.");
uuid_id!(RunbookStepId, "Stable identifier for a typed runbook step.");
uuid_id!(ChangeWindowId, "Stable identifier for a maintenance or freeze window.");
uuid_id!(ChangeScheduleId, "Stable identifier for a scheduled runbook execution.");
uuid_id!(
    IntegrationTargetId,
    "Stable identifier for one tenant-scoped external integration target."
);
uuid_id!(
    IntegrationDeliveryId,
    "Stable identifier for one idempotent integration delivery."
);
uuid_id!(ReleaseId, "Stable identifier for one supervised release workflow.");
uuid_id!(ReleaseReportId, "Stable identifier for one immutable release report.");
uuid_id!(
    AutonomyPolicyId,
    "Stable identifier for a versioned autonomy policy definition."
);
uuid_id!(
    AutonomyCohortId,
    "Stable identifier for an action and cluster qualification cohort."
);
uuid_id!(
    AutonomySampleId,
    "Stable identifier for one immutable autonomy qualification sample."
);
uuid_id!(
    AutonomyOutcomeId,
    "Stable identifier for one reconciled autonomy outcome."
);
uuid_id!(
    DynamicSafetyDecisionId,
    "Stable identifier for one short-lived dynamic safety decision."
);
uuid_id!(AutomationRunId, "Stable identifier for one bounded automation run.");
uuid_id!(
    AutomationFeedbackId,
    "Stable identifier for immutable operator feedback on automation output."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_as_uuid_strings() {
        let id = IncidentId::new();
        let encoded = serde_json::to_string(&id).expect("identifier should serialize");
        let decoded: IncidentId = serde_json::from_str(&encoded).expect("identifier should deserialize");

        assert_eq!(decoded, id);
        assert_eq!(id.to_string().parse::<IncidentId>().expect("UUID is valid"), id);
    }
}
