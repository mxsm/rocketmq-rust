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

use std::sync::LazyLock;

use serde_json::Value;

const PHASE_FIVE_OPENAPI: &str = include_str!("../../../openapi/rocketmq-sre-phase05.openapi.json");

static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    // Invariant: the checked-in document is parsed by this module's tests and
    // by the UI type-generation contract before it can be accepted.
    serde_json::from_str(PHASE_FIVE_OPENAPI).expect("the checked-in Phase 05 OpenAPI document must be valid JSON")
});

pub(crate) fn document() -> Value {
    DOCUMENT.clone()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const REQUIRED_PUBLIC_PATHS: &[&str] = &[
        "/healthz",
        "/readyz",
        "/v1/action-items",
        "/v1/action-items/{id}",
        "/v1/audit/{correlation_id}",
        "/v1/assets",
        "/v1/assets/dashboard-link",
        "/v1/autonomy/outcomes",
        "/v1/autonomy/reports",
        "/v1/capabilities",
        "/v1/capabilities/coverage",
        "/v1/capabilities/phase2-contract",
        "/v1/clusters",
        "/v1/clusters/onboard",
        "/v1/clusters/{id}",
        "/v1/clusters/{id}/capabilities",
        "/v1/clusters/{id}/connector",
        "/v1/clusters/{id}/handshake",
        "/v1/clusters/{id}/health",
        "/v1/clusters/{id}/forecasts",
        "/v1/clusters/{id}/inventory/latest",
        "/v1/clusters/{id}/offboard",
        "/v1/clusters/{id}/slo",
        "/v1/clusters/{id}/readiness/dr",
        "/v1/clusters/{id}/readiness/upgrade",
        "/v1/change-schedules",
        "/v1/change-schedules/preview",
        "/v1/change-schedules/{id}",
        "/v1/change-schedules/{id}/cancel",
        "/v1/change-schedules/{id}/manual-gates/{step_id}/approve",
        "/v1/change-schedules/{id}/manual-gates/{step_id}/reject",
        "/v1/change-schedules/{id}/pause",
        "/v1/change-schedules/{id}/reconcile",
        "/v1/change-schedules/{id}/resume",
        "/v1/change-windows",
        "/v1/conversations",
        "/v1/conversations/{id}",
        "/v1/conversations/{id}/cancel",
        "/v1/conversations/{id}/turns",
        "/v1/conversations/{id}/turns/stream",
        "/v1/event-entries",
        "/v1/events/stream",
        "/v1/evidence",
        "/v1/evidence/{id}",
        "/v1/evidence/{id}/content",
        "/v1/executions",
        "/v1/executions/{id}",
        "/v1/dr/action-items",
        "/v1/dr/action-items/{id}",
        "/v1/dr/exercises",
        "/v1/dr/exercises/{id}/checkpoints",
        "/v1/dr/exercises/{id}/findings",
        "/v1/dr/exercises/{id}/state",
        "/v1/dr/plans",
        "/v1/dr/plans/{id}/backup-assets",
        "/v1/finops/allocation-policy",
        "/v1/finops/budgets",
        "/v1/finops/budgets/evaluate",
        "/v1/finops/ledger",
        "/v1/finops/report",
        "/v1/fleet/assets",
        "/v1/fleet/clusters",
        "/v1/fleet/clusters/{id}/offboard",
        "/v1/fleet/compliance",
        "/v1/fleet/health",
        "/v1/fleet/inspections",
        "/v1/fleet/inspections/{id}/progress",
        "/v1/fleet/onboarding/assess",
        "/v1/fleet/onboarding/register",
        "/v1/fleet/overview",
        "/v1/fleet/quotas",
        "/v1/fleet/quotas/decisions",
        "/v1/fleet/quotas/evaluate",
        "/v1/fleet/regional-endpoints",
        "/v1/fleet/regional-route",
        "/v1/fleet/releases",
        "/v1/fleet/releases/{id}",
        "/v1/fleet/releases/{id}/batches/{sequence}/start",
        "/v1/fleet/releases/{id}/pause",
        "/v1/fleet/releases/{id}/readiness/start",
        "/v1/fleet/releases/{id}/report",
        "/v1/fleet/releases/{id}/resume",
        "/v1/fleet/releases/{id}/targets/{cluster_id}/outcome",
        "/v1/fleet/releases/{id}/targets/{cluster_id}/readiness",
        "/v1/governance/admissions/evaluate",
        "/v1/governance/artifacts",
        "/v1/governance/artifacts/{id}/versions",
        "/v1/governance/audit/export",
        "/v1/governance/compliance",
        "/v1/governance/versions/{id}/impacts",
        "/v1/governance/versions/{id}/transition",
        "/v1/incidents",
        "/v1/incidents/{id}",
        "/v1/incidents/{id}/diagnose",
        "/v1/incidents/{incident_id}/diagnosis-revisions/{revision_id}/confirm-execution",
        "/v1/incidents/{incident_id}/execution-preconditions",
        "/v1/incidents/{id}/notes",
        "/v1/incidents/{id}/operations",
        "/v1/incidents/{id}/postmortems",
        "/v1/incidents/{id}/timeline",
        "/v1/incidents/{id}/topology",
        "/v1/integrations/alertmanager/events",
        "/v1/integrations/approvals/external",
        "/v1/integrations/deliveries",
        "/v1/integrations/descriptors",
        "/v1/integrations/events",
        "/v1/integrations/targets",
        "/v1/integrations/targets/{id}",
        "/v1/integrations/targets/{id}/state",
        "/v1/integrations/webhook/test",
        "/v1/inspections",
        "/v1/inspections/{id}",
        "/v1/inspections/{id}/report",
        "/v1/inspections/{id}/run",
        "/v1/inventory/{id}",
        "/v1/investigations",
        "/v1/investigations/{id}",
        "/v1/investigations/{id}/promote",
        "/v1/knowledge",
        "/v1/knowledge/import",
        "/v1/knowledge/search",
        "/v1/knowledge/{id}",
        "/v1/knowledge/{id}/feedback",
        "/v1/knowledge/{id}/review",
        "/v1/message-journeys",
        "/v1/models/capabilities",
        "/v1/models/invocations",
        "/v1/models/profiles/lifecycle",
        "/v1/models/profiles/{id}/lifecycle",
        "/v1/models/profiles/{id}/rollback",
        "/v1/models/profiles/{id}/smoke",
        "/v1/models/status",
        "/v1/openapi.json",
        "/v1/operations/analytics",
        "/v1/operations/reports",
        "/v1/operations/shift-handoff",
        "/v1/postmortems/{id}",
        "/v1/postmortems/{id}/publish",
        "/v1/plans",
        "/v1/plans/{id}",
        "/v1/plans/{id}/critic",
        "/v1/plans/{id}/approve",
        "/v1/plans/{id}/reject",
        "/v1/recommendations",
        "/v1/recommendations/{id}/disposition",
        "/v1/releases",
        "/v1/releases/{id}",
        "/v1/releases/{id}/complete",
        "/v1/releases/{id}/manual-takeover",
        "/v1/releases/{id}/observations",
        "/v1/releases/{id}/pause",
        "/v1/releases/{id}/prepare",
        "/v1/releases/{id}/resume",
        "/v1/releases/{id}/rollback/complete",
        "/v1/releases/{id}/rollback/start",
        "/v1/releases/{id}/start",
        "/v1/releases/{id}/verification/start",
        "/v1/resource-quarantines",
        "/v1/resource-quarantines/{id}/clear",
        "/v1/runbooks",
        "/v1/runbooks/{id}/versions/{version}",
        "/v1/simulations",
        "/v1/topology",
        "/v1/topology/diff",
    ];

    #[test]
    fn checked_in_document_preserves_the_public_surface_through_phase_five() {
        let document = document();
        let paths = document["paths"].as_object().expect("OpenAPI paths must be an object");
        let actual = paths.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let required = REQUIRED_PUBLIC_PATHS.iter().copied().collect::<BTreeSet<_>>();

        assert_eq!(actual, required);
        assert!(actual.iter().all(|path| !path.starts_with("/internal/")));
    }

    #[test]
    fn every_operation_is_named_versioned_and_has_a_response_contract() {
        let document = document();
        let paths = document["paths"].as_object().expect("OpenAPI paths must be an object");
        let mut operation_ids = BTreeSet::new();

        for (path, path_item) in paths {
            let operations = path_item.as_object().expect("OpenAPI path item must be an object");
            for (method, operation) in operations {
                assert!(
                    matches!(method.as_str(), "get" | "post" | "patch"),
                    "unsupported method {method} at {path}"
                );
                let operation_id = operation["operationId"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| panic!("{method} {path} must have an operationId"));
                assert!(
                    operation_ids.insert(operation_id),
                    "duplicate operationId {operation_id} at {method} {path}"
                );
                assert!(
                    operation["responses"]
                        .as_object()
                        .is_some_and(|value| !value.is_empty()),
                    "{method} {path} must have a response contract"
                );
            }
        }
    }

    #[test]
    fn document_freezes_the_bounded_enterprise_mutation_boundary() {
        let document = document();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(
            document["x-rocketmq-effective-access"],
            "bounded_autonomy_with_supervised_r2"
        );
        assert_eq!(document["x-rocketmq-cluster-mutation-supported"], true);
        assert_eq!(document["x-rocketmq-bounded-r1-autonomy-supported"], true);
        assert_eq!(document["x-rocketmq-r2-supervision-required"], true);
        assert_eq!(document["x-rocketmq-r3-agent-reachable"], false);
        assert_eq!(document["x-rocketmq-unattended-arbitrary-mutation-supported"], false);
        assert_eq!(document["x-rocketmq-production-dr-cutover-supported"], false);
        assert_eq!(document["x-rocketmq-sre-phase"], 5);

        let encoded = serde_json::to_string(&document).expect("OpenAPI JSON");
        for forbidden in [
            "\"delete\":",
            "/apply",
            "/reset",
            "/truncate",
            "arbitrary_patch",
            "raw_shell",
        ] {
            assert!(!encoded.contains(forbidden), "forbidden OpenAPI surface: {forbidden}");
        }
    }

    #[test]
    fn document_contains_the_phase_five_enterprise_contracts() {
        let document = document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas must be an object");
        for required in [
            "FleetOverview",
            "ClusterRegistrationPage",
            "FleetAssetPage",
            "ComplianceFindingPage",
            "FleetInspectionPage",
            "FleetRelease",
            "FleetReleaseTarget",
            "FleetReleaseReport",
            "FleetReleasePage",
            "FleetReleaseView",
            "CreateFleetReleaseRequest",
            "DrPlanPage",
            "DrExercisePage",
            "DrActionItemPage",
            "GovernanceArtifactPage",
            "GovernanceVersionPage",
            "GovernanceComplianceReport",
            "FinOpsLedgerPage",
            "FinOpsBudgetPage",
            "FinOpsBudgetDecisionView",
            "FinOpsReport",
        ] {
            assert!(schemas.contains_key(required), "missing Phase 5 schema {required}");
        }

        assert_eq!(
            document["paths"]["/v1/fleet/overview"]["get"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:read"])
        );
        assert_eq!(
            document["paths"]["/v1/fleet/onboarding/register"]["post"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:fleet:manage"])
        );
        assert_eq!(
            document["paths"]["/v1/fleet/releases"]["post"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:fleet:manage"])
        );
        assert_eq!(
            document["paths"]["/v1/fleet/releases"]["get"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:read"])
        );
        assert_eq!(
            document["paths"]["/v1/fleet/releases"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/CreateFleetReleaseRequest"
        );
        assert_eq!(
            document["paths"]["/v1/dr/exercises"]["post"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:dr:manage"])
        );
        assert_eq!(
            document["paths"]["/v1/governance/versions/{id}/transition"]["post"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:governance:manage"])
        );
        assert_eq!(
            document["paths"]["/v1/finops/budgets/evaluate"]["post"]["security"][0]["oidc"],
            serde_json::json!(["rocketmq:finops:manage"])
        );
        assert_eq!(
            schemas["DrExerciseMode"]["enum"],
            serde_json::json!(["readiness", "tabletop", "supervised_test"])
        );
    }

    #[test]
    fn document_contains_the_phase_three_supervised_contracts() {
        let document = document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas must be an object");
        for required in [
            "ActionPlan",
            "ApprovalGrant",
            "ApprovalRecord",
            "PolicyDecision",
            "ExecutionRequest",
            "AuditEvent",
            "ResourceQuarantine",
            "CreatePlanRequest",
            "ConfirmDiagnosisExecutionRequest",
            "DiagnosisExecutionConfirmation",
            "ApprovalDecisionRequest",
            "CriticReview",
            "CriticReviewRequest",
            "CriticReviewResponse",
            "CriticGateState",
            "SubmitExecutionRequest",
            "RunbookDefinition",
            "RunbookStepPlanBinding",
            "ChangeWindow",
            "ChangeSchedule",
            "ChangeConflict",
            "CreateRunbookRequest",
            "CreateChangeWindowRequest",
            "CreateChangeScheduleRequest",
            "ChangeSchedulePreview",
            "ScheduleTransitionRequest",
            "ManualGateDecisionRequest",
            "IntegrationDescriptor",
            "IntegrationTarget",
            "IntegrationDelivery",
            "ExternalApprovalInput",
            "RegisterIntegrationTargetRequest",
            "SetIntegrationTargetStateRequest",
            "IntegrationTargetView",
            "IntegrationTargetPage",
            "IntegrationDeliveryPage",
            "ExternalApprovalRequest",
            "ExternalApprovalView",
            "ReleaseReadinessSnapshot",
            "ReleaseObservation",
            "ReleaseWorkflow",
            "ReleaseReport",
            "CreateReleaseRequest",
            "PrepareReleaseRequest",
            "ReleaseExecutionRequest",
            "RecordReleaseObservationRequest",
            "ReleaseTransitionRequest",
            "CompleteRollbackRequest",
            "ReleasePage",
            "ReleaseDetail",
            "ReleasePreparationView",
            "ReleaseExecutionView",
            "PrepareExecutionPreconditionRequest",
            "ExecutionPreconditionEvidenceView",
        ] {
            assert!(schemas.contains_key(required), "missing Phase 3 schema {required}");
        }
        assert_eq!(
            document["paths"]["/v1/incidents/{incident_id}/diagnosis-revisions/{revision_id}/confirm-execution"]["post"]
                ["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ConfirmDiagnosisExecutionRequest"
        );
        assert_eq!(
            document["paths"]["/v1/incidents/{incident_id}/execution-preconditions"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/PrepareExecutionPreconditionRequest"
        );
        assert_eq!(
            document["paths"]["/v1/plans/{id}/critic"]["post"]["requestBody"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/CriticReviewRequest"
        );
        assert!(
            schemas["ActionPlanView"]["required"]
                .as_array()
                .expect("ActionPlanView required fields")
                .iter()
                .any(|field| field == "precondition_hash")
        );
        assert_eq!(
            document["paths"]["/v1/plans/{id}/approve"]["post"]["requestBody"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/ApprovalDecisionRequest"
        );
        assert_eq!(
            document["paths"]["/v1/resource-quarantines/{id}/clear"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/ClearQuarantineRequest"
        );
        assert_eq!(
            document["paths"]["/v1/integrations/approvals/external"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/ExternalApprovalRequest"
        );
        assert_eq!(
            document["paths"]["/v1/releases"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/CreateReleaseRequest"
        );
        assert_eq!(
            document["paths"]["/v1/releases/{id}/rollback/complete"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/CompleteRollbackRequest"
        );
        assert!(
            document["paths"]["/v1/releases/{id}/verification/start"]["post"]
                .get("requestBody")
                .is_none()
        );
        assert!(
            document["paths"]["/v1/releases/{id}/complete"]["post"]
                .get("requestBody")
                .is_none()
        );
    }

    #[test]
    fn document_contains_the_phase_two_domain_contracts() {
        let document = document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas must be an object");
        for required in [
            "AlertEvent",
            "TopologySnapshot",
            "ClusterHealthReport",
            "FleetHealthReport",
            "CapacityForecast",
            "ClusterForecastReport",
            "BacklogEta",
            "WhatIfSimulationRequest",
            "WhatIfSimulation",
            "UpgradeReadinessReport",
            "DrReadinessReport",
            "NotificationDelivery",
            "PostmortemDraft",
            "PostmortemRevision",
            "ActionItem",
            "Phase2ContractManifest",
            "IncidentOperationRequest",
            "IncidentOperationResult",
            "IncidentOperationsState",
            "ShiftHandoffSummary",
            "OperationsReport",
        ] {
            assert!(schemas.contains_key(required), "missing Phase 2 schema {required}");
        }
        assert_eq!(
            schemas["EvidenceSnapshot"]["properties"]["exposure"]["$ref"],
            "#/components/schemas/EvidenceSnapshot__EvidenceExposure"
        );
        assert!(
            schemas["EvidenceSnapshot__EvidenceExposure"]
                .to_string()
                .contains("runtime_diagnostics")
        );
        assert!(
            schemas["EvidenceSnapshot__EvidenceExposure"]
                .to_string()
                .contains("execution_agent_api")
        );
    }

    #[test]
    fn autonomy_operations_surface_is_bounded_and_periodic() {
        let document = document();
        let outcome_parameters = document["paths"]["/v1/autonomy/outcomes"]["get"]["parameters"]
            .as_array()
            .expect("outcome query parameters");
        let limit = outcome_parameters
            .iter()
            .find(|parameter| parameter["name"] == "limit")
            .expect("bounded outcome limit");
        assert_eq!(limit["schema"]["maximum"], 200);

        let report_parameters = document["paths"]["/v1/autonomy/reports"]["get"]["parameters"]
            .as_array()
            .expect("report query parameters");
        let period = report_parameters
            .iter()
            .find(|parameter| parameter["name"] == "period")
            .expect("report period");
        assert_eq!(period["schema"]["enum"], serde_json::json!(["weekly", "monthly"]));
        assert_eq!(period["schema"]["default"], "weekly");

        let analytics = &document["paths"]["/v1/operations/analytics"]["get"];
        let analytics_parameters = analytics["parameters"].as_array().expect("analytics query parameters");
        for dimension in ["cluster_id", "scenario", "provider_family", "model_family", "action_id"] {
            assert!(
                analytics_parameters
                    .iter()
                    .any(|parameter| parameter["name"] == dimension),
                "missing operations analytics dimension {dimension}"
            );
        }
        assert_eq!(
            analytics["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/OperationsAnalyticsReport"
        );
        assert_eq!(
            document["components"]["schemas"]["OperationsAnalyticsReport"]["properties"]["schema_version"]["const"],
            "rocketmq-sre.operations-analytics.v1"
        );
    }

    #[test]
    fn alert_correlation_surface_has_typed_bounded_contracts() {
        let document = document();
        let paths = document["paths"].as_object().expect("OpenAPI paths must be an object");

        assert_eq!(
            paths["/v1/integrations/alertmanager/events"]["post"]["requestBody"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/AlertmanagerWebhookRequest"
        );
        assert_eq!(
            paths["/v1/integrations/alertmanager/events"]["post"]["x-max-body-bytes"],
            262_144
        );
        assert_eq!(
            paths["/v1/integrations/events"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/IntegrationEventRequest"
        );
        assert_eq!(
            paths["/v1/event-entries"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/UnifiedEventEntryRequest"
        );
        assert_eq!(
            paths["/v1/event-entries"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/UnifiedEventEntryResult"
        );
        assert_eq!(paths["/v1/event-entries"]["post"]["x-max-body-bytes"], 65_536);
        assert_eq!(
            paths["/v1/incidents/{id}/topology"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/IncidentTopologyView"
        );
        assert_eq!(
            paths["/v1/clusters/{id}/health"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ClusterHealthReport"
        );

        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas must be an object");
        assert_eq!(
            schemas["AlertmanagerWebhookRequest"]["properties"]["alerts"]["maxItems"],
            128
        );
        assert_eq!(
            schemas["UnifiedEventEntryRequest"]["oneOf"]
                .as_array()
                .expect("five unified event entry variants")
                .len(),
            5
        );
        assert_eq!(
            schemas["EventEntrySourceKind"]["enum"]
                .as_array()
                .expect("five source kinds")
                .len(),
            5
        );
        assert_eq!(schemas["IncidentTopologyView"]["properties"]["nodes"]["maxItems"], 128);
        assert_eq!(schemas["IncidentTopologyView"]["properties"]["edges"]["maxItems"], 256);
        assert!(
            !schemas["IntegrationAlertSource"]["enum"]
                .as_array()
                .expect("integration source enum")
                .iter()
                .any(|source| source == "alertmanager")
        );
    }
}
