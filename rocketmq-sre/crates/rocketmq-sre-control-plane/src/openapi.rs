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

const PHASE_THREE_OPENAPI: &str = include_str!("../../../openapi/rocketmq-sre-phase03.openapi.json");

static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    // Invariant: the checked-in document is parsed by this module's tests and
    // by the UI type-generation contract before it can be accepted.
    serde_json::from_str(PHASE_THREE_OPENAPI).expect("the checked-in Phase 03 OpenAPI document must be valid JSON")
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
        "/v1/events/stream",
        "/v1/evidence",
        "/v1/evidence/{id}",
        "/v1/evidence/{id}/content",
        "/v1/executions",
        "/v1/executions/{id}",
        "/v1/fleet/health",
        "/v1/incidents",
        "/v1/incidents/{id}",
        "/v1/incidents/{id}/diagnose",
        "/v1/incidents/{id}/notes",
        "/v1/incidents/{id}/operations",
        "/v1/incidents/{id}/postmortems",
        "/v1/incidents/{id}/timeline",
        "/v1/incidents/{id}/topology",
        "/v1/integrations/alertmanager/events",
        "/v1/integrations/events",
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
        "/v1/models/status",
        "/v1/openapi.json",
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
        "/v1/resource-quarantines",
        "/v1/resource-quarantines/{id}/clear",
        "/v1/runbooks",
        "/v1/runbooks/{id}/versions/{version}",
        "/v1/simulations",
        "/v1/topology",
        "/v1/topology/diff",
    ];

    #[test]
    fn checked_in_document_preserves_the_phase_one_public_surface() {
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
    fn document_freezes_the_human_approved_typed_mutation_boundary() {
        let document = document();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["x-rocketmq-effective-access"], "human_approved_supervised");
        assert_eq!(document["x-rocketmq-cluster-mutation-supported"], true);
        assert_eq!(document["x-rocketmq-unattended-mutation-supported"], false);
        assert_eq!(document["x-rocketmq-arbitrary-mutation-supported"], false);
        assert_eq!(document["x-rocketmq-sre-phase"], 3);

        let encoded = serde_json::to_string(&document).expect("OpenAPI JSON");
        for forbidden in ["\"delete\":", "/apply", "/reset", "/truncate", "arbitrary_patch"] {
            assert!(!encoded.contains(forbidden), "forbidden OpenAPI surface: {forbidden}");
        }
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
        ] {
            assert!(schemas.contains_key(required), "missing Phase 3 schema {required}");
        }
        assert_eq!(
            document["paths"]["/v1/plans/{id}/critic"]["post"]["requestBody"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/CriticReviewRequest"
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
