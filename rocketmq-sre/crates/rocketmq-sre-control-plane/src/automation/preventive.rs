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

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::AUTOMATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::AutomationBudget;
use rocketmq_sre_contracts::AutomationRunId;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_contracts::PreventiveAutomationRequest;
use rocketmq_sre_contracts::PreventiveAutomationRun;
use rocketmq_sre_contracts::PreventiveRiskFamily;
use rocketmq_sre_contracts::RecommendationId;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use super::model::CompletePreventiveRunRequest;
use super::model::PreventiveRunListQuery;
use super::model::PreventiveRunPage;
use super::model::PreventiveScheduleRequest;
use super::model::PreventiveScheduleView;
use super::service::automation_failure_code;
use super::service::require_automation_or_operator;
use super::service::require_automation_reader;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::autonomy::AutonomyService;
use crate::inspection::InspectionService;
use crate::workflow::InspectionCreateRequest;

const MAX_RUN_PAGE: u16 = 200;
const MAX_DUE_RUNS_PER_TICK: u32 = 4;
const SCHEDULED_OUTPUT_BYTES: u32 = 64 * 1_024;
const SCHEDULED_TIMEOUT_SECONDS: u16 = 120;

#[derive(Clone)]
pub(crate) struct PreventiveAutomationService {
    repository: PostgresRepository,
    inspections: InspectionService,
    autonomy: AutonomyService,
}

impl PreventiveAutomationService {
    pub(crate) const fn new(
        repository: PostgresRepository,
        inspections: InspectionService,
        autonomy: AutonomyService,
    ) -> Self {
        Self {
            repository,
            inspections,
            autonomy,
        }
    }

    pub(super) async fn submit(
        &self,
        auth: &AuthContext,
        request: &PreventiveAutomationRequest,
    ) -> Result<PreventiveAutomationRun, ControlPlaneError> {
        self.validate_request(auth, request)?;
        self.submit_with_inspection(auth, request, None).await
    }

    pub(super) async fn list(
        &self,
        auth: &AuthContext,
        query: &PreventiveRunListQuery,
    ) -> Result<PreventiveRunPage, ControlPlaneError> {
        require_automation_reader(auth)?;
        if let Some(cluster_id) = query.cluster_id
            && !auth.clusters.contains(&cluster_id)
        {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "preventive automation query is outside the authenticated cluster scope",
            ));
        }
        let limit = query.limit.clamp(1, MAX_RUN_PAGE);
        let mut items = self
            .repository
            .preventive_runs(auth.tenant_id, query, i64::from(limit) + 1)
            .await?;
        let truncated = items.len() > usize::from(limit);
        items.truncate(usize::from(limit));
        Ok(PreventiveRunPage {
            schema_version: AUTOMATION_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(super) async fn schedule(
        &self,
        auth: &AuthContext,
        request: &PreventiveScheduleRequest,
    ) -> Result<PreventiveScheduleView, ControlPlaneError> {
        require_automation_or_operator(auth)?;
        if !auth.clusters.contains(&request.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "preventive schedule is outside the authenticated cluster scope",
            ));
        }
        let template = preventive_template(request.risk_family);
        let inspection = self
            .inspections
            .create_persisted(
                auth,
                &InspectionCreateRequest {
                    cluster_id: request.cluster_id,
                    template,
                    schedule: Some(request.schedule.trim().to_owned()),
                },
                CorrelationId::new(),
            )
            .await?;
        Ok(PreventiveScheduleView {
            schema_version: AUTOMATION_SCHEMA_VERSION,
            inspection_run_id: inspection.run.id,
            cluster_id: request.cluster_id,
            risk_family: request.risk_family,
            template,
            schedule: request.schedule.trim().to_owned(),
        })
    }

    /// Runs due recurring Inspections. Recognized preventive templates receive
    /// a durable preventive run, risk assessment, and one-way freeze handling;
    /// all other templates retain the existing read-only scheduler behavior.
    pub(crate) async fn run_due(&self) {
        let due = match self.repository.due_inspections(MAX_DUE_RUNS_PER_TICK).await {
            Ok(due) => due,
            Err(error) => {
                tracing::warn!(error = %error, "preventive inspection schedule scan failed");
                return;
            }
        };
        for item in due {
            let auth = scheduler_auth(item.tenant_id, item.cluster_id);
            let inspection = match self.repository.inspection(&auth, item.id).await {
                Ok(inspection) => inspection,
                Err(error) => {
                    tracing::warn!(
                        inspection_id = %item.id,
                        error = %error,
                        "scheduled inspection metadata could not be loaded"
                    );
                    continue;
                }
            };
            let Some(risk_family) = preventive_risk(inspection.run.template) else {
                if let Err(error) = self.inspections.execute(&auth, item.id, CorrelationId::new()).await {
                    tracing::warn!(inspection_id = %item.id, error = %error, "scheduled inspection failed");
                }
                continue;
            };
            let request = scheduled_request(
                item.tenant_id,
                item.cluster_id,
                item.id,
                risk_family,
                inspection.run.created_at,
            );
            if let Err(error) = self.submit_with_inspection(&auth, &request, Some(item.id)).await {
                tracing::warn!(
                    inspection_id = %item.id,
                    risk_family = ?risk_family,
                    error = %error,
                    "scheduled preventive inspection failed"
                );
            }
        }
    }

    async fn submit_with_inspection(
        &self,
        auth: &AuthContext,
        request: &PreventiveAutomationRequest,
        inspection_run_id: Option<InspectionRunId>,
    ) -> Result<PreventiveAutomationRun, ControlPlaneError> {
        let run = self
            .repository
            .create_preventive_run(request, inspection_run_id)
            .await?;
        if run.status.is_terminal() {
            return Ok(run);
        }
        let (run, claimed) = self.repository.claim_preventive_run(auth.tenant_id, run.id).await?;
        if !claimed {
            return Ok(run);
        }
        let automation_auth = automation_auth(auth);
        let inspection_run_id = match run.inspection_run_id {
            Some(id) => id,
            None => {
                let created = self
                    .inspections
                    .create_persisted(
                        &automation_auth,
                        &InspectionCreateRequest {
                            cluster_id: request.cluster_id,
                            template: preventive_template(request.risk_family),
                            schedule: None,
                        },
                        request.correlation_id,
                    )
                    .await;
                match created {
                    Ok(view) => view.run.id,
                    Err(error) => {
                        return self
                            .complete_failed(
                                request,
                                None,
                                automation_failure_code(&error),
                                "Preventive inspection could not be created; no mutation was attempted",
                            )
                            .await;
                    }
                }
            }
        };
        let execution = tokio::time::timeout(
            Duration::from_secs(u64::from(request.budget.timeout_seconds)),
            self.execute_and_assess(&automation_auth, request, inspection_run_id),
        )
        .await;
        let outcome = match execution {
            Ok(outcome) => outcome,
            Err(_) => PreventiveDispatchOutcome::failed(
                inspection_run_id,
                "preventive_timeout",
                "Preventive inspection exceeded its configured timeout; no mutation was attempted",
            ),
        };
        let outcome = bounded_outcome(outcome, request.budget.max_output_bytes);
        self.repository
            .complete_preventive_run(
                request.tenant_id,
                request.id,
                &CompletePreventiveRunRequest {
                    status: outcome.status,
                    inspection_run_id: Some(outcome.inspection_run_id),
                    recommendation_ids: outcome.recommendation_ids,
                    freeze_id: outcome.freeze_id,
                    kill_switch_suggested: outcome.kill_switch_suggested,
                    result_code: outcome.result_code,
                    sanitized_summary: outcome.sanitized_summary,
                    completed_at: Utc::now(),
                },
            )
            .await
    }

    async fn execute_and_assess(
        &self,
        auth: &AuthContext,
        request: &PreventiveAutomationRequest,
        inspection_run_id: InspectionRunId,
    ) -> PreventiveDispatchOutcome {
        let view = match self
            .inspections
            .execute(auth, inspection_run_id, request.correlation_id)
            .await
        {
            Ok(view) => view,
            Err(error) => {
                return PreventiveDispatchOutcome::failed(
                    inspection_run_id,
                    automation_failure_code(&error),
                    "Preventive inspection failed without changing RocketMQ resources",
                );
            }
        };
        let recommendation_ids = view
            .recommendations
            .iter()
            .map(|recommendation| recommendation.id)
            .take(256)
            .collect::<Vec<_>>();
        let critical_count = view
            .recommendations
            .iter()
            .filter(|recommendation| recommendation.severity.eq_ignore_ascii_case("critical"))
            .count();
        if critical_count == 0 {
            let result_code = if view.run.partial {
                "preventive_inspection_partial"
            } else {
                "preventive_inspection_completed"
            };
            return PreventiveDispatchOutcome {
                status: AutomationRunStatus::Succeeded,
                inspection_run_id,
                recommendation_ids,
                freeze_id: None,
                kill_switch_suggested: false,
                result_code: result_code.to_owned(),
                sanitized_summary: format!(
                    "{:?} risk inspection completed with {} recommendations; human review remains required",
                    request.risk_family,
                    view.recommendations.len()
                ),
            };
        }
        let reason = format!(
            "Preventive {:?} inspection {} found {} critical recommendations",
            request.risk_family, inspection_run_id, critical_count
        );
        match self
            .autonomy
            .set_preventive_freeze(auth, request.cluster_id, &reason)
            .await
        {
            Ok(freeze) => PreventiveDispatchOutcome {
                status: AutomationRunStatus::Succeeded,
                inspection_run_id,
                recommendation_ids,
                freeze_id: Some(freeze.id),
                kill_switch_suggested: true,
                result_code: "critical_risk_frozen".to_owned(),
                sanitized_summary: format!(
                    "{:?} inspection found {} critical recommendations; cluster autonomy was frozen and a kill switch \
                     review is suggested",
                    request.risk_family, critical_count
                ),
            },
            Err(_) => PreventiveDispatchOutcome {
                status: AutomationRunStatus::Failed,
                inspection_run_id,
                recommendation_ids,
                freeze_id: None,
                kill_switch_suggested: true,
                result_code: "preventive_freeze_failed".to_owned(),
                sanitized_summary: "Critical preventive findings were recorded, but the safety freeze could not be \
                                    persisted; immediate human review is required"
                    .to_owned(),
            },
        }
    }

    async fn complete_failed(
        &self,
        request: &PreventiveAutomationRequest,
        inspection_run_id: Option<InspectionRunId>,
        result_code: &str,
        summary: &str,
    ) -> Result<PreventiveAutomationRun, ControlPlaneError> {
        self.repository
            .complete_preventive_run(
                request.tenant_id,
                request.id,
                &CompletePreventiveRunRequest {
                    status: AutomationRunStatus::Failed,
                    inspection_run_id,
                    recommendation_ids: Vec::new(),
                    freeze_id: None,
                    kill_switch_suggested: false,
                    result_code: result_code.to_owned(),
                    sanitized_summary: summary.to_owned(),
                    completed_at: Utc::now(),
                },
            )
            .await
    }

    fn validate_request(
        &self,
        auth: &AuthContext,
        request: &PreventiveAutomationRequest,
    ) -> Result<(), ControlPlaneError> {
        require_automation_or_operator(auth)?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_preventive_request", error.to_string()))?;
        if request.tenant_id != auth.tenant_id || request.requested_by != auth.subject {
            return Err(ControlPlaneError::forbidden(
                "preventive_identity_mismatch",
                "preventive request tenant and requester must match the authenticated identity",
            ));
        }
        if !auth.clusters.contains(&request.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "preventive request is outside the authenticated cluster scope",
            ));
        }
        if request.budget.max_model_calls != 0 {
            return Err(ControlPlaneError::validation(
                "model_budget_not_allowed",
                "preventive inspections are deterministic and cannot allocate model calls",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PreventiveDispatchOutcome {
    status: AutomationRunStatus,
    inspection_run_id: InspectionRunId,
    recommendation_ids: Vec<RecommendationId>,
    freeze_id: Option<Uuid>,
    kill_switch_suggested: bool,
    result_code: String,
    sanitized_summary: String,
}

impl PreventiveDispatchOutcome {
    fn failed(inspection_run_id: InspectionRunId, result_code: &str, summary: &str) -> Self {
        Self {
            status: AutomationRunStatus::Failed,
            inspection_run_id,
            recommendation_ids: Vec::new(),
            freeze_id: None,
            kill_switch_suggested: false,
            result_code: result_code.to_owned(),
            sanitized_summary: summary.to_owned(),
        }
    }
}

fn bounded_outcome(mut outcome: PreventiveDispatchOutcome, maximum_bytes: u32) -> PreventiveDispatchOutcome {
    while outcome_size(&outcome) > maximum_bytes as usize && outcome.recommendation_ids.pop().is_some() {}
    if outcome_size(&outcome) <= maximum_bytes as usize {
        return outcome;
    }
    outcome.status = AutomationRunStatus::Failed;
    outcome.result_code = "output_too_large".to_owned();
    outcome.sanitized_summary =
        "Preventive output exceeded its byte budget; inspect the persisted Inspection directly".to_owned();
    outcome.recommendation_ids.clear();
    outcome
}

fn outcome_size(outcome: &PreventiveDispatchOutcome) -> usize {
    serde_json::to_vec(&(
        outcome.status,
        outcome.inspection_run_id,
        &outcome.recommendation_ids,
        outcome.freeze_id,
        outcome.kill_switch_suggested,
        &outcome.result_code,
        &outcome.sanitized_summary,
    ))
    .map_or(usize::MAX, |encoded| encoded.len())
}

fn automation_auth(auth: &AuthContext) -> AuthContext {
    let mut auth = auth.clone();
    auth.roles = BTreeSet::from(["automation_service".to_owned(), "diagnose".to_owned()]);
    auth
}

fn scheduler_auth(
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "rocketmq-sre-preventive-scheduler".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["automation_service".to_owned(), "diagnose".to_owned()]),
    }
}

fn scheduled_request(
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    inspection_run_id: InspectionRunId,
    risk_family: PreventiveRiskFamily,
    requested_at: chrono::DateTime<Utc>,
) -> PreventiveAutomationRequest {
    let run_uuid = deterministic_uuid(&format!("preventive-run:{inspection_run_id}"));
    let correlation_uuid = deterministic_uuid(&format!("preventive-correlation:{inspection_run_id}"));
    PreventiveAutomationRequest {
        schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
        id: AutomationRunId::from_uuid(run_uuid),
        tenant_id,
        cluster_id,
        correlation_id: CorrelationId::from_uuid(correlation_uuid),
        risk_family,
        idempotency_key: format!("preventive:schedule:{inspection_run_id}"),
        budget: AutomationBudget {
            max_model_calls: 0,
            max_output_bytes: SCHEDULED_OUTPUT_BYTES,
            timeout_seconds: SCHEDULED_TIMEOUT_SECONDS,
        },
        requested_by: "rocketmq-sre-preventive-scheduler".to_owned(),
        requested_at,
    }
}

pub(super) const fn preventive_template(risk_family: PreventiveRiskFamily) -> InspectionTemplate {
    match risk_family {
        PreventiveRiskFamily::Capacity => InspectionTemplate::FullCluster,
        PreventiveRiskFamily::Certificate => InspectionTemplate::Security,
        PreventiveRiskFamily::Config => InspectionTemplate::ClusterHealth,
        PreventiveRiskFamily::Route => InspectionTemplate::RoutingProxy,
        PreventiveRiskFamily::Ha => InspectionTemplate::StoreHa,
        PreventiveRiskFamily::Upgrade => InspectionTemplate::Upgrade,
    }
}

const fn preventive_risk(template: InspectionTemplate) -> Option<PreventiveRiskFamily> {
    match template {
        InspectionTemplate::FullCluster => Some(PreventiveRiskFamily::Capacity),
        InspectionTemplate::Security => Some(PreventiveRiskFamily::Certificate),
        InspectionTemplate::ClusterHealth => Some(PreventiveRiskFamily::Config),
        InspectionTemplate::RoutingProxy => Some(PreventiveRiskFamily::Route),
        InspectionTemplate::StoreHa => Some(PreventiveRiskFamily::Ha),
        InspectionTemplate::Upgrade => Some(PreventiveRiskFamily::Upgrade),
        InspectionTemplate::Consumer
        | InspectionTemplate::Broker
        | InspectionTemplate::Telemetry
        | InspectionTemplate::ProducerConsumer
        | InspectionTemplate::DisasterRecovery => None,
    }
}

fn deterministic_uuid(material: &str) -> Uuid {
    let digest = Sha256::digest(material.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_preventive_family_has_a_distinct_read_only_template() {
        let mappings = [
            PreventiveRiskFamily::Capacity,
            PreventiveRiskFamily::Certificate,
            PreventiveRiskFamily::Config,
            PreventiveRiskFamily::Route,
            PreventiveRiskFamily::Ha,
            PreventiveRiskFamily::Upgrade,
        ]
        .map(|risk| (risk, preventive_template(risk)));

        for (index, (risk, template)) in mappings.iter().copied().enumerate() {
            assert!(
                mappings
                    .iter()
                    .enumerate()
                    .all(|(other_index, (_, other))| index == other_index || template != *other)
            );
            assert_eq!(preventive_risk(template), Some(risk));
        }
    }

    #[test]
    fn scheduled_request_identity_is_stable_and_has_zero_model_budget() {
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = rocketmq_sre_contracts::ClusterId::new();
        let inspection_run_id = InspectionRunId::new();
        let observed_at = Utc::now();
        let first = scheduled_request(
            tenant_id,
            cluster_id,
            inspection_run_id,
            PreventiveRiskFamily::Route,
            observed_at,
        );
        let second = scheduled_request(
            tenant_id,
            cluster_id,
            inspection_run_id,
            PreventiveRiskFamily::Route,
            observed_at,
        );

        assert_eq!(first, second);
        assert_eq!(first.budget.max_model_calls, 0);
        assert!(first.validate().is_ok());
    }
}
