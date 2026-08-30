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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::FleetAccessProfile;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetInspectionRun;
use rocketmq_sre_contracts::FleetInspectionRunId;
use rocketmq_sre_contracts::FleetOnboardingAssessment;
use rocketmq_sre_contracts::FleetOnboardingAssessmentId;
use rocketmq_sre_contracts::FleetQuotaDecisionId;
use rocketmq_sre_contracts::FleetQuotaDecisionRecord;
use rocketmq_sre_contracts::FleetQuotaResource;
use rocketmq_sre_contracts::FleetQuotaWorkKind;
use rocketmq_sre_contracts::RegionalEndpoint;
use rocketmq_sre_contracts::RegionalEndpointHealth;
use rocketmq_sre_contracts::is_sha256_digest;
use rocketmq_sre_core::FleetQuotaEvaluator;
use rocketmq_sre_core::FleetWorkPriority;
use rocketmq_sre_core::QuotaDecisionReason;
use rocketmq_sre_core::QuotaRequest;
use rocketmq_sre_core::QuotaResource;
use rocketmq_sre_core::residency_allows_route;

use super::model::ClusterRegistrationPage;
use super::model::ComplianceEvaluationView;
use super::model::ComplianceFindingPage;
use super::model::ComplianceFindingQuery;
use super::model::CreateFleetInspectionRequest;
use super::model::CreateQuotaPolicyRequest;
use super::model::EvaluateComplianceRequest;
use super::model::EvaluateFleetQuotaRequest;
use super::model::FLEET_API_SCHEMA_VERSION;
use super::model::FleetAssetPage;
use super::model::FleetInspectionPage;
use super::model::FleetOffboardRequest;
use super::model::FleetOnboardingRequest;
use super::model::FleetOnboardingView;
use super::model::FleetOverview;
use super::model::FleetQuotaDecisionPage;
use super::model::FleetQuotaDecisionQuery;
use super::model::FleetQuotaDecisionView;
use super::model::FleetScopeQuery;
use super::model::QuotaPolicyView;
use super::model::RegionalEndpointPage;
use super::model::RegionalEndpointQuery;
use super::model::RegionalRouteDecision;
use super::model::RegionalRouteMode;
use super::model::RegionalRouteRequest;
use super::model::RegisterRegionalEndpointRequest;
use super::model::UpdateFleetInspectionRequest;
use super::model::UpsertFleetAssetRequest;
use super::model::bounded_limit;
use super::repository::FleetRepository;
use crate::ControlPlaneError;
use crate::DataSourceAvailability;
use crate::MCP_BUSINESS_SCHEMA;
use crate::MCP_PROTOCOL_VERSION;
use crate::OffboardRequest;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::repository::ClusterRepository;

const ENDPOINT_HEARTBEAT_MAX_AGE_SECONDS: i64 = 120;
const MAX_INSPECTION_CLUSTERS: usize = 100;
const MAX_INSPECTION_CLUSTERS_U32: u32 = 100;
const MAX_INSPECTION_PACKS: usize = 32;
const MAX_DATABASE_I32: u32 = 2_147_483_647;
const MAX_DATABASE_I64: u64 = 9_223_372_036_854_775_807;

#[derive(Clone)]
pub(crate) struct FleetService {
    pub(super) repository: FleetRepository,
    cluster_repository: PostgresRepository,
}

impl FleetService {
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self {
            repository: FleetRepository::new(repository.pool.clone()),
            cluster_repository: repository,
        }
    }

    pub(crate) async fn assess_onboarding(
        &self,
        auth: &AuthContext,
        request: &FleetOnboardingRequest,
    ) -> Result<FleetOnboardingView, ControlPlaneError> {
        self.onboarding_assessment(auth, request, false).await
    }

    pub(crate) async fn onboard_cluster(
        &self,
        auth: &AuthContext,
        request: &FleetOnboardingRequest,
    ) -> Result<FleetOnboardingView, ControlPlaneError> {
        self.onboarding_assessment(auth, request, true).await
    }

    async fn onboarding_assessment(
        &self,
        auth: &AuthContext,
        request: &FleetOnboardingRequest,
        register: bool,
    ) -> Result<FleetOnboardingView, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, request.cluster_id)?;
        validate_onboarding(request)?;
        if !self
            .repository
            .onboarding_scope_exists(auth.tenant_id, request.fleet_id, request.region_id)
            .await?
        {
            return Err(scope_mismatch());
        }
        let cluster = self.cluster_repository.get(request.cluster_id).await?;
        if cluster.tenant_id != auth.tenant_id.to_string() || cluster.owner != request.owner {
            return Err(scope_mismatch());
        }
        if cluster.state.is_terminal() {
            return Err(ControlPlaneError::conflict_code(
                "cluster_offboarded",
                "offboarded clusters cannot re-enter Fleet onboarding",
            ));
        }

        let mut missing_capabilities = request.required_capabilities.clone();
        let mut signal_gaps = request.required_data_sources.clone();
        let mut incompatibilities = std::collections::BTreeSet::new();
        let mut schema_compatible = false;
        match self.cluster_repository.capability(request.cluster_id).await {
            Ok(capability) => {
                schema_compatible = capability.protocol_version == MCP_PROTOCOL_VERSION
                    && capability.schema_version == MCP_BUSINESS_SCHEMA
                    && !capability.mutation_supported;
                if capability.protocol_version != MCP_PROTOCOL_VERSION {
                    incompatibilities.insert("mcp_protocol_incompatible".to_owned());
                }
                if capability.schema_version != MCP_BUSINESS_SCHEMA {
                    incompatibilities.insert("business_schema_incompatible".to_owned());
                }
                if capability.mutation_supported {
                    incompatibilities.insert("mutation_capability_exposed".to_owned());
                }
                let tools = capability_tool_names(&capability.manifest);
                missing_capabilities.retain(|capability| !tools.contains(capability));
                let queryable_sources = capability
                    .data_sources
                    .iter()
                    .filter(|source| source.availability == DataSourceAvailability::Queryable)
                    .map(|source| source.id.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                signal_gaps.retain(|source| !queryable_sources.contains(source.as_str()));
            }
            Err(ControlPlaneError::NotFound) => {
                incompatibilities.insert("capability_manifest_missing".to_owned());
            }
            Err(error) => return Err(error),
        }
        if !request.connector_tls_verified {
            incompatibilities.insert("connector_tls_unverified".to_owned());
        }
        let excessive_scopes = request
            .oauth_scopes
            .iter()
            .filter(|scope| !is_allowed_onboarding_scope(scope))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let eligible = schema_compatible
            && request.connector_tls_verified
            && missing_capabilities.is_empty()
            && excessive_scopes.is_empty()
            && incompatibilities.is_empty();
        let effective_access = if signal_gaps.is_empty() {
            request.requested_access
        } else {
            FleetAccessProfile::ReadOnly
        };
        let assessment = FleetOnboardingAssessment {
            id: FleetOnboardingAssessmentId::new(),
            fleet_id: request.fleet_id,
            tenant_id: auth.tenant_id,
            region_id: request.region_id,
            cluster_id: request.cluster_id,
            requested_access: request.requested_access,
            effective_access,
            connector_tls_verified: request.connector_tls_verified,
            schema_compatible,
            missing_capabilities,
            signal_gaps,
            excessive_scopes,
            incompatibilities,
            eligible,
            observed_at: Utc::now(),
        };
        self.repository.store_onboarding_assessment(&assessment).await?;
        let registration = if register && assessment.eligible {
            Some(
                self.repository
                    .upsert_cluster_registration(
                        auth.tenant_id,
                        request,
                        assessment.effective_access == FleetAccessProfile::ReadOnly
                            && request.requested_access != FleetAccessProfile::ReadOnly,
                    )
                    .await?,
            )
        } else {
            None
        };
        Ok(FleetOnboardingView {
            schema_version: FLEET_API_SCHEMA_VERSION,
            assessment,
            registration,
        })
    }

    pub(crate) async fn offboard_cluster(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        request: &FleetOffboardRequest,
    ) -> Result<ClusterRegistration, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, cluster_id)?;
        validate_bounded(&request.reason, "offboarding reason", 2_048)?;
        self.repository.begin_offboarding(auth.tenant_id, cluster_id).await?;
        self.cluster_repository
            .offboard(
                cluster_id,
                &OffboardRequest {
                    actor_subject: auth.subject.clone(),
                    correlation_id: request.correlation_id,
                    reason: Some(request.reason.clone()),
                },
            )
            .await?;
        self.repository.retire_registration(auth.tenant_id, cluster_id).await
    }

    pub(crate) async fn evaluate_quota(
        &self,
        auth: &AuthContext,
        request: &EvaluateFleetQuotaRequest,
    ) -> Result<FleetQuotaDecisionView, ControlPlaneError> {
        require_operator(auth)?;
        if request.amount == 0 || request.amount > MAX_DATABASE_I64 {
            return Err(invalid_request("quota amount must fit the supported storage range"));
        }
        if let Some(cluster_id) = request.cluster_id {
            authorize_cluster(auth, cluster_id)?;
        }
        let decision = self
            .evaluate_and_record_quota(
                auth.tenant_id,
                request.cluster_id,
                request.work_kind,
                request.resource,
                request.amount,
            )
            .await?;
        Ok(FleetQuotaDecisionView {
            schema_version: FLEET_API_SCHEMA_VERSION,
            decision,
        })
    }

    pub(crate) async fn quota_decisions(
        &self,
        auth: &AuthContext,
        query: &FleetQuotaDecisionQuery,
    ) -> Result<FleetQuotaDecisionPage, ControlPlaneError> {
        require_read_role(auth)?;
        if let Some(cluster_id) = query.cluster_id {
            authorize_cluster(auth, cluster_id)?;
        }
        let (items, truncated) = self.repository.quota_decisions(auth.tenant_id, query).await?;
        Ok(FleetQuotaDecisionPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn overview(&self, auth: &AuthContext) -> Result<FleetOverview, ControlPlaneError> {
        require_read_role(auth)?;
        let allowed = allowed_clusters(auth);
        let (fleet, tenant, regions) = self.repository.tenant_scope(auth.tenant_id, &allowed).await?;
        let (registrations, _) = self
            .repository
            .cluster_registrations(
                auth.tenant_id,
                &allowed,
                &FleetScopeQuery {
                    limit: 200,
                    ..FleetScopeQuery::default()
                },
            )
            .await?;
        Ok(FleetOverview {
            schema_version: FLEET_API_SCHEMA_VERSION,
            fleet,
            tenant,
            regions,
            registrations,
            observed_at: Utc::now(),
        })
    }

    pub(crate) async fn registrations(
        &self,
        auth: &AuthContext,
        query: &FleetScopeQuery,
    ) -> Result<ClusterRegistrationPage, ControlPlaneError> {
        require_read_role(auth)?;
        validate_filter(query.owner.as_deref(), "owner")?;
        validate_filter(query.component_version.as_deref(), "component version")?;
        validate_filter(query.health.as_deref(), "health")?;
        let allowed = allowed_clusters(auth);
        let (items, total) = self
            .repository
            .cluster_registrations(auth.tenant_id, &allowed, query)
            .await?;
        Ok(ClusterRegistrationPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            total,
            limit: bounded_limit(query.limit),
            offset: query.offset,
        })
    }

    pub(crate) async fn create_quota_policy(
        &self,
        auth: &AuthContext,
        request: &CreateQuotaPolicyRequest,
    ) -> Result<QuotaPolicyView, ControlPlaneError> {
        require_operator(auth)?;
        validate_owner(&request.owner)?;
        validate_quota_limits(request)?;
        let (fleet, _, _) = self
            .repository
            .tenant_scope(auth.tenant_id, &allowed_clusters(auth))
            .await?;
        if request.fleet_id != fleet.id {
            return Err(scope_mismatch());
        }
        if let Some(cluster_id) = request.cluster_id {
            authorize_cluster(auth, cluster_id)?;
            let registration = self.repository.cluster_registration(auth.tenant_id, cluster_id).await?;
            if request.region_id != Some(registration.region_id) || request.fleet_id != registration.fleet_id {
                return Err(scope_mismatch());
            }
        }
        let policy = self.repository.create_quota_policy(auth.tenant_id, request).await?;
        let (_, usage) = self.repository.quota_policy(auth.tenant_id, request.cluster_id).await?;
        Ok(QuotaPolicyView {
            schema_version: FLEET_API_SCHEMA_VERSION,
            policy,
            usage,
        })
    }

    pub(crate) async fn quota_policy(
        &self,
        auth: &AuthContext,
        cluster_id: Option<ClusterId>,
    ) -> Result<QuotaPolicyView, ControlPlaneError> {
        require_read_role(auth)?;
        if let Some(cluster_id) = cluster_id {
            authorize_cluster(auth, cluster_id)?;
        }
        let (policy, usage) = self.repository.quota_policy(auth.tenant_id, cluster_id).await?;
        Ok(QuotaPolicyView {
            schema_version: FLEET_API_SCHEMA_VERSION,
            policy,
            usage,
        })
    }

    pub(crate) async fn register_endpoint(
        &self,
        auth: &AuthContext,
        request: &RegisterRegionalEndpointRequest,
    ) -> Result<RegionalEndpoint, ControlPlaneError> {
        require_endpoint_role(auth)?;
        validate_endpoint(&request.endpoint)?;
        if request.endpoint.tenant_id != auth.tenant_id {
            return Err(scope_mismatch());
        }
        if let Some(cluster_id) = request.endpoint.cluster_id {
            authorize_cluster(auth, cluster_id)?;
            let registration = self.repository.cluster_registration(auth.tenant_id, cluster_id).await?;
            if registration.fleet_id != request.endpoint.fleet_id
                || registration.region_id != request.endpoint.region_id
            {
                return Err(scope_mismatch());
            }
        } else {
            let (fleet, _, regions) = self
                .repository
                .tenant_scope(auth.tenant_id, &allowed_clusters(auth))
                .await?;
            if request.endpoint.fleet_id != fleet.id
                || !regions.iter().any(|region| region.id == request.endpoint.region_id)
            {
                return Err(scope_mismatch());
            }
        }
        self.repository.upsert_regional_endpoint(&request.endpoint).await
    }

    pub(crate) async fn endpoints(
        &self,
        auth: &AuthContext,
        query: &RegionalEndpointQuery,
    ) -> Result<RegionalEndpointPage, ControlPlaneError> {
        require_read_role(auth)?;
        if let Some(cluster_id) = query.cluster_id {
            authorize_cluster(auth, cluster_id)?;
        }
        let (items, truncated) = self
            .repository
            .regional_endpoints(auth.tenant_id, &allowed_clusters(auth), query)
            .await?;
        Ok(RegionalEndpointPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn route(
        &self,
        auth: &AuthContext,
        request: &RegionalRouteRequest,
    ) -> Result<RegionalRouteDecision, ControlPlaneError> {
        require_read_role(auth)?;
        authorize_cluster(auth, request.cluster_id)?;
        validate_digest(&request.required_schema_digest, "required schema digest")?;
        validate_version(&request.current_protocol_version, "current protocol version")?;
        validate_version(&request.previous_protocol_version, "previous protocol version")?;
        let registration = self
            .repository
            .cluster_registration(auth.tenant_id, request.cluster_id)
            .await?;
        if registration.region_id != request.source_region_id {
            return Err(scope_mismatch());
        }
        let page = self
            .endpoints(
                auth,
                &RegionalEndpointQuery {
                    region_id: None,
                    cluster_id: Some(request.cluster_id),
                    kind: Some(request.endpoint_kind),
                    limit: 200,
                },
            )
            .await?;
        let now = Utc::now();
        let mut candidates = page
            .items
            .into_iter()
            .filter(|endpoint| {
                endpoint.capacity > 0
                    && residency_allows_route(request.residency, request.source_region_id, endpoint.region_id)
                    && now.signed_duration_since(endpoint.last_heartbeat_at)
                        <= Duration::seconds(ENDPOINT_HEARTBEAT_MAX_AGE_SECONDS)
                    && matches!(
                        endpoint.health,
                        RegionalEndpointHealth::Healthy | RegionalEndpointHealth::Degraded
                    )
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(denied_route("regional_endpoint_unavailable", now));
        }
        candidates.retain(|endpoint| endpoint.schema_digest == request.required_schema_digest);
        if candidates.is_empty() {
            return Ok(denied_route("schema_digest_mismatch", now));
        }
        candidates.retain(|endpoint| {
            request
                .required_capabilities
                .iter()
                .all(|capability| endpoint.capabilities.contains(capability))
        });
        if candidates.is_empty() {
            return Ok(denied_route("capability_mismatch", now));
        }
        candidates.retain(|endpoint| {
            endpoint.protocol_version == request.current_protocol_version
                || endpoint.protocol_version == request.previous_protocol_version
        });
        if candidates.is_empty() {
            return Ok(denied_route("protocol_incompatible", now));
        }
        candidates.sort_by_key(|endpoint| {
            (
                endpoint.protocol_version != request.current_protocol_version,
                endpoint.health != RegionalEndpointHealth::Healthy,
                std::cmp::Reverse(endpoint.capacity),
                endpoint.id.clone(),
            )
        });
        let Some(endpoint) = candidates.into_iter().next() else {
            return Ok(denied_route("regional_endpoint_unavailable", now));
        };
        let mut reasons = Vec::new();
        let mode = if endpoint.protocol_version == request.current_protocol_version {
            RegionalRouteMode::Full
        } else {
            reasons.push("protocol_n_minus_one_read_only".to_owned());
            RegionalRouteMode::ReadOnlyDegraded
        };
        if endpoint.health == RegionalEndpointHealth::Degraded {
            reasons.push("endpoint_degraded".to_owned());
        }
        Ok(RegionalRouteDecision {
            schema_version: FLEET_API_SCHEMA_VERSION,
            mode,
            endpoint: Some(endpoint),
            reason_codes: reasons,
            observed_at: now,
        })
    }

    pub(crate) async fn upsert_asset(
        &self,
        auth: &AuthContext,
        request: &UpsertFleetAssetRequest,
    ) -> Result<FleetAssetIndex, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, request.asset.cluster_id)?;
        validate_asset(&request.asset)?;
        let registration = self
            .repository
            .cluster_registration(auth.tenant_id, request.asset.cluster_id)
            .await?;
        if request.asset.tenant_id != auth.tenant_id
            || request.asset.fleet_id != registration.fleet_id
            || request.asset.region_id != registration.region_id
        {
            return Err(scope_mismatch());
        }
        self.repository.upsert_asset(&request.asset).await
    }

    pub(crate) async fn assets(
        &self,
        auth: &AuthContext,
        query: &FleetScopeQuery,
    ) -> Result<FleetAssetPage, ControlPlaneError> {
        require_read_role(auth)?;
        let (items, total, health_distribution, worst_health) = self
            .repository
            .assets(auth.tenant_id, &allowed_clusters(auth), query)
            .await?;
        Ok(FleetAssetPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            total,
            limit: bounded_limit(query.limit),
            offset: query.offset,
            health_distribution,
            worst_health,
        })
    }

    pub(crate) async fn evaluate_compliance(
        &self,
        auth: &AuthContext,
        request: &EvaluateComplianceRequest,
    ) -> Result<ComplianceEvaluationView, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, request.cluster_id)?;
        validate_compliance(request)?;
        let registration = self
            .repository
            .cluster_registration(auth.tenant_id, request.cluster_id)
            .await?;
        if request.fleet_id != registration.fleet_id || request.region_id != registration.region_id {
            return Err(scope_mismatch());
        }
        if request.expected_digest == request.live_digest {
            let resolved = self
                .repository
                .resolve_matching_findings(auth.tenant_id, request.cluster_id, request.category.trim())
                .await?;
            return Ok(ComplianceEvaluationView {
                schema_version: FLEET_API_SCHEMA_VERSION,
                compliant: true,
                finding: None,
                resolved_findings: resolved,
            });
        }
        let finding = self.repository.upsert_finding(auth.tenant_id, request).await?;
        Ok(ComplianceEvaluationView {
            schema_version: FLEET_API_SCHEMA_VERSION,
            compliant: false,
            finding: Some(finding),
            resolved_findings: 0,
        })
    }

    pub(crate) async fn findings(
        &self,
        auth: &AuthContext,
        query: &ComplianceFindingQuery,
    ) -> Result<ComplianceFindingPage, ControlPlaneError> {
        require_read_role(auth)?;
        if let Some(cluster_id) = query.cluster_id {
            authorize_cluster(auth, cluster_id)?;
        }
        let (items, total) = self
            .repository
            .findings(auth.tenant_id, &allowed_clusters(auth), query)
            .await?;
        Ok(ComplianceFindingPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            total,
            limit: bounded_limit(query.limit),
            offset: query.offset,
        })
    }

    pub(crate) async fn create_inspection(
        &self,
        auth: &AuthContext,
        request: &CreateFleetInspectionRequest,
    ) -> Result<FleetInspectionRun, ControlPlaneError> {
        require_operator(auth)?;
        validate_inspection(request)?;
        for cluster_id in &request.cluster_ids {
            authorize_cluster(auth, *cluster_id)?;
            let registration = self
                .repository
                .cluster_registration(auth.tenant_id, *cluster_id)
                .await?;
            if registration.fleet_id != request.fleet_id || !request.region_ids.contains(&registration.region_id) {
                return Err(scope_mismatch());
            }
        }
        let decision = self
            .evaluate_and_record_quota(
                auth.tenant_id,
                None,
                FleetQuotaWorkKind::Inspection,
                FleetQuotaResource::ConcurrentInspection,
                1,
            )
            .await?;
        if !decision.allowed {
            return Err(ControlPlaneError::conflict_code(
                "quota_exhausted",
                "Fleet inspection quota is exhausted",
            ));
        }
        let inspection = self.repository.create_inspection(auth.tenant_id, request).await?;
        let (policy, _) = self.repository.quota_policy(auth.tenant_id, None).await?;
        self.repository.record_quota_usage(&policy, "inspection", 1).await?;
        Ok(inspection)
    }

    pub(crate) async fn update_inspection(
        &self,
        auth: &AuthContext,
        id: FleetInspectionRunId,
        request: &UpdateFleetInspectionRequest,
    ) -> Result<FleetInspectionRun, ControlPlaneError> {
        require_operator(auth)?;
        if request.completed_clusters.saturating_add(request.failed_clusters) > MAX_INSPECTION_CLUSTERS_U32 {
            return Err(invalid_request("inspection result exceeds the bounded cluster count"));
        }
        self.repository.update_inspection(auth.tenant_id, id, request).await
    }

    pub(crate) async fn inspections(
        &self,
        auth: &AuthContext,
        limit: u16,
    ) -> Result<FleetInspectionPage, ControlPlaneError> {
        require_read_role(auth)?;
        let (items, truncated) = self.repository.inspections(auth.tenant_id, limit).await?;
        Ok(FleetInspectionPage {
            schema_version: FLEET_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    async fn evaluate_and_record_quota(
        &self,
        tenant_id: rocketmq_sre_contracts::TenantId,
        cluster_id: Option<ClusterId>,
        work_kind: FleetQuotaWorkKind,
        resource: FleetQuotaResource,
        amount: u64,
    ) -> Result<FleetQuotaDecisionRecord, ControlPlaneError> {
        let (policy, usage) = self.repository.quota_policy(tenant_id, cluster_id).await?;
        let decision = FleetQuotaEvaluator::evaluate(
            &policy,
            &usage,
            QuotaRequest {
                resource: quota_resource(resource),
                amount,
                priority: work_priority(work_kind),
            },
        );
        let record = FleetQuotaDecisionRecord {
            id: FleetQuotaDecisionId::new(),
            policy_id: policy.id,
            tenant_id,
            cluster_id,
            work_kind,
            resource,
            amount,
            allowed: decision.allowed,
            reason: quota_reason(decision.reason).to_owned(),
            observed: decision.observed,
            limit: decision.limit,
            occurred_at: Utc::now(),
        };
        self.repository.store_quota_decision(&record).await?;
        Ok(record)
    }
}

fn validate_onboarding(request: &FleetOnboardingRequest) -> Result<(), ControlPlaneError> {
    validate_owner(&request.owner)?;
    if request.residency_tags.len() > 64
        || request.oauth_scopes.len() > 32
        || request.required_capabilities.len() > 256
        || request.required_data_sources.len() > 256
    {
        return Err(invalid_request("Fleet onboarding set exceeds the bounded limit"));
    }
    for value in request
        .residency_tags
        .iter()
        .chain(request.oauth_scopes.iter())
        .chain(request.required_capabilities.iter())
        .chain(request.required_data_sources.iter())
    {
        validate_bounded(value, "onboarding value", 256)?;
    }
    Ok(())
}

fn capability_tool_names(manifest: &serde_json::Value) -> std::collections::BTreeSet<String> {
    manifest
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| {
            tool.as_str()
                .or_else(|| tool.get("name").and_then(serde_json::Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

fn is_allowed_onboarding_scope(scope: &str) -> bool {
    matches!(scope, "openid" | "profile" | "rocketmq:read" | "rocketmq:diagnose")
}

fn work_priority(kind: FleetQuotaWorkKind) -> FleetWorkPriority {
    match kind {
        FleetQuotaWorkKind::ActiveIncident
        | FleetQuotaWorkKind::Verification
        | FleetQuotaWorkKind::Rollback
        | FleetQuotaWorkKind::Audit => FleetWorkPriority::SafetyCritical,
        FleetQuotaWorkKind::InteractiveQuery | FleetQuotaWorkKind::Workflow => FleetWorkPriority::Interactive,
        FleetQuotaWorkKind::Inspection
        | FleetQuotaWorkKind::ModelExplanation
        | FleetQuotaWorkKind::Notification
        | FleetQuotaWorkKind::AutomaticAction => FleetWorkPriority::Background,
    }
}

fn quota_resource(resource: FleetQuotaResource) -> QuotaResource {
    match resource {
        FleetQuotaResource::Query => QuotaResource::Query,
        FleetQuotaResource::ModelToken => QuotaResource::ModelToken,
        FleetQuotaResource::ConcurrentWorkflow => QuotaResource::ConcurrentWorkflow,
        FleetQuotaResource::ConcurrentInspection => QuotaResource::ConcurrentInspection,
        FleetQuotaResource::EvidenceByte => QuotaResource::EvidenceByte,
        FleetQuotaResource::Notification => QuotaResource::Notification,
        FleetQuotaResource::AutomaticAction => QuotaResource::AutomaticAction,
    }
}

fn quota_reason(reason: QuotaDecisionReason) -> &'static str {
    match reason {
        QuotaDecisionReason::Allowed => "allowed",
        QuotaDecisionReason::SafetyCriticalReservedCapacity => "safety_critical_reserved_capacity",
        QuotaDecisionReason::PolicyInactive => "policy_inactive",
        QuotaDecisionReason::ScopeMismatch => "scope_mismatch",
        QuotaDecisionReason::LimitExceeded => "limit_exceeded",
    }
}

fn require_read_role(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "rocketmq:diagnose" | "operator" | "approver" | "model-governance"
        )
    }) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "Fleet read access requires a diagnose or operator role",
        ))
    }
}

fn require_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "Fleet mutation requires the operator role",
        ))
    }
}

fn require_endpoint_role(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth
        .roles
        .iter()
        .any(|role| matches!(role.as_str(), "operator" | "executor_service" | "execution_agent"))
    {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "regional endpoint registration requires a service or operator role",
        ))
    }
}

fn authorize_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated Fleet scope",
        ))
    }
}

fn allowed_clusters(auth: &AuthContext) -> Vec<ClusterId> {
    auth.clusters.iter().copied().collect()
}

fn validate_endpoint(endpoint: &RegionalEndpoint) -> Result<(), ControlPlaneError> {
    validate_bounded(&endpoint.id, "endpoint id", 128)?;
    validate_version(&endpoint.component_version, "component version")?;
    validate_version(&endpoint.protocol_version, "protocol version")?;
    validate_digest(&endpoint.schema_digest, "schema digest")?;
    if endpoint.capacity == 0 || endpoint.capacity > 10_000 {
        return Err(invalid_request(
            "regional endpoint capacity must be between 1 and 10000",
        ));
    }
    if endpoint.capabilities.len() > 256 || endpoint.residency_tags.len() > 64 {
        return Err(invalid_request(
            "regional endpoint capability or residency set is too large",
        ));
    }
    Ok(())
}

fn validate_asset(asset: &FleetAssetIndex) -> Result<(), ControlPlaneError> {
    validate_bounded(&asset.owner, "asset owner", 128)?;
    validate_bounded(&asset.component, "asset component", 128)?;
    validate_version(&asset.component_version, "asset component version")?;
    validate_bounded(&asset.health, "asset health", 32)?;
    if asset.attributes.len() > 64 {
        return Err(invalid_request("Fleet asset attributes exceed the bounded limit"));
    }
    for digest in [
        asset.image_digest.as_deref(),
        asset.feature_digest.as_deref(),
        asset.configuration_digest.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_digest(digest, "asset digest")?;
    }
    Ok(())
}

fn validate_compliance(request: &EvaluateComplianceRequest) -> Result<(), ControlPlaneError> {
    validate_bounded(&request.category, "compliance category", 128)?;
    validate_digest(&request.expected_digest, "expected digest")?;
    validate_digest(&request.live_digest, "live digest")?;
    validate_owner(&request.owner)?;
    validate_bounded(&request.recommendation, "compliance recommendation", 2_048)?;
    if request.evidence_ids.len() > 64 {
        return Err(invalid_request(
            "compliance evidence references exceed the bounded limit",
        ));
    }
    Ok(())
}

fn validate_inspection(request: &CreateFleetInspectionRequest) -> Result<(), ControlPlaneError> {
    if request.cluster_ids.is_empty() || request.cluster_ids.len() > MAX_INSPECTION_CLUSTERS {
        return Err(invalid_request(
            "Fleet inspection must target between 1 and 100 clusters",
        ));
    }
    if request.region_ids.is_empty() {
        return Err(invalid_request("Fleet inspection requires at least one region"));
    }
    if request.pack_ids.is_empty() || request.pack_ids.len() > MAX_INSPECTION_PACKS {
        return Err(invalid_request(
            "Fleet inspection must use between 1 and 32 diagnostic packs",
        ));
    }
    if request.max_concurrency == 0 || request.max_concurrency > 32 {
        return Err(invalid_request("Fleet inspection concurrency must be between 1 and 32"));
    }
    if request.timeout_seconds == 0 || request.timeout_seconds > 86_400 {
        return Err(invalid_request(
            "Fleet inspection timeout must be between 1 and 86400 seconds",
        ));
    }
    if request.model_token_budget == 0 || request.model_token_budget > MAX_DATABASE_I64 {
        return Err(invalid_request(
            "Fleet inspection model token budget must fit the supported storage range",
        ));
    }
    if request.evidence_byte_budget == 0 || request.evidence_byte_budget > MAX_DATABASE_I64 {
        return Err(invalid_request(
            "Fleet inspection evidence byte budget must fit the supported storage range",
        ));
    }
    for pack in &request.pack_ids {
        validate_bounded(pack, "diagnostic pack id", 128)?;
    }
    Ok(())
}

fn validate_quota_limits(request: &CreateQuotaPolicyRequest) -> Result<(), ControlPlaneError> {
    let limits = &request.limits;
    if limits.queries_per_minute > MAX_DATABASE_I32
        || limits.model_tokens_per_hour > MAX_DATABASE_I64
        || limits.concurrent_workflows > MAX_DATABASE_I32
        || limits.concurrent_inspections > 32
        || limits.evidence_bytes_per_hour > MAX_DATABASE_I64
        || limits.notifications_per_hour > MAX_DATABASE_I32
        || limits.automatic_actions_per_hour > MAX_DATABASE_I32
    {
        return Err(invalid_request("Fleet quota exceeds the supported storage range"));
    }
    Ok(())
}

fn validate_owner(value: &str) -> Result<(), ControlPlaneError> {
    validate_bounded(value, "owner", 128)
}

fn validate_version(value: &str, field: &str) -> Result<(), ControlPlaneError> {
    validate_bounded(value, field, 128)?;
    semver::Version::parse(value)
        .map(|_| ())
        .map_err(|_| invalid_request(&format!("{field} must be semantic")))
}

fn validate_digest(value: &str, field: &str) -> Result<(), ControlPlaneError> {
    if is_sha256_digest(value) {
        Ok(())
    } else {
        Err(invalid_request(&format!("{field} must be a SHA-256 digest")))
    }
}

fn validate_filter(value: Option<&str>, field: &str) -> Result<(), ControlPlaneError> {
    if let Some(value) = value {
        validate_bounded(value, field, 128)?;
    }
    Ok(())
}

fn validate_bounded(value: &str, field: &str, max: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(invalid_request(&format!("{field} is invalid")))
    } else {
        Ok(())
    }
}

fn scope_mismatch() -> ControlPlaneError {
    ControlPlaneError::forbidden(
        "fleet_scope_mismatch",
        "Fleet, tenant, region, and cluster scope do not match",
    )
}

fn denied_route(reason: &str, observed_at: chrono::DateTime<Utc>) -> RegionalRouteDecision {
    RegionalRouteDecision {
        schema_version: FLEET_API_SCHEMA_VERSION,
        mode: RegionalRouteMode::Denied,
        endpoint: None,
        reason_codes: vec![reason.to_owned()],
        observed_at,
    }
}

fn invalid_request(message: &str) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_request", message)
}
