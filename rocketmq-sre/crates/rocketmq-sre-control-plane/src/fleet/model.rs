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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::ComplianceFinding;
use rocketmq_sre_contracts::ComplianceFindingState;
use rocketmq_sre_contracts::ComplianceSeverity;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DataResidencyClass;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::Fleet;
use rocketmq_sre_contracts::FleetAccessProfile;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetEnvironment;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetInspectionRun;
use rocketmq_sre_contracts::FleetOnboardingAssessment;
use rocketmq_sre_contracts::FleetQuotaDecisionRecord;
use rocketmq_sre_contracts::FleetQuotaResource;
use rocketmq_sre_contracts::FleetQuotaWorkKind;
use rocketmq_sre_contracts::FleetRegion;
use rocketmq_sre_contracts::FleetTenant;
use rocketmq_sre_contracts::QuotaLimits;
use rocketmq_sre_contracts::QuotaPolicy;
use rocketmq_sre_contracts::QuotaUsage;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RegionalEndpoint;
use rocketmq_sre_contracts::RegionalEndpointKind;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const FLEET_API_SCHEMA_VERSION: &str = "rocketmq-sre.fleet-api.v1";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetScopeQuery {
    pub(crate) region_id: Option<RegionId>,
    pub(crate) environment: Option<FleetEnvironment>,
    pub(crate) owner: Option<String>,
    pub(crate) component_version: Option<String>,
    pub(crate) health: Option<String>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
    #[serde(default)]
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetOverview {
    pub(crate) schema_version: &'static str,
    pub(crate) fleet: Fleet,
    pub(crate) tenant: FleetTenant,
    pub(crate) regions: Vec<FleetRegion>,
    pub(crate) registrations: Vec<ClusterRegistration>,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ClusterRegistrationPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ClusterRegistration>,
    pub(crate) total: u64,
    pub(crate) limit: u16,
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetOnboardingRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) fleet_id: FleetId,
    pub(crate) region_id: RegionId,
    pub(crate) environment: FleetEnvironment,
    pub(crate) owner: String,
    #[serde(default)]
    pub(crate) residency_tags: BTreeSet<String>,
    pub(crate) requested_access: FleetAccessProfile,
    pub(crate) connector_tls_verified: bool,
    #[serde(default)]
    pub(crate) oauth_scopes: BTreeSet<String>,
    #[serde(default)]
    pub(crate) required_capabilities: BTreeSet<String>,
    #[serde(default)]
    pub(crate) required_data_sources: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetOnboardingView {
    pub(crate) schema_version: &'static str,
    pub(crate) assessment: FleetOnboardingAssessment,
    pub(crate) registration: Option<ClusterRegistration>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetOffboardRequest {
    pub(crate) reason: String,
    pub(crate) correlation_id: Option<CorrelationId>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateQuotaPolicyRequest {
    pub(crate) fleet_id: FleetId,
    pub(crate) region_id: Option<RegionId>,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) limits: QuotaLimits,
    pub(crate) owner: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct QuotaPolicyView {
    pub(crate) schema_version: &'static str,
    pub(crate) policy: QuotaPolicy,
    pub(crate) usage: QuotaUsage,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuotaPolicyQuery {
    pub(crate) cluster_id: Option<ClusterId>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluateFleetQuotaRequest {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) work_kind: FleetQuotaWorkKind,
    pub(crate) resource: FleetQuotaResource,
    pub(crate) amount: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetQuotaDecisionView {
    pub(crate) schema_version: &'static str,
    pub(crate) decision: FleetQuotaDecisionRecord,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetQuotaDecisionQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) allowed: Option<bool>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetQuotaDecisionPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<FleetQuotaDecisionRecord>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisterRegionalEndpointRequest {
    pub(crate) endpoint: RegionalEndpoint,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegionalEndpointQuery {
    pub(crate) region_id: Option<RegionId>,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) kind: Option<RegionalEndpointKind>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RegionalEndpointPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<RegionalEndpoint>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegionalRouteRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) endpoint_kind: RegionalEndpointKind,
    pub(crate) source_region_id: RegionId,
    pub(crate) residency: DataResidencyClass,
    pub(crate) current_protocol_version: String,
    pub(crate) previous_protocol_version: String,
    pub(crate) required_schema_digest: String,
    #[serde(default)]
    pub(crate) required_capabilities: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegionalRouteMode {
    Full,
    ReadOnlyDegraded,
    Denied,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RegionalRouteDecision {
    pub(crate) schema_version: &'static str,
    pub(crate) mode: RegionalRouteMode,
    pub(crate) endpoint: Option<RegionalEndpoint>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpsertFleetAssetRequest {
    pub(crate) asset: FleetAssetIndex,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetAssetPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<FleetAssetIndex>,
    pub(crate) total: u64,
    pub(crate) limit: u16,
    pub(crate) offset: u32,
    pub(crate) health_distribution: BTreeMap<String, u64>,
    pub(crate) worst_health: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluateComplianceRequest {
    pub(crate) fleet_id: FleetId,
    pub(crate) region_id: RegionId,
    pub(crate) cluster_id: ClusterId,
    pub(crate) category: String,
    pub(crate) expected_digest: String,
    pub(crate) live_digest: String,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<EvidenceId>,
    pub(crate) severity: ComplianceSeverity,
    pub(crate) owner: String,
    pub(crate) recommendation: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComplianceFindingQuery {
    pub(crate) region_id: Option<RegionId>,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) severity: Option<ComplianceSeverity>,
    pub(crate) state: Option<ComplianceFindingState>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
    #[serde(default)]
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ComplianceFindingPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ComplianceFinding>,
    pub(crate) total: u64,
    pub(crate) limit: u16,
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ComplianceEvaluationView {
    pub(crate) schema_version: &'static str,
    pub(crate) compliant: bool,
    pub(crate) finding: Option<ComplianceFinding>,
    pub(crate) resolved_findings: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateFleetInspectionRequest {
    pub(crate) fleet_id: FleetId,
    pub(crate) region_ids: BTreeSet<RegionId>,
    pub(crate) cluster_ids: Vec<ClusterId>,
    pub(crate) pack_ids: Vec<String>,
    pub(crate) max_concurrency: u32,
    pub(crate) timeout_seconds: u32,
    pub(crate) model_token_budget: u64,
    pub(crate) evidence_byte_budget: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FleetInspectionPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<FleetInspectionRun>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetInspectionQuery {
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateFleetInspectionRequest {
    pub(crate) completed_clusters: u32,
    pub(crate) failed_clusters: u32,
    pub(crate) terminal: bool,
}

pub(crate) const fn bounded_limit(limit: u16) -> u16 {
    if limit == 0 {
        1
    } else if limit > 200 {
        200
    } else {
        limit
    }
}

const fn default_limit() -> u16 {
    50
}
