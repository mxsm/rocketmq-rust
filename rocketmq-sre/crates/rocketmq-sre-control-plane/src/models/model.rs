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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CriticAssessment;
use rocketmq_sre_contracts::CriticConclusion;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::ModelProfileId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::postmortem::PostmortemAssembly;
use rocketmq_sre_model_gateway::ProviderProfile;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

pub(super) const RULES_ONLY_REASON: &str = "RulesOnlyDiagnosisNotExecutable";
pub(super) const MODEL_ADOPTED_REASON: &str = "ModelDiagnosisAdopted";
pub(super) const DIAGNOSIS_PROMPT_VERSION: &str = "rocketmq-sre.diagnosis.prompt.v1";
pub(super) const DIAGNOSIS_REPAIR_PROMPT_VERSION: &str = "rocketmq-sre.diagnosis.repair.v1";
pub(super) const DIAGNOSIS_OUTPUT_SCHEMA_VERSION: &str = "rocketmq-sre.model-diagnosis.v1";

#[derive(Clone)]
pub(super) struct RuntimeModelProfile {
    pub(super) id: ModelProfileId,
    pub(super) profile: ProviderProfile,
}

#[derive(Clone, Debug)]
pub(super) struct PersistInvocation {
    pub(super) id: ModelInvocationId,
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) incident_id: IncidentId,
    pub(super) diagnosis_revision_id: Option<DiagnosisRevisionId>,
    pub(super) parent_invocation_id: Option<ModelInvocationId>,
    pub(super) purpose: &'static str,
    pub(super) requested_profile_id: ModelProfileId,
    pub(super) actual_profile_id: ModelProfileId,
    pub(super) provider_family: String,
    pub(super) model_family: String,
    pub(super) actual_model: String,
    pub(super) model_revision: String,
    pub(super) endpoint_instance: String,
    pub(super) fallback_chain: Vec<ModelProfileId>,
    pub(super) prompt_version: &'static str,
    pub(super) schema_version: &'static str,
    pub(super) input_tokens: Option<u32>,
    pub(super) output_tokens: Option<u32>,
    pub(super) cost_micros: Option<u64>,
    pub(super) rationale: String,
    pub(super) error_code: Option<String>,
    pub(super) correlation_id: CorrelationId,
    pub(super) started_at: DateTime<Utc>,
    pub(super) completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct CriticInvocationIdentity {
    pub(crate) id: ModelInvocationId,
    pub(crate) provider_family: String,
    pub(crate) model_family: String,
    pub(crate) profile: String,
    pub(crate) model_revision: String,
    pub(crate) endpoint_instance: String,
    pub(crate) fallback_chain: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ModelCriticDecision {
    pub(crate) status: CriticReviewStatus,
    pub(crate) conclusion: CriticConclusion,
    pub(crate) assessment: Option<CriticAssessment>,
    pub(crate) invocation: Option<CriticInvocationIdentity>,
    pub(crate) payload_hash: String,
    pub(crate) reason_code: &'static str,
    pub(crate) prompt_version: &'static str,
    pub(crate) schema_version: &'static str,
}

#[derive(Clone, Debug)]
pub(crate) struct ModelDiagnosisDecision {
    pub(crate) mode: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) conclusion: Option<Value>,
    pub(crate) invocation_id: Option<ModelInvocationId>,
    pub(crate) input_tokens: u32,
    pub(crate) output_tokens: u32,
    pub(crate) schema_repairs_used: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ModelPostmortemDecision {
    pub(crate) content: PostmortemAssembly,
    pub(crate) invocation_id: Option<ModelInvocationId>,
}

impl ModelDiagnosisDecision {
    pub(crate) fn rules_only() -> Self {
        Self {
            mode: "rules_only",
            reason: RULES_ONLY_REASON,
            conclusion: None,
            invocation_id: None,
            input_tokens: 0,
            output_tokens: 0,
            schema_repairs_used: 0,
        }
    }

    pub(crate) const fn rules_only_with_usage(input_tokens: u32, output_tokens: u32, schema_repairs_used: u8) -> Self {
        Self {
            mode: "rules_only",
            reason: RULES_ONLY_REASON,
            conclusion: None,
            invocation_id: None,
            input_tokens,
            output_tokens,
            schema_repairs_used,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StructuredModelDiagnosis {
    pub(super) summary: String,
    pub(super) assessment: String,
    pub(super) confidence_percent: u8,
    pub(super) cited_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub(super) recommended_read_only_queries: Vec<String>,
    pub(super) rationale: String,
}

impl StructuredModelDiagnosis {
    pub(super) fn validate(&self, allowed_evidence_ids: &[EvidenceId]) -> bool {
        !self.summary.trim().is_empty()
            && self.summary.chars().count() <= 2_000
            && !self.assessment.trim().is_empty()
            && self.assessment.chars().count() <= 4_000
            && self.confidence_percent <= 100
            && self.cited_evidence_ids.len() <= 32
            && self
                .cited_evidence_ids
                .iter()
                .all(|evidence_id| allowed_evidence_ids.contains(evidence_id))
            && self.recommended_read_only_queries.len() <= 8
            && self
                .recommended_read_only_queries
                .iter()
                .all(|query| !query.trim().is_empty() && query.chars().count() <= 500)
            && !self.rationale.trim().is_empty()
            && self.rationale.chars().count() <= 4_000
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelCapabilitiesStatus {
    pub(crate) schema_version: &'static str,
    pub(crate) network_calls_supported: bool,
    pub(crate) network_calls_enabled: bool,
    pub(crate) rules_only_available: bool,
    pub(crate) max_fallbacks: usize,
    pub(crate) profiles: Vec<ModelProfileStatus>,
    pub(crate) fallback_order: Vec<String>,
    pub(crate) providers: Value,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelProfileStatus {
    pub(crate) id: ModelProfileId,
    pub(crate) profile_name: String,
    pub(crate) provider_family: String,
    pub(crate) protocol_family: String,
    pub(crate) model_family: String,
    pub(crate) model_name: String,
    pub(crate) model_revision: String,
    pub(crate) endpoint_instance: String,
    pub(crate) region: String,
    pub(crate) capabilities: Value,
    pub(crate) priority: u16,
    pub(crate) credential_configured: bool,
    pub(crate) credential_owner: String,
    pub(crate) health: String,
    pub(crate) last_health_observed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ModelInvocationListQuery {
    pub(crate) cluster_id: ClusterId,
    pub(crate) incident_id: Option<IncidentId>,
    pub(crate) limit: Option<u32>,
}

impl ModelInvocationListQuery {
    pub(crate) fn bounded_limit(&self) -> u32 {
        self.limit.unwrap_or(50).clamp(1, 200)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelInvocationPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ModelInvocationView>,
    pub(crate) partial: bool,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelInvocationView {
    pub(crate) id: ModelInvocationId,
    pub(crate) tenant_id: TenantId,
    pub(crate) cluster_id: ClusterId,
    pub(crate) incident_id: Option<IncidentId>,
    pub(crate) diagnosis_revision_id: Option<DiagnosisRevisionId>,
    pub(crate) parent_invocation_id: Option<ModelInvocationId>,
    pub(crate) purpose: String,
    pub(crate) requested_profile_id: ModelProfileId,
    pub(crate) actual_profile_id: ModelProfileId,
    pub(crate) provider_family: String,
    pub(crate) model_family: String,
    pub(crate) actual_model: String,
    pub(crate) model_revision: String,
    pub(crate) endpoint_instance: String,
    pub(crate) fallback_chain: Vec<ModelProfileId>,
    pub(crate) prompt_version: String,
    pub(crate) schema_version: String,
    pub(crate) input_tokens: Option<u32>,
    pub(crate) output_tokens: Option<u32>,
    pub(crate) cost_micros: Option<u64>,
    pub(crate) rationale: String,
    pub(crate) error_code: Option<String>,
    pub(crate) correlation_id: Option<CorrelationId>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) completed_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_diagnosis_rejects_unknown_evidence_citations() {
        let allowed = EvidenceId::new();
        let diagnosis = StructuredModelDiagnosis {
            summary: "Lag is increasing".to_owned(),
            assessment: "The consumer is not keeping up".to_owned(),
            confidence_percent: 80,
            cited_evidence_ids: vec![EvidenceId::new()],
            recommended_read_only_queries: Vec::new(),
            rationale: "Observed lag supports the assessment".to_owned(),
        };

        assert!(!diagnosis.validate(&[allowed]));
    }
}
