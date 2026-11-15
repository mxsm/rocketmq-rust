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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::EvidenceId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::ExecutionObservationReason;
use crate::ExecutionStepId;
use crate::PlanStepId;
use crate::PublicErrorCode;
use crate::SreContractError;
use crate::TenantId;
use crate::VerificationProofOrigin;
use crate::canonical_sha256;
use crate::is_sha256_digest;

pub const VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION: &str = "rocketmq-sre.verification-window-proof.v1";

/// Selected operator configuration; a declared version is not target attestation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationWindowPolicy {
    pub policy_digest: String,
    pub policy_revision: String,
    pub declared_target_version: String,
    pub stable_window_seconds: u64,
    pub max_wait_seconds: u64,
    pub max_source_age_seconds: u64,
    pub max_sample_gap_seconds: u64,
}

/// One actual source/capture pair, independent of evaluation/collection clocks.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationWindowSample {
    pub evidence_id: EvidenceId,
    pub content_hash: String,
    pub resource_observed_at: DateTime<Utc>,
    pub technical_sli_observed_at: DateTime<Utc>,
    pub collected_at: DateTime<Utc>,
    pub source_age_seconds: u64,
    /// Elapsed monotonic run time, not a duration inferred from source timestamps.
    pub elapsed_nanos: u64,
    pub complete: bool,
    pub resource_conditions_satisfied: bool,
    pub technical_slis_satisfied: bool,
    pub target_version_matched: bool,
}

/// New result proof, stored append-only beside the original outcome.
///
/// Its digest detects inconsistent content; it is not a signature, target
/// identity proof or authorization token. Only the owned Executor writes the
/// production verification journal. Absent old proof and present invalid proof
/// are deliberately different states.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationWindowProof {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub plan_step_id: PlanStepId,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub target: String,
    pub parameters_digest: String,
    pub policy: Option<VerificationWindowPolicy>,
    pub provenance: VerificationProofOrigin,
    pub target_version_observed: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub observed_stable_seconds: u64,
    pub samples: Vec<VerificationWindowSample>,
    pub missing_conditions: Vec<String>,
    pub reason_codes: Vec<ExecutionObservationReason>,
    pub proof_hash: String,
}

impl VerificationWindowProof {
    /// Seals already bounded proof data without granting it authority.
    ///
    /// # Errors
    /// Rejects malformed bindings, intervals, configuration or sample metadata.
    pub fn seal(mut self) -> Result<Self, SreContractError> {
        self.validate_shape()?;
        self.proof_hash = self.compute_hash()?;
        Ok(self)
    }

    /// Verifies content integrity and bounded shape; consumers also bind the
    /// proof to the exact execution/step/result before presenting qualification.
    ///
    /// # Errors
    /// Present malformed or modified proof must never become legacy missing proof.
    pub fn verify(&self) -> Result<(), SreContractError> {
        self.validate_shape()?;
        if !is_sha256_digest(&self.proof_hash) || self.compute_hash()? != self.proof_hash {
            return Err(SreContractError::new(PublicErrorCode::InvalidContentHash));
        }
        Ok(())
    }

    fn compute_hash(&self) -> Result<String, SreContractError> {
        let mut material =
            serde_json::to_value(self).map_err(|_| SreContractError::new(PublicErrorCode::InvalidContentHash))?;
        let object = material
            .as_object_mut()
            .ok_or_else(|| SreContractError::new(PublicErrorCode::InvalidContentHash))?;
        object.remove("proof_hash");
        canonical_sha256(&material)
    }

    fn validate_shape(&self) -> Result<(), SreContractError> {
        let valid = self.schema_version == VERIFICATION_WINDOW_PROOF_SCHEMA_VERSION
            && bounded_literal(&self.descriptor_version, 64)
            && bounded_literal(&self.target, 1024)
            && is_sha256_digest(&self.parameters_digest)
            && (self.provenance != VerificationProofOrigin::RecordedOnly
                || (self.samples.is_empty()
                    && self.observed_stable_seconds == 0
                    && self.target_version_observed.is_none()))
            && self.completed_at >= self.started_at
            && self.samples.len() <= 10_000
            && self.missing_conditions.len() <= 64
            && self.missing_conditions.iter().all(|name| bounded_literal(name, 160))
            && self.reason_codes.len() <= 64
            && self
                .target_version_observed
                .as_ref()
                .is_none_or(|version| bounded_literal(version, 128));
        if !valid {
            return Err(SreContractError::new(PublicErrorCode::InvalidDescriptor));
        }
        if let Some(policy) = &self.policy
            && (!is_sha256_digest(&policy.policy_digest)
                || !bounded_literal(&policy.policy_revision, 64)
                || !bounded_literal(&policy.declared_target_version, 128)
                || policy.stable_window_seconds == 0
                || policy.stable_window_seconds > policy.max_wait_seconds
                || policy.max_wait_seconds > 86_400
                || !(1..=300).contains(&policy.max_source_age_seconds)
                || !(1..=300).contains(&policy.max_sample_gap_seconds))
        {
            return Err(SreContractError::new(PublicErrorCode::InvalidDescriptor));
        }
        let mut previous_elapsed = None;
        let mut ids = std::collections::BTreeSet::new();
        for sample in &self.samples {
            if !is_sha256_digest(&sample.content_hash)
                || !ids.insert(sample.evidence_id)
                || sample.resource_observed_at > sample.collected_at
                || sample.technical_sli_observed_at > sample.collected_at
                || sample.collected_at < self.started_at
                || sample.collected_at > self.completed_at
                || previous_elapsed.is_some_and(|previous| sample.elapsed_nanos < previous)
            {
                return Err(SreContractError::new(PublicErrorCode::InvalidDescriptor));
            }
            previous_elapsed = Some(sample.elapsed_nanos);
        }
        Ok(())
    }
}

fn bounded_literal(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && value.is_ascii() && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests;
