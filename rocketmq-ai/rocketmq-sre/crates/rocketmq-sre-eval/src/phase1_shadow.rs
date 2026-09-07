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

//! Offline Phase 01 Wave A replay and read-only shadow safety harness.

mod error;
mod fixture;
mod manifest;
mod provider;
mod runner;
mod security;

pub(crate) use error::ShadowEvalFailure;
pub(crate) use error::ShadowEvalSource;
pub use fixture::DiagnosticReplayFixture;
pub use manifest::ScenarioCase;
pub use manifest::ScenarioClass;
pub use manifest::ScenarioDefinition;
pub use manifest::ShadowManifest;
pub use manifest::ShadowPolicy;
pub use provider::ProviderMode;
pub use provider::ProviderModeRejection;
pub use runner::ScenarioResult;
pub use runner::ShadowHarness;
pub use runner::ShadowSuiteSummary;
pub use security::ShadowModelSynthesis;
pub use security::build_model_request;
use std::collections::BTreeSet;
use std::path::Path;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_model_gateway::CanonicalModelResponse;

/// Loads a compact diagnostic fixture into canonical Evidence snapshots.
///
/// # Errors
///
/// Returns an opaque evaluation failure when fixture loading or validation fails.
pub fn load_diagnostic_fixture(
    path: &Path,
    tenant_id: TenantId,
    cluster_id: ClusterId,
) -> Result<crate::EvalOutcome<DiagnosticReplayFixture>, crate::EvalError> {
    match fixture::load_diagnostic_fixture(path, tenant_id, cluster_id) {
        Ok(fixture) => Ok(crate::EvalOutcome::Completed(fixture)),
        Err(failure) => failure.into_boundary(),
    }
}

/// Loads and validates a versioned shadow manifest.
///
/// # Errors
///
/// Returns an opaque evaluation failure when loading or validation fails.
pub fn load_shadow_manifest(path: &Path) -> Result<crate::EvalOutcome<ShadowManifest>, crate::EvalError> {
    match manifest::load_shadow_manifest(path) {
        Ok(manifest) => Ok(crate::EvalOutcome::Completed(manifest)),
        Err(failure) => failure.into_boundary(),
    }
}

/// Validates every cited Evidence ID against the authorized Evidence pack.
///
/// # Errors
///
/// Returns an opaque evaluation failure for an unauthorized citation.
pub fn validate_citations(
    authorized: &BTreeSet<EvidenceId>,
    citations: &[EvidenceId],
) -> Result<crate::EvalOutcome<()>, crate::EvalError> {
    match security::validate_citations(authorized, citations) {
        Ok(()) => Ok(crate::EvalOutcome::Completed(())),
        Err(failure) => failure.into_boundary(),
    }
}

/// Validates provider tool proposals, structured synthesis, and citations.
///
/// # Errors
///
/// Returns an opaque evaluation failure when validation fails closed.
pub fn validate_model_response(
    response: &CanonicalModelResponse,
    authorized: &BTreeSet<EvidenceId>,
    policy: &ShadowPolicy,
) -> Result<crate::EvalOutcome<ShadowModelSynthesis>, crate::EvalError> {
    match security::validate_model_response(response, authorized, policy) {
        Ok(synthesis) => Ok(crate::EvalOutcome::Completed(synthesis)),
        Err(failure) => failure.into_boundary(),
    }
}
