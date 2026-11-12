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

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use rocketmq_sre_contracts::{
    CAPABILITY_VERIFICATION_SCHEMA_VERSION, CapabilityVerificationLevel as Level,
    CapabilityVerificationMethod as Method, CapabilityVerificationOutcome as Outcome, CapabilityVerificationRequest,
    DiagnosticEvidenceQuality, EvidenceSnapshot, is_sha256_digest,
};
use rocketmq_sre_core::diagnostics::full_registry;

use super::super::{CoverageQuery, quality, revision};
use super::VerifierIdentity;
use crate::{ControlPlaneRequestFailure, api::AppState, auth::AuthContext};

pub(super) fn authorize(
    auth: &AuthContext,
    identity: VerifierIdentity,
    request: &CapabilityVerificationRequest,
) -> Result<(), ControlPlaneRequestFailure> {
    if !auth.roles.contains("capability_verifier_service")
        || auth.roles.iter().any(|role| {
            matches!(
                role.as_str(),
                "model_service"
                    | "provider_service"
                    | "connector_service"
                    | "connector"
                    | "executor_service"
                    | "execution_agent"
            )
        })
        || auth.subject.starts_with("model:")
        || auth.subject.starts_with("provider:")
    {
        return Err(ControlPlaneRequestFailure::forbidden(
            "capability_verifier_forbidden",
            "a dedicated verifier workload identity is required",
        ));
    }
    if auth.tenant_id != request.tenant_id || !auth.clusters.contains(&request.cluster_id) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "cluster_not_allowed",
            "verification scope differs from workload authorization",
        ));
    }
    if auth.subject.trim().is_empty() || auth.subject.len() > 256 || auth.subject.chars().any(char::is_control) {
        return Err(invalid("verification_identity_invalid"));
    }
    if identity == VerifierIdentity::Development && request.level == Level::Production {
        return Err(ControlPlaneRequestFailure::forbidden(
            "production_verification_requires_oidc",
            "development authentication cannot write production validation facts",
        ));
    }
    Ok(())
}

pub(super) fn production_passed(request: &CapabilityVerificationRequest) -> bool {
    request.level == Level::Production && request.outcome == Outcome::Passed
}

pub(super) fn validate(
    request: &CapabilityVerificationRequest,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    if request.schema_version != CAPABILITY_VERIFICATION_SCHEMA_VERSION || request.evidence_ids.len() > 64 {
        return Err(invalid("verification_contract_invalid"));
    }
    if request.investigation_id.is_some() != request.revision_id.is_some() {
        return Err(invalid("verification_revision_pair_required"));
    }
    let registry =
        full_registry().map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?;
    let pack = registry
        .resolve(&request.pack_id)
        .ok_or_else(|| invalid("verification_pack_unknown"))?;
    if pack.qualified_id() != request.pack_id || pack.version().to_string() != request.pack_version {
        return Err(invalid("verification_pack_version_mismatch"));
    }
    if request.observed_at > now
        || request.observed_at < now - Duration::days(1)
        || request.time_range.start > request.time_range.end
        || request.time_range.end > request.observed_at
        || request.time_range.start < request.observed_at - Duration::days(1)
        || request.valid_until <= request.observed_at
        || request.valid_until > request.observed_at + Duration::days(30)
    {
        return Err(invalid("verification_time_invalid"));
    }
    if let Some(resource) = &request.resource {
        validate_resource(resource)?;
        if !pack
            .required_evidence()
            .iter()
            .any(|requirement| resource.starts_with(requirement.resource_prefix))
        {
            return Err(invalid("verification_resource_mismatch"));
        }
    }
    if request.source_ref.len() > 512
        || !(request.source_ref.starts_with("urn:") || request.source_ref.starts_with("report:"))
        || !request
            .source_ref
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'/' | b'-' | b'_' | b'.'))
        || !is_sha256_digest(&request.report_digest)
    {
        return Err(invalid("verification_provenance_invalid"));
    }
    if production_passed(request)
        && (request.method != Method::LiveReadOnly || request.resource.is_none() || request.evidence_ids.is_empty())
    {
        return Err(invalid("production_verification_proof_required"));
    }
    Ok(())
}

/// A selected target is a literal canonical Evidence resource, never a URL or
/// an operator navigation resource such as `consumer/group`.
pub(in super::super) fn validate_resource(resource: &str) -> Result<(), ControlPlaneRequestFailure> {
    if resource.is_empty()
        || resource.len() > 512
        || resource.split('/').any(str::is_empty)
        || !resource
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b':' | b'%'))
    {
        return Err(invalid("canonical_resource_invalid"));
    }
    let registry =
        full_registry().map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?;
    if !registry.active_packs().any(|pack| {
        pack.required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .any(|requirement| resource.starts_with(requirement.resource_prefix))
    }) {
        return Err(invalid("canonical_resource_invalid"));
    }
    Ok(())
}

pub(super) async fn evidence(
    state: &AppState,
    auth: &AuthContext,
    request: &CapabilityVerificationRequest,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneRequestFailure> {
    let mut snapshots = Vec::with_capacity(request.evidence_ids.len());
    let bound = revision::load(
        state,
        auth,
        &CoverageQuery {
            cluster_id: Some(request.cluster_id.to_string()),
            investigation_id: request.investigation_id.map(|id| id.to_string()),
            revision_id: request.revision_id.map(|id| id.to_string()),
            resource: None,
        },
    )
    .await?;
    let saved_ids = if let Some(bound) = &bound {
        if bound.cluster_id != request.cluster_id {
            return Err(invalid("verification_revision_scope_mismatch"));
        }
        let (saved, partial) = bound.evidence(state, auth).await?;
        if partial {
            return Err(invalid("verification_revision_scan_incomplete"));
        }
        Some(
            saved
                .into_iter()
                .map(|snapshot| snapshot.evidence_id)
                .collect::<BTreeSet<_>>(),
        )
    } else {
        None
    };
    for id in &request.evidence_ids {
        if saved_ids.as_ref().is_some_and(|ids| !ids.contains(id)) {
            return Err(invalid("verification_revision_reference_mismatch"));
        }
        let snapshot = state.evidence.get(auth, *id).await?;
        if snapshot.cluster_id != request.cluster_id || snapshot.tenant_id != auth.tenant_id {
            return Err(ControlPlaneRequestFailure::forbidden(
                "cluster_not_allowed",
                "verification evidence differs from the authorized scope",
            ));
        }
        if request
            .resource
            .as_ref()
            .is_some_and(|resource| *resource != snapshot.resource)
            || snapshot.observed_at < request.time_range.start
            || snapshot.observed_at > request.time_range.end
            || snapshot.time_range.start < request.time_range.start
            || snapshot.time_range.end > request.time_range.end
        {
            return Err(invalid("verification_evidence_scope_mismatch"));
        }
        snapshots.push(snapshot);
    }
    if production_passed(request) && !qualified(request, &snapshots, now)? {
        return Err(invalid("verification_evidence_not_qualified"));
    }
    Ok(())
}

pub(super) fn qualified(
    request: &CapabilityVerificationRequest,
    snapshots: &[EvidenceSnapshot],
    now: DateTime<Utc>,
) -> Result<bool, ControlPlaneRequestFailure> {
    let registry =
        full_registry().map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?;
    let pack = registry
        .resolve(&request.pack_id)
        .ok_or_else(|| invalid("verification_pack_unknown"))?;
    let projection = quality::project(
        request.tenant_id,
        Some(request.cluster_id),
        now,
        &[pack],
        &BTreeMap::new(),
        snapshots,
        false,
    );
    let required = projection.packs[0]
        .requirements
        .iter()
        .filter(|requirement| requirement.required)
        .collect::<Vec<_>>();
    Ok(!required.is_empty()
        && required
            .iter()
            .all(|requirement| requirement.quality == DiagnosticEvidenceQuality::Available))
}

fn invalid(code: &'static str) -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation(code, "capability verification input is invalid")
}
