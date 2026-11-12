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

mod projection;
#[cfg(test)]
mod tests;
mod validation;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::HeaderMap,
    routing::post,
};
use chrono::Utc;
use rocketmq_sre_contracts::{CapabilityVerificationId, CapabilityVerificationRecord, CapabilityVerificationRequest};

use crate::repository::ClusterRepository;
use crate::{ControlPlaneRequestFailure, api::AppState, auth::AuthContext};

pub(super) use projection::apply;
pub(super) use validation::validate_resource;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VerifierIdentity {
    Development,
    Oidc,
}

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/internal/v1/capability-verifications", post(ingest))
        .layer(DefaultBodyLimit::max(64 * 1024))
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CapabilityVerificationRequest>,
) -> Result<Json<CapabilityVerificationRecord>, ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    let identity = if state.auth.is_development() {
        VerifierIdentity::Development
    } else {
        VerifierIdentity::Oidc
    };
    Ok(Json(record(&state, &auth, identity, request).await?))
}

pub(super) async fn record(
    state: &AppState,
    auth: &AuthContext,
    identity: VerifierIdentity,
    mut request: CapabilityVerificationRequest,
) -> Result<CapabilityVerificationRecord, ControlPlaneRequestFailure> {
    let now = Utc::now();
    validation::authorize(auth, identity, &request)?;
    validation::validate(&request, now)?;
    crate::api::ensure_cluster_authorized(auth, &state.repository.get(request.cluster_id).await?)?;
    request.evidence_ids.sort();
    request.evidence_ids.dedup();
    request.report_digest.make_ascii_lowercase();
    if let Some(existing) = existing(state, &request).await? {
        return same_report(existing, &request);
    }
    validation::evidence(state, auth, &request, now).await?;
    let record = CapabilityVerificationRecord {
        record_id: CapabilityVerificationId::new(),
        verified_by: auth.subject.clone(),
        recorded_at: now,
        assessment: request,
    };
    let assessment = &record.assessment;
    let snapshot = serde_json::to_value(&record).map_err(ControlPlaneRequestFailure::configuration_source)?;
    let inserted=sqlx::query("INSERT INTO capability_verification_records
        (id,tenant_id,cluster_id,pack_id,pack_version,level,resource,investigation_id,revision_id,record_snapshot,recorded_at,outcome,observed_at,report_digest)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING")
        .bind(record.record_id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(assessment.cluster_id.as_uuid())
        .bind(&assessment.pack_id).bind(&assessment.pack_version).bind(level(assessment.level))
        .bind(&assessment.resource).bind(assessment.investigation_id.map(|id|id.as_uuid()))
        .bind(assessment.revision_id.map(|id|id.as_uuid())).bind(snapshot).bind(now)
        .bind(outcome(assessment.outcome)).bind(assessment.observed_at).bind(&assessment.report_digest)
        .execute(&state.repository.pool).await?;
    if inserted.rows_affected() == 0 {
        let existing = existing(state, assessment).await?.ok_or_else(|| {
            ControlPlaneRequestFailure::state("verification_report_conflict", "conflicting report was not visible")
        })?;
        return same_report(existing, assessment);
    }
    Ok(record)
}

fn same_report(
    existing: CapabilityVerificationRecord,
    request: &CapabilityVerificationRequest,
) -> Result<CapabilityVerificationRecord, ControlPlaneRequestFailure> {
    if existing.assessment == *request {
        Ok(existing)
    } else {
        Err(ControlPlaneRequestFailure::conflict_code(
            "verification_report_conflict",
            "the report identity already describes another immutable assessment",
        ))
    }
}

async fn existing(
    state: &AppState,
    request: &CapabilityVerificationRequest,
) -> Result<Option<CapabilityVerificationRecord>, ControlPlaneRequestFailure> {
    let value = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT record_snapshot FROM capability_verification_records
        WHERE tenant_id=$1 AND cluster_id=$2 AND pack_id=$3 AND pack_version=$4 AND level=$5
            AND resource IS NOT DISTINCT FROM $6 AND investigation_id IS NOT DISTINCT FROM $7
            AND revision_id IS NOT DISTINCT FROM $8 AND report_digest=$9",
    )
    .bind(request.tenant_id.as_uuid())
    .bind(request.cluster_id.as_uuid())
    .bind(&request.pack_id)
    .bind(&request.pack_version)
    .bind(level(request.level))
    .bind(&request.resource)
    .bind(request.investigation_id.map(|id| id.as_uuid()))
    .bind(request.revision_id.map(|id| id.as_uuid()))
    .bind(&request.report_digest)
    .fetch_optional(&state.repository.pool)
    .await?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(ControlPlaneRequestFailure::configuration_source)
}

fn level(level: rocketmq_sre_contracts::CapabilityVerificationLevel) -> &'static str {
    match level {
        rocketmq_sre_contracts::CapabilityVerificationLevel::Test => "test",
        rocketmq_sre_contracts::CapabilityVerificationLevel::Production => "production",
    }
}

fn outcome(outcome: rocketmq_sre_contracts::CapabilityVerificationOutcome) -> &'static str {
    use rocketmq_sre_contracts::CapabilityVerificationOutcome as Outcome;
    match outcome {
        Outcome::Passed => "passed",
        Outcome::Failed => "failed",
        Outcome::Inconclusive => "inconclusive",
        Outcome::Revoked => "revoked",
    }
}
