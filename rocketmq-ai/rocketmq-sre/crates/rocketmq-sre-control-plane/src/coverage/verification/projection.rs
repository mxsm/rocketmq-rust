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

use rocketmq_sre_contracts::{
    CapabilityFact, CapabilityFactStatus as Status, CapabilityQualityProjection, CapabilityVerificationLevel as Level,
    CapabilityVerificationOutcome as Outcome, CapabilityVerificationRecord, DiagnosticEvidenceQuality,
    DiagnosticPackQuality, EvidenceSnapshot,
};
use rocketmq_sre_core::diagnostics::full_registry;
use serde_json::Value;

use super::validation;
use crate::{ControlPlaneRequestFailure, api::AppState, auth::AuthContext};

pub(in super::super) async fn apply(
    state: &AppState,
    auth: &AuthContext,
    projection: &mut CapabilityQualityProjection,
    evidence: &[EvidenceSnapshot],
) -> Result<(), ControlPlaneRequestFailure> {
    let Some(cluster) = projection.cluster_id else {
        return Ok(());
    };
    let resources = if let Some(resource) = &projection.resource {
        vec![resource.clone()]
    } else if projection.revision_id.is_some() {
        evidence
            .iter()
            .map(|snapshot| snapshot.resource.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    // Latest per exact validation scope. An expired/revoked newer report must
    // not uncover an older successful report. The global view excludes target
    // records in SQL, before the bounded scan can crowd out cluster facts.
    let rows=sqlx::query_scalar::<_,Value>("SELECT record_snapshot FROM (
        SELECT DISTINCT ON (pack_id,pack_version,level,resource,investigation_id,revision_id)
            record_snapshot,observed_at,report_digest,
            CASE outcome WHEN 'revoked' THEN 3 WHEN 'failed' THEN 2 WHEN 'inconclusive' THEN 1 ELSE 0 END AS outcome_priority
        FROM capability_verification_records
        WHERE tenant_id=$1 AND cluster_id=$2
            AND (resource IS NULL OR resource=ANY($3))
            AND (investigation_id IS NULL OR (investigation_id=$4 AND revision_id=$5))
        ORDER BY pack_id,pack_version,level,resource,investigation_id,revision_id,observed_at DESC,
            CASE outcome WHEN 'revoked' THEN 3 WHEN 'failed' THEN 2 WHEN 'inconclusive' THEN 1 ELSE 0 END DESC,report_digest
        ) selected ORDER BY observed_at DESC,outcome_priority DESC,report_digest LIMIT 257")
        .bind(auth.tenant_id.as_uuid()).bind(cluster.as_uuid()).bind(resources)
        .bind(projection.investigation_id.map(|id|id.as_uuid())).bind(projection.revision_id.map(|id|id.as_uuid()))
        .fetch_all(&state.repository.pool).await?;
    let truncated = rows.len() > 256;
    let mut invalid = false;
    let mut records = rows
        .into_iter()
        .take(256)
        .filter_map(|value| {
            let record = serde_json::from_value::<CapabilityVerificationRecord>(value).ok();
            if record.is_none() {
                invalid = true;
            }
            record
        })
        .collect::<Vec<_>>();
    records.sort_by_key(|record| {
        std::cmp::Reverse((
            record.assessment.investigation_id.is_some(),
            record.assessment.resource.is_some(),
        ))
    });
    let registry =
        full_registry().map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?;
    let context = projection.clone();
    for pack in &mut projection.packs {
        let Some(compiled) = registry.resolve(&pack.pack_id) else {
            continue;
        };
        for level in [Level::Test, Level::Production] {
            let candidate = records.iter().find(|record| {
                let item = &record.assessment;
                item.tenant_id == auth.tenant_id
                    && item.cluster_id == cluster
                    && item.pack_id == pack.pack_id
                    && item.pack_version == compiled.version().to_string()
                    && item.level == level
                    && applies(record, &context, pack, evidence)
            });
            let fact = if let Some(record) = candidate {
                evaluate(record, &context, pack, evidence)?
            } else {
                CapabilityFact {
                    status: Status::Unknown,
                    reason_code: if invalid {
                        "verification_record_invalid"
                    } else if truncated {
                        "verification_record_scan_incomplete"
                    } else {
                        "verification_record_not_provided"
                    }
                    .to_owned(),
                    source_ref: "controlled_capability_verification_records".to_owned(),
                    observed_at: None,
                    verification: None,
                }
            };
            match level {
                Level::Test => pack.layers.test_verified = fact,
                Level::Production => pack.layers.production_verified = fact,
            }
        }
    }
    projection.partial |= truncated || invalid;
    Ok(())
}

fn applies(
    record: &CapabilityVerificationRecord,
    context: &CapabilityQualityProjection,
    pack: &DiagnosticPackQuality,
    evidence: &[EvidenceSnapshot],
) -> bool {
    let item = &record.assessment;
    if item.investigation_id.is_some()
        && (item.investigation_id != context.investigation_id || item.revision_id != context.revision_id)
    {
        return false;
    }
    if let Some(resource) = &item.resource {
        if let Some(selected) = &context.resource {
            if resource != selected {
                return false;
            }
        } else if context.revision_id.is_none() {
            return false;
        } else if !pack
            .requirements
            .iter()
            .filter(|requirement| requirement.required)
            .flat_map(|requirement| &requirement.evidence_ids)
            .any(|id| {
                evidence
                    .iter()
                    .any(|snapshot| snapshot.evidence_id == *id && snapshot.resource == *resource)
            })
        {
            return false;
        }
    }
    true
}

pub(super) fn evaluate(
    record: &CapabilityVerificationRecord,
    context: &CapabilityQualityProjection,
    pack: &DiagnosticPackQuality,
    evidence: &[EvidenceSnapshot],
) -> Result<CapabilityFact, ControlPlaneRequestFailure> {
    let item = &record.assessment;
    if record.recorded_at > context.observed_at
        || validation::validate(item, record.recorded_at).is_err()
        || record.verified_by.is_empty()
        || record.verified_by.len() > 256
        || record.verified_by.chars().any(char::is_control)
    {
        return Ok(CapabilityFact {
            status: Status::Unknown,
            reason_code: "verification_record_invalid".to_owned(),
            source_ref: "controlled_capability_verification_records".to_owned(),
            observed_at: None,
            verification: None,
        });
    }
    let (status, reason) = if item.valid_until <= context.observed_at {
        (Status::Unknown, "verification_record_expired")
    } else {
        match item.outcome {
            Outcome::Passed if validation::production_passed(item) => {
                let ids = item.evidence_ids.iter().copied().collect::<BTreeSet<_>>();
                let required = pack
                    .requirements
                    .iter()
                    .filter(|requirement| requirement.required)
                    .collect::<Vec<_>>();
                let exact = !required.is_empty()
                    && required.iter().all(|requirement| {
                        requirement.quality == DiagnosticEvidenceQuality::Available
                            && !requirement.evidence_ids.is_empty()
                            && requirement.evidence_ids.iter().all(|id| ids.contains(id))
                    });
                let proof = evidence
                    .iter()
                    .filter(|snapshot| ids.contains(&snapshot.evidence_id))
                    .cloned()
                    .collect::<Vec<_>>();
                if exact && validation::qualified(item, &proof, context.observed_at)? {
                    (Status::Present, "controlled_production_verification_passed")
                } else {
                    (Status::Unknown, "verification_evidence_no_longer_qualified")
                }
            }
            Outcome::Passed => (Status::Present, "controlled_test_verification_passed"),
            Outcome::Failed => (Status::Absent, "controlled_verification_failed"),
            Outcome::Inconclusive => (Status::Unknown, "controlled_verification_inconclusive"),
            Outcome::Revoked => (Status::Unknown, "verification_record_revoked"),
        }
    };
    Ok(CapabilityFact {
        status,
        reason_code: reason.to_owned(),
        source_ref: item.source_ref.clone(),
        observed_at: Some(item.observed_at),
        verification: Some(record.clone()),
    })
}
