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

use rocketmq_sre_contracts::{
    CoverageStatus, DiagnosticEvidenceQuality, DiagnosticRequirementQuality, EvidenceContent, EvidenceFieldKind,
    EvidenceSnapshot, InvestigationPackRef, InvestigationRequirementRef, InvestigationRunFailure,
};

use super::super::{DiagnosticPack, DiagnosticPackRegistry, PackVersion};
use super::InvestigationPlanningInput;

pub(super) fn exact_pack<'a>(
    registry: &'a DiagnosticPackRegistry,
    reference: &InvestigationPackRef,
) -> Result<&'a dyn DiagnosticPack, InvestigationRunFailure> {
    let parts = reference
        .pack_version
        .split('.')
        .map(str::parse::<u16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| InvestigationRunFailure::PackNotAllowed)?;
    let [major, minor, patch] = parts.as_slice() else {
        return Err(InvestigationRunFailure::PackNotAllowed);
    };
    let version = PackVersion::new(*major, *minor, *patch);
    let (base, _) = reference
        .pack_id
        .rsplit_once(".v")
        .ok_or(InvestigationRunFailure::PackNotAllowed)?;
    let pack = registry
        .get(base, version)
        .ok_or(InvestigationRunFailure::PackNotAllowed)?;
    if pack.qualified_id() != reference.pack_id || pack.version().to_string() != reference.pack_version {
        return Err(InvestigationRunFailure::PackNotAllowed);
    }
    Ok(pack)
}

pub(super) fn requirements(
    pack: &dyn DiagnosticPack,
    reference: &InvestigationPackRef,
    input: &InvestigationPlanningInput<'_>,
) -> Vec<DiagnosticRequirementQuality> {
    pack.required_evidence()
        .iter()
        .map(|requirement| (requirement, true))
        .chain(pack.optional_evidence().iter().map(|requirement| (requirement, false)))
        .map(|(requirement, required)| {
            let fields = pack.evidence_fields(requirement.key);
            let mut quality = DiagnosticRequirementQuality {
                id: requirement.key.to_owned(),
                source: requirement.source.to_owned(),
                resource_prefix: requirement.resource_prefix.to_owned(),
                required,
                max_age_seconds: pack.max_evidence_freshness_seconds(),
                fields,
                quality: DiagnosticEvidenceQuality::Missing,
                reason_code: "canonical_binding_unavailable".to_owned(),
                observed_at: None,
                evidence_ids: vec![],
                missing_fields: vec![],
            };
            let key = InvestigationRequirementRef {
                pack: reference.clone(),
                requirement_id: requirement.key.to_owned(),
            };
            let Some(binding) = input
                .bindings
                .iter()
                .find(|binding| binding.requirement == key && binding.scope == input.run.effective_scope)
            else {
                return quality;
            };
            let snapshot = input
                .evidence
                .iter()
                .filter(|snapshot| {
                    input.run.evidence_ids.contains(&snapshot.evidence_id)
                        && snapshot.tenant_id == binding.scope.tenant_id
                        && snapshot.cluster_id == binding.scope.cluster_id
                        && snapshot.source == binding.source
                        && snapshot.resource == binding.resource
                        && snapshot.time_range.start.timestamp_micros()
                            == binding.scope.time_range.start.timestamp_micros()
                        && snapshot.time_range.end.timestamp_micros() == binding.scope.time_range.end.timestamp_micros()
                })
                .max_by(|left, right| {
                    left.observed_at
                        .cmp(&right.observed_at)
                        .then_with(|| left.evidence_id.cmp(&right.evidence_id))
                });
            let Some(snapshot) = snapshot else {
                quality.reason_code = "evidence_not_observed".to_owned();
                return quality;
            };
            assess(&mut quality, snapshot, input.as_of_unix_ms);
            if matches!(
                quality.quality,
                DiagnosticEvidenceQuality::Available | DiagnosticEvidenceQuality::Partial
            ) {
                let missing_facts = pack.missing_evidence_facts(requirement.key, snapshot);
                if !missing_facts.is_empty() {
                    quality.missing_fields.extend(missing_facts);
                    quality.quality = DiagnosticEvidenceQuality::Partial;
                    quality.reason_code = "compiled_evidence_facts_incomplete".to_owned();
                }
            }
            quality
        })
        .collect()
}

pub(super) fn input_fingerprint(
    registry: &DiagnosticPackRegistry,
    reference: &InvestigationPackRef,
    qualities: &[DiagnosticRequirementQuality],
    input: &InvestigationPlanningInput<'_>,
) -> Result<String, InvestigationRunFailure> {
    let pack = exact_pack(registry, reference)?;
    let mut facts = vec![];
    for quality in qualities {
        let mut contents = vec![];
        for id in &quality.evidence_ids {
            let snapshot = input
                .evidence
                .iter()
                .find(|snapshot| snapshot.evidence_id == *id)
                .ok_or(InvestigationRunFailure::ScopeMismatch)?;
            let semantic = pack
                .semantic_evidence_fingerprint(snapshot)
                .map_err(|_| InvestigationRunFailure::InvalidInput)?;
            let hash = if let Some(semantic) = semantic {
                semantic
            } else {
                match &snapshot.content {
                    EvidenceContent::Inline(content) => rocketmq_sre_contracts::canonical_sha256(&(
                        &snapshot.source,
                        &snapshot.resource,
                        content,
                        snapshot.coverage,
                        snapshot.partial,
                    )),
                    EvidenceContent::Reference(reference) => rocketmq_sre_contracts::canonical_sha256(&(
                        &snapshot.source,
                        &snapshot.resource,
                        &reference.digest,
                        snapshot.coverage,
                        snapshot.partial,
                    )),
                }
                .map_err(|_| InvestigationRunFailure::InvalidInput)?
            };
            contents.push(hash);
        }
        contents.sort();
        contents.dedup();
        facts.push((&quality.id, quality.required, contents));
    }
    rocketmq_sre_contracts::canonical_sha256(&(reference, &input.run.effective_scope, facts))
        .map_err(|_| InvestigationRunFailure::InvalidInput)
}

fn assess(quality: &mut DiagnosticRequirementQuality, snapshot: &EvidenceSnapshot, as_of_unix_ms: i64) {
    if snapshot.verify_content_hash().is_err() {
        quality.quality = DiagnosticEvidenceQuality::Unknown;
        quality.reason_code = "invalid_evidence_content_hash".to_owned();
        return;
    }
    quality.observed_at = Some(snapshot.observed_at);
    quality.evidence_ids = vec![snapshot.evidence_id];
    let Some(elapsed) = as_of_unix_ms
        .checked_sub(snapshot.observed_at.timestamp_millis())
        .filter(|elapsed| *elapsed >= 0)
    else {
        quality.quality = DiagnosticEvidenceQuality::Unknown;
        quality.reason_code = "future_evidence_observation".to_owned();
        return;
    };
    let age = snapshot
        .freshness_seconds
        .checked_mul(1000)
        .and_then(|age| age.checked_add(elapsed as u64));
    let maximum = quality.max_age_seconds.checked_mul(1000);
    if age.zip(maximum).is_none_or(|(age, maximum)| age > maximum) {
        quality.quality = DiagnosticEvidenceQuality::Stale;
        quality.reason_code = "evidence_max_age_exceeded".to_owned();
        return;
    }
    match snapshot.coverage {
        CoverageStatus::Missing => {
            quality.quality = DiagnosticEvidenceQuality::Missing;
            quality.reason_code = "source_reported_missing".to_owned();
            return;
        }
        CoverageStatus::NotProductionVerified => {
            quality.quality = DiagnosticEvidenceQuality::Unsupported;
            quality.reason_code = "source_reported_unsupported".to_owned();
            return;
        }
        CoverageStatus::Available | CoverageStatus::Partial => {}
    }
    let point_in_window = match snapshot.source.as_str() {
        // These public read adapters cannot request historical state. An echoed
        // query window does not turn their current snapshot into a range read.
        "rocketmq-mcp" | "admin-query" => {
            let observed = snapshot.observed_at.timestamp_micros();
            if observed < snapshot.time_range.start.timestamp_micros()
                || observed > snapshot.time_range.end.timestamp_micros()
            {
                quality.quality = DiagnosticEvidenceQuality::Unknown;
                quality.reason_code = "instant_observation_outside_requested_window".to_owned();
                return;
            }
            true
        }
        // These adapters bind the actual source request to the requested window
        // (or its end for an instant Prometheus query). Later retrieval is valid.
        "prometheus" | "loki" | "tempo" => false,
        _ => {
            quality.quality = DiagnosticEvidenceQuality::Unknown;
            quality.reason_code = "source_temporal_contract_unknown".to_owned();
            return;
        }
    };
    let EvidenceContent::Inline(content) = &snapshot.content else {
        quality.quality = DiagnosticEvidenceQuality::Unknown;
        quality.reason_code = "referenced_content_not_materialized".to_owned();
        return;
    };
    if quality.fields.is_empty() {
        quality.quality = DiagnosticEvidenceQuality::Unknown;
        quality.reason_code = "compiled_field_contract_unknown".to_owned();
        return;
    }
    quality.missing_fields = quality
        .fields
        .iter()
        .filter(|field| {
            let value = field
                .path
                .split('.')
                .try_fold(content, |value, segment| value.get(segment));
            !value.is_some_and(|value| match field.kind {
                EvidenceFieldKind::Boolean => value.is_boolean(),
                EvidenceFieldKind::Number => value.as_f64().is_some_and(f64::is_finite),
                EvidenceFieldKind::Text => value.is_string(),
            })
        })
        .map(|field| field.path.clone())
        .collect();
    if !quality.missing_fields.is_empty() {
        quality.quality = DiagnosticEvidenceQuality::Partial;
        quality.reason_code = "required_fields_missing".to_owned();
    } else if snapshot.partial || snapshot.coverage == CoverageStatus::Partial {
        quality.quality = DiagnosticEvidenceQuality::Partial;
        quality.reason_code = "evidence_partial".to_owned();
    } else {
        quality.quality = DiagnosticEvidenceQuality::Available;
        quality.reason_code = if point_in_window {
            "instant_point_in_requested_window"
        } else {
            "compiled_fields_qualified"
        }
        .to_owned();
    }
}
