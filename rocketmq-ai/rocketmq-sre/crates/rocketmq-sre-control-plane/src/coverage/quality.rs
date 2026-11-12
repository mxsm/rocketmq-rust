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

use chrono::{DateTime, Utc};
use rocketmq_sre_contracts::{
    CapabilityFact, CapabilityFactStatus, CapabilityLayers, CapabilityQualityProjection, ClusterId, CoverageStatus,
    DiagnosticEvidenceQuality, DiagnosticPackQuality, DiagnosticRequirementQuality, DiagnosticScenarioCoverage,
    EvidenceContent, EvidenceExposure, EvidenceFieldContract, EvidenceFieldKind, EvidenceSnapshot, TenantId,
};
use rocketmq_sre_core::diagnostics::{DiagnosticPack, EvidenceRequirement};
use serde_json::Value;

use super::{SourceState, capability_source};

const SOURCE_MAX_AGE_SECONDS: i64 = 300;

pub(super) fn project(
    tenant_id: TenantId,
    cluster_id: Option<ClusterId>,
    now: DateTime<Utc>,
    packs: &[&dyn DiagnosticPack],
    sources: &BTreeMap<&str, &SourceState>,
    evidence: &[EvidenceSnapshot],
    partial: bool,
) -> CapabilityQualityProjection {
    let packs = packs
        .iter()
        .map(|pack| {
            let requirements = pack
                .required_evidence()
                .iter()
                .map(|requirement| (requirement, true))
                .chain(pack.optional_evidence().iter().map(|requirement| (requirement, false)))
                .map(|(requirement, required)| {
                    requirement_quality(
                        *pack,
                        requirement,
                        required,
                        tenant_id,
                        cluster_id,
                        now,
                        evidence,
                        partial,
                    )
                })
                .collect::<Vec<_>>();
            let configured = configured_fact(pack.required_evidence(), sources, now);
            let required = requirements
                .iter()
                .filter(|requirement| requirement.required)
                .collect::<Vec<_>>();
            let target = if required.is_empty() {
                fact(
                    CapabilityFactStatus::Unknown,
                    "required_evidence_contract_not_available",
                    "compiled_diagnostic_pack",
                    None,
                )
            } else if required
                .iter()
                .all(|requirement| requirement.quality == DiagnosticEvidenceQuality::Available)
            {
                fact(
                    CapabilityFactStatus::Present,
                    "bounded_target_sample_observed",
                    "stored_evidence",
                    required.iter().filter_map(|item| item.observed_at).max(),
                )
            } else if required
                .iter()
                .any(|requirement| requirement.quality == DiagnosticEvidenceQuality::Unknown)
            {
                fact(
                    CapabilityFactStatus::Unknown,
                    "target_evidence_not_qualified",
                    "stored_evidence",
                    None,
                )
            } else {
                fact(
                    CapabilityFactStatus::Absent,
                    "target_evidence_not_qualified",
                    "stored_evidence",
                    required.iter().filter_map(|item| item.observed_at).max(),
                )
            };
            DiagnosticPackQuality {
                pack_id: pack.qualified_id(),
                layers: CapabilityLayers {
                    registered: fact(
                        CapabilityFactStatus::Present,
                        "compiled_descriptor_registered",
                        "compiled_diagnostic_registry",
                        Some(now),
                    ),
                    handler: fact(
                        CapabilityFactStatus::Present,
                        "compiled_handler_registered",
                        "compiled_diagnostic_pack",
                        Some(now),
                    ),
                    configured,
                    target_observed: target,
                    test_verified: fact(
                        CapabilityFactStatus::Unknown,
                        "verification_record_not_provided",
                        "deployment_verification_record",
                        None,
                    ),
                    production_verified: fact(
                        CapabilityFactStatus::Unknown,
                        "production_verification_record_not_provided",
                        "deployment_verification_record",
                        None,
                    ),
                },
                requirements,
            }
        })
        .collect::<Vec<_>>();
    let scenarios = [
        ("backlog_store", ["consumer-lag.v2", "store-pressure.v1"]),
        ("send_latency_ha", ["send-latency.v1", "broker-ha.v1"]),
        ("routing_disconnect", ["namesrv-route.v1", "producer-connectivity.v1"]),
    ]
    .into_iter()
    .map(|(id, ids)| DiagnosticScenarioCoverage {
        id: id.to_owned(),
        pack_ids: ids.iter().map(|id| (*id).to_owned()).collect(),
        required_signal_ids: packs
            .iter()
            .filter(|pack| ids.contains(&pack.pack_id.as_str()))
            .flat_map(|pack| {
                pack.requirements
                    .iter()
                    .filter(|requirement| requirement.required)
                    .map(|requirement| format!("{}:{}", pack.pack_id, requirement.id))
            })
            .collect(),
    })
    .collect();
    CapabilityQualityProjection {
        schema_version: "rocketmq-sre.capability-quality.v1".to_owned(),
        tenant_id,
        cluster_id,
        investigation_id: None,
        revision_id: None,
        observed_at: now,
        execution_eligible: false,
        partial,
        packs,
        scenarios,
    }
}

fn configured_fact(
    requirements: &[EvidenceRequirement],
    sources: &BTreeMap<&str, &SourceState>,
    now: DateTime<Utc>,
) -> CapabilityFact {
    if requirements.is_empty() {
        return fact(
            CapabilityFactStatus::Unknown,
            "required_evidence_contract_not_available",
            "compiled_diagnostic_pack",
            None,
        );
    }
    let mut observed_at = None;
    for requirement in requirements {
        let Some(source) = sources.get(capability_source(requirement.source)) else {
            return fact(
                CapabilityFactStatus::Unknown,
                "connector_source_not_observed",
                "connector_heartbeat",
                None,
            );
        };
        let age = now.signed_duration_since(source.observed_at).num_seconds();
        if source.observed_at > now || !(0..=SOURCE_MAX_AGE_SECONDS).contains(&age) {
            return fact(
                CapabilityFactStatus::Unknown,
                "connector_source_state_stale",
                "connector_heartbeat",
                Some(source.observed_at),
            );
        }
        if matches!(source.status.as_str(), "missing" | "unsupported") {
            return fact(
                CapabilityFactStatus::Absent,
                "connector_source_not_enabled",
                "connector_heartbeat",
                Some(source.observed_at),
            );
        }
        if !matches!(source.status.as_str(), "queryable" | "degraded") {
            return fact(
                CapabilityFactStatus::Unknown,
                "connector_source_state_unknown",
                "connector_heartbeat",
                Some(source.observed_at),
            );
        }
        observed_at = Some(observed_at.map_or(source.observed_at, |previous: DateTime<Utc>| {
            previous.min(source.observed_at)
        }));
    }
    fact(
        CapabilityFactStatus::Present,
        "connector_source_configured",
        "connector_heartbeat",
        observed_at,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "scope, evaluation time, and bounded scan completeness are independent validation inputs"
)]
fn requirement_quality(
    pack: &dyn DiagnosticPack,
    requirement: &EvidenceRequirement,
    required: bool,
    tenant_id: TenantId,
    cluster_id: Option<ClusterId>,
    now: DateTime<Utc>,
    evidence: &[EvidenceSnapshot],
    partial: bool,
) -> DiagnosticRequirementQuality {
    let fields = pack.evidence_fields(requirement.key);
    let snapshot = evidence
        .iter()
        .filter(|snapshot| {
            Some(snapshot.cluster_id) == cluster_id
                && snapshot.tenant_id == tenant_id
                && capability_source(&snapshot.source) == capability_source(requirement.source)
                && snapshot.resource.starts_with(requirement.resource_prefix)
        })
        .max_by_key(|snapshot| (snapshot.observed_at, snapshot.evidence_id));
    let (quality, reason, missing_fields) = if let Some(snapshot) = snapshot {
        qualify(snapshot, &fields, pack.max_evidence_freshness_seconds(), now)
    } else if partial {
        (
            DiagnosticEvidenceQuality::Unknown,
            "evidence_scan_incomplete",
            Vec::new(),
        )
    } else {
        (
            DiagnosticEvidenceQuality::Missing,
            "evidence_not_observed",
            fields.iter().map(|field| field.path.clone()).collect(),
        )
    };
    DiagnosticRequirementQuality {
        id: requirement.key.to_owned(),
        source: capability_source(requirement.source).to_owned(),
        resource_prefix: requirement.resource_prefix.to_owned(),
        required,
        max_age_seconds: pack.max_evidence_freshness_seconds(),
        fields,
        quality,
        reason_code: reason.to_owned(),
        observed_at: snapshot.map(|snapshot| snapshot.observed_at),
        evidence_ids: snapshot.map(|snapshot| vec![snapshot.evidence_id]).unwrap_or_default(),
        missing_fields,
    }
}

fn qualify(
    snapshot: &EvidenceSnapshot,
    fields: &[EvidenceFieldContract],
    max_age: u64,
    now: DateTime<Utc>,
) -> (DiagnosticEvidenceQuality, &'static str, Vec<String>) {
    use DiagnosticEvidenceQuality as Quality;
    let age = now.signed_duration_since(snapshot.observed_at).num_seconds();
    if snapshot.observed_at > now {
        return (Quality::Unknown, "observation_in_future", Vec::new());
    }
    if snapshot.freshness_seconds.max(age as u64) > max_age {
        return (Quality::Stale, "evidence_age_exceeded", Vec::new());
    }
    if snapshot.coverage == CoverageStatus::Missing {
        return (Quality::Missing, "source_reported_missing", Vec::new());
    }
    if snapshot.coverage == CoverageStatus::NotProductionVerified {
        return (Quality::Unsupported, "source_not_verified", Vec::new());
    }
    let exposure_matches = match capability_source(&snapshot.source) {
        "rocketmq-mcp" => matches!(
            snapshot.exposure,
            EvidenceExposure::McpTool | EvidenceExposure::McpResource
        ),
        "admin-query" => snapshot.exposure == EvidenceExposure::AdminRpc,
        "prometheus" => snapshot.exposure == EvidenceExposure::PrometheusApi,
        "runtime" => snapshot.exposure == EvidenceExposure::RuntimeDiagnostics,
        "kubernetes" => snapshot.exposure == EvidenceExposure::KubernetesApi,
        _ => !matches!(
            snapshot.exposure,
            EvidenceExposure::Unknown | EvidenceExposure::Synthetic | EvidenceExposure::Unsupported
        ),
    };
    if !exposure_matches {
        return (Quality::Unknown, "transport_not_verified", Vec::new());
    }
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return (Quality::Unknown, "content_not_inline", Vec::new());
    };
    if fields.is_empty() {
        return (Quality::Unknown, "field_contract_not_available", Vec::new());
    }
    let missing = fields
        .iter()
        .filter(|field| !field_matches(content, field))
        .map(|field| field.path.clone())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return (Quality::Partial, "required_fields_missing", missing);
    }
    if snapshot.partial || snapshot.coverage != CoverageStatus::Available {
        return (Quality::Partial, "partial_evidence", Vec::new());
    }
    (Quality::Available, "required_fields_observed", Vec::new())
}

fn field_matches(content: &Value, field: &EvidenceFieldContract) -> bool {
    let value = field.path.split('.').try_fold(content, |value, key| value.get(key));
    match field.kind {
        EvidenceFieldKind::Boolean => value.is_some_and(Value::is_boolean),
        EvidenceFieldKind::Number => value.and_then(Value::as_f64).is_some_and(f64::is_finite),
        EvidenceFieldKind::Text => value
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
    }
}

fn fact(
    status: CapabilityFactStatus,
    reason: &str,
    source: &str,
    observed_at: Option<DateTime<Utc>>,
) -> CapabilityFact {
    CapabilityFact {
        status,
        reason_code: reason.to_owned(),
        source_ref: source.to_owned(),
        observed_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use rocketmq_sre_contracts::{CorrelationId, EvidenceQuery, QueryId, TimeRange, current_evidence_schema};
    use rocketmq_sre_core::diagnostics::full_registry;
    use serde_json::json;

    fn at() -> DateTime<Utc> {
        Utc.timestamp_opt(1_794_398_400, 0).single().unwrap()
    }
    fn snapshot(tenant: TenantId, cluster: ClusterId, observed: DateTime<Utc>, content: Value) -> EvidenceSnapshot {
        let mut evidence = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: tenant,
                cluster_id: cluster,
                source: "rocketmq-mcp".to_owned(),
                resource: "consumer-lag/group/orders".to_owned(),
                time_range: TimeRange::new(observed, observed).unwrap(),
            },
            current_evidence_schema(),
            observed,
            EvidenceContent::Inline(content),
        )
        .unwrap();
        evidence.exposure = EvidenceExposure::McpTool;
        evidence
    }
    fn complete() -> Value {
        json!({"total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,"consume_rate_per_sec":5,"produce_rate_per_sec":6})
    }
    fn source(cluster: ClusterId) -> SourceState {
        SourceState {
            cluster_id: cluster,
            source: "rocketmq-mcp".to_owned(),
            schema_major: 1,
            status: "queryable".to_owned(),
            limits: json!({}),
            last_success_at: None,
            latency_millis: None,
            freshness_seconds: None,
            observed_at: at(),
        }
    }

    #[test]
    fn source_queryability_and_registration_do_not_prove_field_sufficiency_or_verification() {
        let registry = full_registry().unwrap();
        let pack = registry.resolve("consumer-lag.v2").unwrap();
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let source = source(cluster);
        let states = BTreeMap::from([("rocketmq-mcp", &source)]);
        let evidence = snapshot(tenant, cluster, at(), json!({"total_lag":0,"consume_rate_per_sec":"0"}));
        let result = project(
            tenant,
            Some(cluster),
            at(),
            &[pack],
            &states,
            &[evidence.clone()],
            false,
        );
        let pack = &result.packs[0];
        let requirement = &pack.requirements[0];
        assert_eq!(pack.layers.configured.status, CapabilityFactStatus::Present);
        assert_eq!(pack.layers.target_observed.status, CapabilityFactStatus::Absent);
        assert_eq!(requirement.quality, DiagnosticEvidenceQuality::Partial);
        assert!(requirement.missing_fields.contains(&"consume_rate_per_sec".to_owned()));
        assert!(!requirement.missing_fields.contains(&"total_lag".to_owned()));
        assert_eq!(requirement.evidence_ids, vec![evidence.evidence_id]);
        assert_eq!(pack.layers.test_verified.status, CapabilityFactStatus::Unknown);
        assert_eq!(pack.layers.production_verified.status, CapabilityFactStatus::Unknown);
        assert!(!result.execution_eligible);
    }

    #[test]
    fn latest_observation_is_used_without_merging_fields_from_other_samples_or_scopes() {
        let registry = full_registry().unwrap();
        let pack = registry.resolve("consumer-lag.v2").unwrap();
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let old = snapshot(tenant, cluster, at() - Duration::seconds(20), complete());
        let latest = snapshot(tenant, cluster, at() - Duration::seconds(1), json!({"total_lag":1}));
        let foreign_cluster = snapshot(tenant, ClusterId::new(), at(), complete());
        let foreign_tenant = snapshot(TenantId::new(), cluster, at(), complete());
        let result = project(
            tenant,
            Some(cluster),
            at(),
            &[pack],
            &BTreeMap::new(),
            &[latest.clone(), foreign_tenant, foreign_cluster, old],
            false,
        );
        let requirement = &result.packs[0].requirements[0];
        assert_eq!(requirement.quality, DiagnosticEvidenceQuality::Partial);
        assert_eq!(requirement.evidence_ids, vec![latest.evidence_id]);
        assert_eq!(requirement.missing_fields.len(), 4);
    }

    #[test]
    fn stale_partial_future_and_unverified_samples_cannot_qualify_a_target() {
        let registry = full_registry().unwrap();
        let pack = registry.resolve("consumer-lag.v2").unwrap();
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let fields = pack.evidence_fields("consumer-lag");
        let age = pack.max_evidence_freshness_seconds();
        let mut evidence = snapshot(tenant, cluster, at(), complete());
        assert_eq!(
            qualify(&evidence, &fields, age, at()).0,
            DiagnosticEvidenceQuality::Available
        );
        evidence.partial = true;
        assert_eq!(
            qualify(&evidence, &fields, age, at()).0,
            DiagnosticEvidenceQuality::Partial
        );
        evidence.partial = false;
        evidence.exposure = EvidenceExposure::Synthetic;
        assert_eq!(
            qualify(&evidence, &fields, age, at()).0,
            DiagnosticEvidenceQuality::Unknown
        );
        evidence.exposure = EvidenceExposure::McpTool;
        assert_eq!(
            qualify(&evidence, &fields, age, at() + Duration::seconds(age as i64 + 1)).0,
            DiagnosticEvidenceQuality::Stale
        );
        assert_eq!(
            qualify(&evidence, &fields, age, at() - Duration::milliseconds(1)).0,
            DiagnosticEvidenceQuality::Unknown
        );
        evidence.freshness_seconds = age + 1;
        assert_eq!(
            qualify(&evidence, &fields, age, at()).0,
            DiagnosticEvidenceQuality::Stale
        );
    }

    #[test]
    fn an_incomplete_scan_is_unknown_rather_than_a_claim_that_evidence_does_not_exist() {
        let registry = full_registry().unwrap();
        let pack = registry.resolve("consumer-lag.v2").unwrap();
        let result = project(
            TenantId::new(),
            Some(ClusterId::new()),
            at(),
            &[pack],
            &BTreeMap::new(),
            &[],
            true,
        );
        assert_eq!(
            result.packs[0].requirements[0].quality,
            DiagnosticEvidenceQuality::Unknown
        );
        assert!(result.packs[0].requirements[0].evidence_ids.is_empty());
        let mut source = source(ClusterId::new());
        source.observed_at -= Duration::seconds(301);
        let states = BTreeMap::from([("rocketmq-mcp", &source)]);
        assert_eq!(
            configured_fact(pack.required_evidence(), &states, at()).status,
            CapabilityFactStatus::Unknown
        );
    }

    #[test]
    fn a_pack_without_declared_requirements_cannot_claim_target_or_configuration_presence() {
        use rocketmq_sre_contracts::SreContractError;
        use rocketmq_sre_core::diagnostics::{DiagnosticContext, FollowUpQuery, PackVersion, RuleMatch};

        #[derive(Debug)]
        struct NoRequirements;
        impl DiagnosticPack for NoRequirements {
            fn id(&self) -> &'static str {
                "empty-contract"
            }
            fn version(&self) -> PackVersion {
                PackVersion::new(1, 0, 0)
            }
            fn applicable_components(&self) -> &'static [&'static str] {
                &[]
            }
            fn required_evidence(&self) -> &'static [EvidenceRequirement] {
                &[]
            }
            fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
                &[]
            }
            fn rule_codes(&self) -> &'static [&'static str] {
                &[]
            }
            fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
                &[]
            }
            fn evaluate(&self, _: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, SreContractError> {
                Ok(Vec::new())
            }
        }
        let result = project(
            TenantId::new(),
            Some(ClusterId::new()),
            at(),
            &[&NoRequirements],
            &BTreeMap::new(),
            &[],
            false,
        );
        assert_eq!(
            result.packs[0].layers.target_observed.status,
            CapabilityFactStatus::Unknown
        );
        assert_eq!(result.packs[0].layers.configured.status, CapabilityFactStatus::Unknown);
        assert!(!result.execution_eligible);
    }
}
