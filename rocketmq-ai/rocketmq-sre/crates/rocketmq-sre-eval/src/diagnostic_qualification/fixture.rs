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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_core::diagnostics::full_pack_ids;
use rocketmq_sre_core::diagnostics::full_registry;
use serde::Deserialize;
use serde_json::Value;

use super::model::DiagnosticQualificationError;
use super::model::DiagnosticQualificationManifest;
use super::model::QUALIFICATION_PACK_COUNT;
use super::model::QUALIFICATION_SCENARIO_COUNT;
use super::model::QUALIFICATION_SCHEMA;
use super::model::QualificationEvidenceRequirement;
use super::model::QualificationExpectation;
use super::model::QualificationScenario;
use super::model::QualifiedDiagnosticPack;

const WAVE_A_FIXTURES: &[(&str, &str)] = &[
    (
        "tests/fixtures/diagnostics/cluster-topology.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/cluster-topology.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/cluster-topology.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/cluster-topology.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/cluster-topology.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/cluster-topology.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-lag.v2/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-lag.v2/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-lag.v2/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-lag.v2/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-lag.v2/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-lag.v2/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-runtime.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-runtime.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-runtime.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-runtime.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/consumer-runtime.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/consumer-runtime.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/producer-connectivity.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/producer-connectivity.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/producer-connectivity.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/producer-connectivity.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/producer-connectivity.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/producer-connectivity.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/broker-health.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/broker-health.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/broker-health.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/broker-health.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/broker-health.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/broker-health.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/message-path.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/message-path.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/message-path.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/message-path.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/message-path.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/message-path.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/telemetry-pipeline.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/telemetry-pipeline.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/telemetry-pipeline.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/missing.json"),
    ),
    (
        "tests/fixtures/diagnostics/deployment-drift.v1/normal.json",
        include_str!("../../../../tests/fixtures/diagnostics/deployment-drift.v1/normal.json"),
    ),
    (
        "tests/fixtures/diagnostics/deployment-drift.v1/fault.json",
        include_str!("../../../../tests/fixtures/diagnostics/deployment-drift.v1/fault.json"),
    ),
    (
        "tests/fixtures/diagnostics/deployment-drift.v1/missing.json",
        include_str!("../../../../tests/fixtures/diagnostics/deployment-drift.v1/missing.json"),
    ),
];
const WAVE_B_CATALOG_PATH: &str = "tests/fixtures/diagnostics/wave-b/catalog.v1.json";
const WAVE_C_CATALOG_PATH: &str = "tests/fixtures/diagnostics/wave-c/catalog.v1.json";
const WAVE_B_CATALOG: &str = include_str!("../../../../tests/fixtures/diagnostics/wave-b/catalog.v1.json");
const WAVE_C_CATALOG: &str = include_str!("../../../../tests/fixtures/diagnostics/wave-c/catalog.v1.json");
const MAX_FIXTURE_CONTENT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize)]
struct RawFixture {
    pack: String,
    scenario: QualificationScenario,
    expected_status: String,
    #[serde(default)]
    expected_reason_codes: Vec<String>,
    evidence: Vec<RawEvidence>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawEvidence {
    source: String,
    resource: String,
    #[serde(default = "available")]
    coverage: CoverageStatus,
    #[serde(default)]
    partial: bool,
    content: Value,
}

#[derive(Deserialize)]
struct RawFixtureCatalog {
    fixtures: Vec<RawFixture>,
}

pub(super) struct MaterializedPackScenario {
    pub(super) expected: QualificationExpectation,
    pub(super) evidence: Vec<EvidenceSnapshot>,
}

/// Builds the canonical manifest directly from the compiled registry and the
/// checked-in fixture assets.
pub fn generated_manifest() -> Result<DiagnosticQualificationManifest, DiagnosticQualificationError> {
    let fixtures = raw_fixtures()?;
    let expected = expected_by_pack(&fixtures)?;
    let registry = full_registry().map_err(|error| {
        DiagnosticQualificationError::InvalidManifest(format!("built-in diagnostic registry is invalid: {error}"))
    })?;
    let mut packs = Vec::new();
    for id in full_pack_ids() {
        let pack = registry.resolve(&id).ok_or_else(|| {
            DiagnosticQualificationError::InvalidManifest(format!("registered pack `{id}` cannot be resolved"))
        })?;
        let scenarios = expected.get(&id).cloned().ok_or_else(|| {
            DiagnosticQualificationError::InvalidManifest(format!("pack `{id}` has no qualification scenarios"))
        })?;
        packs.push(QualifiedDiagnosticPack {
            inspection_template: inspection_template_for(&id)?.to_owned(),
            id,
            required_evidence: pack
                .required_evidence()
                .iter()
                .map(|requirement| QualificationEvidenceRequirement {
                    key: requirement.key.to_owned(),
                    source: requirement.source.to_owned(),
                    resource_prefix: requirement.resource_prefix.to_owned(),
                })
                .collect(),
            scenarios,
        });
    }
    let manifest = DiagnosticQualificationManifest {
        schema_version: QUALIFICATION_SCHEMA.to_owned(),
        operating_mode: "rules_only".to_owned(),
        model_provider_network_calls: false,
        target_mutation_calls: 0,
        execution_eligible: false,
        pack_count: QUALIFICATION_PACK_COUNT,
        scenario_count: QUALIFICATION_SCENARIO_COUNT,
        pack_scenario_count: QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT,
        inspection_templates: vec![
            "cluster_health".to_owned(),
            "consumer".to_owned(),
            "broker".to_owned(),
            "telemetry".to_owned(),
            "producer_consumer".to_owned(),
        ],
        fixture_assets: WAVE_A_FIXTURES
            .iter()
            .map(|(path, _)| (*path).to_owned())
            .chain([WAVE_B_CATALOG_PATH.to_owned(), WAVE_C_CATALOG_PATH.to_owned()])
            .collect(),
        packs,
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// Loads the committed qualification manifest and rejects generator drift.
pub fn load_committed_manifest(path: &Path) -> Result<DiagnosticQualificationManifest, DiagnosticQualificationError> {
    let raw = fs::read_to_string(path).map_err(|source| DiagnosticQualificationError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest: DiagnosticQualificationManifest = serde_json::from_str(&raw)?;
    validate_manifest(&manifest)?;
    if manifest != generated_manifest()? {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "committed manifest differs from the compiled registry or fixture assets".to_owned(),
        ));
    }
    Ok(manifest)
}

/// Writes the stable generated manifest for deliberate artifact updates.
pub fn write_generated_manifest(path: &Path) -> Result<(), DiagnosticQualificationError> {
    let manifest = generated_manifest()?;
    let mut encoded = serde_json::to_vec_pretty(&manifest)?;
    encoded.push(b'\n');
    fs::write(path, encoded).map_err(|source| DiagnosticQualificationError::Io {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn materialize_pack_scenario(
    pack_id: &str,
    scenario: QualificationScenario,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    observed_at: DateTime<Utc>,
) -> Result<MaterializedPackScenario, DiagnosticQualificationError> {
    let fixture = raw_fixtures()?
        .into_iter()
        .find(|fixture| fixture.pack == pack_id && fixture.scenario == scenario)
        .ok_or_else(|| {
            DiagnosticQualificationError::InvalidFixture(format!(
                "pack `{pack_id}` has no `{}` qualification fixture",
                scenario.as_str()
            ))
        })?;
    let expected = expectation(&fixture);
    let mut evidence = Vec::new();
    for item in fixture.evidence {
        validate_evidence_fixture(&item)?;
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: item.source,
            resource: item.resource,
            time_range: TimeRange::new(observed_at, observed_at)
                .map_err(|error| DiagnosticQualificationError::InvalidFixture(error.to_string()))?,
        };
        let mut snapshot = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            observed_at,
            EvidenceContent::Inline(item.content),
        )
        .map_err(|error| DiagnosticQualificationError::InvalidFixture(error.to_string()))?;
        snapshot.coverage = item.coverage;
        snapshot.partial = item.partial;
        snapshot.content_hash = snapshot
            .compute_content_hash()
            .map_err(|error| DiagnosticQualificationError::InvalidFixture(error.to_string()))?;
        evidence.push(snapshot);
    }
    Ok(MaterializedPackScenario { expected, evidence })
}

fn inspection_template_for(pack_id: &str) -> Result<&'static str, DiagnosticQualificationError> {
    match pack_id {
        "cluster-topology.v1"
        | "deployment-drift.v1"
        | "namesrv-route.v1"
        | "controller-ha.v1"
        | "upgrade-readiness.v1"
        | "capacity-runway.v1"
        | "security-posture.v1"
        | "change-regression.v1" => Ok("cluster_health"),
        "consumer-lag.v2"
        | "consumer-runtime.v1"
        | "retry-dlq.v1"
        | "transaction-message.v1"
        | "pop-revive.v1"
        | "timer-backlog.v1"
        | "queue-hotspot.v1"
        | "topic-subscription-config.v1" => Ok("consumer"),
        "broker-health.v1"
        | "store-pressure.v1"
        | "store-integrity.v1"
        | "rocksdb-health.v1"
        | "tiered-store.v1"
        | "broker-ha.v1"
        | "static-topic-route.v1"
        | "cold-data-flow.v1"
        | "dr-readiness.v1" => Ok("broker"),
        "telemetry-pipeline.v1"
        | "runtime-saturation.v1"
        | "send-latency.v1"
        | "proxy-connectivity.v1"
        | "auth-failure.v1" => Ok("telemetry"),
        "producer-connectivity.v1" | "message-path.v1" => Ok("producer_consumer"),
        _ => Err(DiagnosticQualificationError::InvalidManifest(format!(
            "pack `{pack_id}` has no inspection template"
        ))),
    }
}

fn raw_fixtures() -> Result<Vec<RawFixture>, DiagnosticQualificationError> {
    let mut fixtures = WAVE_A_FIXTURES
        .iter()
        .map(|(path, raw)| {
            serde_json::from_str(raw)
                .map_err(|error| DiagnosticQualificationError::InvalidFixture(format!("`{path}` is invalid: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (path, raw) in [
        (WAVE_B_CATALOG_PATH, WAVE_B_CATALOG),
        (WAVE_C_CATALOG_PATH, WAVE_C_CATALOG),
    ] {
        let catalog: RawFixtureCatalog = serde_json::from_str(raw)
            .map_err(|error| DiagnosticQualificationError::InvalidFixture(format!("`{path}` is invalid: {error}")))?;
        fixtures.extend(catalog.fixtures);
    }
    for fixture in &fixtures {
        for evidence in &fixture.evidence {
            validate_evidence_fixture(evidence)?;
        }
    }
    Ok(fixtures)
}

fn expected_by_pack(
    fixtures: &[RawFixture],
) -> Result<BTreeMap<String, Vec<QualificationExpectation>>, DiagnosticQualificationError> {
    let known = full_pack_ids().into_iter().collect::<BTreeSet<_>>();
    let mut result = BTreeMap::<String, Vec<QualificationExpectation>>::new();
    for fixture in fixtures {
        if !known.contains(&fixture.pack) {
            return Err(DiagnosticQualificationError::InvalidFixture(format!(
                "fixture references unknown pack `{}`",
                fixture.pack
            )));
        }
        result
            .entry(fixture.pack.clone())
            .or_default()
            .push(expectation(fixture));
    }
    for (pack, scenarios) in &mut result {
        scenarios.sort_by_key(|item| item.scenario);
        let actual = scenarios.iter().map(|item| item.scenario).collect::<BTreeSet<_>>();
        if actual != QualificationScenario::ALL.into_iter().collect() || scenarios.len() != QUALIFICATION_SCENARIO_COUNT
        {
            return Err(DiagnosticQualificationError::InvalidFixture(format!(
                "pack `{pack}` must define exactly normal, fault, and missing scenarios"
            )));
        }
    }
    if result.len() != QUALIFICATION_PACK_COUNT || result.keys().cloned().collect::<BTreeSet<_>>() != known {
        return Err(DiagnosticQualificationError::InvalidFixture(
            "fixture catalog does not cover all 32 built-in packs".to_owned(),
        ));
    }
    Ok(result)
}

fn expectation(fixture: &RawFixture) -> QualificationExpectation {
    let mut reason_codes = fixture.expected_reason_codes.clone();
    reason_codes.sort();
    QualificationExpectation {
        scenario: fixture.scenario,
        expected_status: fixture.expected_status.clone(),
        expected_reason_codes: reason_codes,
        partial: fixture.scenario == QualificationScenario::Missing,
        execution_eligible: false,
    }
}

fn validate_manifest(manifest: &DiagnosticQualificationManifest) -> Result<(), DiagnosticQualificationError> {
    if manifest.schema_version != QUALIFICATION_SCHEMA
        || manifest.operating_mode != "rules_only"
        || manifest.model_provider_network_calls
        || manifest.target_mutation_calls != 0
        || manifest.execution_eligible
    {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "qualification must be rules-only, mutation-zero, and execution-ineligible".to_owned(),
        ));
    }
    if manifest.pack_count != QUALIFICATION_PACK_COUNT
        || manifest.scenario_count != QUALIFICATION_SCENARIO_COUNT
        || manifest.pack_scenario_count != QUALIFICATION_PACK_COUNT * QUALIFICATION_SCENARIO_COUNT
        || manifest.packs.len() != QUALIFICATION_PACK_COUNT
    {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "qualification cardinality must be 32 packs by 3 scenarios".to_owned(),
        ));
    }
    let known = full_pack_ids().into_iter().collect::<BTreeSet<_>>();
    let actual = manifest
        .packs
        .iter()
        .map(|pack| pack.id.clone())
        .collect::<BTreeSet<_>>();
    if known != actual {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "qualification pack IDs differ from the compiled registry".to_owned(),
        ));
    }
    for pack in &manifest.packs {
        if !manifest.inspection_templates.contains(&pack.inspection_template) {
            return Err(DiagnosticQualificationError::InvalidManifest(format!(
                "pack `{}` references unknown inspection template `{}`",
                pack.id, pack.inspection_template
            )));
        }
        if pack.required_evidence.is_empty() {
            return Err(DiagnosticQualificationError::InvalidManifest(format!(
                "pack `{}` has no required Evidence contract",
                pack.id
            )));
        }
        let scenarios = pack.scenarios.iter().map(|item| item.scenario).collect::<BTreeSet<_>>();
        if scenarios != QualificationScenario::ALL.into_iter().collect()
            || pack.scenarios.len() != QUALIFICATION_SCENARIO_COUNT
        {
            return Err(DiagnosticQualificationError::InvalidManifest(format!(
                "pack `{}` does not define exactly three scenarios",
                pack.id
            )));
        }
    }
    Ok(())
}

fn validate_evidence_fixture(evidence: &RawEvidence) -> Result<(), DiagnosticQualificationError> {
    let encoded = serde_json::to_vec(&evidence.content)?;
    if encoded.len() > MAX_FIXTURE_CONTENT_BYTES {
        return Err(DiagnosticQualificationError::InvalidFixture(format!(
            "fixture `{}` exceeds the {MAX_FIXTURE_CONTENT_BYTES}-byte bound",
            evidence.resource
        )));
    }
    validate_safe_value(&evidence.content).map_err(|field| {
        DiagnosticQualificationError::InvalidFixture(format!(
            "fixture `{}` contains forbidden sensitive field or value `{field}`",
            evidence.resource
        ))
    })
}

pub(super) fn validate_safe_value(value: &Value) -> Result<(), String> {
    const FORBIDDEN_KEYS: &[&str] = &[
        "access_token",
        "authorization",
        "body",
        "client_ip",
        "client_secret",
        "message_body",
        "payload",
        "private_key",
        "refresh_token",
        "secret_key",
        "tls_key",
    ];
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key.to_ascii_lowercase();
                if FORBIDDEN_KEYS.contains(&normalized.as_str()) {
                    return Err(key.clone());
                }
                validate_safe_value(child)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                validate_safe_value(child)?;
            }
        }
        Value::String(text) => {
            let normalized = text.to_ascii_lowercase();
            if normalized.contains("-----begin private key-----")
                || normalized.starts_with("bearer ")
                || normalized.starts_with("sk-")
            {
                return Err("credential-like-value".to_owned());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

const fn available() -> CoverageStatus {
    CoverageStatus::Available
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed_manifest_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/qualification/diagnostic-packs.v1.json")
    }

    #[test]
    fn generated_manifest_covers_all_pack_scenarios() {
        let manifest = generated_manifest().expect("qualification manifest");

        assert_eq!(manifest.pack_count, 32);
        assert_eq!(manifest.pack_scenario_count, 96);
        assert_eq!(manifest.packs.len(), 32);
        assert!(
            manifest
                .packs
                .iter()
                .all(|pack| manifest.inspection_templates.contains(&pack.inspection_template))
        );
    }

    #[test]
    fn materialized_pack_scenario_is_isolated() {
        let materialized = materialize_pack_scenario(
            "consumer-runtime.v1",
            QualificationScenario::Missing,
            TenantId::new(),
            ClusterId::new(),
            Utc::now(),
        )
        .expect("isolated fixture");

        assert_eq!(materialized.expected.scenario, QualificationScenario::Missing);
        assert!(materialized.expected.partial);
        assert!(
            materialized
                .evidence
                .iter()
                .all(|evidence| evidence.resource.starts_with("consumer-runtime/"))
        );
    }

    #[test]
    fn committed_manifest_matches_registry_and_fixtures() {
        load_committed_manifest(&committed_manifest_path()).expect("committed qualification manifest");
    }

    #[test]
    fn manifest_rejects_unknown_pack_and_missing_scenario() {
        let mut unknown = generated_manifest().expect("qualification manifest");
        unknown.packs[0].id = "unknown-pack.v1".to_owned();
        assert!(validate_manifest(&unknown).is_err());

        let mut missing = generated_manifest().expect("qualification manifest");
        missing.packs[0].scenarios.pop();
        assert!(validate_manifest(&missing).is_err());
    }

    #[test]
    fn fixtures_are_bounded_and_sensitive_field_free() {
        let fixtures = raw_fixtures().expect("all fixtures");

        assert_eq!(fixtures.len(), 96);
        for fixture in fixtures {
            for evidence in fixture.evidence {
                validate_evidence_fixture(&evidence).expect("safe bounded fixture");
            }
        }
    }
}
