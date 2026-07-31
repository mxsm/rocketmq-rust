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

//! Saved-evidence replay types and deterministic dataset loading.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticFinding;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::FindingOutcome;
use rocketmq_sre_core::diagnostics::full_registry;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// Current checked-in replay dataset schema.
pub const DATASET_SCHEMA_VERSION: &str = "rocketmq.sre.replay-dataset.v1";
/// Current saved fixture collection schema.
pub const FIXTURE_SCHEMA_VERSION: &str = "rocketmq.sre.replay-fixtures.v1";
/// Current replay quality configuration schema.
pub const QUALITY_SCHEMA_VERSION: &str = "rocketmq.sre.replay-quality.v1";

const FIXED_TENANT: &str = "00000000-0000-4000-8100-000000000001";
const FIXED_CLUSTER: &str = "00000000-0000-4000-8200-000000000001";
const FIXED_OBSERVED_AT_SECONDS: i64 = 1_735_689_600;

/// Fixed quality thresholds used by the Phase 2 integration test.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayQualityConfig {
    pub schema_version: String,
    pub root_cause_top3_min: f64,
    pub high_confidence_threshold: f64,
    pub citation_coverage_min: f64,
    pub max_readonly_tool_calls: usize,
    pub mutation_calls_allowed: usize,
}

/// One fixed denominator entry in the replay dataset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayManifestEntry {
    pub fixture_id: String,
    pub scenario: String,
    pub expected_root_causes: Vec<String>,
    pub evaluable: bool,
}

/// Dataset routing document. Paths are relative to this manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayDatasetManifest {
    pub schema_version: String,
    pub fixture_file: PathBuf,
    pub quality_file: PathBuf,
    pub fixtures: Vec<ReplayManifestEntry>,
}

/// A stable timeline event stored alongside diagnostic Evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayTimelineEvent {
    pub at: String,
    pub kind: String,
    pub resource: String,
    pub summary: String,
}

/// One saved canonical Evidence input before envelope sealing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayEvidence {
    pub evidence_id: String,
    pub source: String,
    pub resource: String,
    pub content: Value,
    #[serde(default)]
    pub freshness_seconds: u64,
    #[serde(default)]
    pub partial: bool,
}

/// One deterministic pack evaluation in a scenario.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayPackRun {
    pub pack: String,
    pub evidence: Vec<ReplayEvidence>,
}

/// A complete replay scenario with saved Evidence and timeline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayFixture {
    pub id: String,
    pub scenario: String,
    pub description: String,
    pub timeline: Vec<ReplayTimelineEvent>,
    pub pack_runs: Vec<ReplayPackRun>,
}

/// Collection of checked-in replay scenarios.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayFixtureCollection {
    pub schema_version: String,
    pub fixtures: Vec<ReplayFixture>,
}

/// Tool-call category recorded by the rules-only evaluation harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolCallCategory {
    ReadOnly,
    Mutation,
    Model,
}

/// Minimal call ledger that proves replay stays read-only and rules-only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolCallRecorder {
    readonly_calls: usize,
    mutation_calls: usize,
    model_calls: usize,
}

impl ToolCallRecorder {
    /// Records one bounded call.
    pub const fn record(&mut self, category: ToolCallCategory) {
        match category {
            ToolCallCategory::ReadOnly => self.readonly_calls += 1,
            ToolCallCategory::Mutation => self.mutation_calls += 1,
            ToolCallCategory::Model => self.model_calls += 1,
        }
    }

    /// Returns the number of read-only evidence queries.
    #[must_use]
    pub const fn readonly_calls(&self) -> usize {
        self.readonly_calls
    }

    /// Returns the number of cluster mutation calls.
    #[must_use]
    pub const fn mutation_calls(&self) -> usize {
        self.mutation_calls
    }

    /// Returns the number of model calls.
    #[must_use]
    pub const fn model_calls(&self) -> usize {
        self.model_calls
    }
}

/// One ranked root-cause result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedRootCause {
    pub reason_code: String,
    pub confidence_percent: u8,
    pub supporting_evidence_ids: BTreeSet<EvidenceId>,
}

/// Result of replaying one saved scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayFixtureResult {
    pub fixture_id: String,
    pub scenario: String,
    pub statuses: Vec<DiagnosticStatus>,
    pub ranked_root_causes: Vec<RankedRootCause>,
    pub readonly_calls: usize,
    pub mutation_calls: usize,
    pub model_calls: usize,
}

impl ReplayFixtureResult {
    /// Highest deterministic confidence, or zero when required Evidence is missing.
    #[must_use]
    pub fn max_confidence_percent(&self) -> u8 {
        self.ranked_root_causes
            .first()
            .map_or(0, |finding| finding.confidence_percent)
    }
}

/// Loaded dataset and its resolved fixtures.
#[derive(Clone, Debug)]
pub struct LoadedReplayDataset {
    pub manifest: ReplayDatasetManifest,
    pub quality: ReplayQualityConfig,
    fixtures: BTreeMap<String, ReplayFixture>,
}

impl LoadedReplayDataset {
    /// Returns a fixture by its manifest identifier.
    #[must_use]
    pub fn fixture(&self, id: &str) -> Option<&ReplayFixture> {
        self.fixtures.get(id)
    }
}

/// Replay loading or execution failure.
#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("failed to read replay file `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid replay YAML in `{path}`: {source}")]
    Yaml {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("unsupported replay schema `{actual}`; expected `{expected}`")]
    UnsupportedSchema { expected: &'static str, actual: String },
    #[error("duplicate replay fixture `{0}`")]
    DuplicateFixture(String),
    #[error("manifest references unknown replay fixture `{0}`")]
    UnknownFixture(String),
    #[error("fixture `{fixture_id}` scenario differs between manifest and fixture")]
    ScenarioMismatch { fixture_id: String },
    #[error("fixture `{fixture_id}` has no deterministic pack runs")]
    EmptyPackRuns { fixture_id: String },
    #[error("invalid evidence id `{value}` in fixture `{fixture_id}`")]
    InvalidEvidenceId { fixture_id: String, value: String },
    #[error("failed to build Evidence for fixture `{fixture_id}`: {reason}")]
    InvalidEvidence { fixture_id: String, reason: String },
    #[error("diagnostic evaluation failed for fixture `{fixture_id}` and pack `{pack}`: {reason}")]
    Diagnostic {
        fixture_id: String,
        pack: String,
        reason: String,
    },
    #[error("duplicate manifest entry `{0}`")]
    DuplicateManifestEntry(String),
}

/// Loads and validates a manifest, quality config, and saved fixture collection.
///
/// # Errors
///
/// Returns an error when files are missing, schemas differ, identifiers are
/// duplicated, or the manifest and fixture scenarios do not match.
pub fn load_dataset(manifest_path: &Path) -> Result<LoadedReplayDataset, ReplayError> {
    let manifest: ReplayDatasetManifest = read_yaml(manifest_path)?;
    validate_schema(&manifest.schema_version, DATASET_SCHEMA_VERSION)?;
    let base = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let quality_path = base.join(&manifest.quality_file);
    let fixture_path = base.join(&manifest.fixture_file);
    let quality: ReplayQualityConfig = read_yaml(&quality_path)?;
    validate_schema(&quality.schema_version, QUALITY_SCHEMA_VERSION)?;
    let collection: ReplayFixtureCollection = read_yaml(&fixture_path)?;
    validate_schema(&collection.schema_version, FIXTURE_SCHEMA_VERSION)?;

    let mut fixtures = BTreeMap::new();
    for fixture in collection.fixtures {
        if fixture.pack_runs.is_empty() {
            return Err(ReplayError::EmptyPackRuns { fixture_id: fixture.id });
        }
        let fixture_id = fixture.id.clone();
        if fixtures.insert(fixture_id.clone(), fixture).is_some() {
            return Err(ReplayError::DuplicateFixture(fixture_id));
        }
    }

    let mut manifest_ids = BTreeSet::new();
    for entry in &manifest.fixtures {
        if !manifest_ids.insert(entry.fixture_id.clone()) {
            return Err(ReplayError::DuplicateManifestEntry(entry.fixture_id.clone()));
        }
        let fixture = fixtures
            .get(&entry.fixture_id)
            .ok_or_else(|| ReplayError::UnknownFixture(entry.fixture_id.clone()))?;
        if fixture.scenario != entry.scenario {
            return Err(ReplayError::ScenarioMismatch {
                fixture_id: entry.fixture_id.clone(),
            });
        }
    }

    Ok(LoadedReplayDataset {
        manifest,
        quality,
        fixtures,
    })
}

/// Replays one fixture through the compiled full diagnostic registry.
///
/// # Errors
///
/// Returns an error for malformed Evidence or a fail-closed pack evaluation.
pub fn replay_fixture(fixture: &ReplayFixture) -> Result<ReplayFixtureResult, ReplayError> {
    let registry = full_registry().map_err(|error| ReplayError::Diagnostic {
        fixture_id: fixture.id.clone(),
        pack: "registry".to_owned(),
        reason: error.to_string(),
    })?;
    let engine = DiagnosticEngine::new(registry);
    let mut recorder = ToolCallRecorder::default();
    let mut statuses = Vec::with_capacity(fixture.pack_runs.len());
    let mut findings = Vec::new();

    for pack_run in &fixture.pack_runs {
        let evidence = pack_run
            .evidence
            .iter()
            .map(|saved| {
                recorder.record(ToolCallCategory::ReadOnly);
                seal_evidence(&fixture.id, saved)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let report = engine
            .evaluate(&pack_run.pack, &evidence)
            .map_err(|error| ReplayError::Diagnostic {
                fixture_id: fixture.id.clone(),
                pack: pack_run.pack.clone(),
                reason: error.to_string(),
            })?;
        statuses.push(report.status);
        findings.extend(report.findings);
    }

    let ranked_root_causes = rank_findings(findings);
    Ok(ReplayFixtureResult {
        fixture_id: fixture.id.clone(),
        scenario: fixture.scenario.clone(),
        statuses,
        ranked_root_causes,
        readonly_calls: recorder.readonly_calls(),
        mutation_calls: recorder.mutation_calls(),
        model_calls: recorder.model_calls(),
    })
}

fn read_yaml<T>(path: &Path) -> Result<T, ReplayError>
where
    T: for<'de> Deserialize<'de>,
{
    let yaml = fs::read_to_string(path).map_err(|source| ReplayError::Io {
        path: path.display().to_string(),
        source,
    })?;
    serde_yaml::from_str(&yaml).map_err(|source| ReplayError::Yaml {
        path: path.display().to_string(),
        source,
    })
}

fn validate_schema(actual: &str, expected: &'static str) -> Result<(), ReplayError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ReplayError::UnsupportedSchema {
            expected,
            actual: actual.to_owned(),
        })
    }
}

fn seal_evidence(fixture_id: &str, saved: &ReplayEvidence) -> Result<EvidenceSnapshot, ReplayError> {
    let observed_at = Utc
        .timestamp_opt(FIXED_OBSERVED_AT_SECONDS, 0)
        .single()
        .ok_or_else(|| ReplayError::InvalidEvidence {
            fixture_id: fixture_id.to_owned(),
            reason: "fixed observation timestamp is invalid".to_owned(),
        })?;
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: TenantId::from_str(FIXED_TENANT).map_err(|error| ReplayError::InvalidEvidence {
            fixture_id: fixture_id.to_owned(),
            reason: error.to_string(),
        })?,
        cluster_id: ClusterId::from_str(FIXED_CLUSTER).map_err(|error| ReplayError::InvalidEvidence {
            fixture_id: fixture_id.to_owned(),
            reason: error.to_string(),
        })?,
        source: saved.source.clone(),
        resource: saved.resource.clone(),
        time_range: fixed_time_range(observed_at, fixture_id)?,
    };
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(saved.content.clone()),
    )
    .map_err(|error| ReplayError::InvalidEvidence {
        fixture_id: fixture_id.to_owned(),
        reason: error.to_string(),
    })?;
    snapshot.evidence_id = EvidenceId::from_str(&saved.evidence_id).map_err(|_| ReplayError::InvalidEvidenceId {
        fixture_id: fixture_id.to_owned(),
        value: saved.evidence_id.clone(),
    })?;
    snapshot.freshness_seconds = saved.freshness_seconds;
    snapshot.partial = saved.partial;
    if saved.partial {
        snapshot.coverage = CoverageStatus::Partial;
    }
    Ok(snapshot)
}

fn fixed_time_range(observed_at: DateTime<Utc>, fixture_id: &str) -> Result<TimeRange, ReplayError> {
    TimeRange::new(observed_at, observed_at).map_err(|error| ReplayError::InvalidEvidence {
        fixture_id: fixture_id.to_owned(),
        reason: error.to_string(),
    })
}

fn rank_findings(findings: Vec<DiagnosticFinding>) -> Vec<RankedRootCause> {
    let mut ranked = findings
        .into_iter()
        .filter(|finding| finding.outcome == FindingOutcome::Fault)
        .map(|finding| RankedRootCause {
            reason_code: finding.reason_code,
            confidence_percent: finding.confidence.percent,
            supporting_evidence_ids: finding
                .supporting_evidence
                .into_iter()
                .map(|citation| citation.evidence_id)
                .collect(),
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .confidence_percent
            .cmp(&left.confidence_percent)
            .then_with(|| left.reason_code.cmp(&right.reason_code))
    });
    ranked.dedup_by(|left, right| left.reason_code == right.reason_code);
    ranked
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_distinguishes_read_model_and_mutation_calls() {
        let mut recorder = ToolCallRecorder::default();
        recorder.record(ToolCallCategory::ReadOnly);
        recorder.record(ToolCallCategory::Model);
        recorder.record(ToolCallCategory::Mutation);

        assert_eq!(recorder.readonly_calls(), 1);
        assert_eq!(recorder.model_calls(), 1);
        assert_eq!(recorder.mutation_calls(), 1);
    }
}
