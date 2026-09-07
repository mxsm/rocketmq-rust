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

/// Private mixed replay completion channel. Expected validation failures are
/// projected as [`crate::EvalRejection`] and never implement [`std::error::Error`].
pub(crate) enum ReplayFailure {
    Io { path: String, source: std::io::Error },
    Yaml { path: String, source: serde_yaml::Error },
    UnsupportedSchema,
    DuplicateFixture,
    UnknownFixture,
    ScenarioMismatch,
    EmptyDataset,
    EmptyPackRuns,
    InvalidEvidenceId,
    InvalidEvidence,
    EvidenceContract(rocketmq_sre_contracts::SreContractError),
    Uuid(sqlx::types::uuid::Error),
    Diagnostic(rocketmq_sre_contracts::SreContractError),
    RegistryRejected,
    DuplicateManifestEntry,
    NonDeterministic,
}

impl std::fmt::Debug for ReplayFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReplayFailure")
    }
}

/// Operational replay failure with its typed source intact.
#[derive(Debug, Error)]
pub(crate) enum ReplaySource {
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
    #[error("replay evidence violates its contract")]
    EvidenceContract(#[source] rocketmq_sre_contracts::SreContractError),
    #[error("replay identity is invalid")]
    Uuid(#[source] sqlx::types::uuid::Error),
    #[error("diagnostic evaluation contract failed")]
    Diagnostic(#[source] rocketmq_sre_contracts::SreContractError),
}

impl ReplayFailure {
    pub(crate) fn into_boundary<T>(self) -> Result<crate::EvalOutcome<T>, crate::EvalError> {
        let rejection = match self {
            Self::UnsupportedSchema => crate::EvalRejection::InvalidReplaySchema,
            Self::DuplicateFixture => crate::EvalRejection::DuplicateReplayFixture,
            Self::UnknownFixture => crate::EvalRejection::UnknownReplayFixture,
            Self::ScenarioMismatch => crate::EvalRejection::ReplayScenarioMismatch,
            Self::EmptyDataset | Self::EmptyPackRuns => crate::EvalRejection::EmptyReplayRun,
            Self::InvalidEvidenceId | Self::InvalidEvidence => crate::EvalRejection::InvalidReplayEvidence,
            Self::RegistryRejected => crate::EvalRejection::DiagnosticReplayRejected,
            Self::DuplicateManifestEntry => crate::EvalRejection::DuplicateReplayManifestEntry,
            Self::NonDeterministic => crate::EvalRejection::ReplayNonDeterministic,
            Self::EvidenceContract(source) if std::error::Error::source(&source).is_none() => {
                crate::EvalRejection::InvalidReplayEvidence
            }
            Self::Diagnostic(source) if std::error::Error::source(&source).is_none() => {
                crate::EvalRejection::DiagnosticReplayRejected
            }
            Self::Io { path, source } => {
                return Err(crate::EvalError::replay_source(ReplaySource::Io { path, source }));
            }
            Self::Yaml { path, source } => {
                return Err(crate::EvalError::replay_source(ReplaySource::Yaml { path, source }));
            }
            Self::EvidenceContract(source) => {
                return Err(crate::EvalError::replay_source(ReplaySource::EvidenceContract(source)));
            }
            Self::Uuid(source) => return Err(crate::EvalError::replay_source(ReplaySource::Uuid(source))),
            Self::Diagnostic(source) => {
                return Err(crate::EvalError::replay_source(ReplaySource::Diagnostic(source)));
            }
        };
        Ok(crate::EvalOutcome::Rejected(rejection))
    }
}

/// Loads and validates a manifest, quality config, and saved fixture collection.
///
/// # Errors
///
/// Returns an error when replay files cannot be read or decoded. Deterministic
/// dataset validation failures are returned as a closed [`crate::EvalOutcome`].
pub fn load_dataset(manifest_path: &Path) -> Result<crate::EvalOutcome<LoadedReplayDataset>, crate::EvalError> {
    match load_dataset_inner(manifest_path) {
        Ok(dataset) => Ok(crate::EvalOutcome::Completed(dataset)),
        Err(failure) => failure.into_boundary(),
    }
}

pub(crate) fn load_dataset_inner(manifest_path: &Path) -> Result<LoadedReplayDataset, ReplayFailure> {
    let manifest: ReplayDatasetManifest = read_yaml(manifest_path)?;
    validate_schema(&manifest.schema_version, DATASET_SCHEMA_VERSION)?;
    if manifest.fixtures.is_empty() {
        return Err(ReplayFailure::EmptyDataset);
    }
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
            return Err(ReplayFailure::EmptyPackRuns);
        }
        let fixture_id = fixture.id.clone();
        if fixtures.insert(fixture_id.clone(), fixture).is_some() {
            return Err(ReplayFailure::DuplicateFixture);
        }
    }

    let mut manifest_ids = BTreeSet::new();
    for entry in &manifest.fixtures {
        if !manifest_ids.insert(entry.fixture_id.clone()) {
            return Err(ReplayFailure::DuplicateManifestEntry);
        }
        let fixture = fixtures.get(&entry.fixture_id).ok_or(ReplayFailure::UnknownFixture)?;
        if fixture.scenario != entry.scenario {
            return Err(ReplayFailure::ScenarioMismatch);
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
/// Returns an error only for a source-bearing replay operation. Malformed
/// evidence and fail-closed pack evaluation return a closed outcome.
pub fn replay_fixture(fixture: &ReplayFixture) -> Result<crate::EvalOutcome<ReplayFixtureResult>, crate::EvalError> {
    match replay_fixture_inner(fixture) {
        Ok(result) => Ok(crate::EvalOutcome::Completed(result)),
        Err(failure) => failure.into_boundary(),
    }
}

pub(crate) fn replay_fixture_inner(fixture: &ReplayFixture) -> Result<ReplayFixtureResult, ReplayFailure> {
    let registry = full_registry().map_err(|_| ReplayFailure::RegistryRejected)?;
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
            .map_err(ReplayFailure::Diagnostic)?;
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

fn read_yaml<T>(path: &Path) -> Result<T, ReplayFailure>
where
    T: for<'de> Deserialize<'de>,
{
    let yaml = fs::read_to_string(path).map_err(|source| ReplayFailure::Io {
        path: path.display().to_string(),
        source,
    })?;
    serde_yaml::from_str(&yaml).map_err(|source| ReplayFailure::Yaml {
        path: path.display().to_string(),
        source,
    })
}

fn validate_schema(actual: &str, expected: &'static str) -> Result<(), ReplayFailure> {
    if actual == expected {
        Ok(())
    } else {
        let _ = expected;
        Err(ReplayFailure::UnsupportedSchema)
    }
}

fn seal_evidence(fixture_id: &str, saved: &ReplayEvidence) -> Result<EvidenceSnapshot, ReplayFailure> {
    let observed_at = Utc
        .timestamp_opt(FIXED_OBSERVED_AT_SECONDS, 0)
        .single()
        .ok_or(ReplayFailure::InvalidEvidence)?;
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: TenantId::from_str(FIXED_TENANT).map_err(ReplayFailure::Uuid)?,
        cluster_id: ClusterId::from_str(FIXED_CLUSTER).map_err(ReplayFailure::Uuid)?,
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
    .map_err(ReplayFailure::EvidenceContract)?;
    snapshot.evidence_id = EvidenceId::from_str(&saved.evidence_id).map_err(|_| ReplayFailure::InvalidEvidenceId)?;
    snapshot.freshness_seconds = saved.freshness_seconds;
    snapshot.partial = saved.partial;
    if saved.partial {
        snapshot.coverage = CoverageStatus::Partial;
    }
    Ok(snapshot)
}

fn fixed_time_range(observed_at: DateTime<Utc>, fixture_id: &str) -> Result<TimeRange, ReplayFailure> {
    let _ = fixture_id;
    TimeRange::new(observed_at, observed_at).map_err(ReplayFailure::EvidenceContract)
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
    use std::error::Error as _;

    use super::*;

    #[test]
    fn eval_facade_retains_replay_leaf_without_rendering_it() {
        let error = crate::EvalError::replay_source(ReplaySource::Io {
            path: "/private/replay.yml".to_owned(),
            source: std::io::Error::other("private replay token"),
        });

        let replay = error.source().expect("replay leaf");
        assert!(replay.is::<ReplaySource>());
        assert!(replay.source().is_some_and(|source| source.is::<std::io::Error>()));
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("private"));
            assert!(!rendered.contains("replay.yml"));
            assert!(!rendered.contains("token"));
        }
    }

    #[test]
    fn deterministic_replay_failures_are_closed_rejections() {
        let cases = [
            (
                ReplayFailure::UnsupportedSchema,
                crate::EvalRejection::InvalidReplaySchema,
            ),
            (
                ReplayFailure::DuplicateFixture,
                crate::EvalRejection::DuplicateReplayFixture,
            ),
            (
                ReplayFailure::UnknownFixture,
                crate::EvalRejection::UnknownReplayFixture,
            ),
            (
                ReplayFailure::ScenarioMismatch,
                crate::EvalRejection::ReplayScenarioMismatch,
            ),
            (ReplayFailure::EmptyDataset, crate::EvalRejection::EmptyReplayRun),
            (ReplayFailure::EmptyPackRuns, crate::EvalRejection::EmptyReplayRun),
            (
                ReplayFailure::DuplicateManifestEntry,
                crate::EvalRejection::DuplicateReplayManifestEntry,
            ),
            (
                ReplayFailure::NonDeterministic,
                crate::EvalRejection::ReplayNonDeterministic,
            ),
        ];

        for (failure, expected) in cases {
            assert_eq!(
                failure
                    .into_boundary::<()>()
                    .expect("deterministic replay failure must not become an error"),
                crate::EvalOutcome::Rejected(expected)
            );
        }
    }

    #[test]
    fn source_bearing_replay_contract_preserves_the_typed_chain() {
        let error = ReplayFailure::Diagnostic(rocketmq_sre_contracts::SreContractError::with_source(
            rocketmq_sre_contracts::PublicErrorCode::SourceUnavailable,
            std::io::Error::other("private replay source"),
        ))
        .into_boundary::<()>()
        .expect_err("source-bearing replay failure must remain operational");

        let replay = error.source().expect("replay source");
        assert!(replay.is::<ReplaySource>());
        let contract = replay.source().expect("contract source");
        assert!(contract.is::<rocketmq_sre_contracts::SreContractError>());
        assert!(contract.source().is_some_and(|source| source.is::<std::io::Error>()));
    }

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
