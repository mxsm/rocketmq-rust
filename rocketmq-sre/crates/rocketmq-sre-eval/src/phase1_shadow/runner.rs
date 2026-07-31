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
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticReport;
use rocketmq_sre_core::diagnostics::wave_a_registry;
use rocketmq_sre_model_gateway::ModelInvocationOutcome;
use serde::Serialize;

use super::ProviderMode;
use super::ScenarioCase;
use super::ScenarioClass;
use super::ShadowEvalError;
use super::ShadowManifest;
use super::ShadowModelSynthesis;
use super::build_model_request;
use super::fixture::load_diagnostic_fixture;
use super::fixture::status_name;
use super::load_shadow_manifest;
use super::provider::invoke_provider;
use super::validate_model_response;

/// One successful normal, fault, or missing-evidence replay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ScenarioResult {
    pub scenario_id: String,
    pub pack: String,
    pub class: ScenarioClass,
    pub diagnostic_status: String,
    pub reason_codes: BTreeSet<String>,
    pub finding_count: usize,
    pub maximum_confidence_percent: u8,
    pub citation_count: usize,
    pub missing_required_evidence_count: usize,
    pub missing_optional_evidence_count: usize,
    pub elapsed_micros: u128,
    pub model_cost_microusd: u64,
    pub model_mode: String,
    pub execution_eligible: bool,
    pub mutation_calls: u64,
    pub executor_calls: u64,
}

/// Auditable result for all 24 Phase 01 Wave A offline executions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ShadowSuiteSummary {
    pub schema_version: String,
    pub provider_mode: ProviderMode,
    pub pack_count: usize,
    pub fixture_count: usize,
    pub class_counts: BTreeMap<String, usize>,
    pub model_completed_runs: usize,
    pub rules_only_runs: usize,
    pub provider_calls: usize,
    pub total_missing_required_evidence: usize,
    pub total_elapsed_micros: u128,
    pub total_model_cost_microusd: u64,
    pub error_count: usize,
    pub mutation_calls: u64,
    pub executor_calls: u64,
    pub executor_connected: bool,
    pub passed: bool,
    pub results: Vec<ScenarioResult>,
}

/// Offline evaluator that has no Connector, MCP, RocketMQ, or Executor client.
pub struct ShadowHarness {
    manifest: ShadowManifest,
    fixtures_root: PathBuf,
    engine: DiagnosticEngine,
}

impl ShadowHarness {
    /// Loads a validated manifest and the built-in Wave A pack registry.
    ///
    /// # Errors
    ///
    /// Returns manifest, policy, path, or registry validation failures.
    pub fn load(manifest_path: &Path, fixtures_root: &Path) -> Result<Self, ShadowEvalError> {
        let manifest = load_shadow_manifest(manifest_path)?;
        let engine = DiagnosticEngine::new(
            wave_a_registry().map_err(|error| ShadowEvalError::InvalidManifest(error.to_string()))?,
        );
        Ok(Self {
            manifest,
            fixtures_root: fixtures_root.to_path_buf(),
            engine,
        })
    }

    /// Returns the immutable shadow manifest.
    #[must_use]
    pub const fn manifest(&self) -> &ShadowManifest {
        &self.manifest
    }

    /// Runs all Wave A normal, fault, and missing-evidence fixtures.
    ///
    /// # Errors
    ///
    /// Fails closed before fixture access when cluster scope differs, and on
    /// any diagnostic, citation, provider, or expected-result mismatch.
    pub fn run(
        &self,
        provider_mode: ProviderMode,
        requested_cluster: ClusterId,
    ) -> Result<ShadowSuiteSummary, ShadowEvalError> {
        let suite_started_at = Instant::now();
        if requested_cluster != self.manifest.cluster_id {
            return Err(ShadowEvalError::ClusterScopeMismatch {
                requested: requested_cluster.to_string(),
                authorized: self.manifest.cluster_id.to_string(),
            });
        }
        self.manifest.policy.validate()?;

        let mut results = Vec::with_capacity(24);
        let mut class_counts = BTreeMap::new();
        let mut model_completed_runs = 0;
        let mut rules_only_runs = 0;
        let mut provider_calls = 0;

        for scenario in &self.manifest.scenarios {
            for case in &scenario.cases {
                let result = self.run_case(scenario, case, provider_mode)?;
                *class_counts.entry(class_name(case.class).to_owned()).or_insert(0) += 1;
                if result.model_mode == "mock" {
                    model_completed_runs += 1;
                } else {
                    rules_only_runs += 1;
                }
                if provider_mode != ProviderMode::RulesOnly {
                    provider_calls += 1;
                }
                results.push(result);
            }
        }

        let mutation_calls = results.iter().map(|result| result.mutation_calls).sum();
        let executor_calls = results.iter().map(|result| result.executor_calls).sum();
        let total_missing_required_evidence = results
            .iter()
            .map(|result| result.missing_required_evidence_count)
            .sum();
        let total_model_cost_microusd = results.iter().map(|result| result.model_cost_microusd).sum();
        Ok(ShadowSuiteSummary {
            schema_version: self.manifest.schema_version.clone(),
            provider_mode,
            pack_count: self.manifest.scenarios.len(),
            fixture_count: results.len(),
            class_counts,
            model_completed_runs,
            rules_only_runs,
            provider_calls,
            total_missing_required_evidence,
            total_elapsed_micros: suite_started_at.elapsed().as_micros(),
            total_model_cost_microusd,
            error_count: 0,
            mutation_calls,
            executor_calls,
            executor_connected: self.manifest.policy.executor_connected,
            passed: results.len() == 24 && mutation_calls == 0 && executor_calls == 0,
            results,
        })
    }

    fn run_case(
        &self,
        scenario: &super::ScenarioDefinition,
        case: &ScenarioCase,
        provider_mode: ProviderMode,
    ) -> Result<ScenarioResult, ShadowEvalError> {
        let started_at = Instant::now();
        let fixture_path = self.fixtures_root.join(&case.fixture);
        let fixture = load_diagnostic_fixture(&fixture_path, self.manifest.tenant_id, self.manifest.cluster_id)?;
        if fixture.pack != scenario.pack {
            return Err(fixture_mismatch(
                &fixture_path,
                format!(
                    "fixture pack `{}` differs from manifest pack `{}`",
                    fixture.pack, scenario.pack
                ),
            ));
        }
        if fixture.scenario != class_name(case.class) {
            return Err(fixture_mismatch(
                &fixture_path,
                format!(
                    "fixture scenario `{}` differs from manifest class `{}`",
                    fixture.scenario,
                    class_name(case.class)
                ),
            ));
        }
        if status_name(fixture.expected_status) != case.expected_status {
            return Err(fixture_mismatch(
                &fixture_path,
                format!(
                    "fixture expected status `{}` differs from manifest `{}`",
                    status_name(fixture.expected_status),
                    case.expected_status
                ),
            ));
        }

        let report = self.engine.evaluate(&scenario.pack, &fixture.evidence)?;
        let actual_status = status_name(report.status);
        if report.status != fixture.expected_status {
            return Err(fixture_mismatch(
                &fixture_path,
                format!(
                    "diagnostic status `{actual_status}` differs from fixture `{}`",
                    status_name(fixture.expected_status)
                ),
            ));
        }
        let reason_codes = report
            .findings
            .iter()
            .map(|finding| finding.reason_code.clone())
            .collect::<BTreeSet<_>>();
        if reason_codes != fixture.expected_reason_codes {
            return Err(fixture_mismatch(
                &fixture_path,
                format!(
                    "reason codes {reason_codes:?} differ from fixture {:?}",
                    fixture.expected_reason_codes
                ),
            ));
        }

        let authorized = fixture
            .evidence
            .iter()
            .map(|snapshot| snapshot.evidence_id)
            .collect::<BTreeSet<_>>();
        let citations = report_citations(&report);
        super::validate_citations(&authorized, &citations)?;
        let response_content = serde_json::to_string(&ShadowModelSynthesis {
            summary: format!("{} replay completed with status {actual_status}", scenario.pack),
            citations: citations.clone(),
            read_only_recommendations: vec!["Review cited Evidence and the validated Wave A runbook.".to_owned()],
            execution_eligible: false,
        })
        .map_err(|error| ShadowEvalError::InvalidSynthesis(error.to_string()))?;
        let request = build_model_request(&scenario.description, &self.manifest.policy);
        if request.tools.iter().any(|tool| tool.mutates_cluster) {
            return Err(ShadowEvalError::UnsafePolicy(
                "model-visible tool unexpectedly mutates the cluster".to_owned(),
            ));
        }
        let outcome = invoke_provider(provider_mode, &request, response_content)?;
        let model_mode = match outcome {
            ModelInvocationOutcome::Completed(result) => {
                let synthesis = validate_model_response(&result.response, &authorized, &self.manifest.policy)?;
                if result.diagnosis_selection.execution_eligible || synthesis.execution_eligible {
                    return Err(ShadowEvalError::UnsafePolicy(
                        "mock provider made a shadow result executable".to_owned(),
                    ));
                }
                "mock"
            }
            ModelInvocationOutcome::RulesOnly(result) => {
                if result.execution_eligible || result.primary_model_invocation_id.is_some() {
                    return Err(ShadowEvalError::UnsafePolicy(
                        "rules-only fallback became executable".to_owned(),
                    ));
                }
                "rules_only"
            }
        };

        Ok(ScenarioResult {
            scenario_id: scenario.id.clone(),
            pack: scenario.pack.clone(),
            class: case.class,
            diagnostic_status: actual_status.to_owned(),
            reason_codes,
            finding_count: report.findings.len(),
            maximum_confidence_percent: report
                .findings
                .iter()
                .map(|finding| finding.confidence.percent)
                .max()
                .unwrap_or(0),
            citation_count: citations.len(),
            missing_required_evidence_count: report.missing_required_evidence.len(),
            missing_optional_evidence_count: report.missing_optional_evidence.len(),
            elapsed_micros: started_at.elapsed().as_micros(),
            model_cost_microusd: 0,
            model_mode: model_mode.to_owned(),
            execution_eligible: false,
            mutation_calls: 0,
            executor_calls: 0,
        })
    }
}

fn report_citations(report: &DiagnosticReport) -> Vec<EvidenceId> {
    report
        .findings
        .iter()
        .flat_map(|finding| finding.supporting_evidence.iter().chain(&finding.counter_evidence))
        .map(|citation| citation.evidence_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

const fn class_name(class: ScenarioClass) -> &'static str {
    match class {
        ScenarioClass::Normal => "normal",
        ScenarioClass::Fault => "fault",
        ScenarioClass::Missing => "missing",
    }
}

fn fixture_mismatch(path: &Path, detail: String) -> ShadowEvalError {
    ShadowEvalError::InvalidFixture {
        path: path.to_path_buf(),
        detail,
    }
}
