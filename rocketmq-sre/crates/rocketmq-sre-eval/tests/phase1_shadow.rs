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

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_eval::phase1_shadow::ProviderMode;
use rocketmq_sre_eval::phase1_shadow::ShadowHarness;
use rocketmq_sre_eval::phase1_shadow::ShadowModelSynthesis;
use rocketmq_sre_eval::phase1_shadow::build_model_request;
use rocketmq_sre_eval::phase1_shadow::validate_citations;
use rocketmq_sre_eval::phase1_shadow::validate_model_response;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::ModelToolCall;
use serde::Deserialize;

#[derive(Deserialize)]
struct FakeCitationFixture {
    authorized_evidence_ids: BTreeSet<EvidenceId>,
    claimed_evidence_ids: Vec<EvidenceId>,
    expected_error: String,
}

#[derive(Deserialize)]
struct CrossClusterFixture {
    authorized_cluster_id: ClusterId,
    requested_cluster_id: ClusterId,
    expected_error: String,
}

#[derive(Deserialize)]
struct PromptInjectionFixture {
    prompt: String,
    attempted_tool: String,
    expected_error: String,
    expected_mutation_calls: u64,
    expected_executor_calls: u64,
}

#[derive(Deserialize)]
struct ProviderOutageFixture {
    provider_mode: ProviderMode,
    expected_model_mode: String,
    expected_execution_eligible: bool,
    expected_mutation_calls: u64,
    expected_executor_calls: u64,
}

#[test]
fn all_wave_a_cases_run_offline_with_the_mock_provider() {
    let harness = harness();
    let summary = harness
        .run(ProviderMode::Mock, harness.manifest().cluster_id)
        .expect("all mock-provider Wave A scenarios should replay");

    assert!(summary.passed);
    assert_eq!(summary.pack_count, 8);
    assert_eq!(summary.fixture_count, 24);
    assert_eq!(summary.class_counts["normal"], 8);
    assert_eq!(summary.class_counts["fault"], 8);
    assert_eq!(summary.class_counts["missing"], 8);
    assert_eq!(summary.model_completed_runs, 24);
    assert_eq!(summary.rules_only_runs, 0);
    assert_eq!(summary.provider_calls, 24);
    assert_eq!(summary.error_count, 0);
    assert_eq!(summary.total_model_cost_microusd, 0);
    assert!(summary.total_missing_required_evidence > 0);
    assert_eq!(summary.mutation_calls, 0);
    assert_eq!(summary.executor_calls, 0);
    assert!(!summary.executor_connected);
    assert!(summary.results.iter().all(|result| !result.execution_eligible));
    assert!(
        summary
            .results
            .iter()
            .filter(|result| result.diagnostic_status != "inconclusive")
            .all(|result| result.citation_count > 0)
    );
}

#[test]
fn rules_only_replays_all_cases_without_a_provider_call() {
    let harness = harness();
    let summary = harness
        .run(ProviderMode::RulesOnly, harness.manifest().cluster_id)
        .expect("rules-only Wave A scenarios should replay");

    assert!(summary.passed);
    assert_eq!(summary.fixture_count, 24);
    assert_eq!(summary.model_completed_runs, 0);
    assert_eq!(summary.rules_only_runs, 24);
    assert_eq!(summary.provider_calls, 0);
    assert_eq!(summary.mutation_calls, 0);
    assert_eq!(summary.executor_calls, 0);
}

#[test]
fn fake_citation_is_rejected_before_synthesis_is_accepted() {
    let fixture: FakeCitationFixture = read_json_fixture("security/fake-citation.json");
    let error = validate_citations(&fixture.authorized_evidence_ids, &fixture.claimed_evidence_ids)
        .expect_err("invented citation must fail closed");

    assert_eq!(error.code(), fixture.expected_error);
}

#[test]
fn cross_cluster_replay_is_rejected_before_fixture_access() {
    let fixture: CrossClusterFixture = read_json_fixture("security/cross-cluster.json");
    let harness = harness();
    assert_eq!(harness.manifest().cluster_id, fixture.authorized_cluster_id);

    let error = harness
        .run(ProviderMode::Mock, fixture.requested_cluster_id)
        .expect_err("cross-cluster request must fail closed");

    assert_eq!(error.code(), fixture.expected_error);
}

#[test]
fn prompt_injection_cannot_expand_tools_or_connect_an_executor() {
    let fixture: PromptInjectionFixture = read_json_fixture("security/prompt-injection.json");
    let harness = harness();
    let policy = &harness.manifest().policy;
    let request = build_model_request(&fixture.prompt, policy);

    assert!(request.tools.iter().all(|tool| !tool.mutates_cluster));
    assert!(
        request
            .tools
            .iter()
            .all(|tool| policy.model_visible_tools.contains(&tool.name))
    );
    assert!(request.tools.iter().all(|tool| tool.name != fixture.attempted_tool));
    assert!(!policy.executor_connected);

    let content = serde_json::to_string(&ShadowModelSynthesis {
        summary: "attempted privilege expansion".to_owned(),
        citations: Vec::new(),
        read_only_recommendations: Vec::new(),
        execution_eligible: false,
    })
    .expect("security fixture synthesis should encode");
    let mut response = CanonicalModelResponse::text(
        "phase1-shadow-mock",
        "deterministic-synthesis-v1",
        content,
        FinishReason::ToolCalls,
    );
    response.tool_calls.push(ModelToolCall {
        id: "attempt-1".to_owned(),
        name: fixture.attempted_tool,
        arguments: serde_json::json!({"topic": "production-topic"}),
    });
    let error = validate_model_response(&response, &BTreeSet::new(), policy)
        .expect_err("model-proposed mutation must be rejected");

    assert_eq!(error.code(), fixture.expected_error);
    assert_eq!(fixture.expected_mutation_calls, 0);
    assert_eq!(fixture.expected_executor_calls, 0);
}

#[test]
fn shadow_manifest_rejects_mutation_executor_and_unknown_tool_enablement() {
    let harness = harness();

    let mut mutation = harness.manifest().clone();
    mutation.policy.mutation_supported = true;
    assert_eq!(
        mutation.validate().expect_err("mutation enablement must fail").code(),
        "mutation_boundary_violation"
    );

    let mut executor = harness.manifest().clone();
    executor.policy.executor_connected = true;
    assert_eq!(
        executor.validate().expect_err("Executor connection must fail").code(),
        "mutation_boundary_violation"
    );

    let mut tool = harness.manifest().clone();
    tool.policy.model_visible_tools.insert("delete_topic".to_owned());
    assert_eq!(
        tool.validate().expect_err("unknown model tool must fail").code(),
        "mutation_boundary_violation"
    );
}

#[test]
fn provider_outage_falls_back_to_rules_only_for_every_case() {
    let raw = fs::read_to_string(e2e_root().join("security/provider-outage.yaml"))
        .expect("provider outage fixture should be readable");
    let fixture: ProviderOutageFixture = serde_yaml::from_str(&raw).expect("provider outage fixture should parse");
    let harness = harness();
    let summary = harness
        .run(fixture.provider_mode, harness.manifest().cluster_id)
        .expect("provider outage must preserve deterministic diagnosis");

    assert_eq!(summary.fixture_count, 24);
    assert_eq!(summary.rules_only_runs, 24);
    assert_eq!(summary.provider_calls, 24);
    assert!(
        summary
            .results
            .iter()
            .all(|result| result.model_mode == fixture.expected_model_mode)
    );
    assert!(
        summary
            .results
            .iter()
            .all(|result| result.execution_eligible == fixture.expected_execution_eligible)
    );
    assert_eq!(summary.mutation_calls, fixture.expected_mutation_calls);
    assert_eq!(summary.executor_calls, fixture.expected_executor_calls);
}

#[test]
fn message_path_evidence_contains_no_message_body_field() {
    for class in ["normal", "fault", "missing"] {
        let path = fixtures_root()
            .join("diagnostics/message-path.v1")
            .join(format!("{class}.json"));
        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("message path fixture should be readable"))
                .expect("message path fixture should parse");
        assert!(!contains_sensitive_key(&value));
    }
}

fn harness() -> ShadowHarness {
    ShadowHarness::load(&e2e_root().join("wave-a-manifest.v1.yaml"), &fixtures_root())
        .expect("Phase 01 shadow manifest should load")
}

fn read_json_fixture<T: for<'de> Deserialize<'de>>(relative: &str) -> T {
    let path = e2e_root().join(relative);
    serde_json::from_str(&fs::read_to_string(&path).expect("security fixture should be readable"))
        .expect("security fixture should parse")
}

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures")
}

fn e2e_root() -> PathBuf {
    fixtures_root().join("e2e")
}

fn contains_sensitive_key(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => object.iter().any(|(key, value)| {
            matches!(key.to_ascii_lowercase().as_str(), "body" | "message_body" | "payload")
                || contains_sensitive_key(value)
        }),
        serde_json::Value::Array(array) => array.iter().any(contains_sensitive_key),
        _ => false,
    }
}
