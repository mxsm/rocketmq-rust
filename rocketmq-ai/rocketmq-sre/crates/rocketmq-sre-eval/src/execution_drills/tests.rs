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

use std::fs;

use rocketmq_sre_contracts::*;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;

use super::model::*;
use super::*;

fn case() -> Value {
    json!({
        "action":"observability.logger_level_ttl.v1", "descriptor_version":"1.0.0",
        "tenant_id":TenantId::new(), "cluster_id":ClusterId::new(), "target_id":"task-broker-a",
        "target_artifact_identity":{"kind":"deterministic_fixture", "opaque_id":"agent-double-v1"},
        "observed_software_version":null,
        "evidence_class":"protocol_fault", "execution_path":"injected_executor_agent",
        "scenario":"accepted_duplicate", "started_at":"2026-11-16T01:00:00Z",
        "finished_at":"2026-11-16T01:00:01Z", "outcome":"passed", "observed_state":"recorded_effect",
        "guard":null, "guard_reached":null, "original_identity_preserved":true,
        "cleanup_confirmed":true,
        "accepted_identity":{"execution_id":ExecutionId::new(), "correlation_id":CorrelationId::new(),
            "plan_id":ActionPlanId::new(), "plan_step_id":PlanStepId::new(),
            "plan_hash":format!("sha256:{}", "a".repeat(64)),
            "original_selector_digest":format!("sha256:{}", "b".repeat(64)), "lease_epoch":"1"},
        "counters":{"accepted_identities":1, "forward_effects":1, "compensation_effects":0,
            "stale_effects":0, "new_idempotency_keys":0, "quarantines":0},
        "artifact_references":["protocol-observation.json"],
        "limitations":["software_version_unknown", "physical_attestation_unavailable",
            "independent_business_observation_unavailable", "injected_authority"]
    })
}

fn fragment(cases: Vec<Value>) -> Value {
    json!({"schema_version":FRAGMENT_SCHEMA, "run_id":"engineering-drill-1",
        "production_qualified":false, "runtime_admission_authority":false, "cases":cases})
}

fn report(cases: Vec<Value>) -> Result<Report, Rejection> {
    let fragment: Fragment = serde_json::from_value(fragment(cases)).map_err(|_| Rejection::InvalidRecord)?;
    let report = Report {
        schema_version: REPORT_SCHEMA,
        run_id: fragment.run_id,
        production_qualified: false,
        runtime_admission_authority: false,
        physical_truth_validated: false,
        coverage: Vec::new(),
        cases: fragment.cases,
    };
    validation::validate(&report)?;
    Ok(report)
}

#[test]
fn recorded_protocol_success_never_becomes_physical_truth_or_runtime_authority() {
    let report = report(vec![case()]).unwrap();
    assert!(!report.physical_truth_validated);
    assert!(!report.production_qualified && !report.runtime_admission_authority);
    assert!(report.cases[0].observed_software_version.is_none());
    let mut missing_identity = case();
    missing_identity["accepted_identity"] = Value::Null;
    assert_eq!(report_invalid(missing_identity), Rejection::InvalidRecord);
}

fn report_invalid(case: Value) -> Rejection {
    report(vec![case]).unwrap_err()
}

#[test]
fn normal_qualification_denial_is_not_an_accepted_duplicate_positive_control() {
    let mut denied = case();
    denied["evidence_class"] = json!("normal_admission");
    denied["execution_path"] = json!("normal_control_plane");
    denied["observed_state"] = json!("rejected");
    denied["guard"] = json!("qualification_unknown");
    denied["guard_reached"] = json!(true);
    denied["accepted_identity"] = Value::Null;
    denied["original_identity_preserved"] = Value::Null;
    denied["counters"]["accepted_identities"] = json!(0);
    denied["counters"]["forward_effects"] = json!(0);
    assert_eq!(report_invalid(denied.clone()), Rejection::InvalidRecord);
    denied["scenario"] = json!("normal_unqualified_denied");
    report(vec![denied.clone()]).unwrap();
    denied["scenario"] = json!("expired_approval");
    assert_eq!(report_invalid(denied), Rejection::InvalidRecord);
}

fn isolated(version: Option<&str>) -> Value {
    let mut record = case();
    record["scenario"] = json!("guarded_positive_control");
    record["evidence_class"] = json!("isolated_driver");
    record["execution_path"] = json!("direct_handler_adapter");
    record["target_artifact_identity"] = json!({"kind":"deployed_image", "opaque_id":"task-image-1"});
    record["accepted_identity"] = Value::Null;
    record["original_identity_preserved"] = Value::Null;
    record["counters"]["accepted_identities"] = json!(0);
    record["limitations"] = json!([
        "physical_attestation_unavailable",
        "independent_business_observation_unavailable"
    ]);
    if let Some(version) = version {
        record["observed_software_version"] = json!({"value":version, "source":"target_protocol",
            "observation_reference":"protocol-observation.json"});
    } else {
        record["limitations"]
            .as_array_mut()
            .unwrap()
            .push(json!("software_version_unknown"));
    }
    record
}

#[test]
fn artifact_identity_and_generic_kubernetes_mechanics_cannot_fill_product_version() {
    report(vec![isolated(None)]).unwrap();
    let mut mechanics = isolated(Some("1.1.0"));
    mechanics["limitations"]
        .as_array_mut()
        .unwrap()
        .push(json!("generic_kubernetes_mechanics_only"));
    assert_eq!(report_invalid(mechanics), Rejection::InvalidRecord);
    let mut synthetic = case();
    synthetic["observed_software_version"] = json!({"value":"1.1.0", "source":"target_protocol",
        "observation_reference":"protocol-observation.json"});
    assert_eq!(report_invalid(synthetic), Rejection::InvalidRecord);
}

#[test]
fn case_identity_preserves_distinct_versions_and_rejects_repeats_and_conflicting_results() {
    let first = isolated(Some("1.1.0"));
    let mut second = first.clone();
    second["observed_software_version"]["value"] = json!("1.2.0");
    report(vec![first.clone(), second]).unwrap();
    assert_eq!(
        report(vec![first.clone(), first.clone()]).unwrap_err(),
        Rejection::DuplicateCase
    );
    let mut conflict = first.clone();
    conflict["outcome"] = json!("failed");
    assert_eq!(report(vec![first, conflict]).unwrap_err(), Rejection::DuplicateCase);
}

#[test]
fn contradictory_passed_scenarios_cannot_substitute_for_their_actual_guard_or_effect() {
    let mut missing_effect = case();
    missing_effect["scenario"] = json!("response_loss");
    missing_effect["counters"]["forward_effects"] = json!(0);
    assert_eq!(report_invalid(missing_effect), Rejection::InvalidRecord);
    let mut rejected_duplicate = case();
    rejected_duplicate["observed_state"] = json!("rejected");
    assert_eq!(report_invalid(rejected_duplicate), Rejection::InvalidRecord);
    let mut no_write_positive = isolated(None);
    no_write_positive["counters"]["forward_effects"] = json!(0);
    assert_eq!(report_invalid(no_write_positive), Rejection::InvalidRecord);
    let mut unknown = case();
    unknown["scenario"] = json!("verification_unknown");
    unknown["guard"] = json!("source_unknown");
    unknown["guard_reached"] = json!(true);
    assert_eq!(report_invalid(unknown.clone()), Rejection::InvalidRecord);
    unknown["observed_state"] = json!("unknown_effect");
    report(vec![unknown.clone()]).unwrap();
    unknown["counters"]["compensation_effects"] = json!(1);
    assert_eq!(report_invalid(unknown), Rejection::InvalidRecord);
    let mut drift = case();
    drift["scenario"] = json!("target_drift");
    drift["guard"] = json!("generation_changed");
    drift["guard_reached"] = json!(true);
    drift["counters"]["forward_effects"] = json!(0);
    assert_eq!(report_invalid(drift.clone()), Rejection::InvalidRecord);
    drift["observed_state"] = json!("rejected");
    report(vec![drift]).unwrap();
}

#[test]
fn counters_references_version_and_epoch_inputs_fail_closed() {
    for field in ["tenant_id", "cluster_id"] {
        let mut nil = case();
        nil[field] = json!("00000000-0000-0000-0000-000000000000");
        assert_eq!(report_invalid(nil), Rejection::InvalidRecord);
    }
    for field in ["execution_id", "correlation_id", "plan_id", "plan_step_id"] {
        let mut nil = case();
        nil["accepted_identity"][field] = json!("00000000-0000-0000-0000-000000000000");
        assert_eq!(report_invalid(nil), Rejection::InvalidRecord);
    }
    for (field, value) in [
        ("forward_effects", json!(-1)),
        ("forward_effects", json!(1.5)),
        ("forward_effects", json!(u64::MAX)),
        ("stale_effects", json!(1)),
        ("new_idempotency_keys", json!(1)),
    ] {
        let mut record = case();
        record["counters"][field] = value;
        assert!(report(vec![record]).is_err());
    }
    for reference in [
        "../private.json",
        "https://private.invalid/data",
        "secret/ref",
        "x\n.json",
    ] {
        let mut record = case();
        record["artifact_references"] = json!([reference]);
        assert_eq!(report_invalid(record), Rejection::InvalidRecord);
    }
    for epoch in ["0", "01", "-1", "18446744073709551615"] {
        let mut record = case();
        record["accepted_identity"]["lease_epoch"] = json!(epoch);
        assert_eq!(report_invalid(record), Rejection::InvalidRecord);
    }
    let mut old = case();
    old["descriptor_version"] = json!("0.9.0");
    assert_eq!(report_invalid(old), Rejection::InvalidRecord);
}

#[test]
fn bounded_input_rejects_unknown_duplicate_fields_and_authority_claims_without_output() {
    let directory = TempDir::new().unwrap();
    let input = directory.path().join("fragment.json");
    let output = directory.path().join("report.json");
    let valid = fragment(vec![case()]);
    let mut unknown = valid.clone();
    unknown["private_endpoint"] = json!("DO_NOT_RENDER");
    let mut authority = valid.clone();
    authority["production_qualified"] = json!(true);
    let duplicate = serde_json::to_string(&valid).unwrap().replacen(
        "\"production_qualified\":false",
        "\"production_qualified\":false,\"production_qualified\":false",
        1,
    );
    for bytes in [
        serde_json::to_vec(&unknown).unwrap(),
        serde_json::to_vec(&authority).unwrap(),
        duplicate.into_bytes(),
    ] {
        fs::write(&input, bytes).unwrap();
        assert_eq!(
            combine_files(std::slice::from_ref(&input), &output),
            Err(Rejection::InvalidRecord)
        );
        assert!(!output.exists());
    }
    fs::write(&input, vec![b' '; MAX_FRAGMENT_BYTES as usize + 1]).unwrap();
    assert_eq!(combine_files(&[input], &output), Err(Rejection::LimitExceeded));
    assert!(!output.exists());
}

#[test]
fn exclusive_output_preserves_inputs_and_previous_reports_including_aliases() {
    let directory = TempDir::new().unwrap();
    let input = directory.path().join("fragment.json");
    let output = directory.path().join("report.json");
    let original = serde_json::to_vec(&fragment(vec![case()])).unwrap();
    fs::write(&input, &original).unwrap();
    assert_eq!(
        combine_files(std::slice::from_ref(&input), &input),
        Err(Rejection::OutputExists)
    );
    assert_eq!(fs::read(&input).unwrap(), original);
    assert_eq!(combine_files(std::slice::from_ref(&input), &output), Ok(1));
    let previous = fs::read(&output).unwrap();
    let public: Value = serde_json::from_slice(&previous).unwrap();
    assert_eq!(public["coverage"].as_array().unwrap().len(), 36);
    assert_eq!(
        public["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["passed_case_claims"].as_u64().unwrap())
            .sum::<u64>(),
        1
    );
    assert_eq!(
        combine_files(std::slice::from_ref(&input), &output),
        Err(Rejection::OutputExists)
    );
    assert_eq!(fs::read(&output).unwrap(), previous);
    assert_eq!(fs::read(&input).unwrap(), original);
}
