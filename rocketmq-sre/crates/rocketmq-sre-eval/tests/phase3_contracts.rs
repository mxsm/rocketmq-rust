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
use std::path::Path;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_core::ActionCatalog;
use rocketmq_sre_eval::phase3_generated_schemas;

fn descriptors() -> Vec<ActionDescriptor> {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/actions");
    let mut paths = fs::read_dir(directory)
        .expect("action config directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "yaml"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let yaml = fs::read_to_string(&path).expect("descriptor YAML");
            serde_yaml::from_str(&yaml).expect("typed ActionDescriptor")
        })
        .collect()
}

#[test]
fn wave_one_descriptor_skeletons_are_typed_and_fail_closed() {
    let descriptors = descriptors();
    assert_eq!(descriptors.len(), 5);
    let mut catalog = ActionCatalog::default();
    for descriptor in descriptors {
        assert!(matches!(descriptor.risk, ActionRisk::R1 | ActionRisk::R2));
        assert!(!descriptor.execution_supported);
        assert!(!descriptor.parameter_schema.is_null());
        catalog.register(descriptor).expect("known R1/R2 descriptor");
    }
    assert_eq!(catalog.len(), 5);
    assert!(catalog.descriptor(ExecutionAction::ProxyScaleOutOne, "1.0.1").is_err());
}

#[test]
fn r3_and_unknown_actions_cannot_enter_the_execution_catalog() {
    let mut descriptor = descriptors().remove(0);
    descriptor.risk = ActionRisk::R3;
    assert!(ActionCatalog::default().register(descriptor).is_err());

    let mut descriptor = descriptors().remove(0);
    descriptor.id = "broker.raw_request.v1".to_owned();
    assert!(ActionCatalog::default().register(descriptor).is_err());

    let descriptor = descriptors().remove(0);
    let mut value = serde_json::to_value(descriptor).expect("descriptor value");
    value
        .as_object_mut()
        .expect("descriptor object")
        .insert("raw_request_code".to_owned(), serde_json::json!(42));
    assert!(serde_json::from_value::<ActionDescriptor>(value).is_err());
}

#[test]
fn action_plan_json_fixture_round_trips_through_typed_contracts() {
    let fixture = include_str!("../../../tests/fixtures/phase3/action-plan-draft.v1.json");
    let draft: ActionPlanDraft = serde_json::from_str(fixture).expect("typed draft fixture");
    let plan = ActionPlan::seal(draft.clone()).expect("fixture should seal");
    let encoded = serde_json::to_vec(&plan).expect("plan should serialize");
    let decoded: ActionPlan = serde_json::from_slice(&encoded).expect("plan should deserialize");

    assert_eq!(decoded, plan);
    assert_eq!(
        serde_json::to_value(draft).expect("draft value"),
        serde_json::from_str::<serde_json::Value>(fixture).expect("fixture value")
    );
    assert!(decoded.verify_plan_hash().is_ok());
}

#[test]
fn unknown_and_r3_actions_cannot_deserialize_into_execution_requests() {
    let fixture = include_str!("../../../tests/fixtures/phase3/action-plan-draft.v1.json");
    let mut plan_value: serde_json::Value = serde_json::from_str(fixture).expect("draft fixture value");
    plan_value["steps"][0]["action"] = serde_json::Value::String("broker.delete_topic.v1".to_owned());

    assert!(serde_json::from_value::<ActionPlanDraft>(plan_value.clone()).is_err());

    let request_value = serde_json::json!({
        "schema_version": ExecutionRequest::SCHEMA_VERSION,
        "id": "99999999-9999-4999-8999-999999999999",
        "tenant_id": "22222222-2222-4222-8222-222222222222",
        "cluster_id": "33333333-3333-4333-8333-333333333333",
        "correlation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "plan": plan_value,
        "approvals": [],
        "requested_by": "operator-a",
        "idempotency_key": "request-1",
        "issuer": "control-plane",
        "audience": "executor",
        "issued_at": "2026-07-28T00:10:00Z",
        "expires_at": "2026-07-28T00:15:00Z",
        "nonce": "nonce-1",
        "signature": "fixture-signature"
    });
    assert!(serde_json::from_value::<ExecutionRequest>(request_value).is_err());
}

#[test]
fn committed_phase_three_schemas_match_public_contracts() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/schema");
    let generated = phase3_generated_schemas().expect("schemas should generate");
    assert_eq!(generated.len(), 24);

    for (name, schema) in generated {
        let committed = fs::read_to_string(directory.join(name)).expect("committed schema");
        let committed: serde_json::Value = serde_json::from_str(&committed).expect("schema should be JSON");
        assert_eq!(committed, schema, "{name} drifted");
    }
}
