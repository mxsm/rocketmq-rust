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
use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

const GOLDENS: &str = include_str!("../../../../scripts/fixtures/admin-java-55/operation-goldens.json");

#[derive(Debug, Deserialize)]
struct GoldenFixture {
    schema_version: u32,
    java_version: String,
    scope: String,
    counts: GoldenCounts,
    operations: Vec<GoldenOperation>,
}

#[derive(Debug, Deserialize)]
struct GoldenCounts {
    operations: usize,
    scenarios: usize,
}

#[derive(Debug, Deserialize)]
struct GoldenOperation {
    operation_id: String,
    test_id: String,
    cli_command_id: String,
    java_request_codes: Vec<Value>,
    typed_request: Vec<String>,
    typed_response: Vec<String>,
    authorization: GoldenAuthorization,
    side_effect_class: String,
    read_contract: Option<GoldenReadContract>,
    scenarios: Vec<GoldenScenario>,
}

#[derive(Debug, Deserialize)]
struct GoldenAuthorization {
    context: String,
    enforcement: String,
    permission: String,
}

#[derive(Debug, Deserialize)]
struct GoldenReadContract {
    ordering: String,
    pagination: String,
    empty_result: String,
    partial_target_failure: String,
}

#[derive(Debug, Deserialize)]
struct GoldenScenario {
    scenario_id: String,
    case: String,
    outcome: String,
    error_kind: Option<String>,
    expected_error_code: Option<String>,
    expected_exit_code: i32,
    state_before: String,
    state_after: String,
    idempotent: bool,
    partial_failure: bool,
    retry_boundary: String,
    result_shape: String,
}

#[derive(Debug, Default)]
struct GoldenAdminBackend {
    state: BTreeMap<String, String>,
    revisions: BTreeMap<String, u64>,
}

impl GoldenAdminBackend {
    fn with_operation(operation: &GoldenOperation, scenario: &GoldenScenario) -> Self {
        Self {
            state: BTreeMap::from([(operation.operation_id.clone(), scenario.state_before.clone())]),
            revisions: BTreeMap::from([(operation.operation_id.clone(), 0)]),
        }
    }

    fn execute(&mut self, operation: &GoldenOperation, scenario: &GoldenScenario) -> Result<(), String> {
        let operation_id = &operation.operation_id;
        if scenario.outcome == "error" {
            if scenario.partial_failure && operation.side_effect_class != "read-only-query" {
                self.state.insert(operation_id.clone(), scenario.state_after.clone());
                *self.revisions.entry(operation_id.clone()).or_default() += 1;
            }
            return Err(scenario
                .expected_error_code
                .clone()
                .expect("error golden must carry a stable error code"));
        }

        match operation.side_effect_class.as_str() {
            "read-only-query" => {}
            "remote-state-mutation" | "local-artifact-write" if scenario.idempotent => {
                if self.state.get(operation_id) != Some(&scenario.state_after) {
                    self.state.insert(operation_id.clone(), scenario.state_after.clone());
                    *self.revisions.entry(operation_id.clone()).or_default() += 1;
                }
            }
            "message-io" => {
                self.state.insert(operation_id.clone(), scenario.state_after.clone());
                *self.revisions.entry(operation_id.clone()).or_default() += 1;
            }
            effect => panic!("unsupported golden side-effect class: {effect}"),
        }
        Ok(())
    }

    fn state(&self, operation_id: &str) -> (String, u64) {
        (
            self.state
                .get(operation_id)
                .expect("operation state must exist")
                .clone(),
            *self.revisions.get(operation_id).expect("operation revision must exist"),
        )
    }
}

fn fixture() -> GoldenFixture {
    serde_json::from_str(GOLDENS).expect("committed Admin golden fixture must be valid JSON")
}

#[test]
fn active_operations_have_complete_success_and_error_contracts() {
    let fixture = fixture();
    assert_eq!(fixture.schema_version, 1);
    assert_eq!(fixture.java_version, "5.5.0");
    assert_eq!(fixture.scope, "core-release");
    assert_eq!(fixture.counts.operations, 94);
    assert_eq!(fixture.counts.scenarios, 278);
    assert_eq!(fixture.operations.len(), 94);

    let mut operation_ids = BTreeSet::new();
    let mut scenario_ids = BTreeSet::new();
    let mut error_kinds = BTreeSet::new();
    for operation in &fixture.operations {
        assert!(
            operation_ids.insert(&operation.operation_id),
            "duplicate operation golden"
        );
        assert!(!operation.test_id.is_empty());
        assert!(operation.cli_command_id.contains('.'));
        assert!(!operation.typed_request.is_empty());
        assert!(!operation.typed_response.is_empty());
        assert_eq!(operation.authorization.context, "AdminCredentials");
        assert!(!operation.authorization.enforcement.is_empty());
        assert!(!operation.authorization.permission.is_empty());
        assert!(operation.java_request_codes.iter().all(|code| code.is_number()));
        if operation.side_effect_class == "read-only-query" {
            let read = operation
                .read_contract
                .as_ref()
                .expect("read operation must freeze its result contract");
            assert_eq!(read.ordering, "stable");
            assert_eq!(read.empty_result, "typed-empty");
            assert_eq!(
                read.partial_target_failure,
                "preserve-successes-and-warnings-when-multi-target"
            );
            if operation.cli_command_id == "message.queryMsgByKey" {
                assert_eq!(read.pagination, "per-broker-last-key");
            } else {
                assert_eq!(read.pagination, "not-applicable");
            }
        } else {
            assert!(operation.read_contract.is_none());
        }
        let expected_cases = if operation.side_effect_class == "read-only-query" {
            BTreeSet::from(["empty", "error", "partial-failure", "success"])
        } else {
            BTreeSet::from(["error", "success"])
        };
        assert_eq!(
            operation
                .scenarios
                .iter()
                .map(|scenario| scenario.case.as_str())
                .collect::<BTreeSet<_>>(),
            expected_cases,
        );
        assert_eq!(
            operation
                .scenarios
                .iter()
                .map(|scenario| scenario.outcome.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["error", "success"]),
        );
        for scenario in &operation.scenarios {
            assert!(scenario_ids.insert(&scenario.scenario_id), "duplicate scenario golden");
            if let Some(error_kind) = &scenario.error_kind {
                error_kinds.insert(error_kind.as_str());
            }
            assert!(matches!(scenario.retry_boundary.as_str(), "none" | "bounded"));
            assert_eq!(
                scenario.retry_boundary == "bounded",
                scenario.error_kind.as_deref() == Some("timeout")
            );
            match scenario.case.as_str() {
                "success" if operation.side_effect_class == "read-only-query" => {
                    assert_eq!(scenario.result_shape, "ordered-nonempty");
                }
                "empty" => assert_eq!(scenario.result_shape, "empty"),
                "partial-failure" => assert_eq!(scenario.result_shape, "ordered-partial"),
                _ => assert_eq!(scenario.result_shape, "not-applicable"),
            }
        }
    }
    assert_eq!(operation_ids.len(), 94);
    assert_eq!(scenario_ids.len(), 278);
    assert_eq!(
        error_kinds,
        BTreeSet::from(["invalid-input", "not-found", "partial-failure", "permission", "timeout"]),
    );
}

#[test]
fn fake_backend_preserves_state_idempotency_and_partial_failure_contracts() {
    for operation in fixture().operations {
        for scenario in &operation.scenarios {
            let mut backend = GoldenAdminBackend::with_operation(&operation, scenario);
            let initial = backend.state(&operation.operation_id);
            let result = backend.execute(&operation, scenario);
            let first = backend.state(&operation.operation_id);

            if scenario.outcome == "success" {
                assert!(result.is_ok(), "{} should succeed", scenario.scenario_id);
                assert_eq!(first.0, scenario.state_after);
                assert_eq!(scenario.expected_exit_code, 0);
                assert!(scenario.expected_error_code.is_none());
                if scenario.idempotent {
                    backend.execute(&operation, scenario).unwrap();
                    assert_eq!(backend.state(&operation.operation_id), first);
                } else {
                    backend.execute(&operation, scenario).unwrap();
                    assert!(backend.state(&operation.operation_id).1 > first.1);
                }
            } else {
                assert_eq!(result.unwrap_err(), scenario.expected_error_code.as_deref().unwrap());
                if scenario.partial_failure && operation.side_effect_class != "read-only-query" {
                    assert_eq!(first.0, "partially-applied");
                    assert!(first.1 > initial.1);
                } else {
                    assert_eq!(first, initial, "{} must not mutate state", scenario.scenario_id);
                }
            }
        }
    }
}
