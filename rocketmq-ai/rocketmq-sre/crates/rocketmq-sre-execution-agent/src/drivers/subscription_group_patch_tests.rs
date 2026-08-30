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

use std::sync::Mutex;

use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::ReconcileEffectState;
use serde_json::json;

use super::*;
use crate::drivers::test_support;

struct FakeSubscriptionGroupClient {
    state: Mutex<SubscriptionGroupPatchState>,
    before: Mutex<Option<SubscriptionGroupPatch>>,
}

impl FakeSubscriptionGroupClient {
    fn new() -> Self {
        Self {
            state: Mutex::new(SubscriptionGroupPatchState {
                version: 7,
                values: SubscriptionGroupPatch {
                    retry_max_times: Some(3),
                    retry_queue_nums: Some(1),
                    consume_timeout_minutes: Some(15),
                },
                retry_semantics_known: true,
                permissions_unchanged: true,
                last_operation_id: None,
            }),
            before: Mutex::new(None),
        }
    }
}

impl SubscriptionGroupPatchClient for FakeSubscriptionGroupClient {
    fn subscription_group_patch_state<'a>(&'a self, _group: &'a str) -> DriverFuture<'a, SubscriptionGroupPatchState> {
        Box::pin(async move { Ok(self.state.lock().expect("state lock").clone()) })
    }

    fn patch_subscription_group<'a>(
        &'a self,
        request: &'a SubscriptionGroupPatchWrite,
    ) -> DriverFuture<'a, SubscriptionGroupPatchApplyOutcome> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            if state.version != request.expected_version {
                return Ok(SubscriptionGroupPatchApplyOutcome::VersionConflict {
                    expected_version: request.expected_version,
                    actual_version: state.version,
                });
            }
            *self.before.lock().expect("before lock") = Some(state.values.clone());
            merge(&mut state.values, &request.patch);
            let previous_version = state.version;
            state.version += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version,
                version: state.version,
            })
        })
    }

    fn restore_subscription_group<'a>(
        &'a self,
        request: &'a SubscriptionGroupPatchRestore,
    ) -> DriverFuture<'a, SubscriptionGroupPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .before
                .lock()
                .expect("before lock")
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut state = self.state.lock().expect("state lock");
            let previous_version = state.version;
            state.values = before;
            state.version += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version,
                version: state.version,
            })
        })
    }
}

#[tokio::test]
async fn precheck_rejects_unknown_retry_semantics_and_permission_drift() {
    let client = Arc::new(FakeSubscriptionGroupClient::new());
    {
        let mut state = client.state.lock().expect("state lock");
        state.retry_semantics_known = false;
        state.permissions_unchanged = false;
    }
    let result = SubscriptionGroupPatchHandler::new(client)
        .read_state(&read_request(parameters()))
        .await
        .expect("precheck");
    assert!(!result.ready);
    assert!(result.reason_codes.contains(&"retry_semantics_unknown".to_owned()));
    assert!(
        result
            .reason_codes
            .contains(&"subscription_group_permissions_changed".to_owned())
    );
}

#[tokio::test]
async fn apply_reconcile_and_compensate_use_version_cas() {
    let client = Arc::new(FakeSubscriptionGroupClient::new());
    let handler = SubscriptionGroupPatchHandler::new(Arc::clone(&client));
    let step = step_request(parameters());

    assert_eq!(
        handler
            .dispatch(&step, "subscription-forward")
            .await
            .expect("dispatch")
            .outcome_code,
        "subscription_group_patch_applied"
    );
    assert_eq!(
        handler
            .reconcile(&read_request(parameters()), Some("subscription-forward"))
            .await
            .expect("reconcile")
            .state,
        ReconcileEffectState::Applied
    );
    assert_eq!(
        handler
            .compensate(&step, "subscription-rollback")
            .await
            .expect("compensation")
            .outcome_code,
        "subscription_group_inverse_patch_applied"
    );
    let state = client.state.lock().expect("state lock");
    assert_eq!(state.version, 9);
    assert_eq!(state.values.retry_max_times, Some(3));
}

#[tokio::test]
async fn permission_and_delete_fields_are_not_deserializable() {
    let handler = SubscriptionGroupPatchHandler::new(Arc::new(FakeSubscriptionGroupClient::new()));
    for forbidden in ["permissions", "delete_group", "consume_enable"] {
        let mut value = parameters();
        value["patch"][forbidden] = json!(true);
        assert!(matches!(
            handler.read_state(&read_request(value)).await,
            Err(ExecutionAgentError::InvalidRequest)
        ));
    }
}

fn merge(current: &mut SubscriptionGroupPatch, patch: &SubscriptionGroupPatch) {
    if let Some(value) = patch.retry_max_times {
        current.retry_max_times = Some(value);
    }
    if let Some(value) = patch.retry_queue_nums {
        current.retry_queue_nums = Some(value);
    }
    if let Some(value) = patch.consume_timeout_minutes {
        current.consume_timeout_minutes = Some(value);
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "group": "orders-consumer",
        "expected_version": 7,
        "patch": {"retry_max_times": 5}
    })
}

fn read_request(parameters: serde_json::Value) -> AgentReadRequest {
    test_support::read_request(
        ExecutionAction::SubscriptionGroupPatchAllowlisted,
        "subscription-group/orders-consumer",
        parameters,
    )
}

fn step_request(parameters: serde_json::Value) -> AgentStepRequest {
    test_support::step_request(
        ExecutionAction::SubscriptionGroupPatchAllowlisted,
        "subscription-group/orders-consumer",
        parameters,
        ImpactScope::AllowlistedFields,
        &["before_config", "latest_version"],
    )
}
