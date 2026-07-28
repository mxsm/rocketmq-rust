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

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::CompensationSpec;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionStepId;
use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::LeaseEpoch;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::LeaseId;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationSpec;
use serde_json::json;

use super::*;

struct FakeTopicConfigClient {
    state: Mutex<TopicConfigPatchState>,
    before: Mutex<Option<TopicConfigPatch>>,
    writes: Mutex<Vec<TopicConfigPatchWrite>>,
    restores: Mutex<Vec<TopicConfigPatchRestore>>,
}

impl FakeTopicConfigClient {
    fn new() -> Self {
        Self {
            state: Mutex::new(TopicConfigPatchState {
                version: 11,
                values: TopicConfigPatch {
                    read_queue_nums: Some(8),
                    write_queue_nums: Some(8),
                    order: Some(false),
                },
                configuration_consistent: true,
                last_operation_id: None,
            }),
            before: Mutex::new(None),
            writes: Mutex::new(Vec::new()),
            restores: Mutex::new(Vec::new()),
        }
    }
}

impl TopicConfigPatchClient for FakeTopicConfigClient {
    fn topic_config_patch_state<'a>(&'a self, _topic: &'a str) -> DriverFuture<'a, TopicConfigPatchState> {
        Box::pin(async move { Ok(self.state.lock().expect("fake state lock").clone()) })
    }

    fn patch_topic_config<'a>(
        &'a self,
        request: &'a TopicConfigPatchWrite,
    ) -> DriverFuture<'a, TopicConfigPatchApplyOutcome> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if state.version != request.expected_version {
                return Ok(TopicConfigPatchApplyOutcome::VersionConflict {
                    expected_version: request.expected_version,
                    actual_version: state.version,
                });
            }
            *self.before.lock().expect("fake before lock") = Some(state.values.clone());
            merge_patch(&mut state.values, &request.patch);
            let previous_version = state.version;
            state.version += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            self.writes.lock().expect("fake writes lock").push(request.clone());
            Ok(TopicConfigPatchApplyOutcome::Applied {
                previous_version,
                version: state.version,
            })
        })
    }

    fn restore_topic_config<'a>(
        &'a self,
        request: &'a TopicConfigPatchRestore,
    ) -> DriverFuture<'a, TopicConfigPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .before
                .lock()
                .expect("fake before lock")
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut state = self.state.lock().expect("fake state lock");
            let previous_version = state.version;
            state.values = before;
            state.version += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            self.restores.lock().expect("fake restores lock").push(request.clone());
            Ok(TopicConfigPatchApplyOutcome::Applied {
                previous_version,
                version: state.version,
            })
        })
    }
}

#[tokio::test]
async fn precheck_rejects_version_drift_and_inconsistent_config() {
    let client = Arc::new(FakeTopicConfigClient::new());
    {
        let mut state = client.state.lock().expect("fake state lock");
        state.version = 12;
        state.configuration_consistent = false;
    }
    let handler = TopicConfigPatchHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("precheck result");

    assert!(!result.ready);
    assert_eq!(
        result.reason_codes,
        [
            "topic_config_version_changed",
            "topic_config_inconsistent_across_brokers"
        ]
    );
}

#[tokio::test]
async fn apply_verify_and_inverse_patch_are_versioned() {
    let client = Arc::new(FakeTopicConfigClient::new());
    let handler = TopicConfigPatchHandler::new(Arc::clone(&client));
    let request = step_request();

    let applied = handler
        .dispatch(&request, "topic-forward")
        .await
        .expect("versioned apply");
    assert_eq!(applied.outcome_code, "topic_config_patch_applied");
    assert_eq!(client.writes.lock().expect("fake writes lock").len(), 1);
    let verified = handler
        .reconcile(&read_request(), Some("topic-forward"))
        .await
        .expect("version verification");
    assert_eq!(verified.state, ReconcileEffectState::Applied);

    let restored = handler
        .compensate(&request, "topic-rollback")
        .await
        .expect("inverse versioned patch");
    assert_eq!(restored.outcome_code, "topic_config_inverse_patch_applied");
    let state = client.state.lock().expect("fake state lock");
    assert_eq!(state.version, 13);
    assert_eq!(state.values.read_queue_nums, Some(8));
}

#[tokio::test]
async fn unknown_or_permission_fields_are_not_deserializable() {
    let mut value = parameters();
    value["patch"]["perm"] = json!(6);
    let handler = TopicConfigPatchHandler::new(Arc::new(FakeTopicConfigClient::new()));

    assert!(matches!(
        handler.read_state(&read_request_with(value)).await,
        Err(ExecutionAgentError::InvalidRequest)
    ));
}

fn merge_patch(current: &mut TopicConfigPatch, patch: &TopicConfigPatch) {
    if let Some(value) = patch.read_queue_nums {
        current.read_queue_nums = Some(value);
    }
    if let Some(value) = patch.write_queue_nums {
        current.write_queue_nums = Some(value);
    }
    if let Some(value) = patch.order {
        current.order = Some(value);
    }
}

fn read_request() -> AgentReadRequest {
    read_request_with(parameters())
}

fn read_request_with(parameters: serde_json::Value) -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::TopicConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        target: "topic/orders".to_owned(),
        parameters,
    }
}

fn step_request() -> AgentStepRequest {
    let now = Utc::now();
    let execution_id = ExecutionId::new();
    let step_id = ExecutionStepId::new();
    let plan_step_id = PlanStepId::new();
    let cluster_id = ClusterId::new();
    let parameters = parameters();
    let step = PlanStep {
        id: plan_step_id,
        sequence: 1,
        action: ExecutionAction::TopicConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        resource: "topic/orders".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: Vec::new(),
        precondition_hash: digest('a'),
        max_impact: ImpactScope::AllowlistedFields,
        verification: VerificationSpec::default(),
        compensation: CompensationSpec {
            mode: CompensationMode::Automatic,
            required_before_fields: vec!["before_config".to_owned(), "latest_version".to_owned()],
            timeout_seconds: 600,
        },
    };
    AgentStepRequest {
        intent: StepIntent {
            execution_id,
            step_id,
            plan_hash: digest('b'),
            step,
            attempt: 1,
            idempotency_key: "topic-config-test".to_owned(),
            fence_grant: LeaseFenceGrant {
                lease_id: LeaseId::new(),
                owner: "executor".to_owned(),
                cluster_id,
                epoch: LeaseEpoch(1),
                execution_id,
                step_id,
                plan_step_id,
                action: ExecutionAction::TopicConfigPatchAllowlisted,
                resource: "topic/orders".to_owned(),
                audience: "execution-agent".to_owned(),
                issued_at: now,
                expires_at: now + TimeDelta::minutes(1),
                nonce: "nonce".to_owned(),
                signature: "signature".to_owned(),
            },
            intended_at: now,
            compensation: false,
        },
        action: ExecutionAction::TopicConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        target: "topic/orders".to_owned(),
        parameters,
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "topic": "orders",
        "expected_version": 11,
        "patch": {
            "read_queue_nums": 12
        }
    })
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
