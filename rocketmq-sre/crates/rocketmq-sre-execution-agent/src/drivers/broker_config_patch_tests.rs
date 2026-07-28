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

use std::collections::BTreeSet;
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

struct FakeBrokerConfigClient {
    state: Mutex<BrokerConfigPatchState>,
    before: Mutex<Option<BrokerConfigPatch>>,
    writes: Mutex<Vec<BrokerConfigPatchWrite>>,
    restores: Mutex<Vec<BrokerConfigPatchRestore>>,
}

impl FakeBrokerConfigClient {
    fn new() -> Self {
        Self {
            state: Mutex::new(BrokerConfigPatchState {
                generation: 7,
                values: BrokerConfigPatch {
                    send_message_thread_pool_nums: Some(16),
                    pull_message_thread_pool_nums: Some(16),
                    flush_delay_offset_interval_ms: Some(10_000),
                },
                supported_fields: [
                    "send_message_thread_pool_nums".to_owned(),
                    "pull_message_thread_pool_nums".to_owned(),
                    "flush_delay_offset_interval_ms".to_owned(),
                ]
                .into_iter()
                .collect(),
                restart_required_fields: BTreeSet::new(),
                last_operation_id: None,
            }),
            before: Mutex::new(None),
            writes: Mutex::new(Vec::new()),
            restores: Mutex::new(Vec::new()),
        }
    }
}

impl BrokerConfigPatchClient for FakeBrokerConfigClient {
    fn broker_config_patch_state<'a>(&'a self, _broker_addr: &'a str) -> DriverFuture<'a, BrokerConfigPatchState> {
        Box::pin(async move { Ok(self.state.lock().expect("fake state lock").clone()) })
    }

    fn patch_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchWrite,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if state.generation != request.expected_generation {
                return Ok(BrokerConfigPatchApplyOutcome::GenerationConflict {
                    expected_generation: request.expected_generation,
                    actual_generation: state.generation,
                });
            }
            *self.before.lock().expect("fake before lock") = Some(state.values.clone());
            merge_patch(&mut state.values, &request.patch);
            let previous_generation = state.generation;
            state.generation += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            self.writes.lock().expect("fake writes lock").push(request.clone());
            Ok(BrokerConfigPatchApplyOutcome::Applied {
                previous_generation,
                generation: state.generation,
            })
        })
    }

    fn restore_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchRestore,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .before
                .lock()
                .expect("fake before lock")
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut state = self.state.lock().expect("fake state lock");
            let previous_generation = state.generation;
            state.values = before;
            state.generation += 1;
            state.last_operation_id = Some(request.operation_id.clone());
            self.restores.lock().expect("fake restores lock").push(request.clone());
            Ok(BrokerConfigPatchApplyOutcome::Applied {
                previous_generation,
                generation: state.generation,
            })
        })
    }
}

#[tokio::test]
async fn precheck_rejects_generation_drift_and_restart_required_fields() {
    let client = Arc::new(FakeBrokerConfigClient::new());
    {
        let mut state = client.state.lock().expect("fake state lock");
        state.generation = 8;
        state
            .restart_required_fields
            .insert("send_message_thread_pool_nums".to_owned());
    }
    let handler = BrokerConfigPatchHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("precheck result");

    assert!(!result.ready);
    assert_eq!(
        result.reason_codes,
        ["broker_config_generation_changed", "broker_config_restart_required"]
    );
}

#[tokio::test]
async fn apply_verify_and_inverse_patch_advance_generation() {
    let client = Arc::new(FakeBrokerConfigClient::new());
    let handler = BrokerConfigPatchHandler::new(Arc::clone(&client));
    let request = step_request();

    let applied = handler.dispatch(&request, "broker-forward").await.expect("CAS apply");
    assert_eq!(applied.outcome_code, "broker_config_patch_applied");
    assert_eq!(client.writes.lock().expect("fake writes lock").len(), 1);
    let verified = handler
        .reconcile(&read_request(), Some("broker-forward"))
        .await
        .expect("CAS verification");
    assert_eq!(verified.state, ReconcileEffectState::Applied);

    let restored = handler
        .compensate(&request, "broker-rollback")
        .await
        .expect("inverse CAS");
    assert_eq!(restored.outcome_code, "broker_config_inverse_patch_applied");
    let state = client.state.lock().expect("fake state lock");
    assert_eq!(state.generation, 9);
    assert_eq!(state.values.send_message_thread_pool_nums, Some(16));
}

#[tokio::test]
async fn generation_conflict_is_known_and_never_overwrites() {
    let client = Arc::new(FakeBrokerConfigClient::new());
    client.state.lock().expect("fake state lock").generation = 9;
    let handler = BrokerConfigPatchHandler::new(Arc::clone(&client));

    let outcome = handler
        .dispatch(&step_request(), "broker-conflict")
        .await
        .expect("known non-applied outcome");

    assert_eq!(outcome.outcome_code, "broker_config_generation_conflict");
    assert!(client.writes.lock().expect("fake writes lock").is_empty());
    assert_eq!(
        client
            .state
            .lock()
            .expect("fake state lock")
            .values
            .send_message_thread_pool_nums,
        Some(16)
    );
}

fn merge_patch(current: &mut BrokerConfigPatch, patch: &BrokerConfigPatch) {
    if let Some(value) = patch.send_message_thread_pool_nums {
        current.send_message_thread_pool_nums = Some(value);
    }
    if let Some(value) = patch.pull_message_thread_pool_nums {
        current.pull_message_thread_pool_nums = Some(value);
    }
    if let Some(value) = patch.flush_delay_offset_interval_ms {
        current.flush_delay_offset_interval_ms = Some(value);
    }
}

fn read_request() -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::BrokerConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        target: "broker/127.0.0.1:10911".to_owned(),
        parameters: parameters(),
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
        action: ExecutionAction::BrokerConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        resource: "broker/127.0.0.1:10911".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: Vec::new(),
        precondition_hash: digest('a'),
        max_impact: ImpactScope::AllowlistedFields,
        verification: VerificationSpec::default(),
        compensation: CompensationSpec {
            mode: CompensationMode::Automatic,
            required_before_fields: vec!["before_config".to_owned(), "latest_generation".to_owned()],
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
            idempotency_key: "broker-config-test".to_owned(),
            fence_grant: LeaseFenceGrant {
                lease_id: LeaseId::new(),
                owner: "executor".to_owned(),
                cluster_id,
                epoch: LeaseEpoch(1),
                execution_id,
                step_id,
                plan_step_id,
                action: ExecutionAction::BrokerConfigPatchAllowlisted,
                resource: "broker/127.0.0.1:10911".to_owned(),
                audience: "execution-agent".to_owned(),
                issued_at: now,
                expires_at: now + TimeDelta::minutes(1),
                nonce: "nonce".to_owned(),
                signature: "signature".to_owned(),
            },
            intended_at: now,
            compensation: false,
        },
        action: ExecutionAction::BrokerConfigPatchAllowlisted,
        descriptor_version: "1.0.0".to_owned(),
        target: "broker/127.0.0.1:10911".to_owned(),
        parameters,
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "broker": "127.0.0.1:10911",
        "expected_generation": 7,
        "patch": {
            "send_message_thread_pool_nums": 32
        }
    })
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
