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
use rocketmq_admin_core::core::proxy::ProxyDrainPending;
use rocketmq_admin_core::core::proxy::ProxyDrainPhase;
use rocketmq_admin_core::core::proxy::ProxyDrainState;
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

struct FakeRestartClient {
    state: Mutex<ProxyRestartState>,
    restart_calls: Mutex<Vec<ProxyRestartOneWrite>>,
    restore_calls: Mutex<Vec<ProxyRestartRestore>>,
}

impl FakeRestartClient {
    fn accepting() -> Self {
        Self {
            state: Mutex::new(ProxyRestartState {
                drain_supported: true,
                pod_uid: "uid-before".to_owned(),
                pod_ready: true,
                remaining_replicas_healthy: true,
                replacement_ready: false,
                synthetic_path_healthy: true,
                slo_healthy: true,
                active_operation_id: None,
                last_operation_id: None,
                drain: Some(accepting_drain()),
            }),
            restart_calls: Mutex::new(Vec::new()),
            restore_calls: Mutex::new(Vec::new()),
        }
    }
}

impl ProxyRestartClient for FakeRestartClient {
    fn proxy_restart_state<'a>(&'a self, _namespace: &'a str, _pod: &'a str) -> DriverFuture<'a, ProxyRestartState> {
        Box::pin(async move { Ok(self.state.lock().expect("fake state lock").clone()) })
    }

    fn restart_one_drained<'a>(&'a self, request: &'a ProxyRestartOneWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if !state.drain_supported || state.pod_uid != request.expected_uid {
                return Err(ExecutionAgentError::DriverFailed);
            }
            state.pod_uid = "uid-after".to_owned();
            state.pod_ready = true;
            state.replacement_ready = true;
            state.active_operation_id = None;
            state.last_operation_id = Some(request.operation_id.clone());
            state.drain = Some(accepting_drain());
            self.restart_calls
                .lock()
                .expect("fake restart calls lock")
                .push(request.clone());
            Ok(())
        })
    }

    fn cancel_restart_and_restore<'a>(
        &'a self,
        request: &'a ProxyRestartRestore,
    ) -> DriverFuture<'a, ProxyRestartRestoreOutcome> {
        Box::pin(async move {
            self.restore_calls
                .lock()
                .expect("fake restore calls lock")
                .push(request.clone());
            let state = self.state.lock().expect("fake state lock");
            if state.pod_uid == request.expected_uid {
                Ok(ProxyRestartRestoreOutcome::IngressRestored)
            } else {
                Ok(ProxyRestartRestoreOutcome::ManualTakeoverRequired)
            }
        })
    }
}

#[tokio::test]
async fn precheck_fails_closed_without_authenticated_drain_state() {
    let client = Arc::new(FakeRestartClient::accepting());
    {
        let mut state = client.state.lock().expect("fake state lock");
        state.drain_supported = false;
        state.drain = None;
    }
    let handler = ProxyRestartOneHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("precheck response");

    assert!(!result.ready);
    assert_eq!(result.reason_codes, ["authenticated_drain_state_unavailable"]);
}

#[tokio::test]
async fn precheck_fails_closed_without_a_healthy_remaining_replica() {
    let client = Arc::new(FakeRestartClient::accepting());
    client.state.lock().expect("fake state lock").remaining_replicas_healthy = false;
    let handler = ProxyRestartOneHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("precheck response");

    assert!(!result.ready);
    assert_eq!(result.reason_codes, ["proxy_remaining_replicas_unhealthy"]);
}

#[tokio::test]
async fn precheck_rejects_only_active_restart_operation() {
    let client = Arc::new(FakeRestartClient::accepting());
    client.state.lock().expect("fake state lock").active_operation_id = Some("restart-active".to_owned());
    let handler = ProxyRestartOneHandler::new(Arc::clone(&client));

    let active = handler.read_state(&read_request()).await.expect("active precheck response");

    assert!(!active.ready);
    assert_eq!(active.reason_codes, ["proxy_restart_operation_not_clear"]);
    assert_eq!(active.resource_conditions.get("restart_operation_clear"), Some(&false));

    {
        let mut state = client.state.lock().expect("fake state lock");
        state.active_operation_id = None;
        state.last_operation_id = Some("restart-complete".to_owned());
    }
    let completed = handler
        .read_state(&read_request())
        .await
        .expect("completed operation precheck response");

    assert!(completed.ready);
    assert!(completed.reason_codes.is_empty());
    assert_eq!(
        completed.resource_conditions.get("restart_operation_clear"),
        Some(&true)
    );
}

#[tokio::test]
async fn apply_and_verify_one_expected_uid_then_require_manual_takeover() {
    let client = Arc::new(FakeRestartClient::accepting());
    let handler = ProxyRestartOneHandler::new(Arc::clone(&client));
    let request = step_request();

    handler
        .dispatch(&request, "restart-forward")
        .await
        .expect("drained restart");
    {
        let calls = client.restart_calls.lock().expect("fake restart calls lock");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].expected_uid, "uid-before");
        assert_eq!(calls[0].drain_timeout_seconds, 300);
    }
    let verified = handler
        .reconcile(&read_request(), Some("restart-forward"))
        .await
        .expect("replacement verification");
    assert_eq!(verified.state, ReconcileEffectState::Applied);
    let conditions = handler
        .read_state(&read_request())
        .await
        .expect("restart verification conditions");
    assert_eq!(conditions.resource_conditions.get("replacement_ready"), Some(&true));
    assert_eq!(
        conditions.resource_conditions.get("remaining_replicas_healthy"),
        Some(&true)
    );
    assert_eq!(conditions.resource_conditions.get("accepting_and_routed"), Some(&true));

    let compensation = handler
        .compensate(&request, "restart-compensate")
        .await
        .expect("manual compensation result");
    assert_eq!(compensation.outcome_code, "proxy_restart_manual_takeover_required");
    assert_eq!(client.restore_calls.lock().expect("fake restore calls lock").len(), 1);
}

fn accepting_drain() -> ProxyDrainState {
    ProxyDrainState {
        schema_version: "rocketmq.proxy-drain.v1".to_owned(),
        phase: ProxyDrainPhase::Accepting,
        operation_id: None,
        admission_open: true,
        routing_open: true,
        readiness_published: true,
        zero_pending: true,
        pending: ProxyDrainPending::default(),
    }
}

fn read_request() -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ProxyRestartOne,
        descriptor_version: "1.0.0".to_owned(),
        target: "pod/rocketmq-proxy-0".to_owned(),
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
        action: ExecutionAction::ProxyRestartOne,
        descriptor_version: "1.0.0".to_owned(),
        resource: "pod/rocketmq-proxy-0".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: Vec::new(),
        precondition_hash: digest('a'),
        max_impact: ImpactScope::SingleInstance,
        verification: VerificationSpec::default(),
        compensation: CompensationSpec {
            mode: CompensationMode::ManualTakeover,
            required_before_fields: vec![
                "admission_state".to_owned(),
                "readiness_state".to_owned(),
                "routing_state".to_owned(),
            ],
            timeout_seconds: 300,
        },
    };
    AgentStepRequest {
        intent: StepIntent {
            execution_id,
            step_id,
            plan_hash: digest('b'),
            step,
            attempt: 1,
            idempotency_key: "restart-test".to_owned(),
            fence_grant: LeaseFenceGrant {
                lease_id: LeaseId::new(),
                owner: "executor".to_owned(),
                cluster_id,
                epoch: LeaseEpoch(1),
                execution_id,
                step_id,
                plan_step_id,
                action: ExecutionAction::ProxyRestartOne,
                resource: "pod/rocketmq-proxy-0".to_owned(),
                compensation: false,
                audience: "execution-agent".to_owned(),
                issued_at: now,
                expires_at: now + TimeDelta::minutes(1),
                nonce: "nonce".to_owned(),
                signature: "signature".to_owned(),
            },
            dynamic_safety: None,
            intended_at: now,
            compensation: false,
        },
        action: ExecutionAction::ProxyRestartOne,
        descriptor_version: "1.0.0".to_owned(),
        target: "pod/rocketmq-proxy-0".to_owned(),
        parameters,
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "namespace": "rocketmq",
        "pod": "rocketmq-proxy-0",
        "expected_uid": "uid-before"
    })
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
