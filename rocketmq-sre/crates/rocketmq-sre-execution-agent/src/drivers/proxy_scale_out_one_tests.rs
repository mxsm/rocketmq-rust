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

struct FakeScaleClient {
    state: Mutex<ProxyScaleState>,
    writes: Mutex<Vec<ProxyScaleOutOneWrite>>,
    restores: Mutex<Vec<ProxyScaleRestore>>,
}

impl FakeScaleClient {
    fn healthy(replicas: u32) -> Self {
        Self {
            state: Mutex::new(ProxyScaleState {
                desired_replicas: replicas,
                ready_replicas: replicas,
                unavailable_replicas: 0,
                quota_available: true,
                capacity_available: true,
                pdb_healthy: true,
                last_operation_id: None,
            }),
            writes: Mutex::new(Vec::new()),
            restores: Mutex::new(Vec::new()),
        }
    }
}

impl ProxyScaleClient for FakeScaleClient {
    fn proxy_scale_state<'a>(&'a self, _namespace: &'a str, _workload: &'a str) -> DriverFuture<'a, ProxyScaleState> {
        Box::pin(async move { Ok(self.state.lock().expect("fake state lock").clone()) })
    }

    fn scale_out_one<'a>(&'a self, request: &'a ProxyScaleOutOneWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if state.desired_replicas != request.expected_replicas
                || request.target_replicas != request.expected_replicas + 1
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            state.desired_replicas = request.target_replicas;
            state.ready_replicas = request.target_replicas;
            state.last_operation_id = Some(request.operation_id.clone());
            self.writes.lock().expect("fake writes lock").push(request.clone());
            Ok(())
        })
    }

    fn restore_proxy_replicas<'a>(&'a self, request: &'a ProxyScaleRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if state.desired_replicas != request.original_replicas + 1 {
                return Err(ExecutionAgentError::DriverFailed);
            }
            state.desired_replicas = request.original_replicas;
            state.ready_replicas = request.original_replicas;
            state.last_operation_id = Some(request.operation_id.clone());
            self.restores.lock().expect("fake restores lock").push(request.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn precheck_reports_every_unhealthy_capacity_condition() {
    let client = Arc::new(FakeScaleClient::healthy(3));
    {
        let mut state = client.state.lock().expect("fake state lock");
        state.ready_replicas = 2;
        state.unavailable_replicas = 1;
        state.quota_available = false;
        state.capacity_available = false;
        state.pdb_healthy = false;
    }
    let handler = ProxyScaleOutOneHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("precheck result");

    assert!(!result.ready);
    assert_eq!(
        result.reason_codes,
        [
            "proxy_workload_not_fully_ready",
            "namespace_quota_unavailable",
            "cluster_capacity_unavailable",
            "proxy_pdb_not_healthy"
        ]
    );
}

#[tokio::test]
async fn apply_verify_and_compensate_are_exactly_one_replica() {
    let client = Arc::new(FakeScaleClient::healthy(3));
    let handler = ProxyScaleOutOneHandler::new(Arc::clone(&client));
    let request = step_request();

    handler.dispatch(&request, "scale-forward").await.expect("scale out");
    {
        let writes = client.writes.lock().expect("fake writes lock");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].expected_replicas, 3);
        assert_eq!(writes[0].target_replicas, 4);
    }
    let verified = handler
        .reconcile(&read_request(), Some("scale-forward"))
        .await
        .expect("verify scale");
    assert_eq!(verified.state, ReconcileEffectState::Applied);
    let conditions = handler
        .read_state(&read_request())
        .await
        .expect("scale verification conditions");
    assert_eq!(
        conditions.resource_conditions.get("desired_replicas_plus_one"),
        Some(&true)
    );
    assert_eq!(conditions.resource_conditions.get("new_replica_ready"), Some(&true));

    handler
        .compensate(&request, "scale-rollback")
        .await
        .expect("restore original replicas");
    assert_eq!(client.restores.lock().expect("fake restores lock").len(), 1);
    assert_eq!(client.state.lock().expect("fake state lock").desired_replicas, 3);
}

fn read_request() -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ProxyScaleOutOne,
        descriptor_version: "1.0.0".to_owned(),
        target: "deployment/proxy".to_owned(),
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
        action: ExecutionAction::ProxyScaleOutOne,
        descriptor_version: "1.0.0".to_owned(),
        resource: "deployment/proxy".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: Vec::new(),
        precondition_hash: digest('a'),
        max_impact: ImpactScope::OneReplica,
        verification: VerificationSpec::default(),
        compensation: CompensationSpec {
            mode: CompensationMode::Automatic,
            required_before_fields: vec!["expected_replicas".to_owned()],
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
            idempotency_key: "scale-test".to_owned(),
            fence_grant: LeaseFenceGrant {
                lease_id: LeaseId::new(),
                owner: "executor".to_owned(),
                cluster_id,
                epoch: LeaseEpoch(1),
                execution_id,
                step_id,
                plan_step_id,
                action: ExecutionAction::ProxyScaleOutOne,
                resource: "deployment/proxy".to_owned(),
                audience: "execution-agent".to_owned(),
                issued_at: now,
                expires_at: now + TimeDelta::minutes(1),
                nonce: "nonce".to_owned(),
                signature: "signature".to_owned(),
            },
            intended_at: now,
            compensation: false,
        },
        action: ExecutionAction::ProxyScaleOutOne,
        descriptor_version: "1.0.0".to_owned(),
        target: "deployment/proxy".to_owned(),
        parameters,
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "namespace": "rocketmq",
        "workload": "rocketmq-proxy",
        "expected_replicas": 3
    })
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
