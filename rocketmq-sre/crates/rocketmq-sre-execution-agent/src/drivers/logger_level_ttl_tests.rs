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

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use chrono::TimeDelta;
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
use crate::ConfigWriteClient;
use crate::LoggerLevelControlClient;
use crate::LoggerLevelTtlRestore;
use crate::LoggerLevelTtlWrite;

#[derive(Debug)]
struct FakeLoggerClient {
    state: Mutex<LoggerLevelState>,
    original_level: Mutex<Option<String>>,
    writes: Mutex<Vec<LoggerLevelTtlWrite>>,
    restores: Mutex<Vec<LoggerLevelTtlRestore>>,
}

impl FakeLoggerClient {
    fn new(level: &str) -> Self {
        Self {
            state: Mutex::new(LoggerLevelState {
                level: level.to_owned(),
                active_operation_id: None,
                last_completed_operation_id: None,
            }),
            original_level: Mutex::new(None),
            writes: Mutex::new(Vec::new()),
            restores: Mutex::new(Vec::new()),
        }
    }
}

impl ConfigWriteClient for FakeLoggerClient {
    fn set_logger_level_ttl<'a>(
        &'a self,
        request: &'a LoggerLevelTtlWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionAgentError>> + Send + 'a>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("fake state lock");
            if state.active_operation_id.is_some() {
                return Err(ExecutionAgentError::DriverFailed);
            }
            *self.original_level.lock().expect("fake original lock") = Some(state.level.clone());
            state.level = request.level.clone();
            state.active_operation_id = Some(request.operation_id.clone());
            self.writes.lock().expect("fake writes lock").push(request.clone());
            Ok(())
        })
    }
}

impl LoggerLevelControlClient for FakeLoggerClient {
    fn logger_level_state<'a>(&'a self, _component: &'a str, _logger: &'a str) -> DriverFuture<'a, LoggerLevelState> {
        Box::pin(async move { Ok(self.state.lock().expect("fake state lock").clone()) })
    }

    fn restore_logger_level<'a>(
        &'a self,
        request: &'a LoggerLevelTtlRestore,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionAgentError>> + Send + 'a>> {
        Box::pin(async move {
            let original = self
                .original_level
                .lock()
                .expect("fake original lock")
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut state = self.state.lock().expect("fake state lock");
            state.level = original;
            state.active_operation_id = None;
            state.last_completed_operation_id = Some(request.operation_id.clone());
            self.restores.lock().expect("fake restores lock").push(request.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn precheck_rejects_payload_and_out_of_range_logger_requests() {
    let client = Arc::new(FakeLoggerClient::new("WARN"));
    let handler = LoggerLevelTtlHandler::new(Arc::clone(&client));
    let request = read_request(json!({
        "component": "broker",
        "logger": "rocketmq_broker::store::message_body",
        "level": "TRACE",
        "ttl_seconds": 901
    }));

    let result = handler.read_state(&request).await.expect("sanitized rejection result");

    assert!(!result.ready);
    assert_eq!(
        result.reason_codes,
        [
            "logger_level_not_allowlisted",
            "logger_ttl_out_of_range",
            "payload_or_security_logger_forbidden"
        ]
    );
    assert!(client.writes.lock().expect("fake writes lock").is_empty());
}

#[tokio::test]
async fn apply_verify_and_compensate_use_one_typed_call_each() {
    let client = Arc::new(FakeLoggerClient::new("WARN"));
    let handler = LoggerLevelTtlHandler::new(Arc::clone(&client));
    let request = step_request();

    let applied = handler.dispatch(&request, "op-forward").await.expect("typed apply");
    assert_eq!(applied.outcome_code, "logger_level_ttl_applied");
    {
        let writes = client.writes.lock().expect("fake writes lock");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].logger, "rocketmq_proxy::service");
        assert!(writes[0].expires_at > Utc::now() + TimeDelta::seconds(250));
    }

    let verified = handler
        .reconcile(&read_request(request.parameters.clone()), Some("op-forward"))
        .await
        .expect("typed verification");
    assert_eq!(verified.state, ReconcileEffectState::Applied);

    let compensated = handler
        .compensate(&request, "op-compensate")
        .await
        .expect("typed compensation");
    assert_eq!(compensated.outcome_code, "logger_level_restored");
    assert_eq!(client.restores.lock().expect("fake restores lock").len(), 1);
    assert_eq!(client.state.lock().expect("fake state lock").level, "WARN");
}

fn read_request(parameters: serde_json::Value) -> AgentReadRequest {
    AgentReadRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        descriptor_version: "1.0.0".to_owned(),
        target: "component/proxy".to_owned(),
        parameters,
    }
}

fn step_request() -> AgentStepRequest {
    let now = Utc::now();
    let execution_id = ExecutionId::new();
    let step_id = ExecutionStepId::new();
    let plan_step_id = PlanStepId::new();
    let cluster_id = ClusterId::new();
    let parameters = json!({
        "component": "proxy",
        "logger": "rocketmq_proxy::service",
        "level": "DEBUG",
        "ttl_seconds": 300
    });
    let step = PlanStep {
        id: plan_step_id,
        sequence: 1,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        descriptor_version: "1.0.0".to_owned(),
        resource: "component/proxy".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: Vec::new(),
        precondition_hash: digest('a'),
        max_impact: ImpactScope::SingleResource,
        verification: VerificationSpec::default(),
        compensation: CompensationSpec {
            mode: CompensationMode::Automatic,
            required_before_fields: vec!["previous_level".to_owned()],
            timeout_seconds: 60,
        },
    };
    AgentStepRequest {
        intent: StepIntent {
            execution_id,
            step_id,
            plan_hash: digest('b'),
            step,
            attempt: 1,
            idempotency_key: "logger-test".to_owned(),
            fence_grant: LeaseFenceGrant {
                lease_id: LeaseId::new(),
                owner: "executor".to_owned(),
                cluster_id,
                epoch: LeaseEpoch(1),
                execution_id,
                step_id,
                plan_step_id,
                action: ExecutionAction::ObservabilityLoggerLevelTtl,
                resource: "component/proxy".to_owned(),
                audience: "execution-agent".to_owned(),
                issued_at: now,
                expires_at: now + TimeDelta::minutes(1),
                nonce: "nonce".to_owned(),
                signature: "signature".to_owned(),
            },
            intended_at: now,
            compensation: false,
        },
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        descriptor_version: "1.0.0".to_owned(),
        target: "component/proxy".to_owned(),
        parameters,
    }
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
