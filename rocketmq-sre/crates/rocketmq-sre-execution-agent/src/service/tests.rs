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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
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
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationSpec;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

use super::ExecutionAgent;
use crate::AgentActionHandler;
use crate::AgentDriverRegistry;
use crate::AgentEffectStore;
use crate::AuthorityFuture;
use crate::ConfigDriver;
use crate::DispatchBarrier;
use crate::DriverDispatchOutcome;
use crate::DriverFuture;
use crate::ExecutionAgentError;
use crate::FenceAckSigner;
use crate::LeaseAuthorityClient;

#[derive(Clone)]
struct UnavailableAuthority;

impl LeaseAuthorityClient for UnavailableAuthority {
    fn verify_fence_grant<'a>(&'a self, _tenant_id: TenantId, _grant: &'a LeaseFenceGrant) -> AuthorityFuture<'a> {
        Box::pin(async { Err(ExecutionAgentError::AuthorityUnavailable) })
    }

    fn verify_reconcile_grant<'a>(&'a self, _tenant_id: TenantId, _grant: &'a ReconcileGrant) -> AuthorityFuture<'a> {
        Box::pin(async { Err(ExecutionAgentError::AuthorityUnavailable) })
    }
}

#[derive(Clone)]
struct CountingConfigDriver {
    calls: Arc<AtomicUsize>,
}

impl AgentActionHandler for CountingConfigDriver {
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash: sha256('b'),
                ready: true,
                reason_codes: Vec::new(),
                resource_conditions: Default::default(),
                observed_at: Utc::now(),
            })
        })
    }

    fn dispatch<'a>(
        &'a self,
        _request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "applied".to_owned(),
                sanitized_summary: "bounded test change".to_owned(),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _request: &'a AgentReadRequest,
        _operation_id: Option<&str>,
    ) -> DriverFuture<'a, ReconcileEffectResponse> {
        Box::pin(async {
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: ReconcileEffectState::Unknown,
                outcome_code: "unknown".to_owned(),
                sanitized_summary: "no mutation retry".to_owned(),
                observed_at: Utc::now(),
            })
        })
    }

    fn compensate<'a>(
        &'a self,
        _request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "compensated".to_owned(),
                sanitized_summary: "bounded test compensation".to_owned(),
            })
        })
    }
}

impl ConfigDriver for CountingConfigDriver {}

#[tokio::test]
async fn authority_outage_prevents_driver_invocation_before_database_access() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("syntactically valid lazy PostgreSQL URL");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = AgentDriverRegistry::empty();
    registry
        .register_config(
            ExecutionAction::ObservabilityLoggerLevelTtl,
            CountingConfigDriver {
                calls: Arc::clone(&calls),
            },
        )
        .expect("closed config action registration");
    let agent = ExecutionAgent::new(
        AgentEffectStore::new(pool.clone()),
        DispatchBarrier::new(pool),
        Arc::new(UnavailableAuthority),
        registry,
        FenceAckSigner::new("agent-test-signing-key-at-least-32-bytes", "execution-agent-test").expect("test signer"),
        Duration::from_secs(1),
    );

    assert!(matches!(
        agent.dispatch(&dispatch_request()).await,
        Err(ExecutionAgentError::AuthorityUnavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(agent.metrics().fence_rejections_total, 1);
}

fn dispatch_request() -> AgentDispatchRequest {
    let now = Utc::now();
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let execution_id = ExecutionId::new();
    let step_id = ExecutionStepId::new();
    let plan_step_id = PlanStepId::new();
    let parameters = json!({
        "component": "rocketmq_proxy",
        "logger": "rocketmq_proxy::service",
        "level": "DEBUG",
        "ttl_seconds": 300
    });
    let step = PlanStep {
        id: plan_step_id,
        sequence: 1,
        action: ExecutionAction::ObservabilityLoggerLevelTtl,
        descriptor_version: "1.0.0".to_owned(),
        resource: "component/rocketmq_proxy".to_owned(),
        parameters: parameters.clone(),
        evidence_ids: vec![],
        precondition_hash: sha256('b'),
        max_impact: ImpactScope::SingleInstance,
        verification: VerificationSpec {
            resource_conditions: vec!["logger_level_active".to_owned()],
            technical_slis: vec!["log_export_success".to_owned()],
            stable_window_seconds: 30,
            max_wait_seconds: 120,
        },
        compensation: CompensationSpec {
            mode: CompensationMode::Automatic,
            required_before_fields: vec!["previous_level".to_owned()],
            timeout_seconds: 60,
        },
    };
    let grant = LeaseFenceGrant {
        lease_id: LeaseId::new(),
        owner: "executor-test".to_owned(),
        cluster_id,
        epoch: LeaseEpoch(7),
        execution_id,
        step_id,
        plan_step_id,
        action: step.action,
        resource: step.resource.clone(),
        audience: "execution-agent".to_owned(),
        issued_at: now - TimeDelta::seconds(1),
        expires_at: now + TimeDelta::seconds(20),
        nonce: "test-grant-nonce".to_owned(),
        signature: "test-grant-signature".to_owned(),
    };
    AgentDispatchRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id,
        request: AgentStepRequest {
            intent: StepIntent {
                execution_id,
                step_id,
                plan_hash: sha256('a'),
                step,
                attempt: 1,
                idempotency_key: "authority-outage-test".to_owned(),
                fence_grant: grant,
                intended_at: now,
                compensation: false,
            },
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            descriptor_version: "1.0.0".to_owned(),
            target: "component/rocketmq_proxy".to_owned(),
            parameters,
        },
    }
}

fn sha256(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
