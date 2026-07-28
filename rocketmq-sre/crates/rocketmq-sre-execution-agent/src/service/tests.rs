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
use rocketmq_sre_contracts::AUTONOMY_SCHEMA_VERSION;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AgentDispatchAuthorization;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::CompensationSpec;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::DynamicSafetyDecisionId;
use rocketmq_sre_contracts::DynamicSafetyVerification;
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

    fn verify_dynamic_safety<'a>(
        &'a self,
        _tenant_id: TenantId,
        _decision: &'a DynamicSafetyDecision,
    ) -> AuthorityFuture<'a, DynamicSafetyVerification> {
        Box::pin(async { Err(ExecutionAgentError::AuthorityUnavailable) })
    }
}

#[derive(Clone)]
struct AcceptingAuthority;

impl LeaseAuthorityClient for AcceptingAuthority {
    fn verify_fence_grant<'a>(&'a self, _tenant_id: TenantId, grant: &'a LeaseFenceGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            Ok(rocketmq_sre_contracts::GrantVerification {
                schema_version: rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                valid: true,
                cluster_id: grant.cluster_id,
                epoch: grant.epoch,
                expires_at: grant.expires_at,
            })
        })
    }

    fn verify_reconcile_grant<'a>(&'a self, _tenant_id: TenantId, grant: &'a ReconcileGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            Ok(rocketmq_sre_contracts::GrantVerification {
                schema_version: rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                valid: true,
                cluster_id: grant.cluster_id,
                epoch: grant.pending_epoch,
                expires_at: grant.expires_at,
            })
        })
    }

    fn verify_dynamic_safety<'a>(
        &'a self,
        tenant_id: TenantId,
        decision: &'a DynamicSafetyDecision,
    ) -> AuthorityFuture<'a, DynamicSafetyVerification> {
        Box::pin(async move {
            Ok(DynamicSafetyVerification {
                schema_version: AUTONOMY_SCHEMA_VERSION.to_owned(),
                valid: true,
                decision_id: decision.id,
                tenant_id,
                cluster_id: decision.cluster_id,
                plan_id: decision.plan_id,
                execution_id: decision.execution_id,
                execution_step_id: decision.execution_step_id,
                expires_at: decision.expires_at,
            })
        })
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

#[tokio::test]
async fn autonomous_forward_requires_live_safety_but_compensation_remains_available() {
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
        Arc::new(AcceptingAuthority),
        registry,
        FenceAckSigner::new("agent-test-signing-key-at-least-32-bytes", "execution-agent-test").expect("test signer"),
        Duration::from_secs(1),
    );

    let mut missing = autonomous_dispatch_request(false, false);
    assert!(matches!(
        agent.verify_dispatch_safety(&missing).await,
        Err(ExecutionAgentError::AuthorityRejected)
    ));

    missing.request.intent.dynamic_safety = Some(dynamic_safety(&missing));
    missing
        .request
        .intent
        .dynamic_safety
        .as_mut()
        .expect("decision")
        .expires_at = Utc::now();
    assert!(matches!(
        agent.verify_dispatch_safety(&missing).await,
        Err(ExecutionAgentError::AuthorityRejected)
    ));

    let valid = autonomous_dispatch_request(false, true);
    agent
        .verify_dispatch_safety(&valid)
        .await
        .expect("current signed autonomous decision");

    let compensation = autonomous_dispatch_request(true, false);
    agent
        .verify_dispatch_safety(&compensation)
        .await
        .expect("compensation does not depend on dynamic safety");
    assert!(agent.registry.validate_dispatch(&compensation.request).is_ok());

    let mut forged = compensation;
    forged.request.intent.fence_grant.compensation = false;
    assert!(matches!(
        agent.registry.validate_dispatch(&forged.request),
        Err(ExecutionAgentError::InvalidRequest)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
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
        compensation: false,
        audience: "execution-agent".to_owned(),
        issued_at: now - TimeDelta::seconds(1),
        expires_at: now + TimeDelta::seconds(20),
        nonce: "test-grant-nonce".to_owned(),
        signature: "test-grant-signature".to_owned(),
    };
    AgentDispatchRequest {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        tenant_id,
        plan_id: Some(ActionPlanId::new()),
        authorization: AgentDispatchAuthorization::HumanApproved,
        request: AgentStepRequest {
            intent: StepIntent {
                execution_id,
                step_id,
                plan_hash: sha256('a'),
                step,
                attempt: 1,
                idempotency_key: "authority-outage-test".to_owned(),
                fence_grant: grant,
                dynamic_safety: None,
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

fn autonomous_dispatch_request(compensation: bool, with_decision: bool) -> AgentDispatchRequest {
    let mut request = dispatch_request();
    request.authorization = AgentDispatchAuthorization::Autonomous;
    request.request.intent.compensation = compensation;
    request.request.intent.fence_grant.compensation = compensation;
    if with_decision {
        request.request.intent.dynamic_safety = Some(dynamic_safety(&request));
    }
    request
}

fn dynamic_safety(request: &AgentDispatchRequest) -> DynamicSafetyDecision {
    let now = Utc::now();
    DynamicSafetyDecision {
        id: DynamicSafetyDecisionId::new(),
        tenant_id: request.tenant_id,
        cluster_id: request.request.intent.fence_grant.cluster_id,
        action: request.request.action,
        action_version: request.request.descriptor_version.clone(),
        plan_id: request.plan_id.expect("test plan id"),
        plan_hash: request.request.intent.plan_hash.clone(),
        execution_id: request.request.intent.execution_id,
        execution_step_id: request.request.intent.step_id,
        policy_definition_version: 1,
        lifecycle_revision: 1,
        error_budget_available: true,
        freeze_revision: 0,
        kill_switch_revision: 0,
        evidence_fresh: true,
        allowed: true,
        reason_codes: Vec::new(),
        issued_at: now,
        expires_at: now + TimeDelta::seconds(30),
        nonce: "agent-dynamic-safety-test".to_owned(),
        signature: "agent-dynamic-safety-signature".to_owned(),
    }
}

fn sha256(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
