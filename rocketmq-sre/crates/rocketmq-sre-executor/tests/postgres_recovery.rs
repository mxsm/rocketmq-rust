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

#[path = "postgres_recovery/support.rs"]
mod support;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::EffectState;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::NotificationTargetId;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::ResourceLockId;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::VerificationOutcome;
use rocketmq_sre_contracts::VerificationResult;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_execution_agent::AgentEffectStore;
use rocketmq_sre_executor::ExecutionJournal;
use rocketmq_sre_executor::JournalError;
use rocketmq_sre_executor::LeaseCoordinator;
use rocketmq_sre_executor::ResourceLockRequest;
use rocketmq_sre_executor::ResourceSafetyStore;
use rocketmq_sre_executor::VerificationPhase;
use serde_json::json;
use uuid::Uuid;

use support::assert_critic_review_is_immutable;
use support::assert_phase_three_tables;
use support::audit;
use support::cleanup_schema;
use support::execution_request;
use support::isolated_pool;
use support::seed_fixture;
use support::step_intent;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn migrations_journal_locks_fences_and_restart_recovery_are_durable() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    assert_phase_three_tables(&pool).await;

    let fixture = seed_fixture(&pool).await;
    assert_critic_review_is_immutable(&pool, &fixture).await;

    let wrong_audience_journal = ExecutionJournal::new(pool.clone(), "wrong-audience");
    assert!(
        wrong_audience_journal
            .create_execution(
                &fixture.request,
                "deployment/default/proxy",
                ExecutionAction::ProxyScaleOutOne,
                Utc::now()
            )
            .await
            .is_err()
    );
    let journal = ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor");
    let now = Utc::now();
    let first = journal
        .create_execution(
            &fixture.request,
            "deployment/default/proxy",
            ExecutionAction::ProxyScaleOutOne,
            now,
        )
        .await
        .expect("first execution");
    assert!(first.created);
    let retry = journal
        .create_execution(
            &fixture.request,
            "deployment/default/proxy",
            ExecutionAction::ProxyScaleOutOne,
            now,
        )
        .await
        .expect("identical retry");
    assert!(!retry.created);
    let execution_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions WHERE idempotency_key = $1")
        .bind(&fixture.request.idempotency_key)
        .fetch_one(&pool)
        .await
        .expect("execution count");
    assert_eq!(execution_count, 1);

    let second_request = execution_request(&fixture.plan, "execution-request-2");
    journal
        .create_execution(
            &second_request,
            "deployment/default/proxy",
            ExecutionAction::ProxyScaleOutOne,
            Utc::now(),
        )
        .await
        .expect("second execution");

    let verification_observed_at = now + TimeDelta::seconds(1);
    let verification_evidence = rocketmq_sre_contracts::EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: fixture.request.correlation_id,
            tenant_id: fixture.tenant_id,
            cluster_id: fixture.cluster_id,
            source: "executor-verification-test".to_owned(),
            resource: "deployment/default/proxy".to_owned(),
            time_range: TimeRange::new(verification_observed_at, verification_observed_at).expect("time range"),
        },
        current_evidence_schema(),
        verification_observed_at,
        EvidenceContent::Inline(json!({"ready": true, "error_ratio_healthy": true})),
    )
    .expect("verification Evidence");
    let verification_step_id = rocketmq_sre_contracts::ExecutionStepId::new();
    let verification_capture_audit = audit(
        &fixture,
        AuditEventKind::VerificationCaptured,
        "verification_pre_captured",
        verification_observed_at,
    );
    assert!(
        journal
            .append_verification_evidence_with_audit(
                fixture.request.id,
                verification_step_id,
                1,
                VerificationPhase::Pre,
                &verification_evidence,
                &verification_capture_audit,
            )
            .await
            .expect("append verification Evidence")
    );
    assert!(
        !journal
            .append_verification_evidence_with_audit(
                fixture.request.id,
                verification_step_id,
                1,
                VerificationPhase::Pre,
                &verification_evidence,
                &verification_capture_audit,
            )
            .await
            .expect("idempotent verification Evidence")
    );
    let verification_result = VerificationResult {
        step_id: verification_step_id,
        outcome: VerificationOutcome::Succeeded,
        started_at: verification_observed_at,
        completed_at: verification_observed_at + TimeDelta::seconds(30),
        pre_evidence_ids: vec![verification_evidence.evidence_id],
        during_evidence_ids: Vec::new(),
        post_evidence_ids: vec![EvidenceId::new()],
        satisfied_conditions: vec!["resource:ready".to_owned(), "sli:error_ratio".to_owned()],
        failed_conditions: Vec::new(),
        stable_window_seconds: 30,
    };
    assert!(
        journal
            .append_verification_result_with_audit(
                fixture.request.id,
                1,
                false,
                &verification_result,
                &audit(
                    &fixture,
                    AuditEventKind::VerificationCompleted,
                    "verification_completed",
                    verification_result.completed_at,
                ),
            )
            .await
            .expect("append verification result")
    );
    let evidence_records = journal
        .verification_evidence(fixture.request.id)
        .await
        .expect("load verification Evidence");
    assert_eq!(evidence_records.len(), 1);
    assert_eq!(evidence_records[0].phase, VerificationPhase::Pre);
    assert_eq!(evidence_records[0].evidence, verification_evidence);
    assert!(
        sqlx::query("UPDATE execution_verifications SET outcome = 'failed' WHERE execution_id = $1")
            .bind(fixture.request.id.as_uuid())
            .execute(&pool)
            .await
            .is_err(),
        "verification journal must be append-only"
    );

    let safety = ResourceSafetyStore::new(pool.clone());
    let first_lock = ResourceLockRequest {
        id: ResourceLockId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        resource_key: "deployment/default/proxy".to_owned(),
        action: ExecutionAction::ProxyScaleOutOne,
        holder_execution_id: fixture.request.id,
        acquired_at: now,
        expires_at: now + TimeDelta::minutes(5),
    };
    safety.acquire(&first_lock).await.expect("first lock");
    let competing_lock = ResourceLockRequest {
        id: ResourceLockId::new(),
        holder_execution_id: second_request.id,
        ..first_lock.clone()
    };
    assert!(matches!(
        safety.acquire(&competing_lock).await,
        Err(JournalError::ResourceLocked)
    ));
    safety
        .release(
            first_lock.id,
            fixture.request.id,
            now + TimeDelta::seconds(1),
            "handoff_to_quarantine",
        )
        .await
        .expect("release lock");

    let quarantine = ResourceQuarantine {
        id: ResourceQuarantineId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        resource_key: first_lock.resource_key.clone(),
        action_id: Some(first_lock.action.id().to_owned()),
        reason_code: "verification_inconclusive".to_owned(),
        source_execution_id: Some(fixture.request.id),
        evidence_ids: vec![EvidenceId::new()],
        created_by: "executor-service".to_owned(),
        created_at: now + TimeDelta::seconds(2),
        cleared_by: None,
        clear_reason: None,
        clear_evidence_ids: Vec::new(),
        cleared_at: None,
    };
    assert!(safety.quarantine(&quarantine).await.expect("quarantine"));
    assert!(matches!(
        safety.acquire(&competing_lock).await,
        Err(JournalError::ResourceQuarantined)
    ));
    assert!(
        safety
            .clear_quarantine(
                quarantine.id,
                "approver-a",
                "verified healthy",
                &[],
                now + TimeDelta::seconds(3)
            )
            .await
            .is_err()
    );
    safety
        .clear_quarantine(
            quarantine.id,
            "approver-a",
            "verified healthy",
            &[EvidenceId::new()],
            now + TimeDelta::seconds(3),
        )
        .await
        .expect("audited quarantine clear");
    safety.acquire(&competing_lock).await.expect("lock after audited clear");

    let leases = LeaseCoordinator::new(pool.clone());
    let first_lease = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            "executor-a",
            "pending-a",
            now + TimeDelta::seconds(4),
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("first lease");
    let second_lease = leases
        .begin_takeover(
            fixture.tenant_id,
            fixture.cluster_id,
            "executor-b",
            "pending-b",
            now + TimeDelta::seconds(5),
            now + TimeDelta::minutes(10),
        )
        .await
        .expect("takeover lease");
    assert!(second_lease.epoch > first_lease.epoch);

    let ack = FenceAck {
        cluster_id: fixture.cluster_id,
        epoch: second_lease.epoch,
        pending_nonce: second_lease.pending_nonce.clone(),
        agent_subject: "execution-agent-a".to_owned(),
        acknowledged_at: now + TimeDelta::seconds(6),
        signature: "fixture-signature".to_owned(),
    };
    assert!(matches!(
        leases.activate(&second_lease, &ack).await,
        Err(JournalError::LeaseRejected)
    ));
    let agent = AgentEffectStore::new(pool.clone());
    assert!(
        agent
            .accept_fence(fixture.tenant_id, second_lease.id, &ack)
            .await
            .expect("persist fence")
    );
    let active_lease = leases
        .activate(&second_lease, &ack)
        .await
        .expect("activate after fence ack");

    let stale_intent = step_intent(
        &fixture,
        first_lease.id,
        first_lease.epoch,
        &first_lease.owner,
        first_lease.expires_at,
        "stale-intent",
        now + TimeDelta::seconds(7),
    );
    assert!(matches!(
        journal
            .append_intent_with_audit(
                &stale_intent,
                &audit(
                    &fixture,
                    AuditEventKind::StepIntentPersisted,
                    "stale-intent-audit",
                    now + TimeDelta::seconds(7)
                )
            )
            .await,
        Err(JournalError::LeaseRejected)
    ));
    let stale_audit_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE reason_code = 'stale-intent-audit'")
            .fetch_one(&pool)
            .await
            .expect("stale audit count");
    assert_eq!(stale_audit_count, 0);

    let intent = step_intent(
        &fixture,
        active_lease.id,
        active_lease.epoch,
        &active_lease.owner,
        active_lease.expires_at,
        "active-intent",
        now + TimeDelta::seconds(8),
    );
    assert!(
        journal
            .append_intent_with_audit(
                &intent,
                &audit(
                    &fixture,
                    AuditEventKind::StepIntentPersisted,
                    "active-intent-audit",
                    now + TimeDelta::seconds(8)
                )
            )
            .await
            .expect("persist active intent")
    );

    let agent_request = AgentStepRequest {
        intent: intent.clone(),
        action: intent.step.action,
        descriptor_version: intent.step.descriptor_version.clone(),
        target: intent.step.resource.clone(),
        parameters: intent.step.parameters.clone(),
    };
    let prepared = agent
        .prepare(fixture.tenant_id, &agent_request, now + TimeDelta::seconds(9))
        .await
        .expect("prepared before dispatch");
    assert!(prepared.created);
    let restarted_agent = AgentEffectStore::new(pool.clone());
    assert_eq!(
        restarted_agent
            .highest_epoch(fixture.cluster_id)
            .await
            .expect("restored fence"),
        Some(active_lease.epoch)
    );
    let duplicate = restarted_agent
        .prepare(fixture.tenant_id, &agent_request, now + TimeDelta::seconds(9))
        .await
        .expect("idempotent prepared effect");
    assert!(!duplicate.created);
    assert_eq!(duplicate.effect.state, EffectState::Prepared);
    restarted_agent
        .mark_dispatched(&intent.idempotency_key, "operation-1", now + TimeDelta::seconds(10))
        .await
        .expect("durable dispatch marker");

    let restarted_journal = ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor");
    let pending = restarted_journal
        .pending_intents(100)
        .await
        .expect("restart recovery query");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent, intent);

    restarted_agent
        .confirm(
            &intent.idempotency_key,
            "applied",
            "one proxy replica accepted",
            now + TimeDelta::seconds(11),
        )
        .await
        .expect("confirmed effect");
    let result = StepResult {
        step_id: intent.step_id,
        state: ExecutionState::Verifying,
        agent_result: None,
        verification: None,
        reason_code: "agent_effect_confirmed".to_owned(),
        completed_at: now + TimeDelta::seconds(12),
    };
    restarted_journal
        .append_result_with_audit(
            fixture.request.id,
            intent.attempt,
            &result,
            &audit(
                &fixture,
                AuditEventKind::StepResultPersisted,
                "active-result-audit",
                now + TimeDelta::seconds(12),
            ),
        )
        .await
        .expect("append result");
    assert!(
        restarted_journal
            .pending_intents(100)
            .await
            .expect("no unresolved intents")
            .is_empty()
    );
    let step_record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM execution_steps
         WHERE execution_id = $1 AND step_id = $2",
    )
    .bind(fixture.request.id.as_uuid())
    .bind(intent.step_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("append-only step records");
    assert_eq!(step_record_count, 2);

    cleanup_schema(&pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn rollback_failure_atomically_escalates_quarantines_and_notifies() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let fixture = seed_fixture(&pool).await;
    let target_id = NotificationTargetId::new();
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO notification_targets (
            id, tenant_id, cluster_id, name, channel, endpoint,
            enabled, created_at, updated_at
         ) VALUES ($1, $2, $3, 'phase3-on-call', 'pager', 'mock://phase3-on-call', TRUE, $4, $4)",
    )
    .bind(target_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(now)
    .execute(&pool)
    .await
    .expect("notification target");

    let journal = ExecutionJournal::new(pool.clone(), "rocketmq-sre-executor");
    journal
        .create_execution(
            &fixture.request,
            "deployment/default/proxy",
            ExecutionAction::ProxyScaleOutOne,
            now,
        )
        .await
        .expect("execution");
    for transition in [
        ExecutionTransition {
            from: ExecutionState::Pending,
            to: ExecutionState::Prechecking,
            reason_code: "precheck_started".to_owned(),
            occurred_at: now + TimeDelta::seconds(1),
        },
        ExecutionTransition {
            from: ExecutionState::Prechecking,
            to: ExecutionState::Compensating,
            reason_code: "rollback_required".to_owned(),
            occurred_at: now + TimeDelta::seconds(2),
        },
    ] {
        assert!(
            journal
                .transition(fixture.request.id, &transition)
                .await
                .expect("state transition")
        );
    }

    let occurred_at = now + TimeDelta::seconds(3);
    let quarantine = ResourceQuarantine {
        id: ResourceQuarantineId::new(),
        tenant_id: fixture.tenant_id,
        cluster_id: fixture.cluster_id,
        resource_key: "deployment/default/proxy".to_owned(),
        action_id: Some(ExecutionAction::ProxyScaleOutOne.id().to_owned()),
        reason_code: "rollback_verification_failed".to_owned(),
        source_execution_id: Some(fixture.request.id),
        evidence_ids: vec![EvidenceId::new()],
        created_by: "executor-service".to_owned(),
        created_at: occurred_at,
        cleared_by: None,
        clear_reason: None,
        clear_evidence_ids: Vec::new(),
        cleared_at: None,
    };
    let transition = ExecutionTransition {
        from: ExecutionState::Compensating,
        to: ExecutionState::Escalated,
        reason_code: "manual_takeover_required".to_owned(),
        occurred_at,
    };
    let state_audit = audit(
        &fixture,
        AuditEventKind::StateChanged,
        "manual_takeover_required",
        occurred_at,
    );
    let quarantine_audit = audit(
        &fixture,
        AuditEventKind::QuarantineCreated,
        "rollback_verification_failed",
        occurred_at,
    );
    let manual_audit = audit(
        &fixture,
        AuditEventKind::ManualTakeoverRequired,
        "manual_takeover_required",
        occurred_at,
    );
    let escalation = journal
        .escalate_manual_takeover(
            fixture.request.id,
            &transition,
            &quarantine,
            &state_audit,
            &quarantine_audit,
            &manual_audit,
        )
        .await
        .expect("atomic escalation");
    assert!(escalation.escalated);
    assert!(escalation.quarantine_created);
    assert_eq!(escalation.notifications_enqueued, 1);
    assert_eq!(
        journal
            .execution_state(fixture.request.id)
            .await
            .expect("execution state"),
        ExecutionState::Escalated
    );
    let active_quarantine: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM resource_quarantines
         WHERE id = $1 AND cleared_at IS NULL",
    )
    .bind(quarantine.id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("quarantine count");
    assert_eq!(active_quarantine, 1);
    let notification_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification_outbox
         WHERE delivery_key = $1 AND status = 'pending'",
    )
    .bind(format!("manual-takeover:{}:{target_id}", fixture.request.id))
    .fetch_one(&pool)
    .await
    .expect("notification count");
    assert_eq!(notification_count, 1);

    let replay = journal
        .escalate_manual_takeover(
            fixture.request.id,
            &transition,
            &quarantine,
            &state_audit,
            &quarantine_audit,
            &manual_audit,
        )
        .await
        .expect("idempotent escalation");
    assert!(!replay.escalated);
    assert_eq!(replay.notifications_enqueued, 0);

    cleanup_schema(&pool, &schema).await;
}
