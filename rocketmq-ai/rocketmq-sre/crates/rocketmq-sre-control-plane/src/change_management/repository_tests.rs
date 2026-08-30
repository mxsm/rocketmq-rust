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

use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleId;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ChangeWindowId;
use rocketmq_sre_contracts::ChangeWindowKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::RunbookStepPlanBinding;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::ChangeCalendar;
use serde_json::json;
use uuid::Uuid;

use super::ChangeManagementService;
use super::model::ManualGateDecision;
use super::model::ManualGateDecisionRecord;
use super::service::audit_event;
use super::service::schedule_event;
use super::service::scheduler_auth;
use crate::PostgresRepository;
use crate::models::ModelGatewayService;
use crate::supervised_execution::ExecutorSubmissionClient;
use crate::supervised_execution::SupervisedExecutionService;
use crate::workflow::WorkflowEventBus;
use crate::workflow::WorkflowService;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_runbook_calendar_schedule_and_gate_are_durable_and_fail_closed() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let auth = scheduler_auth(tenant_id, cluster_id);
    let correlation_id = CorrelationId::new();
    let now = Utc::now().with_nanosecond(0).expect("second-aligned clock");
    let mut definition: RunbookDefinition =
        serde_yaml::from_str(include_str!("../../../../config/runbooks/telemetry-recovery.v1.yaml"))
            .expect("runbook fixture");
    definition.id = rocketmq_sre_contracts::RunbookId::new();
    definition.created_at = now;
    definition.steps.swap(0, 1);
    definition.steps[0].sequence = 1;
    definition.steps[0].depends_on.clear();
    definition.steps[1].sequence = 2;
    definition.steps[1].depends_on = [definition.steps[0].id].into_iter().collect();
    let runbook_audit = audit_event(
        &auth,
        cluster_id,
        correlation_id,
        AuditEventKind::RunbookCreated,
        "scheduler",
        "runbook",
        definition.id.to_string(),
        "RepositoryTestRunbookCreated",
        json!({}),
        now,
    );
    assert!(
        repository
            .store_runbook_definition(tenant_id, cluster_id, &auth.subject, &definition, &runbook_audit,)
            .await
            .expect("store runbook")
    );
    assert!(
        !repository
            .store_runbook_definition(tenant_id, cluster_id, &auth.subject, &definition, &runbook_audit,)
            .await
            .expect("idempotent runbook")
    );

    let window = ChangeWindow {
        schema_version: ChangeWindow::SCHEMA_VERSION.to_owned(),
        id: ChangeWindowId::new(),
        tenant_id,
        cluster_id,
        name: "repository test maintenance".to_owned(),
        kind: ChangeWindowKind::Maintenance,
        timezone: "UTC".to_owned(),
        starts_at: now,
        ends_at: now + Duration::hours(2),
        resource_keys: BTreeSet::new(),
        max_parallelism: 1,
        reason: "repository durability test".to_owned(),
        created_by: auth.subject.clone(),
        created_at: now,
    };
    let window_audit = audit_event(
        &auth,
        cluster_id,
        correlation_id,
        AuditEventKind::ChangeWindowCreated,
        "scheduler",
        "change_window",
        window.id.to_string(),
        "RepositoryTestWindowCreated",
        json!({}),
        now,
    );
    repository
        .store_change_window(&window, &window_audit)
        .await
        .expect("store window");

    let action_step = definition
        .steps
        .iter()
        .find(|step| matches!(step.body, RunbookStepBody::Action { .. }))
        .expect("action step");
    let gate_step = definition
        .steps
        .iter()
        .find(|step| matches!(step.body, RunbookStepBody::ManualGate { .. }))
        .expect("gate step");
    let mut schedule = ChangeSchedule {
        schema_version: ChangeSchedule::SCHEMA_VERSION.to_owned(),
        id: ChangeScheduleId::new(),
        tenant_id,
        cluster_id,
        correlation_id,
        runbook_id: definition.id,
        runbook_version: definition.version.clone(),
        plan_bindings: vec![RunbookStepPlanBinding {
            step_id: action_step.id,
            plan_id: ActionPlanId::new(),
            plan_hash: digest('1'),
            precondition_hash: digest('2'),
        }],
        scheduled_start: now - Duration::minutes(1),
        scheduled_end: now + Duration::minutes(55),
        resource_keys: ["pod/observability/otel-collector-0".to_owned()].into_iter().collect(),
        status: ChangeScheduleStatus::Scheduled,
        intent_persisted: false,
        next_step_sequence: 1,
        active_execution_id: None,
        waiting_manual_gate: None,
        completed_steps: BTreeSet::new(),
        pause_requested_at: None,
        cancel_requested_at: None,
        created_by: auth.subject.clone(),
        created_at: now,
        updated_at: now,
    };
    let created_event = schedule_event(
        &schedule,
        None,
        "RepositoryTestScheduleCreated",
        &auth.subject,
        json!({}),
    );
    let created_audit = audit_event(
        &auth,
        cluster_id,
        correlation_id,
        AuditEventKind::ChangeScheduleCreated,
        "scheduler",
        "change_schedule",
        schedule.id.to_string(),
        "RepositoryTestScheduleCreated",
        json!({}),
        now,
    );
    repository
        .store_change_schedule(&schedule, 1, &created_event, &created_audit)
        .await
        .expect("store schedule");
    let stored = repository
        .change_schedule(tenant_id, schedule.id)
        .await
        .expect("reload schedule");
    assert_eq!(stored, schedule);

    let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(16));
    let supervised_execution = SupervisedExecutionService::new_with_executor(
        repository.clone(),
        workflow,
        b"repository-test-signing-key-not-exported",
        ModelGatewayService::disabled(repository.clone()),
        ExecutorSubmissionClient::disabled(),
    )
    .expect("supervised execution service");
    let change_management =
        ChangeManagementService::new(repository.clone(), supervised_execution).expect("change management service");
    change_management.run_due().await;
    schedule = repository
        .change_schedule(tenant_id, schedule.id)
        .await
        .expect("scheduler projection");
    assert_eq!(schedule.status, ChangeScheduleStatus::AwaitingManualGate);
    assert_eq!(schedule.waiting_manual_gate, Some(gate_step.id));

    let mut conflicting = schedule.clone();
    conflicting.id = ChangeScheduleId::new();
    conflicting.correlation_id = CorrelationId::new();
    let conflicting_event = schedule_event(&conflicting, None, "RepositoryTestConflict", &auth.subject, json!({}));
    let conflicting_audit = audit_event(
        &auth,
        cluster_id,
        conflicting.correlation_id,
        AuditEventKind::ChangeScheduleCreated,
        "scheduler",
        "change_schedule",
        conflicting.id.to_string(),
        "RepositoryTestConflict",
        json!({}),
        now,
    );
    assert!(
        repository
            .store_change_schedule(&conflicting, 1, &conflicting_event, &conflicting_audit)
            .await
            .is_err()
    );

    let previous_status = schedule.status;
    let previous_updated_at = schedule.updated_at;
    let paused_at = schedule.updated_at + Duration::microseconds(1);
    ChangeCalendar::pause(&mut schedule, paused_at).expect("pause");
    let paused_event = schedule_event(
        &schedule,
        Some(previous_status),
        "RepositoryTestPaused",
        &auth.subject,
        json!({}),
    );
    let paused_audit = audit_event(
        &auth,
        cluster_id,
        correlation_id,
        AuditEventKind::ChangeScheduleStateChanged,
        "scheduler",
        "change_schedule",
        schedule.id.to_string(),
        "RepositoryTestPaused",
        json!({}),
        schedule.updated_at,
    );
    repository
        .update_change_schedule(
            &schedule,
            previous_status,
            previous_updated_at,
            &paused_event,
            &paused_audit,
        )
        .await
        .expect("persist pause");

    let paused_at = schedule.updated_at;
    schedule.status = ChangeScheduleStatus::AwaitingManualGate;
    schedule.next_step_sequence = gate_step.sequence;
    schedule.waiting_manual_gate = Some(gate_step.id);
    schedule.updated_at += Duration::microseconds(1);
    let awaiting_event = schedule_event(
        &schedule,
        Some(ChangeScheduleStatus::Paused),
        "RepositoryTestAwaitingGate",
        &auth.subject,
        json!({}),
    );
    let awaiting_audit = audit_event(
        &auth,
        cluster_id,
        correlation_id,
        AuditEventKind::ChangeScheduleStateChanged,
        "scheduler",
        "change_schedule",
        schedule.id.to_string(),
        "RepositoryTestAwaitingGate",
        json!({}),
        schedule.updated_at,
    );
    repository
        .update_change_schedule(
            &schedule,
            ChangeScheduleStatus::Paused,
            paused_at,
            &awaiting_event,
            &awaiting_audit,
        )
        .await
        .expect("persist waiting gate");

    let gate_updated_at = schedule.updated_at;
    schedule.waiting_manual_gate = None;
    schedule.completed_steps.insert(gate_step.id);
    schedule.next_step_sequence = gate_step.sequence.saturating_add(1);
    schedule.status = ChangeScheduleStatus::Running;
    schedule.updated_at += Duration::microseconds(1);
    let decision = ManualGateDecisionRecord {
        id: Uuid::new_v4(),
        schedule_id: schedule.id,
        step_id: gate_step.id,
        decision: ManualGateDecision::Approved,
        actor_subject: "repository-test-approver".to_owned(),
        actor_role: "approver".to_owned(),
        reason: "bounded repository test approval".to_owned(),
        occurred_at: schedule.updated_at,
    };
    let gate_event = schedule_event(
        &schedule,
        Some(ChangeScheduleStatus::AwaitingManualGate),
        "RepositoryTestGateApproved",
        &decision.actor_subject,
        json!({}),
    );
    let gate_auth = super::service::scheduler_auth(tenant_id, cluster_id);
    let gate_audit = audit_event(
        &gate_auth,
        cluster_id,
        correlation_id,
        AuditEventKind::ManualGateDecided,
        "approver",
        "change_schedule",
        schedule.id.to_string(),
        "RepositoryTestGateApproved",
        json!({}),
        schedule.updated_at,
    );
    repository
        .record_manual_gate_decision(&schedule, gate_updated_at, &decision, &gate_event, &gate_audit)
        .await
        .expect("persist gate decision");
    let completed = repository
        .change_schedule(tenant_id, schedule.id)
        .await
        .expect("reload completed schedule");
    assert_eq!(completed.status, ChangeScheduleStatus::Running);
    assert!(completed.completed_steps.contains(&gate_step.id));

    assert!(
        sqlx::query("UPDATE runbook_definitions SET created_by = 'tampered' WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(definition.id.as_uuid())
            .execute(&repository.pool)
            .await
            .is_err()
    );
}

async fn seed_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES ($1, $2, $3, 'test', 'local', 'test', 'docker',
                   'phase3-runbook-test', 'read_only', 'read_only', 'ready_read_only')",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("phase3-runbook-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("cluster");
}

fn digest(value: char) -> String {
    format!("sha256:{}", value.to_string().repeat(64))
}
