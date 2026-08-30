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

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ChangeConflictCode;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleId;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ChangeWindowId;
use rocketmq_sre_contracts::ChangeWindowKind;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CompensationEdge;
use rocketmq_sre_contracts::CompensationTrigger;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ManualGate;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::RunbookStep;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::RunbookStepId;
use rocketmq_sre_contracts::RunbookStepPlanBinding;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_core::ActionCatalog;
use rocketmq_sre_core::ChangeCalendar;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use rocketmq_sre_core::RunbookValidator;
use serde_json::json;

#[test]
fn runbook_risk_serial_dependencies_and_parameters_fail_closed() {
    let catalog = catalog();
    let mut base_runbook = runbook();
    RunbookValidator::validate(&base_runbook, &catalog).expect("valid composite runbook");

    base_runbook.risk = ActionRisk::R1;
    assert!(RunbookValidator::validate(&base_runbook, &catalog).is_err());

    let mut unsafe_runbook = runbook();
    if let RunbookStepBody::Action { parameters, .. } = &mut unsafe_runbook.steps[0].body {
        parameters["shell"] = json!("rm -rf /");
    }
    assert!(RunbookValidator::validate(&unsafe_runbook, &catalog).is_err());

    let mut non_serial = runbook();
    non_serial.steps[2].depends_on.clear();
    assert!(RunbookValidator::validate(&non_serial, &catalog).is_err());

    let mut plan_only = runbook();
    if let RunbookStepBody::Action {
        action: action_id,
        descriptor_version: version,
        ..
    } = &mut plan_only.steps[2].body
    {
        *action_id = ExecutionAction::ControllerElect;
        *version = "1.0.0".to_owned();
    }
    assert!(RunbookValidator::validate(&plan_only, &catalog).is_err());

    let mut invalid_compensation = runbook();
    invalid_compensation.compensation_edges = vec![CompensationEdge {
        from_step: invalid_compensation.steps[0].id,
        compensation_step: invalid_compensation.steps[0].id,
        trigger: CompensationTrigger::StepFailed,
    }];
    assert!(RunbookValidator::validate(&invalid_compensation, &catalog).is_err());
}

#[test]
fn committed_runbook_templates_are_typed_bounded_and_valid() {
    let catalog = catalog();
    let templates = [
        include_str!("../../../config/runbooks/proxy-canary-rollout.v1.yaml"),
        include_str!("../../../config/runbooks/broker-one-by-one-restart.v1.yaml"),
        include_str!("../../../config/runbooks/credential-rotation-overlap.v1.yaml"),
        include_str!("../../../config/runbooks/telemetry-recovery.v1.yaml"),
    ];
    for yaml in templates {
        let definition: RunbookDefinition = serde_yaml::from_str(yaml).expect("typed runbook template");
        RunbookValidator::validate(&definition, &catalog).expect("valid registered runbook");
        assert_eq!(definition.max_parallelism, 1);
        assert!(!definition.compensation_edges.is_empty());
        assert!(
            definition
                .steps
                .iter()
                .all(|step| !matches!(&step.body, RunbookStepBody::Action { parameters, .. }
                    if contains_forbidden_parameter(parameters)))
        );
        let now = Utc::now();
        let mut scheduled = schedule(TenantId::new(), ClusterId::new(), "runbook/template", now);
        scheduled.runbook_id = definition.id;
        scheduled.runbook_version.clone_from(&definition.version);
        scheduled.plan_bindings = definition
            .steps
            .iter()
            .filter(|step| matches!(step.body, RunbookStepBody::Action { .. }))
            .map(|step| RunbookStepPlanBinding {
                step_id: step.id,
                plan_id: ActionPlanId::new(),
                plan_hash: sha256_fixture(),
                precondition_hash: sha256_fixture(),
            })
            .collect();
        RunbookValidator::validate_schedule_bindings(&definition, &scheduled).expect("every typed action is bound");
        scheduled.plan_bindings.pop();
        assert!(RunbookValidator::validate_schedule_bindings(&definition, &scheduled).is_err());
    }
}

#[test]
fn declared_parallelism_and_multi_resource_scope_raise_the_risk_floor() {
    let catalog = catalog();
    let mut definition: RunbookDefinition =
        serde_yaml::from_str(include_str!("../../../config/runbooks/telemetry-recovery.v1.yaml"))
            .expect("telemetry template");
    definition.max_parallelism = 2;
    assert!(RunbookValidator::validate(&definition, &catalog).is_err());
    definition.risk = ActionRisk::R2;
    RunbookValidator::validate(&definition, &catalog).expect("conservatively classified runbook");

    let mut multi_resource = definition;
    multi_resource.max_parallelism = 1;
    let first = multi_resource.steps[0].clone();
    let previous = multi_resource.steps[1].id;
    let mut second = first;
    second.id = RunbookStepId::new();
    second.sequence = 3;
    second.name = "restart second telemetry Collector".to_owned();
    second.depends_on = [previous].into_iter().collect();
    if let RunbookStepBody::Action { resource, .. } = &mut second.body {
        *resource = "pod/observability/otel-collector-1".to_owned();
    }
    multi_resource.steps.push(second);
    multi_resource.risk = ActionRisk::R1;
    assert!(RunbookValidator::validate(&multi_resource, &catalog).is_err());
}

#[test]
fn calendar_detects_freeze_resource_and_parallelism_conflicts() {
    let now = Utc::now();
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let resource = "deployment/rocketmq/proxy".to_owned();
    let requested_schedule = schedule(tenant_id, cluster_id, &resource, now);
    let maintenance = window(
        tenant_id,
        cluster_id,
        ChangeWindowKind::Maintenance,
        now,
        now + TimeDelta::hours(2),
        BTreeSet::new(),
        1,
    );
    let freeze = window(
        tenant_id,
        cluster_id,
        ChangeWindowKind::Freeze,
        now + TimeDelta::minutes(15),
        now + TimeDelta::minutes(45),
        [resource.clone()].into_iter().collect(),
        1,
    );
    let existing = schedule(tenant_id, cluster_id, &resource, now);

    let conflicts = ChangeCalendar::conflicts(&requested_schedule, 1, &[maintenance, freeze], &[existing])
        .expect("conflict evaluation");
    let codes = conflicts.iter().map(|conflict| conflict.code).collect::<BTreeSet<_>>();
    assert!(codes.contains(&ChangeConflictCode::FreezeWindow));
    assert!(codes.contains(&ChangeConflictCode::ResourceOverlap));
    assert!(codes.contains(&ChangeConflictCode::ParallelismExceeded));
    assert!(!codes.contains(&ChangeConflictCode::OutsideMaintenanceWindow));
}

#[test]
fn calendar_rejects_invalid_timezones_and_reports_blackout_or_missing_maintenance() {
    let now = Utc::now();
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let resource = "deployment/rocketmq/proxy";
    let requested = schedule(tenant_id, cluster_id, resource, now);
    let mut invalid = window(
        tenant_id,
        cluster_id,
        ChangeWindowKind::Maintenance,
        now,
        now + TimeDelta::hours(2),
        BTreeSet::new(),
        1,
    );
    invalid.timezone = "../Asia/Shanghai".to_owned();
    assert!(ChangeCalendar::validate_window(&invalid).is_err());

    let blackout = window(
        tenant_id,
        cluster_id,
        ChangeWindowKind::Blackout,
        now,
        now + TimeDelta::hours(2),
        BTreeSet::new(),
        1,
    );
    let conflicts = ChangeCalendar::conflicts(&requested, 1, &[blackout], &[]).expect("conflicts");
    let codes = conflicts.iter().map(|conflict| conflict.code).collect::<BTreeSet<_>>();
    assert!(codes.contains(&ChangeConflictCode::BlackoutWindow));
    assert!(codes.contains(&ChangeConflictCode::OutsideMaintenanceWindow));
}

#[test]
fn pause_and_cancel_preserve_post_intent_reconciliation() {
    let now = Utc::now();
    let mut scheduled = schedule(TenantId::new(), ClusterId::new(), "broker/broker-a", now);
    ChangeCalendar::pause(&mut scheduled, now).expect("pause");
    assert_eq!(scheduled.status, ChangeScheduleStatus::Paused);
    ChangeCalendar::resume(&mut scheduled, now).expect("resume");
    assert_eq!(scheduled.status, ChangeScheduleStatus::Scheduled);
    ChangeCalendar::cancel(&mut scheduled, now).expect("pre-intent cancel");
    assert_eq!(scheduled.status, ChangeScheduleStatus::Cancelled);

    let mut running = schedule(TenantId::new(), ClusterId::new(), "broker/broker-b", now);
    running.status = ChangeScheduleStatus::Running;
    running.intent_persisted = true;
    ChangeCalendar::cancel(&mut running, now).expect("post-intent safe stop");
    assert_eq!(running.status, ChangeScheduleStatus::SafeStopping);
    ChangeCalendar::begin_reconcile(&mut running, now).expect("reconcile");
    assert_eq!(running.status, ChangeScheduleStatus::Reconciling);
    assert!(ChangeCalendar::pause(&mut running, now).is_err());
}

fn catalog() -> ActionCatalog {
    let mut catalog = ActionCatalog::default();
    for yaml in EMBEDDED_ACTION_DESCRIPTOR_YAMLS {
        let descriptor: ActionDescriptor = serde_yaml::from_str(yaml).expect("descriptor");
        catalog.register(descriptor).expect("catalog registration");
    }
    catalog
}

fn runbook() -> RunbookDefinition {
    let first = RunbookStepId::new();
    let gate = RunbookStepId::new();
    let rotate = RunbookStepId::new();
    RunbookDefinition {
        schema_version: RunbookDefinition::SCHEMA_VERSION.to_owned(),
        id: RunbookId::new(),
        name: "proxy-and-credential".to_owned(),
        version: "1.0.0".to_owned(),
        owner: "messaging-platform".to_owned(),
        description: "Scale one Proxy, require a manual gate, then rotate credentials.".to_owned(),
        risk: ActionRisk::R2,
        max_parallelism: 1,
        steps: vec![
            RunbookStep {
                id: first,
                sequence: 1,
                name: "scale proxy".to_owned(),
                depends_on: BTreeSet::new(),
                parallel_group: None,
                condition: None,
                body: RunbookStepBody::Action {
                    action: ExecutionAction::ProxyScaleOutOne,
                    descriptor_version: "1.0.0".to_owned(),
                    resource: "deployment/rocketmq/proxy".to_owned(),
                    parameters: json!({
                        "namespace": "rocketmq",
                        "workload": "proxy",
                        "expected_replicas": 2
                    }),
                },
            },
            RunbookStep {
                id: gate,
                sequence: 2,
                name: "operator confirmation".to_owned(),
                depends_on: [first].into_iter().collect(),
                parallel_group: None,
                condition: None,
                body: RunbookStepBody::ManualGate {
                    gate: ManualGate {
                        gate_id: "verify-proxy".to_owned(),
                        title: "Verify Proxy".to_owned(),
                        instructions: "Confirm the new replica is healthy.".to_owned(),
                        required_role: "approver".to_owned(),
                        timeout_seconds: 1800,
                    },
                },
            },
            RunbookStep {
                id: rotate,
                sequence: 3,
                name: "rotate credential".to_owned(),
                depends_on: [gate].into_iter().collect(),
                parallel_group: None,
                condition: None,
                body: RunbookStepBody::Action {
                    action: ExecutionAction::SecurityCredentialRotateOverlap,
                    descriptor_version: "1.0.0".to_owned(),
                    resource: "credential-set/broker-api".to_owned(),
                    parameters: json!({
                        "credential_set": "broker-api",
                        "active_version": "v1",
                        "candidate_version": "v2",
                        "candidate_secret_ref": "vault://rocketmq/v2",
                        "overlap_seconds": 300,
                        "validation_probe_topic": "SRE_PROBE_ROTATION"
                    }),
                },
            },
        ],
        compensation_edges: Vec::new(),
        created_at: Utc::now(),
    }
}

fn window(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    kind: ChangeWindowKind,
    starts_at: chrono::DateTime<Utc>,
    ends_at: chrono::DateTime<Utc>,
    resource_keys: BTreeSet<String>,
    max_parallelism: u16,
) -> ChangeWindow {
    ChangeWindow {
        schema_version: ChangeWindow::SCHEMA_VERSION.to_owned(),
        id: ChangeWindowId::new(),
        tenant_id,
        cluster_id,
        name: format!("{kind:?} window"),
        kind,
        timezone: "Asia/Shanghai".to_owned(),
        starts_at,
        ends_at,
        resource_keys,
        max_parallelism,
        reason: "planned maintenance".to_owned(),
        created_by: "operator".to_owned(),
        created_at: Utc::now(),
    }
}

fn schedule(tenant_id: TenantId, cluster_id: ClusterId, resource: &str, now: chrono::DateTime<Utc>) -> ChangeSchedule {
    ChangeSchedule {
        schema_version: ChangeSchedule::SCHEMA_VERSION.to_owned(),
        id: ChangeScheduleId::new(),
        tenant_id,
        cluster_id,
        correlation_id: CorrelationId::new(),
        runbook_id: RunbookId::new(),
        runbook_version: "1.0.0".to_owned(),
        plan_bindings: Vec::new(),
        scheduled_start: now + TimeDelta::minutes(10),
        scheduled_end: now + TimeDelta::minutes(50),
        resource_keys: [resource.to_owned()].into_iter().collect(),
        status: ChangeScheduleStatus::Scheduled,
        intent_persisted: false,
        next_step_sequence: 1,
        active_execution_id: None,
        waiting_manual_gate: None,
        completed_steps: BTreeSet::new(),
        pause_requested_at: None,
        cancel_requested_at: None,
        created_by: "operator".to_owned(),
        created_at: now,
        updated_at: now,
    }
}

fn sha256_fixture() -> String {
    format!("sha256:{}", "0".repeat(64))
}

fn contains_forbidden_parameter(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(values) => values.iter().any(|(field, value)| {
            [
                "shell",
                "command",
                "args",
                "raw_request_code",
                "json_patch",
                "arbitrary_patch",
                "secret",
                "secret_value",
                "private_key",
            ]
            .contains(&field.as_str())
                || contains_forbidden_parameter(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(contains_forbidden_parameter),
        _ => false,
    }
}
