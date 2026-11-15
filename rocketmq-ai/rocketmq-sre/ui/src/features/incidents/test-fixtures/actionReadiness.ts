import type {
  ActionPlanView, ActionReadinessRequest, ActionReadinessView, CreatePlanResponse, DiagnosisExecutionConfirmation,
  DiagnosisRevision, ExecutionPreconditionEvidenceView, Incident,
} from "@/api/types";
import { phase1Evidence } from "@/data/phase1Demo";

/** API-shaped synthetic engineering data; no target or production qualification. */
export const READINESS_NOW = "2026-11-17T10:00:00Z";
export const READINESS_SUBJECT = "rocketmq-sre-development";
export const DIAGNOSIS_EVIDENCE_ID = "32000000-0000-4000-8000-000000000001";
export const PREPARATION_EVIDENCE_ID = "32000000-0000-4000-8000-000000000002";
export const UNCONFIRMED_REVISION_ID = "31000000-0000-4000-8000-000000000001";
export const CONFIRMED_REVISION_ID = "31000000-0000-4000-8000-000000000002";
const hash = (letter: string) => `sha256:${letter.repeat(64)}`;

export function actionReadinessFixture({ confirmed = true, prepared = false, target = "broker/broker-a" } = {}) {
  const incident: Incident = {
    id: "30000000-0000-4000-8000-000000000001",
    tenant_id: "00000000-0000-4000-8000-000000000001",
    cluster_id: "10000000-0000-4000-8000-000000000001",
    title: "Synthetic logger readiness",
    resource: "broker/broker-a", status: "monitoring", occurrence_count: 1,
    created_at: READINESS_NOW, updated_at: READINESS_NOW,
  };
  const revision: DiagnosisRevision = {
    id: confirmed ? CONFIRMED_REVISION_ID : UNCONFIRMED_REVISION_ID,
    incident_id: incident.id, revision: confirmed ? 2 : 1,
    status: confirmed ? "confirmed" : "monitoring",
    rule_result: { diagnosis_mode: "model_assisted", provider_id: "synthetic-provider" },
    hypotheses: [], evidence_ids: [DIAGNOSIS_EVIDENCE_ID],
    primary_model_invocation_id: "35000000-0000-4000-8000-000000000001",
    execution_eligible: confirmed, partial: false, created_at: READINESS_NOW,
  };
  const request: ActionReadinessRequest = {
    cluster_id: incident.cluster_id, diagnosis_revision_id: revision.id,
    action_id: "observability.logger_level_ttl.v1", descriptor_version: "1.0.0",
    resource: target,
    parameters: { component: "broker", logger: "rocketmq_broker::processor", level: "DEBUG", ttl_seconds: 60 },
    ...(prepared ? { precondition_evidence_id: PREPARATION_EVIDENCE_ID } : {}),
  };
  type Fact = ActionReadinessView["stages"]["descriptor"];
  const fact = (state: Fact["state"], source: Fact["source"], reason: Fact["reason_codes"][number]): Fact => ({
    state, source, reason_codes: [reason], evidence_ids: [], observed_at: READINESS_NOW, valid_until: null,
  });
  const view: ActionReadinessView = {
    schema_version: "rocketmq-sre.action-readiness.v1",
    observed_at: READINESS_NOW, valid_until: "2026-11-17T10:01:00Z",
    binding: {
      tenant_id: incident.tenant_id, cluster_id: incident.cluster_id, incident_id: incident.id,
      diagnosis_revision_id: revision.id, latest_diagnosis_revision_id: revision.id,
      action_id: request.action_id, descriptor_version: request.descriptor_version,
      resource: request.resource, parameters: request.parameters, parameters_digest: hash("b"),
      target_scope_digest: hash("a"),
      diagnostic_evidence: [{ evidence_id: DIAGNOSIS_EVIDENCE_ID, content_hash: hash("a") }],
      precondition_evidence: prepared ? { evidence_id: PREPARATION_EVIDENCE_ID, content_hash: hash("c") } : null,
      precondition_hash: prepared ? hash("d") : null,
      policy_version: "synthetic-readiness-policy.v1", actor_subject: READINESS_SUBJECT,
      plan_id: null, plan_step_id: null, plan_hash: null, binding_digest: hash("e"),
    },
    diagnosis: {
      is_current: true, is_confirmed: confirmed, is_complete: true,
      primary_model_lineage_valid: true,
      reason_codes: confirmed ? [] : ["diagnosis_not_confirmed"],
    },
    stages: {
      descriptor: fact("satisfied", "descriptor_catalog", "observation_available"),
      handler: prepared ? fact("satisfied", "stored_preparation", "handler_read_observed") : fact("unknown", "not_observed", "handler_not_observed"),
      enabled: fact("satisfied", "executor_configuration", "observation_available"),
      target: fact("unknown", prepared ? "stored_preparation" : "not_observed", "target_support_not_observed"),
      evidence: prepared ? fact("satisfied", "stored_preparation", "observation_available") : fact("blocked", "current_diagnosis", "preparation_binding_missing"),
      policy: prepared ? fact("satisfied", "policy_evaluation", "observation_available") : fact("blocked", "policy_evaluation", "evidence_missing"),
      approval: fact("not_applicable", "not_observed", "plan_not_created"),
      qualification: fact("unknown", "qualification_unavailable", "action_target_qualification_unavailable"),
      compensation: fact("unknown", "not_observed", "compensation_not_observed"),
    },
    execution_readiness: "unknown",
    operations: {
      confirm_diagnosis: { allowed: !confirmed, reason_codes: confirmed ? ["diagnosis_already_confirmed"] : [] },
      prepare_precondition: { allowed: confirmed, reason_codes: confirmed ? [] : ["diagnosis_not_confirmed"] },
      create_plan: { allowed: confirmed && prepared, reason_codes: !confirmed ? ["diagnosis_not_confirmed"] : prepared ? [] : ["preparation_binding_missing"] },
    },
  };
  const owner = {
    tenantId: incident.tenant_id, subject: READINESS_SUBJECT,
    incidentId: incident.id, evidenceIds: revision.evidence_ids,
  };
  const preparation: ExecutionPreconditionEvidenceView = {
    schema_version: "rocketmq-sre.execution-precondition-evidence.v1",
    incident_id: incident.id, diagnosis_revision_id: revision.id,
    precondition_hash: hash("d"),
    evidence: {
      ...phase1Evidence.items[0], evidence_id: PREPARATION_EVIDENCE_ID,
      tenant_id: incident.tenant_id, cluster_id: incident.cluster_id,
      source: "execution-agent", exposure: "synthetic", resource: request.resource,
      observed_at: READINESS_NOW, time_range: { start: READINESS_NOW, end: READINESS_NOW },
      content_hash: hash("c"), freshness_seconds: 0, partial: false,
      content: { storage: "inline", value: { classification: "synthetic_fixture_no_origin_attestation" } },
    },
  };
  const confirmation: DiagnosisExecutionConfirmation = {
    schema_version: "rocketmq-sre.diagnosis-execution-confirmation.v1",
    incident_id: incident.id, source_revision_id: UNCONFIRMED_REVISION_ID,
    confirmed_revision_id: CONFIRMED_REVISION_ID, revision: 2, cluster_id: incident.cluster_id,
    primary_model_invocation_id: revision.primary_model_invocation_id!, evidence_ids: revision.evidence_ids,
    execution_eligible: true, confirmed_by: READINESS_SUBJECT, reason: "synthetic operator review",
    correlation_id: "33000000-0000-4000-8000-000000000001", confirmed_at: READINESS_NOW,
  };
  const plan: CreatePlanResponse = {
    kind: "action_plan", risk: "r1", precondition_hash: hash("d"),
    plan: {
      schema_version: "rocketmq-sre.action-plan.v1", id: "34000000-0000-4000-8000-000000000001",
      tenant_id: incident.tenant_id, cluster_id: incident.cluster_id, incident_id: incident.id,
      diagnosis_revision: revision.id, primary_model_invocation_id: revision.primary_model_invocation_id!,
      diagnosis_execution_eligible: true, version: 1, created_by: READINESS_SUBJECT,
      created_at: READINESS_NOW, expires_at: "2026-11-17T10:15:00Z", evidence_hash: hash("e"),
      status: "ready_for_approval", submitted_at: null, plan_hash: hash("f"),
      steps: [{
        id: "36000000-0000-4000-8000-000000000001", sequence: 1,
        action: "observability.logger_level_ttl.v1", descriptor_version: "1.0.0",
        resource: request.resource, parameters: request.parameters,
        evidence_ids: [DIAGNOSIS_EVIDENCE_ID, PREPARATION_EVIDENCE_ID], precondition_hash: hash("d"),
        max_impact: "single_resource",
        verification: { resource_conditions: ["logger_level_applied"], technical_slis: [], stable_window_seconds: 30, max_wait_seconds: 120 },
        compensation: { mode: "automatic", required_before_fields: ["previous_level"], timeout_seconds: 60 },
      }],
    },
    policy_decision: {
      id: "37000000-0000-4000-8000-000000000001", tenant_id: incident.tenant_id,
      cluster_id: incident.cluster_id, plan_id: "34000000-0000-4000-8000-000000000001",
      plan_hash: hash("f"), policy_version: "synthetic-readiness-policy.v1", input_hash: hash("e"),
      effect: "require_approval", reason_codes: ["HumanApprovalRequired"],
      evaluated_at: READINESS_NOW, evaluated_by: "synthetic-policy",
    },
  };
  return { incident, revision, request, owner, view, preparation, confirmation, plan };
}

export function actionPlanFixture(target = "broker/broker-a") {
  const fixture = actionReadinessFixture({ prepared: true, target });
  if (fixture.plan.kind !== "action_plan") throw new Error("expected action plan fixture");
  const planView: ActionPlanView = {
    plan: fixture.plan.plan, risk: "r1", precondition_hash: fixture.plan.precondition_hash,
    critic_state: "unreviewed_not_required", latest_critic_review: null,
    latest_policy_decision: fixture.plan.policy_decision, latest_approval: null,
  };
  return { ...fixture, planView };
}

export function planReadinessFixture(view: ActionPlanView, index = 0, subject = READINESS_SUBJECT) {
  const fixture = actionReadinessFixture({ prepared: true });
  const step = view.plan.steps[index];
  const result = fixture.view;
  result.binding = {
    ...result.binding, tenant_id: view.plan.tenant_id, cluster_id: view.plan.cluster_id,
    incident_id: view.plan.incident_id, diagnosis_revision_id: view.plan.diagnosis_revision,
    latest_diagnosis_revision_id: view.plan.diagnosis_revision,
    action_id: step.action, descriptor_version: step.descriptor_version,
    resource: step.resource, parameters: step.parameters, actor_subject: subject,
    plan_id: view.plan.id, plan_step_id: step.id, plan_hash: view.plan.plan_hash,
  };
  result.operations = {
    confirm_diagnosis: { allowed: false, reason_codes: [] },
    prepare_precondition: { allowed: false, reason_codes: ["plan_immutable"] },
    create_plan: { allowed: false, reason_codes: ["plan_immutable"] },
  };
  result.stages.approval = {
    state: "blocked", source: "stored_approval",
    reason_codes: [subject === view.plan.created_by ? "self_approval_forbidden" : "approval_required"],
    evidence_ids: [], observed_at: null, valid_until: null,
  };
  return result;
}
