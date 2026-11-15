import type { ExecutionLookupView, ExecutionObservationView, PlanExecutionPage } from "@/api/types";
import { READINESS_NOW } from "@/features/incidents/test-fixtures/actionReadiness";
import { executionFixture } from "./execution";

/** Synthetic recorded history; no physical target effect or qualification. */
export function observationFixture() {
  const { receipt, selector, owner } = executionFixture();
  const { execution } = receipt;
  const step = execution.plan.steps[0];
  const binding: ExecutionObservationView["binding"] = {
    execution_id: execution.id, tenant_id: execution.tenant_id, cluster_id: execution.cluster_id,
    plan_id: execution.plan.id, plan_hash: execution.plan.plan_hash, precondition_hash: selector.precondition_hash,
    incident_id: execution.plan.incident_id, diagnosis_revision_id: execution.plan.diagnosis_revision,
    correlation_id: execution.correlation_id, requested_by: execution.requested_by,
    accepted_at: receipt.submitted_at, state: receipt.state,
  };
  const times = { observed_at: READINESS_NOW, valid_until: "2026-11-17T10:00:05Z" };
  const schema_version = "rocketmq-sre.execution-observation.v1";
  const view: ExecutionObservationView = {
    schema_version, binding, ...times,
    timeline_partial: false,
    history: { state: "observed", source: "step_journal", reason_codes: ["intent_recorded"] },
    reconciliation: { state: "unknown", source: "reconciliation_journal", reason_codes: ["reconciliation_required"] },
    manual_takeover: { state: "not_observed", source: "not_observed", reason_codes: ["manual_takeover_not_recorded"] },
    business_recovery: { state: "unknown", source: "not_observed", reason_codes: ["independent_business_observation_missing"] },
    steps: [{
      plan_step_id: step.id, sequence: step.sequence, action: step.action, descriptor_version: step.descriptor_version,
      target: step.resource, declared_verification: step.verification, partial: false,
      effect: { state: "unknown", source: "step_journal", reason_codes: ["effect_unknown"] },
      compensation: { state: "not_observed", source: "not_observed", reason_codes: ["compensation_not_observed"] },
      attempts: [{
        step_id: "39000000-0000-4000-8000-000000000001", attempt: 1, compensation: false,
        intended_at: READINESS_NOW, result_at: null, effect_state: "unknown",
        lease_id: "3a000000-0000-4000-8000-000000000001", lease_epoch: "9007199254740993",
        fact: { state: "unknown", source: "step_journal", reason_codes: ["effect_unknown"] },
      }],
      verifications: [],
    }],
    timeline: [{ sequence: "9007199254740993", kind: "intent_persisted", occurred_at: READINESS_NOW,
      step_id: "39000000-0000-4000-8000-000000000001", state: "intent_persisted", reason: "intent_recorded" }],
  };
  const page: PlanExecutionPage = {
    schema_version, tenant_id: binding.tenant_id, cluster_id: binding.cluster_id,
    plan_id: binding.plan_id, plan_hash: binding.plan_hash, items: [], partial: false, ...times,
  };
  const lookup: ExecutionLookupView = {
    schema_version, tenant_id: binding.tenant_id, actor_subject: binding.requested_by,
    request: selector, status: "accepted", receipt: binding, ...times,
  };
  const verification: ExecutionObservationView["steps"][number]["verifications"][number] = {
    step_id: view.steps[0].attempts[0].step_id, attempt: 1, compensation: false,
    recorded_outcome: "succeeded", proof_state: "valid_historical", provenance: "synthetic_fixture",
    started_at: "2026-11-17T09:59:00Z", completed_at: "2026-11-17T09:59:30Z",
    required_stable_window_seconds: 30, observed_stable_seconds: 30, max_wait_seconds: 120,
    sample_count: 4, max_source_age_seconds: 10, max_sample_gap_seconds: 15,
    source_first_at: "2026-11-17T09:59:00Z", source_last_at: "2026-11-17T09:59:30Z",
    resource_observed_at: "2026-11-17T09:59:30Z", technical_sli_observed_at: null,
    collected_at: "2026-11-17T09:59:31Z", policy_digest: `sha256:${"a".repeat(64)}`,
    satisfied_conditions: ["resource:logger_level_applied"], failed_conditions: [], missing_conditions: [],
    declared_target_version: "synthetic-target-version", target_version_verified: false,
    evidence_ids: ["32000000-0000-4000-8000-000000000001"], reason_codes: ["policy_window_satisfied", "target_version_not_observed"],
  };
  return { view, page, lookup, receipt, selector, owner, verification };
}
