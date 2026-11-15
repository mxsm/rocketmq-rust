import type { ExecutionLookupView, ExecutionObservationView, PlanExecutionPage, SubmitExecutionRequest } from "@/api/types";
import type { ExecutionReadOwner } from "./executionPresentation";

const FAMILY = "rocketmq-sre.execution-observation.v1";
const states = new Set(["pending", "prechecking", "intent_persisted", "applying", "unknown", "reconciling", "verifying", "compensating", "succeeded", "rolled_back", "escalated"]);
const factStates = new Set(["not_observed", "unknown", "observed", "satisfied", "not_satisfied"]);
const sources = new Set(["accepted_request", "step_journal", "verification_journal", "reconciliation_journal", "quarantine_journal", "independent_business_observation", "not_observed"]);
const eventKinds = new Set(["accepted", "state_changed", "intent_persisted", "result_recorded", "verification_captured", "verification_completed", "manual_takeover_required", "quarantine_created", "quarantine_cleared", "other"]);
const effects = new Set(["prepared", "dispatched", "confirmed", "unknown"]);
const proofStates = new Set(["not_observed", "legacy_missing", "invalid", "unavailable", "inconclusive", "valid_historical"]);
const origins = new Set(["recorded_only", "synthetic_fixture", "production_read"]);
const object = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === "object" && !Array.isArray(value));
const list = (value: unknown, limit: number): value is unknown[] => Array.isArray(value) && value.length <= limit;
const text = (value: unknown, limit = 512): value is string => typeof value === "string" && value.length > 0 && value.length <= limit && [...value].every((char) => char.charCodeAt(0) >= 32 && char.charCodeAt(0) !== 127);
const id = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/iu.test(value);
const digest = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/u.test(value);
const date = (value: unknown): value is string => typeof value === "string" && Number.isFinite(Date.parse(value));
const code = (value: unknown): value is string => typeof value === "string" && /^[a-z][a-z0-9_]{0,127}$/u.test(value);
const optionalNumber = (value: unknown) => value == null || typeof value === "number" && Number.isFinite(value) && value >= 0;
const optionalDate = (value: unknown) => value == null || date(value);

/** Decimal identity stays a string; browser Number must never order a u64. */
export function unsignedDecimal(value: unknown): value is string {
  return typeof value === "string" && /^(0|[1-9]\d{0,19})$/u.test(value) && BigInt(value) <= 18446744073709551615n;
}

function fact(value: unknown) {
  return object(value) && typeof value.state === "string" && factStates.has(value.state) &&
    typeof value.source === "string" && sources.has(value.source) &&
    !(value.source === "not_observed" && ["observed", "satisfied", "not_satisfied"].includes(value.state)) &&
    list(value.reason_codes, 64) && value.reason_codes.every(code);
}

function snapshot(value: Record<string, unknown>) {
  return value.schema_version === FAMILY && date(value.observed_at) && date(value.valid_until) &&
    Date.parse(value.valid_until) >= Date.parse(value.observed_at);
}

type Receipt = ExecutionObservationView["binding"];
export function executionReferenceMatches(value: unknown, owner: ExecutionReadOwner): value is Receipt {
  return object(value) && value.tenant_id === owner.tenantId && typeof value.cluster_id === "string" && owner.clusterIds.includes(value.cluster_id) &&
    id(value.execution_id) && (!owner.executionId || value.execution_id === owner.executionId) &&
    id(value.plan_id) && digest(value.plan_hash) && digest(value.precondition_hash) && id(value.incident_id) &&
    id(value.diagnosis_revision_id) && id(value.correlation_id) && text(value.requested_by) && date(value.accepted_at) &&
    typeof value.state === "string" && states.has(value.state);
}

export function executionLookupMatches(value: unknown, owner: ExecutionReadOwner & { subject: string }, original: SubmitExecutionRequest): value is ExecutionLookupView {
  if (!object(value) || !snapshot(value) || value.tenant_id !== owner.tenantId || value.actor_subject !== owner.subject ||
    !object(value.request) || !Object.entries(original).every(([key, field]) => object(value.request) && value.request[key] === field)) return false;
  if (value.status === "not_observed") return value.receipt == null;
  return value.status === "accepted" && executionReferenceMatches(value.receipt, owner) &&
    value.receipt.requested_by === owner.subject && value.receipt.plan_id === original.plan_id &&
    value.receipt.plan_hash === original.plan_hash && value.receipt.precondition_hash === original.precondition_hash;
}

export function planExecutionsMatch(value: unknown, owner: ExecutionReadOwner, planId: string, planHash: string): value is PlanExecutionPage {
  return object(value) && snapshot(value) && value.tenant_id === owner.tenantId && typeof value.cluster_id === "string" &&
    owner.clusterIds.includes(value.cluster_id) && value.plan_id === planId && value.plan_hash === planHash && typeof value.partial === "boolean" &&
    list(value.items, 20) && value.items.every((item) => executionReferenceMatches(item, { ...owner, executionId: undefined }) &&
      item.cluster_id === value.cluster_id && item.plan_id === planId && item.plan_hash === planHash) &&
    new Set(value.items.map((item) => object(item) ? item.execution_id : undefined)).size === value.items.length;
}

export function executionObservationMatches(value: unknown, owner: ExecutionReadOwner): value is ExecutionObservationView {
  if (!object(value) || !snapshot(value) || !executionReferenceMatches(value.binding, owner) ||
    !fact(value.history) || !fact(value.reconciliation) || !fact(value.manual_takeover) || !fact(value.business_recovery) ||
    !list(value.steps, 16) || !list(value.timeline, 256) || typeof value.timeline_partial !== "boolean") return false;
  const business = value.business_recovery;
  if (object(business) && ["observed", "satisfied", "not_satisfied"].includes(String(business.state)) && business.source !== "independent_business_observation") return false;
  const planSteps = new Set<string>();
  const attempts = new Map<string, string>();
  const attemptKeys = new Set<string>();
  const verificationKeys = new Set<string>();
  let attemptCount = 0;
  let verificationCount = 0;
  for (const step of value.steps) {
    if (!object(step) || !id(step.plan_step_id) || planSteps.has(step.plan_step_id) ||
      !Number.isSafeInteger(step.sequence) || Number(step.sequence) < 1 || Number(step.sequence) > 65535 ||
      !text(step.action, 160) || !text(step.descriptor_version, 64) || !text(step.target) ||
      !fact(step.effect) || !fact(step.compensation) || typeof step.partial !== "boolean" ||
      !list(step.attempts, 64) || !list(step.verifications, 64) || !declaration(step.declared_verification)) return false;
    planSteps.add(step.plan_step_id);
    attemptCount += step.attempts.length;
    verificationCount += step.verifications.length;
    if (attemptCount > 64 || verificationCount > 64) return false;
    for (const attempt of step.attempts) {
      if (!object(attempt) || !id(attempt.step_id) || !u16(attempt.attempt) || typeof attempt.compensation !== "boolean" ||
        !date(attempt.intended_at) || !optionalDate(attempt.result_at) || !id(attempt.lease_id) || !unsignedDecimal(attempt.lease_epoch) ||
        !(attempt.effect_state == null || typeof attempt.effect_state === "string" && effects.has(attempt.effect_state)) || !fact(attempt.fact)) return false;
      const existing = attempts.get(attempt.step_id);
      if (existing && existing !== step.plan_step_id) return false;
      const key = `${attempt.step_id}:${attempt.attempt}:${attempt.compensation}`;
      if (attemptKeys.has(key)) return false;
      attemptKeys.add(key);
      attempts.set(attempt.step_id, step.plan_step_id);
    }
    for (const verification of step.verifications) {
      if (!verificationRecord(verification)) return false;
      const existing = attempts.get(String(verification.step_id));
      if (existing && existing !== step.plan_step_id) return false;
      const key = `${verification.step_id}:${verification.attempt}:${verification.compensation}`;
      if (verificationKeys.has(key)) return false;
      verificationKeys.add(key);
      attempts.set(String(verification.step_id), step.plan_step_id);
    }
  }
  let previous = -1n;
  for (const event of value.timeline) {
    if (!object(event) || !unsignedDecimal(event.sequence) || BigInt(event.sequence) <= previous ||
      typeof event.kind !== "string" || !eventKinds.has(event.kind) || !date(event.occurred_at) ||
      !(event.step_id == null || id(event.step_id)) || !(event.state == null || typeof event.state === "string" && states.has(event.state)) || !code(event.reason)) return false;
    previous = BigInt(event.sequence);
  }
  return true;
}

function u16(value: unknown) { return typeof value === "number" && Number.isSafeInteger(value) && value > 0 && value <= 65535; }
function conditions(value: unknown) {
  return list(value, 64) && value.every((item) => typeof item === "string" && /^(?:(resource|sli):)?[a-z][a-z0-9_]{0,127}$/u.test(item));
}
function declaration(value: unknown) {
  return value == null || object(value) && conditions(value.resource_conditions) && conditions(value.technical_slis) &&
    typeof value.stable_window_seconds === "number" && optionalNumber(value.stable_window_seconds) &&
    typeof value.max_wait_seconds === "number" && optionalNumber(value.max_wait_seconds);
}
function verificationRecord(value: unknown): value is Record<string, unknown> {
  return object(value) && id(value.step_id) && u16(value.attempt) && typeof value.compensation === "boolean" &&
    typeof value.recorded_outcome === "string" && ["succeeded", "failed", "inconclusive"].includes(value.recorded_outcome) &&
    typeof value.proof_state === "string" && proofStates.has(value.proof_state) && typeof value.provenance === "string" && origins.has(value.provenance) &&
    date(value.started_at) && date(value.completed_at) &&
    typeof value.required_stable_window_seconds === "number" &&
    [value.required_stable_window_seconds, value.observed_stable_seconds, value.max_wait_seconds, value.sample_count, value.max_source_age_seconds, value.max_sample_gap_seconds].every(optionalNumber) &&
    [value.source_first_at, value.source_last_at, value.resource_observed_at, value.technical_sli_observed_at, value.collected_at].every(optionalDate) &&
    (value.policy_digest == null || digest(value.policy_digest)) && (value.declared_target_version == null || text(value.declared_target_version, 128)) &&
    typeof value.target_version_verified === "boolean" &&
    conditions(value.satisfied_conditions) && conditions(value.failed_conditions) && conditions(value.missing_conditions) &&
    list(value.evidence_ids, 64) && value.evidence_ids.every(id) && list(value.reason_codes, 64) && value.reason_codes.every(code);
}

export function observationScopeObject(view: ExecutionObservationView) {
  const targets = [...new Set(view.steps.map((step) => step.target))];
  return { tenant_id: view.binding.tenant_id, cluster_id: view.binding.cluster_id, ...(targets.length === 1 ? { resource: targets[0] } : {}) };
}
