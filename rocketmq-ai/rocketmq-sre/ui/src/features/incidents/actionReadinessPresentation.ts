import type {
  ActionReadinessRequest, ActionReadinessView, CreatePlanResponse,
  ExecutionPreconditionEvidenceView,
} from "@/api/types";
import { isReadinessReason, isReadinessSource } from "@/i18n/actionReadinessReasons";

export const readinessStages = [
  "descriptor", "handler", "enabled", "target", "evidence",
  "policy", "approval", "qualification", "compensation",
] as const satisfies ReadonlyArray<keyof ActionReadinessView["stages"]>;

const operations = ["confirm_diagnosis", "prepare_precondition", "create_plan"] as const;
const states = new Set(["satisfied", "blocked", "unknown", "not_applicable"]);
const summaries = new Set(["satisfied", "blocked", "unknown", "awaiting_approval"]);
const digest = /^sha256:[0-9a-f]{64}$/u;

export interface ReadinessOwner {
  tenantId: string;
  subject: string;
  incidentId: string;
  evidenceIds: readonly string[];
  planHash?: string;
}

export class ActionReadinessScopeError extends Error {}

/** Validate the whole projection before exposing even its source/time metadata. */
export function readinessMatches(
  value: unknown,
  request: ActionReadinessRequest,
  owner: ReadinessOwner,
): value is ActionReadinessView {
  const view = record(value);
  const binding = record(view?.binding);
  const diagnosis = record(view?.diagnosis);
  const stages = record(view?.stages);
  const available = record(view?.operations);
  if (
    view?.schema_version !== "rocketmq-sre.action-readiness.v1" ||
    !binding || !diagnosis || !stages || !available ||
    binding.tenant_id !== owner.tenantId ||
    binding.actor_subject !== owner.subject ||
    binding.incident_id !== owner.incidentId ||
    binding.cluster_id !== request.cluster_id ||
    binding.diagnosis_revision_id !== request.diagnosis_revision_id ||
    binding.action_id !== request.action_id ||
    binding.descriptor_version !== request.descriptor_version ||
    binding.resource !== request.resource ||
    !sameOptional(binding.plan_id, request.plan_id) ||
    !sameOptional(binding.plan_step_id, request.plan_step_id) ||
    (owner.planHash ? binding.plan_hash !== owner.planHash : binding.plan_hash != null) ||
    !isDigest(binding.binding_digest) ||
    typeof binding.policy_version !== "string" ||
    binding.policy_version.length > 128 ||
    !validDate(view.observed_at) ||
    !optionalDate(view.valid_until) ||
    !summaries.has(String(view.execution_readiness))
  ) return false;

  for (const key of ["is_current", "is_confirmed", "is_complete", "primary_model_lineage_valid"]) {
    if (typeof diagnosis[key] !== "boolean") return false;
  }
  if (
    !reasons(diagnosis.reason_codes) ||
    (diagnosis.is_current === true && binding.latest_diagnosis_revision_id !== request.diagnosis_revision_id)
  ) return false;

  const diagnostic = evidenceBindings(binding.diagnostic_evidence);
  const preparation = binding.precondition_evidence == null
    ? undefined
    : evidenceBinding(binding.precondition_evidence);
  if (
    !diagnostic ||
    diagnostic.some((item) => !owner.evidenceIds.includes(item.evidence_id)) ||
    (binding.precondition_evidence != null && !preparation) ||
    (preparation && request.precondition_evidence_id && preparation.evidence_id !== request.precondition_evidence_id) ||
    (preparation && !request.precondition_evidence_id && !request.plan_id) ||
    (binding.precondition_hash != null && !isDigest(binding.precondition_hash)) ||
    (binding.parameters_digest != null && !isDigest(binding.parameters_digest))
    || (binding.target_scope_digest != null && !isDigest(binding.target_scope_digest))
  ) return false;
  const evidenceIds = new Set([
    ...diagnostic.map((item) => item.evidence_id),
    ...(preparation ? [preparation.evidence_id] : []),
    ...(request.precondition_evidence_id ? [request.precondition_evidence_id] : []),
  ]);
  if (request.plan_id && preparation && !owner.evidenceIds.includes(preparation.evidence_id)) return false;

  if (
    Object.keys(stages).length !== readinessStages.length ||
    !readinessStages.every((key) => fact(stages[key], evidenceIds)) ||
    Object.keys(available).length !== operations.length
  ) return false;
  for (const key of operations) {
    const operation = record(available[key]);
    if (!operation || typeof operation.allowed !== "boolean" || !reasons(operation.reason_codes)) return false;
    // An allowed affordance cannot simultaneously carry a blocking explanation.
    if (operation.allowed && (operation.reason_codes as unknown[]).length) return false;
  }
  const candidateAllowed = operations.slice(1).some((key) => record(available[key])?.allowed === true);
  const confirmationAllowed = record(available.confirm_diagnosis)?.allowed === true;
  const parametersMatch = binding.parameters != null && sameJson(binding.parameters, request.parameters);
  if (
    (binding.parameters != null && !parametersMatch) ||
    (candidateAllowed && (!parametersMatch || !isDigest(binding.parameters_digest) || !isDigest(binding.target_scope_digest))) ||
    ((candidateAllowed || confirmationAllowed) && view.valid_until == null) ||
    (candidateAllowed && (!diagnosis.is_current || !diagnosis.is_confirmed || !diagnosis.is_complete || !diagnosis.primary_model_lineage_valid)) ||
    (confirmationAllowed && (!diagnosis.is_current || diagnosis.is_confirmed || !diagnosis.is_complete || !diagnosis.primary_model_lineage_valid))
  ) return false;
  if (view.execution_readiness === "satisfied" && readinessStages.some((key) => {
    const state = record(stages[key])?.state;
    return state === "blocked" || state === "unknown";
  })) return false;
  return true;
}

export function readinessOperation(
  view: ActionReadinessView | undefined,
  operation: keyof ActionReadinessView["operations"],
  now = Date.now(),
) {
  return Boolean(
    view?.valid_until &&
    Date.parse(view.valid_until) > now &&
    view.operations[operation].allowed === true &&
    view.operations[operation].reason_codes.length === 0,
  );
}

export function readinessExpired(view: ActionReadinessView | undefined, now = Date.now()) {
  return Boolean(view?.valid_until && Date.parse(view.valid_until) <= now);
}

export function preparedEvidenceMatches(
  value: unknown,
  request: ActionReadinessRequest,
  owner: ReadinessOwner,
): value is ExecutionPreconditionEvidenceView {
  const view = record(value);
  const evidence = record(view?.evidence);
  return Boolean(
    view?.schema_version === "rocketmq-sre.execution-precondition-evidence.v1" &&
    view.incident_id === owner.incidentId &&
    view.diagnosis_revision_id === request.diagnosis_revision_id &&
    isDigest(view.precondition_hash) && evidence &&
    typeof evidence.evidence_id === "string" &&
    evidence.tenant_id === owner.tenantId && evidence.cluster_id === request.cluster_id &&
    evidence.resource === request.resource && evidence.source === "execution-agent" &&
    isDigest(evidence.content_hash) && validDate(evidence.observed_at),
  );
}

export function createdPlanMatches(
  value: unknown,
  request: ActionReadinessRequest,
  owner: ReadinessOwner,
  evidenceIds: readonly string[],
): value is CreatePlanResponse {
  const response = record(value);
  if (response?.kind === "manual_runbook") {
    const runbook = record(response.runbook);
    return Boolean(runbook && runbook.tenant_id === owner.tenantId &&
      runbook.cluster_id === request.cluster_id && runbook.incident_id === owner.incidentId &&
      runbook.diagnosis_revision === request.diagnosis_revision_id &&
      runbook.action_id === request.action_id && runbook.execution_supported === false);
  }
  const plan = record(response?.plan);
  if (response?.kind !== "action_plan" || !plan ||
      plan.tenant_id !== owner.tenantId || plan.cluster_id !== request.cluster_id ||
      plan.incident_id !== owner.incidentId || plan.diagnosis_revision !== request.diagnosis_revision_id ||
      plan.created_by !== owner.subject || typeof plan.id !== "string" || !isDigest(plan.plan_hash) ||
      !Array.isArray(plan.steps) || plan.steps.length !== 1) return false;
  const step = record(plan.steps[0]);
  return Boolean(step && step.action === request.action_id &&
    step.descriptor_version === request.descriptor_version && step.resource === request.resource &&
    sameJson(step.parameters, request.parameters) && Array.isArray(step.evidence_ids) &&
    step.evidence_ids.length === evidenceIds.length &&
    new Set(step.evidence_ids).size === evidenceIds.length &&
    step.evidence_ids.every((id) => typeof id === "string" && evidenceIds.includes(id)));
}

export function sameJson(left: unknown, right: unknown, depth = 0): boolean {
  if (depth > 12) return false;
  if (left === right) return typeof left !== "number" || Number.isFinite(left);
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left) && Array.isArray(right) && left.length <= 128 &&
      left.length === right.length && left.every((item, index) => sameJson(item, right[index], depth + 1));
  }
  const a = record(left);
  const b = record(right);
  if (!a || !b) return false;
  const keys = Object.keys(a).sort();
  const otherKeys = Object.keys(b).sort();
  return keys.length <= 64 && keys.length === otherKeys.length && keys.every(
    (key, index) => key === otherKeys[index] && sameJson(a[key], b[key], depth + 1),
  );
}

function fact(value: unknown, evidenceIds: Set<string>) {
  const item = record(value);
  return Boolean(
    item && states.has(String(item.state)) && reasons(item.reason_codes) &&
    isReadinessSource(item.source) && optionalDate(item.observed_at) && optionalDate(item.valid_until) &&
    Array.isArray(item.evidence_ids) && item.evidence_ids.length <= 128 &&
    item.evidence_ids.every((id) => typeof id === "string" && evidenceIds.has(id)),
  );
}

function evidenceBindings(value: unknown) {
  if (!Array.isArray(value) || value.length > 128) return undefined;
  const items = value.map(evidenceBinding);
  if (items.some((item) => !item)) return undefined;
  const valid = items.filter((item) => item !== undefined);
  return new Set(valid.map((item) => item.evidence_id)).size === valid.length ? valid : undefined;
}

function evidenceBinding(value: unknown) {
  const item = record(value);
  return item && typeof item.evidence_id === "string" && isDigest(item.content_hash)
    ? { evidence_id: item.evidence_id, content_hash: item.content_hash as string }
    : undefined;
}

function reasons(value: unknown) {
  return Array.isArray(value) && value.length <= 32 && value.every(isReadinessReason);
}

function isDigest(value: unknown) {
  return typeof value === "string" && digest.test(value);
}

function validDate(value: unknown) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function optionalDate(value: unknown) {
  return value == null || validDate(value);
}

function sameOptional(left: unknown, right: unknown) {
  return left == null && right == null || left === right;
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}
