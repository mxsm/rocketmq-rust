import type { ExecutionSubmissionView, SubmitExecutionRequest } from "@/api/types";

export interface ExecutionReadOwner {
  tenantId: string;
  clusterIds: readonly string[];
  executionId?: string;
}

export class ExecutionScopeError extends Error {
  constructor() { super("Execution response does not match its authorized read"); }
}

const STATES = new Set([
  "pending", "prechecking", "intent_persisted", "applying", "unknown", "reconciling",
  "verifying", "compensating", "succeeded", "rolled_back", "escalated",
]);
const digest = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/u.test(value);
const record = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === "object" && !Array.isArray(value));
const optionalAutonomy = (value: unknown): unknown => record(value) ? value.autonomy_grant : undefined;

/** Structural/scope check only; a returned signed request is never a UI grant. */
export function executionReceiptMatches(value: unknown, owner: ExecutionReadOwner): value is ExecutionSubmissionView {
  if (!record(value) || !record(value.execution)) return false;
  const request = value.execution;
  const plan = request.plan;
  if (!record(plan) || !owner.tenantId || request.tenant_id !== owner.tenantId ||
    typeof request.cluster_id !== "string" || !owner.clusterIds.includes(request.cluster_id) ||
    typeof request.id !== "string" || !request.id || (owner.executionId && request.id !== owner.executionId) ||
    request.schema_version !== "rocketmq-sre.execution-request.v1" ||
    plan.tenant_id !== request.tenant_id || plan.cluster_id !== request.cluster_id ||
    typeof plan.id !== "string" || !plan.id || !digest(plan.plan_hash) ||
    !Array.isArray(plan.steps) || !plan.steps.length || plan.steps.length > 16 ||
    !plan.steps.every((step) => record(step) && typeof step.id === "string" && typeof step.resource === "string") ||
    new Set(plan.steps.map((step) => (step as Record<string, unknown>).id)).size !== plan.steps.length ||
    typeof request.requested_by !== "string" || !request.requested_by ||
    typeof request.idempotency_key !== "string" || !request.idempotency_key ||
    !Array.isArray(request.approvals) || !request.approvals.every(record) ||
    (request.autonomy_grant != null && !record(request.autonomy_grant)) ||
    typeof value.state !== "string" || !STATES.has(value.state) ||
    typeof value.submitted_at !== "string" || !Number.isFinite(Date.parse(value.submitted_at))) return false;
  return true;
}

/** An accepted or historical replay must still refer to the exact original intent. */
export function originalSubmissionMatches(
  value: unknown,
  owner: ExecutionReadOwner & { subject: string },
  original: SubmitExecutionRequest,
): value is ExecutionSubmissionView {
  if (!executionReceiptMatches(value, owner)) return false;
  const request = value.execution;
  if (request.plan.id !== original.plan_id || request.plan.plan_hash !== original.plan_hash ||
    request.idempotency_key !== original.idempotency_key || request.requested_by !== owner.subject) return false;
  // Inspect only the immutable binding, without rendering, validating or renewing
  // a grant. Accepted replay remains readable after its original deadline.
  const autonomy = optionalAutonomy(request);
  const bindings = [...request.approvals, ...(record(autonomy) ? [autonomy] : [])];
  return bindings.length > 0 && bindings.every((binding) =>
    binding.tenant_id === request.tenant_id && binding.cluster_id === request.cluster_id &&
    binding.plan_id === original.plan_id && binding.plan_hash === original.plan_hash &&
    binding.precondition_hash === original.precondition_hash);
}
