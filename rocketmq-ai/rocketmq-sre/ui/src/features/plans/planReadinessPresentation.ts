import type { ActionPlanView, ActionReadinessView } from "@/api/types";
import { parseReadOnlyUrlContext, projectWorkspaceSearch, type AbsoluteWindow } from "@/context/workspaceScope";

/** A narrow workspace may show a Plan only when all steps share its exact target. */
export function planScopeObject(plan: ActionPlanView["plan"]) {
  const resources = [...new Set(plan.steps.map((step) => step.resource))];
  return {
    tenant_id: plan.tenant_id,
    cluster_id: plan.cluster_id,
    ...(resources.length === 1 ? { resource: resources[0] } : {}),
  };
}

/** Explicitly opening an authorized Plan selects its saved target, not the Incident alias. */
export function planNavigationSearch(plan: ActionPlanView["plan"], window?: AbsoluteWindow) {
  const params = new URLSearchParams({ cluster_id: plan.cluster_id });
  const resource = planScopeObject(plan).resource;
  const separator = resource?.indexOf("/") ?? -1;
  if (resource && separator > 0) {
    params.set("resource_kind", resource.slice(0, separator));
    params.set("resource_key", resource.slice(separator + 1));
  }
  const parsed = parseReadOnlyUrlContext(params.toString(), [plan.cluster_id]);
  const scope = {
    clusterId: plan.cluster_id,
    ...(parsed.status === "valid" ? parsed.context : {}),
  };
  if (window) return projectWorkspaceSearch("", { ...scope, window });
  if (parsed.status !== "valid") {
    params.delete("resource_kind");
    params.delete("resource_key");
  }
  return `?${params.toString()}`;
}

export function planReadinessSatisfied(
  plan: ActionPlanView["plan"],
  snapshots: readonly ActionReadinessView[] | undefined,
  now = Date.now(),
) {
  return Boolean(snapshots && snapshots.length === plan.steps.length && plan.steps.length > 0 &&
    new Set(snapshots.map((item) => item.binding.plan_step_id)).size === plan.steps.length &&
    plan.steps.every((step) => snapshots.some((item) =>
      item.binding.plan_id === plan.id && item.binding.plan_hash === plan.plan_hash &&
      item.binding.plan_step_id === step.id && item.execution_readiness === "satisfied" &&
      Object.values(item.stages).every((fact) => fact.state === "satisfied" || fact.state === "not_applicable") &&
      item.valid_until && Date.parse(item.valid_until) > now,
    )));
}

export function firstReadinessExpiry(snapshots: readonly ActionReadinessView[] | undefined) {
  return snapshots?.filter((item) => item.valid_until).reduce<ActionReadinessView | undefined>(
    (earliest, item) => !earliest || Date.parse(item.valid_until!) < Date.parse(earliest.valid_until!) ? item : earliest,
    undefined,
  );
}

/** Conservative UI blocking only; the approval endpoint makes the actual decision. */
export function planApprovalChecksPass(
  plan: ActionPlanView["plan"],
  snapshots: readonly ActionReadinessView[] | undefined,
  subject: string,
  now = Date.now(),
) {
  if (subject === plan.created_by || plan.status !== "ready_for_approval" ||
      !snapshots || snapshots.length !== plan.steps.length || !plan.steps.length) return false;
  return plan.steps.every((step) => snapshots.some((item) =>
    item.binding.plan_id === plan.id && item.binding.plan_hash === plan.plan_hash &&
    item.binding.plan_step_id === step.id && item.binding.actor_subject === subject &&
    item.valid_until && Date.parse(item.valid_until) > now &&
    item.diagnosis.is_current && item.diagnosis.is_confirmed && item.diagnosis.is_complete &&
    item.diagnosis.primary_model_lineage_valid && item.stages.evidence.state === "satisfied" &&
    !item.stages.approval.reason_codes.some((reason) => [
      "self_approval_forbidden", "approval_binding_mismatch", "plan_binding_mismatch", "plan_expired", "plan_completed",
    ].includes(reason)) &&
    !item.stages.policy.reason_codes.some((reason) => [
      "resource_quarantined", "resource_busy", "maintenance_window_closed",
    ].includes(reason)),
  ));
}
