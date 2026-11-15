import type { ActionPlanView } from "@/api/types";
import { actionDefinition, type ActionField } from "@/features/actions/actionDefinitions";
import { reviewedActionParameters } from "@/features/actions/actionParameters";

type Step = ActionPlanView["plan"]["steps"][number];
export function reviewPlanStep(step: Step) {
  const definition = actionDefinition(step.action, step.descriptor_version);
  const parameters = definition && reviewedActionParameters(definition, step.parameters);
  if (!definition || !parameters) return undefined;
  return {
    definition,
    rows: definition.fields.flatMap((field): { field: ActionField; value: string | number | boolean }[] => {
      const value = field.patch ? (parameters.patch as Record<string, string | number | boolean>)[field.key] : parameters[field.key];
      return typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? [{ field, value }] : [];
    }),
  };
}

/** Display compatibility is conservative; it supplies no server permission. */
export function supportedPlanReview(plan: ActionPlanView["plan"]) {
  return plan.schema_version === "rocketmq-sre.action-plan.v1" && plan.steps.length > 0 && plan.steps.length <= 16 &&
    new Set(plan.steps.map((step) => step.id)).size === plan.steps.length && plan.steps.every((step) => Boolean(reviewPlanStep(step)) &&
      [step.verification.stable_window_seconds, step.verification.max_wait_seconds, step.compensation.timeout_seconds]
        .every((value) => Number.isSafeInteger(value) && value >= 0));
}
