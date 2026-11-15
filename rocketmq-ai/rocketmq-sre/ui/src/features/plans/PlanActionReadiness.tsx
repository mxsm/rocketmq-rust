import { useCallback, useEffect, useState } from "react";

import { ApiError } from "@/api/client";
import type { SupervisedSreApi } from "@/api/supervisedClient";
import type { ActionPlanView, ActionReadinessRequest, ActionReadinessView } from "@/api/types";
import { ActionReadinessPanel } from "@/features/incidents/ActionReadinessPanel";
import { ActionReadinessScopeError, readinessMatches } from "@/features/incidents/actionReadinessPresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useActionReadinessMessages } from "@/i18n/actionReadinessMessages";

type Step = ActionPlanView["plan"]["steps"][number];

export function PlanActionReadiness({
  view, api, subject, disabled = false, onReadiness,
}: {
  view: ActionPlanView;
  api: SupervisedSreApi;
  subject: string;
  disabled?: boolean;
  onReadiness: (source: ActionPlanView, snapshots?: readonly ActionReadinessView[]) => void;
}) {
  const m = useActionReadinessMessages();
  const [selectedId, setSelectedId] = useState(view.plan.steps[0]?.id ?? "");
  const selected = view.plan.steps.find((step) => step.id === selectedId) ?? view.plan.steps[0];
  const load = useCallback(async (signal: AbortSignal) => {
    if (disabled) return undefined;
    if (!view.plan.steps.length || view.plan.steps.length > 16 ||
        new Set(view.plan.steps.map((step) => step.id)).size !== view.plan.steps.length) {
      throw new ActionReadinessScopeError();
    }
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) controller.abort();
    const results: ActionReadinessView[] = [];
    let next = 0;
    let failed = false;
    let failure: unknown;
    const worker = async () => {
      while (!controller.signal.aborted && next < view.plan.steps.length) {
        const index = next++;
        const step = view.plan.steps[index];
        try {
          const request = requestFor(view, step);
          const value = await api.readActionReadiness(view.plan.incident_id, request, controller.signal);
          if (controller.signal.aborted) return;
          if (!readinessMatches(value, request, ownerFor(view, step, subject))) throw new ActionReadinessScopeError();
          results[index] = value;
        } catch (error) {
          if (!failed && !signal.aborted) {
            failed = true;
            failure = error;
          }
          controller.abort();
        }
      }
    };
    try {
      await Promise.all(Array.from({ length: Math.min(4, view.plan.steps.length) }, worker));
      if (signal.aborted) throw new DOMException("Cancelled", "AbortError");
      if (failed) throw failure instanceof Error ? failure : new Error("Readiness unavailable");
      if (results.length !== view.plan.steps.length) throw new ActionReadinessScopeError();
      return results;
    } finally {
      signal.removeEventListener("abort", abort);
    }
  }, [api, disabled, subject, view]);
  const resource = useAsyncResource(load);
  const snapshots = resource.error || resource.loading ? undefined : resource.data;
  useEffect(() => onReadiness(view, snapshots), [onReadiness, snapshots, view]);
  const current = snapshots?.find((item) => item.binding.plan_step_id === selected?.id);
  const error = resource.error instanceof ActionReadinessScopeError ? "scope" :
    resource.error instanceof ApiError && [401, 403, 404].includes(resource.error.status) ? "forbidden" :
      resource.error instanceof ApiError && resource.error.status === 409 ? "conflict" :
        resource.error ? "read" : undefined;
  if (!selected) return <p role="alert">{m.scopeMismatch}</p>;

  return (
    <section className="data-surface plan-action-readiness">
      <header className="surface-heading">
        <div><h2>{m.planReadiness}</h2><p>{m.historicalPlan}</p></div>
      </header>
      {view.plan.steps.length > 1 && (
        <label className="form-field plan-readiness-selection">
          <span>{m.planStep}</span>
          <select value={selected.id} onChange={(event) => setSelectedId(event.target.value)}>
            {view.plan.steps.map((step, index) => (
              <option key={step.id} value={step.id}>{index + 1}. {step.action} · {step.resource}</option>
            ))}
          </select>
        </label>
      )}
      <ActionReadinessPanel
        view={current} request={requestFor(view, selected)} owner={ownerFor(view, selected, subject)}
        loading={resource.loading || disabled} disabled={disabled} error={error}
        onRefresh={() => {
          onReadiness(view, undefined);
          void resource.reload();
        }}
      />
    </section>
  );
}

function requestFor(view: ActionPlanView, step: Step): ActionReadinessRequest {
  return {
    cluster_id: view.plan.cluster_id, diagnosis_revision_id: view.plan.diagnosis_revision,
    action_id: step.action, descriptor_version: step.descriptor_version,
    resource: step.resource, parameters: step.parameters,
    plan_id: view.plan.id, plan_step_id: step.id,
  };
}

function ownerFor(view: ActionPlanView, step: Step, subject: string) {
  return {
    tenantId: view.plan.tenant_id, subject, incidentId: view.plan.incident_id,
    evidenceIds: step.evidence_ids, planHash: view.plan.plan_hash,
  };
}
