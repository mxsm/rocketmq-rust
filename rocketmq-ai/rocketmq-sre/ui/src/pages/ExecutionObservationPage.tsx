import { useCallback, useEffect } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { ApiError } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { projectWorkspaceSearch } from "@/context/workspaceScope";
import { useOptionalWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { ExecutionTimeline } from "@/features/executions/ExecutionTimeline";
import { ExecutionObservationPanel, ExecutionFact } from "@/features/executions/ExecutionObservationPanel";
import { ExecutionScopeError } from "@/features/executions/executionPresentation";
import { executionObservationMatches, observationScopeObject } from "@/features/executions/executionObservationPresentation";
import { ObjectScopeBoundary } from "@/features/scope/ObjectScopeBoundary";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useDeadlineClock } from "@/hooks/useDeadlineClock";
import { useSerialResourceRefresh } from "@/hooks/useSerialResourceRefresh";
import { useSupervisedApi } from "@/hooks/useSupervisedApi";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";
import { useExecutionObservationMessages } from "@/i18n/executionObservationMessages";

export function ExecutionPage() {
  const { executionId = "" } = useParams();
  const context = useSupervisedApi();
  const scope = useOptionalWorkspaceScope();
  const location = useLocation();
  return <ExecutionSession key={`${context.identityKey}:${scope?.scopeKey ?? location.search}:${executionId}`} executionId={executionId} {...context} />;
}

function ExecutionSession({ executionId, auth, api }: ReturnType<typeof useSupervisedApi> & { executionId: string }) {
  const navigate = useNavigate();
  const scope = useOptionalWorkspaceScope();
  const { m } = useExecutionObservationMessages();
  const tenantId = auth.session?.tenantId ?? "";
  const claims = JSON.stringify([...(auth.session?.clusterIds ?? [])].sort());
  const load = useCallback(async (signal: AbortSignal) => {
    if (!api || !executionId || !tenantId) throw new ExecutionScopeError();
    const value = await api.getExecutionObservation(executionId, signal);
    if (!executionObservationMatches(value, { tenantId, clusterIds: JSON.parse(claims) as string[], executionId })) throw new ExecutionScopeError();
    return value;
  }, [api, claims, executionId, tenantId]);
  const resource = useAsyncResource(load);
  const denied = resource.error instanceof ExecutionScopeError || resource.error instanceof ApiError && [401, 403, 404].includes(resource.error.status);
  const view = denied ? undefined : resource.data;
  const refresh = useSerialResourceRefresh(resource.reload);
  const hasView = Boolean(view);
  // Only this route owns the periodic read. Manual and stream invalidations use
  // the same single-flight slot, with at most one queued read and no new write.
  useEffect(() => {
    if (!hasView) return;
    const timer = window.setInterval(() => { void refresh(); }, 5000);
    return () => window.clearInterval(timer);
  }, [hasView, refresh]);
  const progress = useWorkflowProgress(view?.binding.cluster_id ?? "", undefined, { aggregateId: executionId });
  const event = progress.events[0];
  const eventKey = event && event.payload?.provisional !== true
    ? event.event_id ?? `${event.aggregate_id}:${event.event_type}:${event.occurred_at}:${event.correlation_id}` : undefined;
  useEffect(() => { if (eventKey) void refresh(); }, [eventKey, refresh]);
  const now = useDeadlineClock([view?.valid_until]);
  const current = Boolean(view && !resource.error && !resource.loading && Date.parse(view.valid_until) > now);
  const error = resource.error instanceof ExecutionScopeError ? m.mismatch :
    resource.error instanceof ApiError && [401, 403, 404].includes(resource.error.status) ? m.forbidden : resource.error ? m.readError : undefined;
  if (!view) return <div className="page">
    <PageHeader eyebrow="EXECUTION / OBSERVATION" title={m.title} description={m.introduction} />
    {error ? <p role="alert" className="inline-alert warning">{error}</p> : <p role="status">{m.loading}</p>}
    <Button disabled={resource.loading} variant="outline" onClick={() => void refresh()}>{m.refresh}</Button>
  </div>;
  const clusterSearch = scope
    ? projectWorkspaceSearch("", { clusterId: view.binding.cluster_id, window: scope.window })
    : `?cluster_id=${encodeURIComponent(view.binding.cluster_id)}`;
  return <ObjectScopeBoundary object={observationScopeObject(view)}>
    <div className="page execution-observation-workspace">
      <PageHeader eyebrow="EXECUTION / OBSERVATION" title={m.title} description={m.introduction}
        actions={<Button disabled={resource.loading} variant="outline" onClick={() => void refresh()}>{m.refresh}</Button>} />
      {error && <p role="alert" className="inline-alert warning">{error}</p>}
      {!current && !resource.loading && !error && <p className="inline-alert warning" role="status">{m.expired}</p>}
      {resource.loading && <p role="status">{m.loading}</p>}
      <ExecutionTimeline view={view} transport={progress.transport} current={current} />
      <ExecutionObservationPanel view={view} current={current} />
      <section className="data-surface execution-handoff-panel">
        <header className="surface-heading"><div><h2>{m.reconciliation}</h2><p>{m.compensationHint}</p></div></header>
        <div className="execution-fact-grid">
          <section><h3>{m.reconciliation}</h3><ExecutionFact fact={view.reconciliation} current={current} /></section>
          <section><h3>{m.manual}</h3><ExecutionFact fact={view.manual_takeover} current={current} /></section>
        </div>
        <div className="execution-read-links">
          <Button variant="outline" onClick={() => navigate(`/changes/audit/${view.binding.correlation_id}${clusterSearch}`)}>{m.openAudit}</Button>
          <Button variant="outline" onClick={() => navigate(`/changes/quarantines${clusterSearch}`)}>{m.openQuarantine}</Button>
        </div>
      </section>
    </div>
  </ObjectScopeBoundary>;
}
