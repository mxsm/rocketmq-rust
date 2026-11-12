import { ArrowUpRight } from "lucide-react";
import { useCallback } from "react";
import { Link } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { resourcePath, withinWindow } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";

import { investigationInScope } from "./investigationPresentation";

export function DutyQueue() {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const m = useWorkspaceMessages();
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!scope.clusterId) return undefined;
      const [investigations, incidents] = await Promise.allSettled([
        api.listInvestigations(scope.clusterId, signal),
        api.listIncidents(scope.clusterId, signal),
      ]);
      return {
        investigations:
          investigations.status === "fulfilled"
            ? investigations.value
            : undefined,
        incidents:
          incidents.status === "fulfilled" ? incidents.value : undefined,
      };
    },
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  useWorkflowProgress(scope.clusterId, resource.reload);
  const path = resourcePath(scope);
  const tenantId = auth.session?.tenantId ?? "";
  const investigations =
    resource.data?.investigations?.items.filter(
      (view) =>
        investigationInScope(view, scope, tenantId) &&
        !["closed", "promoted"].includes(view.investigation.status),
    ) ?? [];
  const incidents =
    resource.data?.incidents?.items.filter(
      ({ incident }) =>
        incident.tenant_id === tenantId &&
        incident.cluster_id === scope.clusterId &&
        (!path ||
          scope.resourceKind === "cluster" ||
          incident.resource === path) &&
        withinWindow(incident.updated_at, scope.window) &&
        incident.status !== "resolved",
    ) ?? [];
  const rows = [
    ...investigations.map(({ investigation }) => ({
      id: investigation.id,
      title: investigation.title,
      status: investigation.status,
      to: `/investigations/${investigation.id}`,
    })),
    ...incidents.map(({ incident }) => ({
      id: incident.id,
      title: incident.title,
      status: incident.status,
      to: `/incidents/${incident.id}`,
    })),
  ].sort(
    (a, b) =>
      Number(b.status === "needs_evidence") -
      Number(a.status === "needs_evidence"),
  );
  const unavailable =
    resource.data &&
    (!resource.data.investigations || !resource.data.incidents);
  const partial =
    resource.data?.investigations?.partial || resource.data?.incidents?.partial;

  return (
    <DataSurface
      className="duty-queue"
      title={m.queue}
      description={m.queueHint}
      meta={
        <Button asChild size="sm" variant="ghost">
          <Link to="/operations">
            {m.handoff}
            <ArrowUpRight size={14} />
          </Link>
        </Button>
      }
    >
      <div className="duty-queue-summary">
        <span>
          {m.pendingDecision}{" "}
          <strong>
            {unavailable
              ? "—"
              : rows.filter((row) => row.status !== "needs_evidence").length}
          </strong>
        </span>
        <span>
          {m.pendingEvidence}{" "}
          <strong>
            {unavailable
              ? "—"
              : rows.filter((row) => row.status === "needs_evidence").length}
          </strong>
        </span>
        <Link className="table-link" to="/changes">
          {m.pendingApproval} →
        </Link>
      </div>
      <DataState
        loading={resource.loading && !resource.data}
        error={
          resource.error || (unavailable ? new Error("unavailable") : undefined)
        }
        empty={!resource.loading && !unavailable && !rows.length}
        emptyTitle={m.empty}
        emptyDescription={m.timeFiltered}
        onRetry={resource.reload}
      />
      {partial && <p className="inline-alert warning">{m.partial}</p>}
      {rows.length > 0 && (
        <ul className="duty-queue-list">
          {rows.slice(0, 8).map((row) => (
            <li key={row.id}>
              <Badge
                variant={
                  row.status === "needs_evidence" ? "warning" : "outline"
                }
              >
                {row.status === "needs_evidence"
                  ? m.pendingEvidence
                  : m.pendingDecision}
              </Badge>
              <Link to={{ pathname: row.to, search: scope.searchFor() }}>
                {row.title}
              </Link>
              <ArrowUpRight aria-hidden="true" size={14} />
            </li>
          ))}
        </ul>
      )}
      {rows.length > 8 && (
        <div className="investigation-related-links">
          <Link className="table-link" to="/investigations">
            {m.investigations} →
          </Link>
          <Link className="table-link" to="/incidents">
            {m.incidents} →
          </Link>
        </div>
      )}
    </DataSurface>
  );
}
