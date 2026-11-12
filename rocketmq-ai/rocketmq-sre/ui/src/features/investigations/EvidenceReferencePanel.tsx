import { DatabaseZap, ExternalLink } from "lucide-react";
import { useCallback } from "react";
import { Link } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { DataState, DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { resourcePath, withinWindow } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

export function EvidenceReferencePanel({
  evidenceId,
}: {
  evidenceId?: string;
}) {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const m = useWorkspaceMessages();
  const { formatDateTime } = useOperatorPreferences();
  const load = useCallback(
    (signal: AbortSignal) =>
      evidenceId
        ? api.getEvidence(evidenceId, signal)
        : Promise.resolve(undefined),
    [api, evidenceId],
  );
  const resource = useAsyncResource(load);
  const evidence = resource.data;
  const path = resourcePath(scope);
  const allowed =
    evidence &&
    evidence.evidence_id === evidenceId &&
    evidence.tenant_id === auth.session?.tenantId &&
    evidence.cluster_id === scope.clusterId &&
    (!path || scope.resourceKind === "cluster" || evidence.resource === path);

  return (
    <aside
      aria-label={m.provenance}
      className="investigation-evidence-panel data-surface"
    >
      <div className="surface-heading">
        <div>
          <h2>
            <DatabaseZap aria-hidden="true" size={16} /> {m.provenance}
          </h2>
          <p>{allowed ? evidence.source : m.selectEvidence}</p>
        </div>
      </div>
      {evidenceId && (
        <DataState
          loading={resource.loading && !evidence}
          error={resource.error}
          empty={false}
          onRetry={resource.reload}
        />
      )}
      {!evidenceId && <p className="workspace-empty">{m.selectEvidence}</p>}
      {evidence && !allowed && (
        <p className="workspace-empty" role="alert">
          {m.evidenceUnavailable}
        </p>
      )}
      {allowed && (
        <>
          <div className="investigation-badges">
            <Badge variant={evidence.partial ? "warning" : "outline"}>
              {evidence.partial ? m.partial : m.complete}
            </Badge>
            <Badge variant="secondary">{evidence.coverage}</Badge>
          </div>
          {!withinWindow(evidence.observed_at, scope.window) && (
            <p className="inline-alert warning" role="status">
              {m.outsideWindow}
            </p>
          )}
          <DefinitionGrid
            items={[
              { label: "Source", value: evidence.source },
              { label: m.resourceScope, value: evidence.resource },
              {
                label: "Observed",
                value: formatDateTime(evidence.observed_at),
              },
              { label: "Freshness", value: `${evidence.freshness_seconds}s` },
              {
                label: "Time range",
                value: `${formatDateTime(evidence.time_range.start)} → ${formatDateTime(evidence.time_range.end)}`,
              },
              {
                label: "Sensitivity / exposure",
                value: `${evidence.sensitivity} / ${evidence.exposure}`,
              },
            ]}
          />
          <details className="investigation-audit-details">
            <summary>{m.auditDetails}</summary>
            <DefinitionGrid
              items={[
                {
                  label: "Evidence ID",
                  value: evidence.evidence_id,
                  mono: true,
                },
                {
                  label: "Content hash",
                  value: evidence.content_hash,
                  mono: true,
                },
                {
                  label: "Correlation",
                  value: evidence.correlation_id,
                  mono: true,
                },
              ]}
            />
          </details>
          {evidence.warnings?.length ? (
            <p className="inline-alert warning">
              {evidence.warnings.join(" · ")}
            </p>
          ) : null}
          <Button
            asChild
            className="investigation-evidence-link"
            size="sm"
            variant="outline"
          >
            <Link
              to={{
                pathname: "/evidence",
                search: scope.searchFor(
                  `?evidence_id=${encodeURIComponent(evidence.evidence_id)}`,
                ),
              }}
            >
              <ExternalLink size={14} />
              {m.evidenceContentLink}
            </Link>
          </Button>
        </>
      )}
    </aside>
  );
}
