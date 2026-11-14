import { RefreshCw } from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import type { InvestigationView } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { DataSurface, DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { projectWorkspaceSearch } from "@/context/workspaceScope";
import { useRunMessages } from "@/i18n/runMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import { RunCommands } from "./RunCommands";
import { RunAssessment } from "./RunAssessment";
import { RunProgress } from "./RunProgress";
import { RunRevision } from "./RunRevision";
import {
  resourceForWorkspace,
  runInWorkspace,
  RunScopeError,
  selectionSearch,
} from "./runPresentation";
import { useRunLabel } from "./runLabels";
import { useInvestigationRun } from "./useInvestigationRun";

export function InvestigationRunDetail({
  investigation,
  runId,
  revisionId,
}: {
  investigation: InvestigationView["investigation"];
  runId: string;
  revisionId?: string;
}) {
  const m = useRunMessages();
  const label = useRunLabel();
  const { formatDateTime } = useOperatorPreferences();
  const scope = useWorkspaceScope();
  const location = useLocation();
  const navigate = useNavigate();
  const state = useInvestigationRun(investigation.id, runId);
  const run = state.run;
  const matched = !!run && runInWorkspace(run, scope);
  const align = () => {
    if (!run) return;
    navigate({
      pathname: location.pathname,
      search: projectWorkspaceSearch(
        selectionSearch(location.search, run.id, revisionId),
        {
          clusterId: run.effective_scope.cluster_id,
          ...resourceForWorkspace(run.effective_scope.resource),
          window: run.effective_scope.time_range,
        },
      ),
      hash: location.hash,
    });
  };
  const errorMessage =
    state.error instanceof RunScopeError
      ? m.scopeFailure
      : state.error instanceof ApiError &&
          [401, 403, 404].includes(state.error.status)
        ? m.forbidden
        : m.loadFailure;
  return (
    <div className="run-detail">
      <PageHeader
        eyebrow={m.runs}
        title={investigation.title}
        description={m.advisory}
        actions={
          <Button
            size="sm"
            variant="outline"
            onClick={() => void state.refresh()}
            disabled={state.loading}
          >
            <RefreshCw
              size={14}
              className={state.loading ? "spin" : undefined}
            />
            {m.refresh}
          </Button>
        }
      />
      {state.loading && !run && <p role="status">{m.loading}</p>}
      {Boolean(state.error) && (
        <div role="alert" className="scope-error">
          {errorMessage}
          <Button size="sm" onClick={() => void state.refresh()}>
            {m.retry}
          </Button>
        </div>
      )}
      {run && (
        <>
          <DataSurface
            title={m.status}
            meta={
              <Badge
                variant={run.status === "completed" ? "success" : "outline"}
              >
                {label(run.status)}
              </Badge>
            }
          >
            <DefinitionGrid
              items={[
                {
                  label: m.actualWindow,
                  value: (
                    <span className="run-window">
                      {run.effective_scope.time_range.start} →{" "}
                      {run.effective_scope.time_range.end}
                    </span>
                  ),
                },
                {
                  label: m.actualResource,
                  value: run.effective_scope.resource
                    ? `${run.effective_scope.resource.kind} · ${run.effective_scope.resource.key}`
                    : run.effective_scope.cluster_id,
                },
                {
                  label: m.packs,
                  value: run.effective_packs
                    .map((pack) => `${pack.pack_id} · ${pack.pack_version}`)
                    .join(", "),
                },
                { label: m.deadline, value: formatDateTime(run.deadline) },
                { label: m.updated, value: formatDateTime(run.updated_at) },
                ...(run.stop_reason
                  ? [
                      {
                        label: m.unavailableReason,
                        value: label(run.stop_reason),
                      },
                    ]
                  : []),
              ]}
            />
            <details className="run-audit">
              <summary>{m.audit}</summary>
              <DefinitionGrid
                items={[
                  { label: "Run", value: run.id, mono: true },
                  { label: m.revision, value: run.revision, mono: true },
                  { label: m.epoch, value: run.control_epoch, mono: true },
                ]}
              />
            </details>
          </DataSurface>
          {!matched && (
            <div className="scope-error" role="status">
              <span>{m.scopeMismatch}</span>
              <Button size="sm" onClick={align}>
                {m.align}
              </Button>
            </div>
          )}
          <div className="run-detail-grid">
            <div className="run-primary">
              <RunAssessment run={run} />
              <RunRevision
                run={run}
                revisionId={revisionId}
                observedAt={state.controls?.observed_at}
              />
              <RunProgress run={run} controls={state.controls} />
            </div>
            <aside className="run-secondary">
              <DataSurface title={m.commandsTitle}>
                {matched ? (
                  <RunCommands
                    run={run}
                    controls={state.controls}
                    submit={state.submit}
                    refresh={state.refresh}
                  />
                ) : (
                  <p>{m.scopeMismatch}</p>
                )}
              </DataSurface>
              <DataSurface title={m.events}>
                {state.resynchronized && <p role="status">{m.snapshotReset}</p>}
                {!state.events.length && <p className="muted">{m.noEvents}</p>}
                <ol className="run-events">
                  {[...state.events].reverse().map((event) => (
                    <li key={event.sequence}>
                      <strong>{label(event.event.kind)}</strong>
                      <time>{formatDateTime(event.occurred_at)}</time>
                      <small>
                        {m.revision} {event.revision} · {label(event.status)}
                      </small>
                    </li>
                  ))}
                </ol>
              </DataSurface>
            </aside>
          </div>
        </>
      )}
    </div>
  );
}
