import { useCallback, useState, type ReactNode } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import type { InvestigationView } from "@/api/types";
import { ApiError } from "@/api/client";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { projectWorkspaceSearch } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import { useRunMessages } from "@/i18n/runMessages";
import { CreateRunForm } from "./CreateRunForm";
import { InvestigationRunDetail } from "./InvestigationRunDetail";
import {
  decimal,
  resourceForWorkspace,
  runSelection,
  RunScopeError,
  selectionSearch,
  sameWindow,
} from "./runPresentation";
import { useRunLabel } from "./runLabels";

export function InvestigationRunWorkspace({
  investigation,
  children,
}: {
  investigation: InvestigationView["investigation"];
  children: ReactNode;
}) {
  const scope = useWorkspaceScope();
  return (
    <RunWorkspaceSession
      key={`${scope.scopeKey}:${investigation.id}`}
      investigation={investigation}
    >
      {children}
    </RunWorkspaceSession>
  );
}

function RunWorkspaceSession({
  investigation,
  children,
}: {
  investigation: InvestigationView["investigation"];
  children: ReactNode;
}) {
  const m = useRunMessages();
  const label = useRunLabel();
  const { api } = useSreData();
  const scope = useWorkspaceScope();
  const location = useLocation();
  const navigate = useNavigate();
  const { formatDateTime } = useOperatorPreferences();
  const selection = runSelection(location.search);
  const [history, setHistory] = useState(false);
  const [create, setCreate] = useState(false);
  const [paging, setPaging] = useState<{
    cursors: Array<string | undefined>;
    index: number;
  }>({ cursors: [undefined], index: 0 });
  const cursor = paging.cursors[paging.index];
  const start = scope.window.start;
  const end = scope.window.end;
  const kind =
    scope.resourceKind === "cluster"
      ? undefined
      : scope.resourceKind === "consumer"
        ? "consumer_group"
        : scope.resourceKind === "producer"
          ? "producer_group"
          : scope.resourceKind;
  const resourceKey = kind ? scope.resourceKey : undefined;
  const unsupportedResource =
    kind === "connection" || kind === "pod_disruption_budget";
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!history && unsupportedResource) throw new RunScopeError();
      const page = await api
        .listInvestigationRuns(
          investigation.id,
          {
            cursor,
            limit: 20,
            ...(history ? {} : { start, end, resourceKind: kind, resourceKey }),
          },
          signal,
        )
        .catch((error: unknown) => {
          // A mutable run may leave this exact filter while its page cursor is open.
          // Only the still-current request can reset once; first-page failures stay visible.
          if (
            cursor &&
            !signal.aborted &&
            error instanceof ApiError &&
            error.status === 400
          ) {
            setPaging((current) =>
              current.cursors[current.index] === cursor
                ? { cursors: [undefined], index: 0 }
                : current,
            );
          }
          throw error;
        });
      if (
        page.schema_version !== "rocketmq-sre.investigation-run.v1" ||
        page.tenant_id !== investigation.tenant_id ||
        page.cluster_id !== investigation.cluster_id ||
        page.investigation_id !== investigation.id ||
        !Array.isArray(page.items) ||
        page.items.length > 50 ||
        !page.creation ||
        typeof page.creation.allowed !== "boolean" ||
        (page.creation.allowed && page.creation.blocked_reason != null) ||
        new Set(page.items.map((run) => run.id)).size !== page.items.length ||
        page.items.some(
          (run) =>
            !decimal(run.revision) ||
            (!history &&
              (!sameWindow(run.effective_scope.time_range, { start, end }) ||
                (kind &&
                  (run.effective_scope.resource?.kind !== kind ||
                    run.effective_scope.resource?.key !== resourceKey)))) ||
            [run.initial_scope, run.effective_scope].some(
              (item) =>
                item.tenant_id !== page.tenant_id ||
                item.cluster_id !== page.cluster_id ||
                item.investigation_id !== page.investigation_id,
            ),
        )
      )
        throw new RunScopeError();
      return page;
    },
    [
      api,
      cursor,
      end,
      history,
      investigation.cluster_id,
      investigation.id,
      investigation.tenant_id,
      kind,
      resourceKey,
      start,
      unsupportedResource,
    ],
  );
  const resource = useAsyncResource(load);
  const page = resource.error ? undefined : resource.data;
  const next =
    page?.next_cursor &&
    !paging.cursors.slice(0, paging.index + 1).includes(page.next_cursor)
      ? page.next_cursor
      : undefined;
  const select = (id?: string) =>
    navigate({
      pathname: location.pathname,
      search: selectionSearch(location.search, id),
      hash: location.hash,
    });
  const parent = resourceForWorkspace(page?.creation.parent_resource);
  const resourceMatchesParent =
    !scope.resourceKind ||
    scope.resourceKind === "cluster" ||
    (parent.resourceKind === scope.resourceKind &&
      parent.resourceKey === scope.resourceKey);
  const clearToParent = () =>
    navigate({
      pathname: location.pathname,
      search: projectWorkspaceSearch(selectionSearch(location.search), {
        clusterId: scope.clusterId,
        ...parent,
        window: scope.window,
      }),
    });
  return (
    <div className="run-workspace">
      <section className="run-discovery" aria-label={m.runs}>
        <div className="run-discovery-heading">
          <strong>{m.runs}</strong>
          <div className="inline-actions">
            <Button
              size="sm"
              variant="ghost"
              onClick={() => void resource.reload()}
              disabled={resource.loading}
            >
              {m.refreshList}
            </Button>
            {page?.creation.allowed && resourceMatchesParent && (
              <Button size="sm" onClick={() => setCreate(true)}>
                {m.create}
              </Button>
            )}
          </div>
        </div>
        <div className="run-discovery-inputs">
          <label>
            {m.selection}
            <select
              aria-label={m.selection}
              value={selection.kind === "run" ? selection.id : ""}
              onChange={(event) => select(event.target.value || undefined)}
            >
              <option value="">{m.manual}</option>
              {selection.kind === "run" &&
                !page?.items.some((run) => run.id === selection.id) && (
                  <option value={selection.id}>
                    {m.selection} · {selection.id.slice(0, 8)}
                  </option>
                )}
              {page?.items.map((run) => (
                <option value={run.id} key={run.id}>
                  {formatDateTime(run.created_at)} · {label(run.status)} ·{" "}
                  {run.id.slice(0, 8)}
                </option>
              ))}
            </select>
          </label>
          <label className="run-checkbox">
            <input
              type="checkbox"
              checked={history}
              onChange={(event) => {
                setHistory(event.target.checked);
                setPaging({ cursors: [undefined], index: 0 });
              }}
            />
            {m.allHistory}
          </label>
          <div className="inline-actions">
            <Button
              size="sm"
              variant="outline"
              disabled={resource.loading || paging.index === 0}
              onClick={() => setPaging({ ...paging, index: paging.index - 1 })}
            >
              {m.previous}
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={resource.loading || !next}
              onClick={() => {
                if (next)
                  setPaging({
                    cursors: [
                      ...paging.cursors.slice(0, paging.index + 1),
                      next,
                    ],
                    index: paging.index + 1,
                  });
              }}
            >
              {m.next}
            </Button>
          </div>
        </div>
        <p className="muted" role="status">
          {resource.error
            ? unsupportedResource && !history
              ? label("unsupported_resource")
              : m.unavailable
            : resource.loading && !page
              ? m.loading
              : history
                ? m.historyHint
                : m.currentFilter}
          {!resource.loading && page && !page.items.length
            ? ` · ${m.empty}`
            : ""}
        </p>
        {page?.creation.allowed && !resourceMatchesParent && (
          <div className="scope-error" role="status">
            <span>{m.scopeMismatch}</span>
            <Button size="sm" variant="outline" onClick={clearToParent}>
              {m.align}
            </Button>
          </div>
        )}
        {page && !page.creation.allowed && page.creation.blocked_reason && (
          <p className="muted">
            {m.unavailableReason}: {label(page.creation.blocked_reason)}
          </p>
        )}
      </section>
      {selection.kind === "invalid" ? (
        <div className="state-panel" role="alert">
          {m.invalidLink}
          <Button onClick={() => select()}>{m.readManual}</Button>
        </div>
      ) : selection.kind === "run" ? (
        <InvestigationRunDetail
          key={selection.id}
          investigation={investigation}
          runId={selection.id}
          revisionId={selection.revisionId}
        />
      ) : (
        children
      )}
      {create && page?.creation.allowed && resourceMatchesParent && (
        <CreateRunForm
          page={page}
          onClose={() => setCreate(false)}
          onCreated={(id) => {
            setCreate(false);
            select(id);
            void resource.reload();
          }}
        />
      )}
    </div>
  );
}
