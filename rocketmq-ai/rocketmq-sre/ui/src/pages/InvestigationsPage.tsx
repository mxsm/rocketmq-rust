import { Compass, RefreshCw, Search } from "lucide-react";
import { useCallback, useState } from "react";
import { Link } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import {
  DataState,
  DataSurface,
  LiveTransport,
  PartialNotice,
} from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import {
  investigationInScope,
  investigationStatusLabels,
} from "@/features/investigations/investigationPresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

export function InvestigationsPage() {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const m = useWorkspaceMessages();
  const { locale, formatDateTime } = useOperatorPreferences();
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [pagination, setPagination] = useState<{
    owner: string;
    cursors: Array<string | undefined>;
    index: number;
    local: number;
  }>({ owner: scope.scopeKey, cursors: [undefined], index: 0, local: 0 });
  const paging =
    pagination.owner === scope.scopeKey
      ? pagination
      : { owner: scope.scopeKey, cursors: [undefined], index: 0, local: 0 };
  const cursor = paging.cursors[paging.index];
  const load = useCallback(
    async (signal: AbortSignal) => {
      const page = scope.clusterId
        ? api.listInvestigations(scope.clusterId, signal, { cursor, limit: 50 })
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: "",
            next_cursor: null,
          });
      return { ...(await page), ownerScope: scope.scopeKey };
    },
    [api, cursor, scope.clusterId, scope.scopeKey],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(scope.clusterId, resource.reload, {
    aggregateType: "investigation",
  });
  const items = (resource.data?.items ?? [])
    .filter(
      (view) =>
        investigationInScope(view, scope, auth.session?.tenantId ?? "") &&
        (status === "all" || view.investigation.status === status) &&
        `${view.investigation.title} ${view.investigation.resource ?? ""}`
          .toLocaleLowerCase()
          .includes(query.trim().toLocaleLowerCase()),
    )
    .sort(
      (a, b) =>
        Date.parse(b.investigation.updated_at) -
        Date.parse(a.investigation.updated_at),
    );
  const pages = Math.max(1, Math.ceil(items.length / 50));
  const currentPage = Math.min(paging.local, pages - 1);
  const offeredCursor = resource.error ? undefined : resource.data?.next_cursor;
  const nextCursor =
    offeredCursor &&
    !paging.cursors.slice(0, paging.index + 1).includes(offeredCursor)
      ? offeredCursor
      : undefined;
  const previous = () =>
    setPagination({
      ...paging,
      index: currentPage > 0 ? paging.index : Math.max(0, paging.index - 1),
      local: currentPage > 0 ? currentPage - 1 : 0,
    });
  const next = () => {
    if (currentPage < pages - 1) {
      setPagination({ ...paging, local: currentPage + 1 });
    } else if (nextCursor) {
      setPagination({
        ...paging,
        cursors: [...paging.cursors.slice(0, paging.index + 1), nextCursor],
        index: paging.index + 1,
        local: 0,
      });
    }
  };

  return (
    <div className="page investigations-page">
      <PageHeader
        eyebrow="INVESTIGATIONS"
        title={m.investigationTitle}
        description={m.investigationDescription}
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <Button asChild size="sm">
              <Link to="/ask">
                <Compass size={15} />
                {m.newInvestigation}
              </Link>
            </Button>
            <Button
              disabled={resource.loading}
              onClick={resource.reload}
              size="sm"
              variant="outline"
            >
              <RefreshCw size={14} />
              {m.refresh}
            </Button>
          </>
        }
      />
      <p className="investigation-mode">{m.manualHint}</p>
      <DataSurface title={m.workspace} meta={<span>{items.length}</span>}>
        <div className="filter-bar">
          <label className="search-field">
            <Search size={14} />
            <span className="sr-only">{m.search}</span>
            <input
              aria-label={m.search}
              onChange={(event) => {
                setQuery(event.target.value);
                setPagination({ ...paging, local: 0 });
              }}
              placeholder={m.search}
              type="search"
              value={query}
            />
          </label>
          <select
            aria-label={m.status}
            className="native-select"
            onChange={(event) => {
              setStatus(event.target.value);
              setPagination({ ...paging, local: 0 });
            }}
            value={status}
          >
            <option value="all">{m.allStatus}</option>
            {Object.entries(investigationStatusLabels).map(([key, labels]) => (
              <option key={key} value={key}>
                {labels[locale === "en-US" ? 1 : 0]}
              </option>
            ))}
          </select>
        </div>
        <p className="workspace-empty">{m.pageFilterHint}</p>
        <PartialNotice envelope={resource.data} />
        <DataState
          loading={resource.loading && !resource.data}
          error={resource.error}
          empty={!resource.loading && !items.length}
          onRetry={resource.reload}
          emptyTitle={m.empty}
          emptyDescription={m.timeFiltered}
        />
        {!!items.length && (
          <div className="table-scroll">
            <table className="phase1-table">
              <thead>
                <tr>
                  <th>{m.investigations}</th>
                  <th>{m.status}</th>
                  <th>{m.resourceScope}</th>
                  <th>{m.revisions}</th>
                  <th>{m.updated}</th>
                </tr>
              </thead>
              <tbody>
                {items
                  .slice(currentPage * 50, currentPage * 50 + 50)
                  .map((view) => (
                    <tr key={view.investigation.id}>
                      <td>
                        <Link
                          className="table-link"
                          to={{
                            pathname: `/investigations/${view.investigation.id}`,
                            search: scope.searchFor(),
                          }}
                        >
                          {view.investigation.title}
                        </Link>
                      </td>
                      <td>
                        <Badge
                          variant={
                            view.investigation.status === "needs_evidence"
                              ? "warning"
                              : "outline"
                          }
                        >
                          {
                            investigationStatusLabels[
                              view.investigation.status
                            ][locale === "en-US" ? 1 : 0]
                          }
                        </Badge>
                      </td>
                      <td>{view.investigation.resource ?? "cluster"}</td>
                      <td
                        title={view.diagnosis_revisions.length
                          ? undefined : m.unknownRevisionCount}
                      >
                        {view.diagnosis_revisions.length || "—"}
                      </td>
                      <td>{formatDateTime(view.investigation.updated_at)}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        )}
        {(pages > 1 || paging.index > 0 || nextCursor) && (
          <div className="investigation-pagination">
            <Button
              aria-label={m.previousPage}
              disabled={resource.loading || (!currentPage && !paging.index)}
              onClick={previous}
              size="sm"
              variant="outline"
            >
              ←
            </Button>
            <span>
              {m.page} {paging.index + 1}
              {pages > 1 ? ` · ${currentPage + 1} / ${pages}` : ""}
            </span>
            <Button
              aria-label={m.nextPage}
              disabled={
                resource.loading || Boolean(resource.error) ||
                (currentPage === pages - 1 && !nextCursor)
              }
              onClick={next}
              size="sm"
              variant="outline"
            >
              →
            </Button>
          </div>
        )}
      </DataSurface>
    </div>
  );
}
