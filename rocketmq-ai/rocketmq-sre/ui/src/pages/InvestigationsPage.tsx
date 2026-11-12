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
  const [page, setPage] = useState(0);
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listInvestigations(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: "",
          }),
    [api, scope.clusterId],
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
  const currentPage = Math.min(page, pages - 1);

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
                setPage(0);
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
              setPage(0);
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
                      <td>{view.diagnosis_revisions.length}</td>
                      <td>{formatDateTime(view.investigation.updated_at)}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        )}
        {pages > 1 && (
          <div className="investigation-pagination">
            <Button
              disabled={!currentPage}
              onClick={() => setPage(currentPage - 1)}
              size="sm"
              variant="outline"
            >
              ←
            </Button>
            <span>
              {currentPage + 1} / {pages}
            </span>
            <Button
              disabled={currentPage === pages - 1}
              onClick={() => setPage(currentPage + 1)}
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
