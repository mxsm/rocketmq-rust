import { DatabaseZap, RefreshCw, Search } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { resourcePath } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { EvidenceDrawer } from "@/features/evidence/EvidenceDrawer";
import {
  evidenceAvailability,
  evidenceOutsideWindow,
  type EvidenceAvailability,
} from "@/features/evidence/evidencePresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useEvidenceMessages } from "@/i18n/evidenceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

const PAGE_SIZE = 50;

export function EvidenceExplorerPage() {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const m = useEvidenceMessages();
  const { formatDateTime } = useOperatorPreferences();
  const [params, setParams] = useSearchParams();
  const requested = params.getAll("evidence_id");
  const evidenceId =
    requested.length === 1 && requested[0].length <= 512
      ? requested[0]
      : undefined;
  const invalidReference = requested.length > 0 && !evidenceId;
  const [showList, setShowList] = useState(!evidenceId && !invalidReference);
  const [query, setQuery] = useState("");
  const [quality, setQuality] = useState<
    EvidenceAvailability | "all" | "outside"
  >("all");
  const [page, setPage] = useState(0);
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId && showList
        ? api.listEvidence(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: "",
          }),
    [api, scope.clusterId, showList],
  );
  const resource = useAsyncResource(load);
  const selectedResource = resourcePath(scope);
  const items = useMemo(
    () =>
      (resource.data?.items ?? []).filter(
        (item) =>
          item.tenant_id === auth.session?.tenantId &&
          item.cluster_id === scope.clusterId &&
          (!selectedResource ||
            scope.resourceKind === "cluster" ||
            item.resource === selectedResource) &&
          (quality === "all" ||
            (quality === "outside"
              ? evidenceOutsideWindow(item, scope)
              : evidenceAvailability(item) === quality)) &&
          `${item.source} ${item.resource} ${item.evidence_id} ${item.correlation_id}`
            .toLocaleLowerCase()
            .includes(query.trim().toLocaleLowerCase()),
      ),
    [
      auth.session?.tenantId,
      quality,
      query,
      resource.data?.items,
      scope,
      selectedResource,
    ],
  );
  const pages = Math.max(1, Math.ceil(items.length / PAGE_SIZE));
  const currentPage = Math.min(page, pages - 1);
  const close = useCallback(() => {
    setShowList(true);
    const next = new URLSearchParams(params);
    next.delete("evidence_id");
    setParams(next, { replace: true });
  }, [params, setParams]);
  const open = (id: string) => {
    const next = new URLSearchParams(params);
    next.set("evidence_id", id);
    setParams(next);
  };

  return (
    <div className="page evidence-explorer-page">
      <PageHeader
        eyebrow="CANONICAL EVIDENCE"
        title={m.explorer}
        description={m.description}
        actions={
          <Button
            disabled={resource.loading || !!evidenceId}
            onClick={resource.reload}
            size="sm"
            variant="outline"
          >
            <RefreshCw size={14} />
            {m.refresh}
          </Button>
        }
      />
      {invalidReference && (
        <p className="inline-alert warning" role="alert">
          {m.mismatch}
        </p>
      )}
      {evidenceId && (
        <div className="inline-alert" role="status">
          <span>{m.located}</span>
          <Button onClick={close} size="sm" variant="ghost">
            {m.showAll}
          </Button>
        </div>
      )}
      <DataSurface title={m.list} meta={<span>{items.length}</span>}>
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
            aria-label={m.quality}
            className="native-select"
            onChange={(event) => {
              setQuality(event.target.value as typeof quality);
              setPage(0);
            }}
            value={quality}
          >
            <option value="all">{m.all}</option>
            {(
              [
                "available",
                "partial",
                "missing",
                "not_verified",
                "outside",
              ] as const
            ).map((key) => (
              <option key={key} value={key}>
                {m[key]}
              </option>
            ))}
          </select>
        </div>
        {resource.data?.partial && (
          <p className="inline-alert warning" role="status">
            {m.partial}
          </p>
        )}
        <DataState
          loading={resource.loading && !resource.data}
          error={resource.error}
          empty={
            !resource.loading &&
            !resource.error &&
            !items.length &&
            !evidenceId &&
            !invalidReference
          }
          emptyTitle={m.empty}
          onRetry={resource.reload}
        />
        {!!items.length && (
          <div className="table-scroll">
            <table className="phase1-table evidence-list-table">
              <thead>
                <tr>
                  <th>{m.source}</th>
                  <th>{m.resource}</th>
                  <th>{m.quality}</th>
                  <th>{m.observed}</th>
                  <th>{m.view}</th>
                </tr>
              </thead>
              <tbody>
                {items
                  .slice(
                    currentPage * PAGE_SIZE,
                    currentPage * PAGE_SIZE + PAGE_SIZE,
                  )
                  .map((item) => (
                    <tr key={item.evidence_id}>
                      <td>
                        <DatabaseZap aria-hidden="true" size={14} />{" "}
                        {item.source}
                      </td>
                      <td>{item.resource}</td>
                      <td>
                        <Badge
                          variant={
                            evidenceAvailability(item) === "available"
                              ? "outline"
                              : "warning"
                          }
                        >
                          {m[evidenceAvailability(item)]}
                        </Badge>
                        {evidenceOutsideWindow(item, scope) && (
                          <span className="evidence-outside-label">
                            {m.outside}
                          </span>
                        )}
                      </td>
                      <td>{formatDateTime(item.observed_at)}</td>
                      <td>
                        <Button
                          onClick={() => open(item.evidence_id)}
                          size="sm"
                          variant="outline"
                        >
                          {m.view}
                        </Button>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        )}
        {pages > 1 && (
          <nav
            aria-label={`${m.list} · ${m.page}`}
            className="investigation-pagination"
          >
            <Button
              disabled={!currentPage}
              onClick={() => setPage(currentPage - 1)}
              size="sm"
              variant="outline"
            >
              {m.previous}
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
              {m.next}
            </Button>
          </nav>
        )}
      </DataSurface>
      {evidenceId && <EvidenceDrawer evidenceId={evidenceId} onClose={close} />}
    </div>
  );
}
