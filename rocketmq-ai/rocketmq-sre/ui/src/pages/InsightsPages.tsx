import {
  ArrowRight,
  CheckCircle2,
  Fingerprint,
  Search,
  TriangleAlert,
} from "lucide-react";
import {
  type FormEvent,
  useCallback,
  useState,
} from "react";

import { PageHeader } from "@/components/PageHeader";
import {
  ClusterScopeSelect,
  DataState,
  DataSurface,
  formatTime,
} from "@/components/Phase1Primitives";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";

export { EvidenceExplorerPage } from "./EvidenceExplorerPage";
export { KnowledgePage } from "./KnowledgePage";

export function MessageJourneyPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const [query, setQuery] = useState(
    "7F00000100002A9F0000000000008C21",
  );
  const [submitted, setSubmitted] = useState(query);
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId && submitted
        ? api.getMessageJourney(scope.clusterId, submitted, signal)
        : Promise.resolve(undefined),
    [api, scope.clusterId, submitted],
  );
  const resource = useAsyncResource(load);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setSubmitted(query.trim());
  };

  return (
    <div className="page">
      <PageHeader
        eyebrow="MESSAGE JOURNEY"
        title="消息旅程"
        description="仅使用 Message ID/Trace ID 关联生产、代理、Broker、Store 与消费证据；永不读取或展示消息正文。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            value={scope.clusterId}
            onChange={scope.setClusterId}
          />
        }
      />
      <ReadOnlyBoundary />
      <form className="journey-search" onSubmit={submit}>
        <label className="search-field">
          <Fingerprint size={15} />
          <span className="sr-only">Message ID 或 Trace ID</span>
          <input
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Message ID / Trace ID（不接受消息正文）"
            required
            value={query}
          />
        </label>
        <Button disabled={!query.trim() || !scope.clusterId} type="submit">
          <Search size={14} />
          查询只读链路
        </Button>
      </form>
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
        emptyTitle="输入标识以查询消息旅程"
      />
      {resource.data && (
        <>
          {resource.data.partial && (
            <div className="partial-notice">
              <TriangleAlert size={15} />
              <div>
                <strong>旅程不完整</strong>
                <span>{resource.data.warnings.join("；")}</span>
              </div>
            </div>
          )}
          <section className="summary-strip phase1-summary">
            <JourneySummary
              label="Topic"
              value={resource.data.topic ?? "missing"}
            />
            <JourneySummary
              label="Queue"
              value={String(resource.data.queue_id ?? "missing")}
            />
            <JourneySummary
              label="Trace fingerprint"
              value={resource.data.trace_fingerprint}
            />
            <JourneySummary label="消息正文" value="不可用" safe />
          </section>
          <DataSurface
            title="证据链"
            description="missing hop 明确显示，不以成功状态补齐。"
          >
            <ol className="journey-timeline">
              {resource.data.hops.map((hop, index) => (
                <li key={`${hop.stage}-${hop.observed_at}`}>
                  <div className={`journey-marker ${hop.status}`}>
                    {hop.status === "observed" ? (
                      <CheckCircle2 size={15} />
                    ) : (
                      <TriangleAlert size={15} />
                    )}
                  </div>
                  <div className="journey-hop">
                    <header>
                      <strong>{hop.stage}</strong>
                      <Badge
                        variant={
                          hop.status === "observed"
                            ? "success"
                            : hop.status === "partial"
                              ? "warning"
                              : "destructive"
                        }
                      >
                        {hop.status}
                      </Badge>
                    </header>
                    <span>{hop.component}</span>
                    <p>{hop.detail}</p>
                    <footer>
                      <span>{formatTime(hop.observed_at)}</span>
                      <span>
                        {hop.latency_ms == null
                          ? "latency missing"
                          : `${hop.latency_ms}ms`}
                      </span>
                    </footer>
                  </div>
                  {index < (resource.data?.hops.length ?? 0) - 1 && (
                    <ArrowRight className="journey-arrow" size={16} />
                  )}
                </li>
              ))}
            </ol>
          </DataSurface>
        </>
      )}
    </div>
  );
}

function JourneySummary({
  label,
  value,
  safe = false,
}: {
  label: string;
  value: string;
  safe?: boolean;
}) {
  return (
    <div className="summary-item">
      <span>{label}</span>
      <strong className={safe ? "success" : undefined}>{value}</strong>
    </div>
  );
}
