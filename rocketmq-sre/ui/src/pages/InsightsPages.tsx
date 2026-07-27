import {
  ArrowRight,
  BookOpenCheck,
  Bot,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  DatabaseZap,
  FileJson,
  Fingerprint,
  KeyRound,
  Search,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import {
  type FormEvent,
  useCallback,
  useMemo,
  useState,
} from "react";

import type {
  EvidenceCollectionStatus,
  EvidenceRecord,
  KnowledgeReviewStatus,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  ClusterScopeSelect,
  DataState,
  DataSurface,
  DefinitionGrid,
  PartialNotice,
  formatTime,
} from "@/components/Phase1Primitives";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";

const evidenceVariants = {
  complete: "success",
  partial: "warning",
  unavailable: "destructive",
} as const;

const knowledgeVariants: Record<
  KnowledgeReviewStatus,
  "success" | "warning" | "secondary" | "destructive"
> = {
  validated: "success",
  in_review: "warning",
  draft: "secondary",
  deprecated: "secondary",
  expired: "destructive",
};

export function EvidenceExplorerPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const [search, setSearch] = useState("");
  const [status, setStatus] =
    useState<EvidenceCollectionStatus | "all">("all");
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listEvidence(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const filtered = useMemo(
    () =>
      (resource.data?.items ?? []).filter((item) => {
        const query = search.trim().toLocaleLowerCase();
        return (
          (status === "all" || evidenceStatus(item) === status) &&
          (!query ||
            item.source.toLocaleLowerCase().includes(query) ||
            item.resource.toLocaleLowerCase().includes(query) ||
            item.correlation_id.toLocaleLowerCase().includes(query))
        );
      }),
    [resource.data?.items, search, status],
  );

  return (
    <div className="page">
      <PageHeader
        eyebrow="CANONICAL EVIDENCE EXPLORER"
        title="证据浏览器"
        description="按来源、资源、correlation id 与完整性搜索 Canonical Evidence；restricted 字段只显示脱敏摘要。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            value={scope.clusterId}
            onChange={scope.setClusterId}
          />
        }
      />
      <ReadOnlyBoundary compact />
      <PartialNotice envelope={resource.data} />
      <DataSurface
        title="Evidence Snapshot"
        description="Hash 覆盖 schema、来源、资源、时间范围和内容；采集元数据不影响内容寻址。"
        meta={<span>{filtered.length} 条</span>}
      >
        <div className="filter-bar">
          <label className="search-field">
            <Search size={14} />
            <span className="sr-only">搜索证据</span>
            <input
              onChange={(event) => setSearch(event.target.value)}
              placeholder="source / resource / correlation id"
              type="search"
              value={search}
            />
          </label>
          <label>
            <span className="sr-only">证据状态</span>
            <select
              className="native-select"
              onChange={(event) =>
                setStatus(
                  event.target.value as EvidenceCollectionStatus | "all",
                )
              }
              value={status}
            >
              <option value="all">全部完整性</option>
              <option value="complete">complete</option>
              <option value="partial">partial</option>
              <option value="unavailable">unavailable</option>
            </select>
          </label>
        </div>
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && filtered.length === 0}
          onRetry={resource.reload}
          emptyTitle="没有匹配的 Evidence"
        />
        {!resource.loading && !resource.error && filtered.length > 0 && (
          <div className="evidence-card-list">
            {filtered.map((item) => (
              <EvidenceCard evidence={item} key={item.evidence_id} />
            ))}
          </div>
        )}
      </DataSurface>
    </div>
  );
}

function EvidenceCard({ evidence }: { evidence: EvidenceRecord }) {
  const { api } = useSreData();
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<EvidenceRecord>();
  const [content, setContent] = useState<unknown>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const status = evidenceStatus(evidence);
  const snapshot = detail ?? evidence;
  const warnings = snapshot.warnings ?? [];
  const expand = async () => {
    const nextExpanded = !expanded;
    setExpanded(nextExpanded);
    if (!nextExpanded || detail) {
      return;
    }
    setLoading(true);
    setError(undefined);
    try {
      const [nextDetail, nextContent] = await Promise.all([
        api.getEvidence(evidence.evidence_id),
        api.getEvidenceContent(evidence.evidence_id),
      ]);
      setDetail(nextDetail);
      setContent(nextContent);
    } catch {
      setError(
        "证据详情暂不可用；不会用列表摘要替代缺失的原始内容。",
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <article>
      <header>
        <div>
          <DatabaseZap size={16} />
          <strong>{sourceLabel(snapshot.source)}</strong>
          <code>{snapshot.source}</code>
        </div>
        <Badge variant={evidenceVariants[status]}>{status}</Badge>
        <Button
          aria-expanded={expanded}
          onClick={() => void expand()}
          size="sm"
          variant="ghost"
        >
          {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          {expanded ? "收起详情" : "展开详情"}
        </Button>
      </header>
      <DefinitionGrid
        items={[
          { label: "资源", value: snapshot.resource },
          {
            label: "观测时间",
            value: formatTime(snapshot.observed_at),
          },
          {
            label: "新鲜度",
            value: `${snapshot.freshness_seconds}s`,
          },
          {
            label: "敏感级别",
            value: snapshot.sensitivity,
          },
          {
            label: "Exposure",
            value: snapshot.exposure,
          },
          {
            label: "Correlation",
            value: snapshot.correlation_id,
            mono: true,
          },
          {
            label: "Content hash",
            value: snapshot.content_hash,
            mono: true,
          },
        ]}
      />
      {expanded && (
        <div className="evidence-detail">
          {loading ? (
            <div className="state-message">正在读取有界 Evidence 内容…</div>
          ) : error ? (
            <div className="inline-alert warning">{error}</div>
          ) : (
            <>
              <DefinitionGrid
                items={[
                  {
                    label: "Schema",
                    value: `${snapshot.schema.family}.v${snapshot.schema.major}.${snapshot.schema.minor}`,
                    mono: true,
                  },
                  {
                    label: "Query ID",
                    value: snapshot.query_id,
                    mono: true,
                  },
                  {
                    label: "Coverage",
                    value: snapshot.coverage,
                  },
                  {
                    label: "存储",
                    value: snapshot.content.storage,
                  },
                  {
                    label: "时间范围开始",
                    value: formatTime(snapshot.time_range.start),
                  },
                  {
                    label: "时间范围结束",
                    value: formatTime(snapshot.time_range.end),
                  },
                ]}
              />
              <div className="evidence-content">
                <div>
                  <FileJson size={15} />
                  <strong>已校验内容</strong>
                  <span>敏感键在浏览器中再次脱敏并限制为 8 KiB。</span>
                </div>
                <pre>{safeEvidenceJson(content)}</pre>
              </div>
            </>
          )}
        </div>
      )}
      {warnings.length > 0 && (
        <footer>
          <TriangleAlert size={13} />
          {warnings.join("；")}
        </footer>
      )}
    </article>
  );
}

function evidenceStatus(
  evidence: EvidenceRecord,
): EvidenceCollectionStatus {
  if (evidence.coverage === "missing") {
    return "unavailable";
  }
  if (evidence.partial || evidence.coverage !== "available") {
    return "partial";
  }
  return "complete";
}

function sourceLabel(source: string) {
  return source
    .split(/[._/-]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toLocaleUpperCase() + part.slice(1))
    .join(" ");
}

function safeEvidenceJson(value: unknown) {
  const sensitive =
    /(^|_)(body|payload|token|secret|password|credential|private_key|tls|acl)($|_)/i;
  const serialized =
    JSON.stringify(
      value ?? { status: "missing" },
      (key, item) => (sensitive.test(key) ? "[redacted]" : item),
      2,
    ) ?? "missing";
  return serialized.length > 8_192
    ? `${serialized.slice(0, 8_192)}\n… truncated`
    : serialized;
}

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

export function KnowledgePage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const [search, setSearch] = useState("");
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listKnowledge(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const filtered = useMemo(() => {
    const query = search.trim().toLocaleLowerCase();
    return (resource.data?.items ?? []).filter(
      (item) =>
        !query ||
        item.title.toLocaleLowerCase().includes(query) ||
        item.component.toLocaleLowerCase().includes(query) ||
        item.summary.toLocaleLowerCase().includes(query),
    );
  }, [resource.data?.items, search]);

  return (
    <div className="page">
      <PageHeader
        eyebrow="CURATED KNOWLEDGE"
        title="知识库"
        description="展示带来源、版本、Owner、审阅状态和冲突标记的可检索知识；模型不能把未验证内容当作事实。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            value={scope.clusterId}
            onChange={scope.setClusterId}
          />
        }
      />
      <PartialNotice envelope={resource.data} />
      <DataSurface
        title="知识条目"
        description="搜索结果按当前 tenant/cluster 范围过滤。"
        meta={<span>{filtered.length} 条</span>}
      >
        <div className="filter-bar">
          <label className="search-field">
            <Search size={14} />
            <span className="sr-only">搜索知识</span>
            <input
              onChange={(event) => setSearch(event.target.value)}
              placeholder="搜索标题、组件或摘要"
              type="search"
              value={search}
            />
          </label>
        </div>
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && filtered.length === 0}
          onRetry={resource.reload}
          emptyTitle="没有匹配的知识条目"
        />
        {!resource.loading && !resource.error && filtered.length > 0 && (
          <div className="knowledge-grid">
            {filtered.map((item) => (
              <article key={item.id}>
                <header>
                  <BookOpenCheck size={17} />
                  <Badge variant={knowledgeVariants[item.review_status]}>
                    {item.review_status}
                  </Badge>
                  {item.conflict && (
                    <Badge variant="destructive">conflict</Badge>
                  )}
                </header>
                <h3>{item.title}</h3>
                <p>{item.summary}</p>
                <DefinitionGrid
                  items={[
                    { label: "组件", value: item.component },
                    { label: "版本范围", value: item.rocketmq_version_range },
                    { label: "Owner", value: item.owner },
                    { label: "来源版本", value: item.source_version },
                    {
                      label: "Review due",
                      value: formatTime(item.review_due_at),
                    },
                    { label: "敏感级别", value: item.sensitivity },
                  ]}
                />
                <footer>
                  <code>{item.source_uri}</code>
                  <span>{formatTime(item.updated_at)}</span>
                </footer>
              </article>
            ))}
          </div>
        )}
      </DataSurface>
    </div>
  );
}

export function ModelsPage() {
  const { api } = useSreData();
  const load = useCallback(
    (signal: AbortSignal) => api.getModelCapabilities(signal),
    [api],
  );
  const resource = useAsyncResource(load);

  return (
    <div className="page">
      <PageHeader
        eyebrow="MODEL GATEWAY"
        title="模型能力"
        description="展示协议适配与实际 profile 健康，不显示 credential ref、token 或完整 endpoint。"
      />
      <ReadOnlyBoundary compact />
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <>
          <section className="summary-strip phase1-summary">
            <ModelSummary
              label="Provider"
              value={String(resource.data.providers.length)}
            />
            <ModelSummary
              label="Profile"
              value={String(resource.data.profiles?.length ?? 0)}
            />
            <ModelSummary
              label="Rules-only"
              value={resource.data.rules_only_available ? "可用" : "不可用"}
              safe={resource.data.rules_only_available}
            />
            <ModelSummary
              label="网络调用"
              value={
                resource.data.network_calls_enabled ? "已启用" : "已禁用"
              }
              safe={!resource.data.network_calls_enabled}
            />
          </section>
          <DataSurface
            title="协议适配矩阵"
            description="包含 DeepSeek、智谱 GLM 与 Kimi/Moonshot；Descriptor 不代表外部调用已启用。"
            meta={<span>{resource.data.providers.length} providers</span>}
          >
            <div className="table-scroll">
              <table className="phase1-table">
                <thead>
                  <tr>
                    <th>Provider</th>
                    <th>协议</th>
                    <th>Streaming</th>
                    <th>Tools</th>
                    <th>Structured</th>
                    <th>Embedding</th>
                  </tr>
                </thead>
                <tbody>
                  {resource.data.providers.map((provider) => (
                    <tr key={provider.id}>
                      <td>
                        <strong>{provider.id}</strong>
                      </td>
                      <td>{provider.protocols.join(", ")}</td>
                      <td>{yes(provider.supports_streaming)}</td>
                      <td>{yes(provider.supports_tools)}</td>
                      <td>{yes(provider.supports_structured_output)}</td>
                      <td>{yes(provider.supports_embeddings)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </DataSurface>
          <DataSurface
            title="路由 Profile"
            description="credential_present 只表示 secret 是否配置，不返回引用和值。"
            meta={<span>{resource.data.profiles?.length ?? 0} profiles</span>}
          >
            {!resource.data.profiles ||
            resource.data.profiles.length === 0 ? (
              <div className="state-message">
                后端未返回 profile；仅显示 ProviderDescriptor。
              </div>
            ) : (
              <div className="model-profile-grid">
                {resource.data.profiles.map((profile) => (
                  <article key={profile.id}>
                    <header>
                      <Bot size={17} />
                      <strong>{profile.profile_name}</strong>
                      <Badge
                        variant={
                          profile.health === "healthy"
                            ? "success"
                            : profile.health === "degraded"
                              ? "warning"
                              : "secondary"
                        }
                      >
                        {profile.health}
                      </Badge>
                    </header>
                    <DefinitionGrid
                      items={[
                        {
                          label: "Provider",
                          value: profile.provider_family,
                        },
                        {
                          label: "Model family",
                          value: profile.model_family,
                        },
                        {
                          label: "Model",
                          value: profile.model_name,
                        },
                        {
                          label: "Revision",
                          value: profile.model_revision,
                        },
                        {
                          label: "Region",
                          value: profile.region,
                        },
                        {
                          label: "Residency",
                          value: profile.data_residency,
                        },
                      ]}
                    />
                    <footer>
                      <span>
                        {profile.capabilities.join(" · ") || "no capability"}
                      </span>
                      <span>
                        <KeyRound size={12} />
                        credential{" "}
                        {profile.credential_present ? "present" : "absent"}
                      </span>
                    </footer>
                  </article>
                ))}
              </div>
            )}
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

function ModelSummary({
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

function yes(value: boolean) {
  return value ? (
    <span className="boolean yes">
      <ShieldCheck size={13} /> yes
    </span>
  ) : (
    <span className="boolean no">no</span>
  );
}
