import {
  Activity,
  ArrowLeft,
  Bot,
  CalendarClock,
  ChevronRight,
  CircleDot,
  Download,
  FileText,
  MessageSquareText,
  Play,
  SearchCheck,
  Send,
  ShieldCheck,
  Square,
} from "lucide-react";
import {
  type FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import type {
  ConversationStreamEvent,
  ConversationTurnStatus,
  InspectionReport,
  InspectionTemplate,
  InvestigationStatus,
  Recommendation,
  RecommendationStatus,
  WorkflowStreamEvent,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  ClusterScopeSelect,
  DataState,
  DataSurface,
  DefinitionGrid,
  LiveTransport,
  PartialNotice,
  Timeline,
  formatTime,
} from "@/components/Phase1Primitives";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { DiagnosisRevisionList } from "@/features/incidents/DiagnosisRevisionList";
import {
  IncidentInboxCards,
  IncidentInboxFilters,
} from "@/features/incidents/IncidentInbox";
import { IncidentTopology } from "@/features/incidents/IncidentTopology";
import {
  filterAndSortIncidents,
  incidentOwnerOptions,
  incidentStatusLabels,
} from "@/features/incidents/incidentPresentation";
import { SupervisedDiagnosisPanel } from "@/features/incidents/SupervisedDiagnosisPanel";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";

const investigationLabels: Record<InvestigationStatus, string> = {
  open: "已开启",
  collecting: "采集中",
  diagnosing: "诊断中",
  needs_evidence: "需要证据",
  monitoring: "监测中",
  promoted: "已升级",
  closed: "已关闭",
};

const templateLabels: Record<InspectionTemplate, string> = {
  cluster_health: "集群健康",
  consumer: "Consumer",
  broker: "Broker",
  telemetry: "Telemetry",
  full_cluster: "全量集群",
  producer_consumer: "Producer / Consumer",
  store_ha: "Store / HA",
  routing_proxy: "Routing / Proxy",
  security: "Security",
  upgrade: "Upgrade",
  disaster_recovery: "DR",
};

export function AskSrePage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const navigate = useNavigate();
  const appliedContext = useRef("");
  const [question, setQuestion] = useState("");
  const [resourcePath, setResourcePath] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string>();
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listConversations(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(scope.clusterId, resource.reload);
  useEffect(() => {
    const context = scope.urlContext;
    if (!context?.resourceKind || !context.resourceKey) {
      return;
    }
    const fingerprint = `${context.clusterId}:${context.resourceKind}:${context.resourceKey}`;
    if (appliedContext.current === fingerprint) {
      return;
    }
    appliedContext.current = fingerprint;
    setResourcePath(`${context.resourceKind}/${context.resourceKey}`);
  }, [scope.urlContext]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!scope.clusterId || !question.trim()) {
      return;
    }
    setSubmitting(true);
    setSubmitError(undefined);
    try {
      const result = await api.createConversation({
        cluster_id: scope.clusterId,
        question: question.trim(),
        resource: resourcePath.trim() || undefined,
        persist_investigation: true,
      });
      navigate(`/conversations/${result.conversation.id}`);
    } catch {
      setSubmitError(
        "无法创建只读会话。请确认 diagnose scope 与集群范围。",
      );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="page">
      <PageHeader
        eyebrow="ASK ROCKETMQ SRE"
        title="Ask SRE"
        description="把运维问题转换为集群范围内的只读调查；AI 只能请求证据和生成诊断，不能触发副作用。"
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <ClusterScopeSelect
              clusters={scope.clusters}
              value={scope.clusterId}
              onChange={scope.setClusterId}
            />
          </>
        }
      />
      <ReadOnlyBoundary />
      <div className="ask-layout">
        <DataSurface
          title="提出运维问题"
          description="不要粘贴 token、密码、TLS 材料或消息正文。"
          className="ask-composer"
        >
          <form onSubmit={(event) => void submit(event)}>
            <label className="form-field">
              <span>问题</span>
              <textarea
                maxLength={8192}
                onChange={(event) => setQuestion(event.target.value)}
                placeholder="例如：orders Topic 的 order-worker 为什么在过去 30 分钟持续积压？"
                required
                rows={5}
                value={question}
              />
            </label>
            <label className="form-field">
              <span>资源范围（可选）</span>
              <input
                className="text-input"
                onChange={(event) => setResourcePath(event.target.value)}
                placeholder="consumer-groups/order-worker"
                value={resourcePath}
              />
            </label>
            {submitError && (
              <div className="inline-alert warning">{submitError}</div>
            )}
            <div className="composer-footer">
              <span>{question.length}/8192 · persist investigation</span>
              <Button
                disabled={
                  submitting || !scope.clusterId || !question.trim()
                }
                type="submit"
              >
                <Send size={15} />
                {submitting ? "正在建立调查…" : "开始只读调查"}
              </Button>
            </div>
          </form>
        </DataSurface>
        <DataSurface
          title="实时进度"
          description="SSE 不可用时自动回退为 10 秒 polling。"
          meta={<LiveTransport transport={progress.transport} />}
        >
          {progress.events.length === 0 ? (
            <div className="state-message">等待 scoped workflow event…</div>
          ) : (
            <ol className="compact-activity">
              {progress.events.map((event) => (
                <li
                  key={`${event.event_id ?? event.correlation_id}-${event.occurred_at}`}
                >
                  <CircleDot size={13} />
                  <div>
                    <strong>{workflowEventSummary(event)}</strong>
                    <small>
                      {event.event_type} · {formatTime(event.occurred_at)}
                    </small>
                  </div>
                </li>
              ))}
            </ol>
          )}
        </DataSurface>
      </div>
      <PartialNotice envelope={resource.data} />
      <DataSurface
        title="最近会话"
        description="会话与调查按 tenant/cluster 隔离。"
        meta={<span>{resource.data?.items.length ?? 0} 条</span>}
      >
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && (resource.data?.items.length ?? 0) === 0}
          onRetry={resource.reload}
          emptyTitle="尚无 SRE 会话"
          emptyDescription="提交上方问题后，会话与调查进度会出现在这里。"
        />
        {resource.data && resource.data.items.length > 0 && (
          <div className="table-scroll">
            <table className="phase1-table">
              <thead>
                <tr>
                  <th>问题</th>
                  <th>资源范围</th>
                  <th>状态</th>
                  <th>调查</th>
                  <th>更新时间</th>
                  <th aria-label="详情" />
                </tr>
              </thead>
              <tbody>
                {resource.data.items.map((view) => (
                  <tr key={view.conversation.id}>
                    <td>
                      <Link
                        className="table-link"
                        to={`/conversations/${view.conversation.id}`}
                      >
                        {view.conversation.question}
                      </Link>
                      <small>{view.conversation.created_by.display_name}</small>
                    </td>
                    <td>{view.conversation.resource ?? "cluster"}</td>
                    <td>
                      <Badge variant="info">{view.conversation.status}</Badge>
                    </td>
                    <td>
                      {view.investigation
                        ? investigationLabels[view.investigation.status]
                        : "未持久化"}
                    </td>
                    <td>{formatTime(view.conversation.updated_at)}</td>
                    <td>
                      <Button asChild size="icon" variant="ghost">
                        <Link
                          aria-label="查看会话"
                          to={`/conversations/${view.conversation.id}`}
                        >
                          <ChevronRight size={15} />
                        </Link>
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </DataSurface>
    </div>
  );
}

export function ConversationDetailPage() {
  const { conversationId = "" } = useParams();
  const { api } = useSreData();
  const loadConversation = useCallback(
    (signal: AbortSignal) => api.getConversation(conversationId, signal),
    [api, conversationId],
  );
  const loadTurns = useCallback(
    (signal: AbortSignal) => api.listConversationTurns(conversationId, signal),
    [api, conversationId],
  );
  const conversation = useAsyncResource(loadConversation);
  const turns = useAsyncResource(loadTurns);
  const [question, setQuestion] = useState("");
  const [resourcePath, setResourcePath] = useState("");
  const [windowSeconds, setWindowSeconds] = useState(900);
  const [submitting, setSubmitting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [submitError, setSubmitError] = useState<string>();
  const [streamEvents, setStreamEvents] = useState<ConversationStreamEvent[]>([]);
  const [provisionalAnswer, setProvisionalAnswer] = useState("");
  const streamAbort = useRef<AbortController>();

  useEffect(() => () => streamAbort.current?.abort(), []);

  useEffect(() => {
    if (!conversation.data || question || resourcePath) {
      return;
    }
    setQuestion(conversation.data.conversation.question);
    setResourcePath(conversation.data.conversation.resource ?? "");
  }, [conversation.data, question, resourcePath]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!question.trim() || submitting) {
      return;
    }
    setSubmitting(true);
    setSubmitError(undefined);
    setStreamEvents([]);
    setProvisionalAnswer("");
    const controller = new AbortController();
    streamAbort.current = controller;
    try {
      await api.streamConversationTurn(
        conversationId,
        {
          question: question.trim(),
          resource: resourcePath.trim() || undefined,
          window_seconds: windowSeconds,
        },
        (streamEvent) => {
          setStreamEvents((current) => [...current, streamEvent].slice(-16));
          if (streamEvent.event_type === "answer_delta") {
            setProvisionalAnswer((current) => current + (streamEvent.delta ?? ""));
          } else if (streamEvent.event_type === "preview_reset") {
            setProvisionalAnswer("");
          } else if (streamEvent.final_turn) {
            setProvisionalAnswer("");
          }
        },
        controller.signal,
      );
      setQuestion("");
      turns.reload();
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        return;
      }
      setSubmitError(
        "只读查询未完成。请检查 Connector、数据源范围和模型配置。",
      );
    } finally {
      if (streamAbort.current === controller) {
        streamAbort.current = undefined;
      }
      setSubmitting(false);
    }
  };

  const cancel = async () => {
    setCancelling(true);
    setSubmitError(undefined);
    streamAbort.current?.abort();
    try {
      await api.cancelConversationQuery(conversationId);
    } catch {
      setSubmitError("无法确认取消状态；服务端查询仍受固定超时约束。");
    } finally {
      setCancelling(false);
    }
  };

  return (
    <div className="page conversation-page">
      <Button asChild className="back-link" variant="ghost">
        <Link to="/ask">
          <ArrowLeft size={15} />
          返回 Ask SRE
        </Link>
      </Button>
      <PageHeader
        eyebrow="CONVERSATION"
        title="对话式指标诊断"
        description="自然语言只会映射到已注册的只读工具；回答必须引用真实 Evidence。"
        actions={
          <Badge variant="success">
            <ShieldCheck size={13} />
            read-only
          </Badge>
        }
      />
      <ReadOnlyBoundary />
      <DataState
        loading={conversation.loading}
        error={conversation.error}
        empty={!conversation.loading && !conversation.data}
        onRetry={conversation.reload}
      />
      {conversation.data && (
        <div className="conversation-workspace">
          <div className="conversation-main">
            <DataSurface
              title="询问当前集群"
              description="支持集群概览、Topic、Consumer Lag、Broker runtime 和白名单指标。"
              className="conversation-composer"
              meta={
                submitting ? (
                  <Badge variant="info">
                    <Activity size={13} />
                    正在采集证据
                  </Badge>
                ) : (
                  <span>数据源 15 秒超时 · 模型阶段可取消</span>
                )
              }
            >
              <form onSubmit={(event) => void submit(event)}>
                <label className="form-field">
                  <span>运维问题</span>
                  <textarea
                    disabled={submitting}
                    maxLength={8192}
                    onChange={(event) => setQuestion(event.target.value)}
                    placeholder="例如：当前 rocketmq_broker_up 是否异常？"
                    required
                    rows={4}
                    value={question}
                  />
                </label>
                <div className="conversation-query-options">
                  <label className="form-field">
                    <span>资源范围（可选）</span>
                    <input
                      className="text-input"
                      disabled={submitting}
                      onChange={(event) => setResourcePath(event.target.value)}
                      placeholder="consumer-groups/order-worker/lag/orders 或 metrics/instant/rocketmq_broker_up"
                      value={resourcePath}
                    />
                  </label>
                  <label className="form-field">
                    <span>查询窗口</span>
                    <select
                      className="native-select"
                      disabled={submitting}
                      onChange={(event) => setWindowSeconds(Number(event.target.value))}
                      value={windowSeconds}
                    >
                      <option value={300}>5 分钟</option>
                      <option value={900}>15 分钟</option>
                      <option value={1800}>30 分钟</option>
                      <option value={3600}>1 小时</option>
                    </select>
                  </label>
                </div>
                {submitError && (
                  <div className="inline-alert warning" role="alert">
                    {submitError}
                  </div>
                )}
                <div className="composer-footer">
                  <span>{question.length}/8192 · 禁止任意 PromQL 和集群变更</span>
                  <div className="conversation-actions">
                    {submitting && (
                      <Button
                        disabled={cancelling}
                        onClick={() => void cancel()}
                        type="button"
                        variant="outline"
                      >
                        <Square size={13} />
                        {cancelling ? "正在取消…" : "取消查询"}
                      </Button>
                    )}
                    <Button disabled={submitting || !question.trim()} type="submit">
                      <Send size={15} />
                      {submitting ? "正在查询…" : "运行只读查询"}
                    </Button>
                  </div>
                </div>
              </form>
            </DataSurface>

            {submitting && streamEvents.length > 0 && (
              <DataSurface
                title="Live diagnostic progress"
                description="Provisional text is held behind local schema, citation, and sensitive-data checks. Only the completed revision is authoritative."
                className="conversation-live-progress"
                meta={
                  <Badge variant="info">
                    <Activity size={13} />
                    {streamEventLabel(streamEvents.at(-1)?.event_type)}
                  </Badge>
                }
              >
                <div className="conversation-progress-rail" aria-label="Diagnostic progress">
                  {streamEvents
                    .filter((item) => item.event_type !== "answer_delta")
                    .map((item) => (
                      <span className="conversation-progress-step" key={item.sequence}>
                        <CircleDot size={12} />
                        {streamEventLabel(item.event_type)}
                      </span>
                    ))}
                </div>
                <div className="conversation-provisional-answer" aria-live="polite">
                  <div>
                    <Bot size={16} />
                    <strong>AI SRE provisional answer</strong>
                    <Badge variant="outline">not persisted</Badge>
                  </div>
                  <p>
                    {provisionalAnswer ||
                      "Collecting bounded Evidence and evaluating the selected diagnostic pack…"}
                  </p>
                </div>
              </DataSurface>
            )}

            <DataSurface
              title="对话记录"
              description="每个回答均保留查询意图、correlation ID、模型模式和 Evidence 引用。"
              className="conversation-history"
              meta={<span>{turns.data?.items.length ?? 0} 轮</span>}
            >
              <DataState
                loading={turns.loading}
                error={turns.error}
                empty={!turns.loading && (turns.data?.items.length ?? 0) === 0}
                onRetry={turns.reload}
                emptyTitle="尚未运行只读查询"
                emptyDescription="提交上方问题后，指标证据和 AI 回答会显示在这里。"
              />
              {turns.data && turns.data.items.length > 0 && (
                <ol className="conversation-turn-list" aria-live="polite">
                  {turns.data.items.map(
                    ({ turn, answer, diagnosis_revision }) => (
                    <li className="conversation-turn" key={turn.id}>
                      <div className="conversation-question-row">
                        <MessageSquareText size={16} />
                        <div>
                          <strong>{turn.question}</strong>
                          <span>
                            #{turn.sequence} · {formatTime(turn.created_at)} · {turn.correlation_id}
                          </span>
                        </div>
                        <ConversationTurnBadge status={turn.status} />
                      </div>
                      {turn.query_intent && (
                        <div className="conversation-intent">
                          <code>{turn.query_intent.kind}</code>
                          <span>{turn.query_intent.source}</span>
                          <span>{turn.query_intent.resource}</span>
                          <span>{turn.query_intent.window_seconds}s</span>
                        </div>
                      )}
                      {answer && (
                        <div className="conversation-answer">
                          <div className="conversation-answer-heading">
                            <Bot size={17} />
                            <strong>AI SRE</strong>
                            <Badge variant={answer.mode === "model_assisted" ? "info" : "outline"}>
                              {answer.mode}
                            </Badge>
                            {answer.partial && <Badge variant="warning">partial</Badge>}
                            {diagnosis_revision && (
                              <Badge
                                variant={
                                  diagnosis_revision.status === "fault"
                                    ? "destructive"
                                    : diagnosis_revision.status === "healthy"
                                      ? "success"
                                      : "warning"
                                }
                              >
                                {diagnosis_revision.pack_id}@
                                {diagnosis_revision.pack_version} · {diagnosis_revision.status}
                              </Badge>
                            )}
                          </div>
                          <p>{answer.answer}</p>
                          {answer.citations.length > 0 && (
                            <div className="conversation-citations">
                              {answer.citations.map((citation) => (
                                <article key={citation.evidence_id}>
                                  <SearchCheck size={14} />
                                  <div>
                                    <strong>{citation.source}</strong>
                                    <span>{citation.resource}</span>
                                    <code>{citation.evidence_id}</code>
                                    <code>{citation.content_hash}</code>
                                  </div>
                                  <small>
                                    {formatTime(citation.observed_at)} · freshness {citation.freshness_seconds}s · {citation.partial ? "partial" : "complete"}
                                  </small>
                                </article>
                              ))}
                            </div>
                          )}
                          {diagnosis_revision && (
                            <div className="conversation-diagnosis-provenance">
                              <span>
                                Diagnosis revision #{diagnosis_revision.revision}
                              </span>
                              <code>{diagnosis_revision.id}</code>
                              <span>
                                execution_eligible=
                                {String(diagnosis_revision.execution_eligible)}
                              </span>
                              <span>
                                Evidence {diagnosis_revision.evidence_ids.length}
                              </span>
                            </div>
                          )}
                          {answer.warnings.length > 0 && (
                            <div className="conversation-warnings">
                              {answer.warnings.join(" · ")}
                            </div>
                          )}
                        </div>
                      )}
                    </li>
                    ),
                  )}
                </ol>
              )}
            </DataSurface>
          </div>

          <aside className="conversation-context">
          <DataSurface
            title="会话范围"
            description="创建时的集群和资源范围不可由模型扩大。"
          >
            <blockquote className="question-block">
              <MessageSquareText size={18} />
              <p>{conversation.data.conversation.question}</p>
            </blockquote>
            <DefinitionGrid
              items={[
                {
                  label: "状态",
                  value: conversation.data.conversation.status,
                },
                {
                  label: "资源",
                  value: conversation.data.conversation.resource ?? "cluster",
                },
                {
                  label: "发起人",
                  value:
                    conversation.data.conversation.created_by.display_name ??
                    conversation.data.conversation.created_by.subject,
                },
                {
                  label: "创建时间",
                  value: formatTime(conversation.data.conversation.created_at),
                },
              ]}
            />
          </DataSurface>
          <DataSurface
            title="持久调查"
            description="Evidence 缺失会进入 needs_evidence，不会由模型猜测。"
          >
            {conversation.data.investigation ? (
              <div className="linked-record">
                <SearchCheck size={20} />
                <div>
                  <strong>{conversation.data.investigation.title}</strong>
                  <span>
                    {
                      investigationLabels[
                        conversation.data.investigation.status
                      ]
                    }
                  </span>
                </div>
                <Button asChild variant="outline">
                  <Link
                    to={`/investigations/${conversation.data.investigation.id}`}
                  >
                    查看调查
                  </Link>
                </Button>
              </div>
            ) : (
              <div className="state-message">此会话未持久化为调查。</div>
            )}
          </DataSurface>
          <DataSurface
            title="安全边界"
            description="Evidence 内容视为不可信输入。"
          >
            <ul className="conversation-safety-list">
              <li><ShieldCheck size={14} />固定只读工具注册表</li>
              <li><ShieldCheck size={14} />指标名称和时间窗口白名单</li>
              <li><ShieldCheck size={14} />未知证据引用直接拒绝</li>
              <li><ShieldCheck size={14} />不支持 Apply、Delete、Reset</li>
            </ul>
          </DataSurface>
          </aside>
        </div>
      )}
    </div>
  );
}

function ConversationTurnBadge({ status }: { status: ConversationTurnStatus }) {
  const variant =
    status === "answered"
      ? "success"
      : status === "collecting"
        ? "info"
        : status === "needs_scope" || status === "needs_evidence"
          ? "warning"
          : status === "failed"
            ? "destructive"
            : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function streamEventLabel(eventType?: ConversationStreamEvent["event_type"]): string {
  switch (eventType) {
    case "accepted":
      return "Accepted";
    case "evidence_ready":
      return "Evidence ready";
    case "diagnosis_ready":
      return "Diagnosis ready";
    case "answer_delta":
      return "Answer streaming";
    case "preview_reset":
      return "Preview reset";
    case "completed":
      return "Revision committed";
    case "cancelled":
      return "Cancelled";
    case "failed":
      return "Failed safely";
    default:
      return "Starting";
  }
}

export function InvestigationDetailPage() {
  const { investigationId = "" } = useParams();
  const { api } = useSreData();
  const navigate = useNavigate();
  const [promotionTitle, setPromotionTitle] = useState("");
  const [promotionReason, setPromotionReason] = useState(
    "证据与影响范围已确认，需要进入 Incident 持续跟踪。",
  );
  const [promoting, setPromoting] = useState(false);
  const [promotionError, setPromotionError] = useState<string>();
  const load = useCallback(
    (signal: AbortSignal) =>
      api.getInvestigation(investigationId, signal),
    [api, investigationId],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(
    resource.data?.investigation.cluster_id ?? "",
    resource.reload,
  );
  const promote = async (event: FormEvent) => {
    event.preventDefault();
    if (!promotionReason.trim()) {
      return;
    }
    setPromoting(true);
    setPromotionError(undefined);
    try {
      const incident = await api.promoteInvestigation(investigationId, {
        title: promotionTitle.trim() || undefined,
        reason: promotionReason.trim(),
      });
      navigate(`/incidents/${incident.incident.id}`);
    } catch {
      setPromotionError(
        "调查升级失败；工作流状态未改变，也没有修改 RocketMQ 资源。",
      );
    } finally {
      setPromoting(false);
    }
  };

  return (
    <div className="page">
      <PageHeader
        eyebrow="INVESTIGATION"
        title={resource.data?.investigation.title ?? "调查详情"}
        description="展示只读采集、规则诊断、缺失证据与 Incident 关联。"
        actions={<LiveTransport transport={progress.transport} />}
      />
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <>
          <section className="summary-strip phase1-summary">
            <Summary
              label="状态"
              value={
                investigationLabels[resource.data.investigation.status]
              }
            />
            <Summary
              label="症状族"
              value={resource.data.investigation.symptom_family}
            />
            <Summary
              label="资源范围"
              value={resource.data.investigation.resource ?? "cluster"}
            />
            <Summary
              label="Fingerprint"
              value={resource.data.investigation.fingerprint}
            />
            <Summary
              label="Diagnosis revisions"
              value={String(resource.data.diagnosis_revisions.length)}
            />
          </section>
          <DataSurface
            title="Conversation diagnosis revisions"
            description="Each immutable revision binds the conversation turn, answer, diagnostic pack, and Evidence. Conversation revisions never grant execution authority."
          >
            {resource.data.diagnosis_revisions.length === 0 ? (
              <div className="empty-state compact">
                <h3>No diagnostic revision yet</h3>
                <p>Run a bounded read-only query from the linked conversation.</p>
              </div>
            ) : (
              <div className="diagnosis-list investigation-diagnosis-list">
                {resource.data.diagnosis_revisions.map((revision) => (
                  <article key={revision.id}>
                    <header>
                      <div>
                        <strong>
                          Revision {revision.revision} · {revision.pack_id}@
                          {revision.pack_version}
                        </strong>
                        <span>{formatTime(revision.created_at)}</span>
                      </div>
                      <Badge
                        variant={
                          revision.status === "fault"
                            ? "destructive"
                            : revision.status === "healthy"
                              ? "success"
                              : "warning"
                        }
                      >
                        {revision.status}
                      </Badge>
                    </header>
                    <DefinitionGrid
                      items={[
                        {
                          label: "Conversation turn",
                          value: revision.turn_id,
                          mono: true,
                        },
                        {
                          label: "Answer revision",
                          value: revision.answer_revision_id,
                          mono: true,
                        },
                        {
                          label: "Evidence",
                          value: revision.evidence_ids.length,
                        },
                        {
                          label: "Execution eligible",
                          value: String(revision.execution_eligible),
                        },
                      ]}
                    />
                    <footer>
                      <code>{revision.correlation_id}</code>
                      {revision.partial && <Badge variant="warning">partial</Badge>}
                      {revision.conversation_id && (
                        <Button asChild size="sm" variant="outline">
                          <Link to={`/conversations/${revision.conversation_id}`}>
                            Open conversation
                          </Link>
                        </Button>
                      )}
                    </footer>
                  </article>
                ))}
              </div>
            )}
          </DataSurface>
          <div className="phase1-two-column detail-balance">
            <DataSurface
              title="调查时间线"
              description="所有事件按 correlation id 归并。"
            >
              <Timeline events={resource.data.timeline} />
            </DataSurface>
            <DataSurface
              title="关联状态"
              description="升级 Incident 只改变 SRE 工作流，不修改 RocketMQ。"
            >
              <DefinitionGrid
                items={[
                  {
                    label: "Conversation",
                    value:
                      resource.data.investigation.conversation_id ??
                      "无",
                    mono: true,
                  },
                  {
                    label: "Incident",
                    value: resource.data.investigation.incident_id ? (
                      <Link
                        className="table-link"
                        to={`/incidents/${resource.data.investigation.incident_id}`}
                      >
                        查看关联 Incident
                      </Link>
                    ) : (
                      "尚未升级"
                    ),
                  },
                  {
                    label: "最近更新",
                    value: formatTime(
                      resource.data.investigation.updated_at,
                    ),
                  },
                  {
                    label: "变更能力",
                    value: "disabled",
                  },
                ]}
              />
              {!resource.data.investigation.incident_id &&
                !["promoted", "closed"].includes(
                  resource.data.investigation.status,
                ) && (
                  <form
                    className="workflow-action-form"
                    onSubmit={(event) => void promote(event)}
                  >
                    <div>
                      <strong>升级为 Incident</strong>
                      <span>
                        仅升级 SRE 工作流，用于持续诊断与人工跟踪。
                      </span>
                    </div>
                    <label className="form-field">
                      <span>Incident 标题（可选）</span>
                      <input
                        className="text-input"
                        maxLength={512}
                        onChange={(event) =>
                          setPromotionTitle(event.target.value)
                        }
                        placeholder={resource.data.investigation.title}
                        value={promotionTitle}
                      />
                    </label>
                    <label className="form-field">
                      <span>升级原因</span>
                      <textarea
                        maxLength={2048}
                        onChange={(event) =>
                          setPromotionReason(event.target.value)
                        }
                        required
                        rows={3}
                        value={promotionReason}
                      />
                    </label>
                    {promotionError && (
                      <div className="inline-alert warning">
                        {promotionError}
                      </div>
                    )}
                    <Button
                      disabled={promoting || !promotionReason.trim()}
                      type="submit"
                    >
                      <ShieldCheck size={14} />
                      {promoting ? "升级中…" : "确认升级 Incident"}
                    </Button>
                  </form>
                )}
            </DataSurface>
          </div>
        </>
      )}
    </div>
  );
}

export function IncidentsPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const [severity, setSeverity] = useState("all");
  const [status, setStatus] = useState("all");
  const [owner, setOwner] = useState("all");
  const [query, setQuery] = useState("");
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listIncidents(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(scope.clusterId, resource.reload);
  const incidents = useMemo(
    () =>
      filterAndSortIncidents(resource.data?.items ?? [], {
        severity,
        status,
        owner,
        query,
      }),
    [owner, query, resource.data?.items, severity, status],
  );
  const owners = useMemo(
    () => incidentOwnerOptions(resource.data?.items ?? []),
    [resource.data?.items],
  );
  const now = useMemo(
    () => new Date(resource.data?.observed_at ?? Date.now()),
    [resource.data?.observed_at],
  );

  return (
    <div className="page">
      <PageHeader
        eyebrow="INCIDENT WORKSPACE"
        title="Incident"
        description="按状态跟踪已确认的运维事件、诊断 revision 和证据缺口。"
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <ClusterScopeSelect
              clusters={scope.clusters}
              value={scope.clusterId}
              onChange={scope.setClusterId}
            />
          </>
        }
      />
      <ReadOnlyBoundary compact />
      <PartialNotice envelope={resource.data} />
      <DataSurface
        title="事件列表"
        description="严重度优先、最近更新次之；终态事件不可重新进入运行态。"
        meta={
          <span>
            {incidents.length} / {resource.data?.items.length ?? 0} 个
          </span>
        }
      >
        <IncidentInboxFilters
          severity={severity}
          status={status}
          owner={owner}
          query={query}
          owners={owners}
          onSeverityChange={setSeverity}
          onStatusChange={setStatus}
          onOwnerChange={setOwner}
          onQueryChange={setQuery}
        />
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && (resource.data?.items.length ?? 0) === 0}
          onRetry={resource.reload}
          emptyTitle="当前没有 Incident"
          emptyDescription="可从 Ask SRE 调查或 Inspection recommendation 升级为 Incident。"
        />
        {resource.data &&
          resource.data.items.length > 0 &&
          incidents.length === 0 && (
            <div className="empty-state compact">
              <h3>没有匹配当前筛选的 Incident</h3>
              <p>调整严重度、状态、Owner 或关键词后重试。</p>
            </div>
          )}
        {incidents.length > 0 && (
          <IncidentInboxCards incidents={incidents} now={now} />
        )}
      </DataSurface>
    </div>
  );
}

export function IncidentDetailPage() {
  const { incidentId = "" } = useParams();
  const { api } = useSreData();
  const [dispatch, setDispatch] = useState<string>();
  const [dispatching, setDispatching] = useState(false);
  const load = useCallback(
    (signal: AbortSignal) => api.getIncident(incidentId, signal),
    [api, incidentId],
  );
  const resource = useAsyncResource(load);
  const topologyLoad = useCallback(
    (signal: AbortSignal) =>
      api.getIncidentTopology(incidentId, signal),
    [api, incidentId],
  );
  const topology = useAsyncResource(topologyLoad);
  const reloadIncident = resource.reload;
  const reloadTopology = topology.reload;
  const reload = useCallback(() => {
    reloadIncident();
    reloadTopology();
  }, [reloadIncident, reloadTopology]);
  const progress = useWorkflowProgress(
    resource.data?.incident.cluster_id ?? "",
    reload,
  );

  const diagnose = async () => {
    setDispatching(true);
    try {
      const result = await api.diagnoseIncident(incidentId);
      setDispatch(
        `只读诊断已进入 ${result.status}；execution_eligible=${String(
          result.execution_eligible,
        )}`,
      );
    } catch {
      setDispatch("诊断服务暂不可用，没有执行任何变更。");
    } finally {
      setDispatching(false);
    }
  };

  return (
    <div className="page">
      <Button asChild className="back-link" variant="ghost">
        <Link to="/incidents">
          <ArrowLeft size={15} />
          返回 Incident
        </Link>
      </Button>
      <PageHeader
        eyebrow="INCIDENT DETAIL"
        title={resource.data?.incident.title ?? "Incident 详情"}
        description="诊断 revision、假设、反证和时间线均绑定稳定 Evidence 引用。"
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <Button
              disabled={dispatching || !resource.data}
              onClick={() => void diagnose()}
              variant="outline"
            >
              <Bot size={15} />
              {dispatching ? "排队中…" : "运行只读诊断"}
            </Button>
            <Button asChild>
              <Link to={`/incidents/${incidentId}/postmortem`}>
                <FileText size={15} />
                复盘与改进
              </Link>
            </Button>
          </>
        }
      />
      {dispatch && <div className="inline-alert warning">{dispatch}</div>}
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <>
          <section className="summary-strip phase1-summary incident-summary">
            <Summary
              label="状态"
              value={
                incidentStatusLabels[resource.data.incident.status]
              }
            />
            <Summary
              label="严重度"
              value={resource.data.incident.severity ?? "未分类"}
            />
            <Summary
              label="Revision"
              value={String(resource.data.diagnosis_revisions.length)}
            />
            <Summary
              label="Owner"
              value={resource.data.incident.owner ?? "未分派"}
            />
            <Summary
              label="合并告警"
              value={String(resource.data.incident.occurrence_count)}
            />
            <Summary
              label="执行资格"
              value={String(
                resource.data.diagnosis_revisions.at(-1)
                  ?.execution_eligible ?? false,
              )}
              safe={Boolean(
                resource.data.diagnosis_revisions.at(-1)
                  ?.execution_eligible,
              )}
            />
          </section>
          <SupervisedDiagnosisPanel
            incident={resource.data.incident}
            onChanged={resource.reload}
            revisions={resource.data.diagnosis_revisions}
          />
          <div className="phase1-two-column detail-balance">
            <DataSurface
              title="诊断 Revision"
              description="Rules-only revision 永远不可执行。"
            >
              <DiagnosisRevisionList
                revisions={resource.data.diagnosis_revisions}
              />
            </DataSurface>
            <DataSurface
              title="Incident 时间线"
              description="SSE 与持久事件读取使用相同 cluster scope。"
            >
              <Timeline events={resource.data.timeline} />
            </DataSurface>
          </div>
          <DataSurface
            title="Incident 拓扑"
            description="显示受影响资源、组件和集群关系；节点与边均有界且保留 partial 状态。"
            meta={
              topology.data ? (
                <span>
                  {topology.data.nodes.length} 节点 ·{" "}
                  {topology.data.edges.length} 关系
                </span>
              ) : undefined
            }
          >
            <IncidentTopology topology={topology} />
          </DataSurface>
        </>
      )}
    </div>
  );
}

export function InspectionsPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const [template, setTemplate] =
    useState<InspectionTemplate>("cluster_health");
  const [schedule, setSchedule] = useState("");
  const [creating, setCreating] = useState(false);
  const [createMessage, setCreateMessage] = useState<string>();
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listInspections(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(scope.clusterId, resource.reload);
  const recommendations = useMemo(
    () =>
      resource.data?.items.flatMap((view) => view.recommendations) ?? [],
    [resource.data?.items],
  );

  const create = async () => {
    if (!scope.clusterId) {
      return;
    }
    setCreating(true);
    setCreateMessage(undefined);
    try {
      const view = await api.createInspection({
        cluster_id: scope.clusterId,
        template,
        schedule: schedule || undefined,
      });
      setCreateMessage(
        schedule
          ? `检查计划已创建：${view.run.id.slice(0, 8)} · ${schedule}`
          : `只读检查已完成：${view.run.id.slice(0, 8)} · ${view.run.status}`,
      );
      resource.reload();
    } catch {
      setCreateMessage("检查创建失败；没有修改 RocketMQ 资源。");
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="page">
      <PageHeader
        eyebrow="READ-ONLY INSPECTIONS"
        title="检查与建议"
        description="运行有界只读检查，并把发现转成可分派、可升级的建议；不提供自动修复。"
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <ClusterScopeSelect
              clusters={scope.clusters}
              value={scope.clusterId}
              onChange={scope.setClusterId}
            />
          </>
        }
      />
      <ReadOnlyBoundary compact />
      <div className="inspection-toolbar">
        <label>
          <span>检查模板</span>
          <select
            className="native-select"
            onChange={(event) =>
              setTemplate(event.target.value as InspectionTemplate)
            }
            value={template}
          >
            {Object.entries(templateLabels).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>运行方式</span>
          <select
            className="native-select"
            onChange={(event) => setSchedule(event.target.value)}
            value={schedule}
          >
            <option value="">立即运行</option>
            <option value="@hourly">每小时</option>
            <option value="@daily">每天</option>
            <option value="@weekly">每周</option>
            <option value="every 15m">每 15 分钟</option>
          </select>
        </label>
        <Button
          disabled={creating || !scope.clusterId}
          onClick={() => void create()}
        >
          {schedule ? (
            <CalendarClock size={14} />
          ) : (
            <Play size={14} />
          )}
          {creating
            ? "创建中…"
            : schedule
              ? "创建检查计划"
              : "运行只读检查"}
        </Button>
        {createMessage && <span>{createMessage}</span>}
      </div>
      <PartialNotice envelope={resource.data} />
      <div className="phase1-two-column detail-balance">
        <DataSurface
          title="检查运行"
          description="运行状态通过 SSE 更新，断线自动轮询。"
          meta={<span>{resource.data?.items.length ?? 0} 次</span>}
        >
          <DataState
            loading={resource.loading}
            error={resource.error}
            empty={
              !resource.loading && (resource.data?.items.length ?? 0) === 0
            }
            onRetry={resource.reload}
            emptyTitle="尚无检查运行"
          />
          {resource.data && resource.data.items.length > 0 && (
            <div className="inspection-list">
              {resource.data.items.map((view) => (
                <Link
                  key={view.run.id}
                  to={`/inspections/${view.run.id}`}
                >
                  <Activity size={16} />
                  <div>
                    <strong>{templateLabels[view.run.template]}</strong>
                    <small>
                      {formatTime(view.run.created_at)} ·{" "}
                      {view.run.finding_count} findings
                      {view.run.schedule
                        ? ` · ${view.run.schedule}`
                        : " · 单次"}
                    </small>
                  </div>
                  <Badge
                    variant={
                      view.run.status === "completed"
                        ? "success"
                        : view.run.status === "failed"
                          ? "destructive"
                          : "info"
                    }
                  >
                    {view.run.status}
                  </Badge>
                </Link>
              ))}
            </div>
          )}
        </DataSurface>
        <DataSurface
          title="Recommendation"
          description="建议只描述证据与下一步人工调查，不含执行入口。"
          meta={<span>{recommendations.length} 条</span>}
        >
          {recommendations.length === 0 ? (
            <div className="state-message">当前没有检查建议。</div>
          ) : (
            <div className="recommendation-list">
              {recommendations.map((recommendation) => (
                <article key={recommendation.id}>
                  <header>
                    <Badge
                      variant={
                        recommendation.severity === "critical"
                          ? "destructive"
                          : recommendation.severity === "warning"
                            ? "warning"
                            : "info"
                      }
                    >
                      {recommendation.severity}
                    </Badge>
                    <Badge variant="outline">
                      {recommendation.status}
                    </Badge>
                  </header>
                  <h3>{recommendation.title}</h3>
                  <p>{recommendation.rationale}</p>
                  <footer>
                    <span>
                      {recommendation.evidence_ids.length} Evidence
                    </span>
                    <span>
                      {recommendation.assignee ?? "尚未分派"}
                    </span>
                  </footer>
                  <RecommendationActions
                    recommendation={recommendation}
                    onChanged={resource.reload}
                  />
                </article>
              ))}
            </div>
          )}
        </DataSurface>
      </div>
    </div>
  );
}

export function InspectionDetailPage() {
  const { inspectionId = "" } = useParams();
  const { api } = useSreData();
  const [operation, setOperation] = useState<string>();
  const [running, setRunning] = useState(false);
  const [reportLoading, setReportLoading] = useState<
    "markdown" | "html"
  >();
  const [report, setReport] = useState<InspectionReport>();
  const load = useCallback(
    (signal: AbortSignal) => api.getInspection(inspectionId, signal),
    [api, inspectionId],
  );
  const resource = useAsyncResource(load);
  const progress = useWorkflowProgress(
    resource.data?.run.cluster_id ?? "",
    resource.reload,
  );
  const run = async () => {
    setRunning(true);
    setOperation(undefined);
    try {
      await api.runInspection(inspectionId);
      setOperation("计划检查已完成一次只读运行。");
      resource.reload();
    } catch {
      setOperation("检查运行失败；没有执行任何 RocketMQ 变更。");
    } finally {
      setRunning(false);
    }
  };
  const downloadReport = async (format: "markdown" | "html") => {
    setReportLoading(format);
    setOperation(undefined);
    try {
      const next = await api.getInspectionReport(
        inspectionId,
        format,
      );
      setReport(next);
      const url = URL.createObjectURL(
        new Blob([next.content], { type: next.media_type }),
      );
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = next.file_name;
      document.body.append(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
      setOperation(`${next.file_name} 已生成并下载。`);
    } catch {
      setOperation("报告生成失败；检查结果仍保持只读可查询。");
    } finally {
      setReportLoading(undefined);
    }
  };

  return (
    <div className="page">
      <Button asChild className="back-link" variant="ghost">
        <Link to="/inspections">
          <ArrowLeft size={15} />
          返回检查列表
        </Link>
      </Button>
      <PageHeader
        eyebrow="INSPECTION DETAIL"
        title={
          resource.data
            ? `${templateLabels[resource.data.run.template]}检查`
            : "检查详情"
        }
        description="检查结果保留 partial、时间范围和关联 Evidence。"
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            {resource.data?.run.status === "scheduled" && (
              <Button
                disabled={running}
                onClick={() => void run()}
                variant="outline"
              >
                <Play size={14} />
                {running ? "运行中…" : "立即运行一次"}
              </Button>
            )}
            <Button
              disabled={!resource.data || reportLoading !== undefined}
              onClick={() => void downloadReport("markdown")}
              variant="outline"
            >
              <FileText size={14} />
              Markdown
            </Button>
            <Button
              disabled={!resource.data || reportLoading !== undefined}
              onClick={() => void downloadReport("html")}
              variant="outline"
            >
              <Download size={14} />
              HTML
            </Button>
          </>
        }
      />
      {operation && <div className="inline-alert warning">{operation}</div>}
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <>
          <div className="phase1-two-column">
            <DataSurface
              title="运行摘要"
              description="检查不会请求 Admin mutation capability。"
            >
              <DefinitionGrid
                items={[
                  {
                    label: "状态",
                    value: resource.data.run.status,
                  },
                  {
                    label: "模板",
                    value: templateLabels[resource.data.run.template],
                  },
                  {
                    label: "计划",
                    value: resource.data.run.schedule ?? "单次运行",
                  },
                  {
                    label: "发现",
                    value: resource.data.run.finding_count,
                  },
                  {
                    label: "完整性",
                    value: resource.data.run.partial
                      ? "partial"
                      : "complete",
                  },
                  {
                    label: "开始",
                    value: formatTime(resource.data.run.started_at),
                  },
                  {
                    label: "完成",
                    value: formatTime(resource.data.run.completed_at),
                  },
                  {
                    label: "Pack diff",
                    value: `${resource.data.pack_diffs?.length ?? 0} 项`,
                  },
                ]}
              />
            </DataSurface>
            <DataSurface
              title="建议"
              description="只有人工工作流状态，不包含 Apply 或自动变更。"
            >
              {resource.data.recommendations.length === 0 ? (
                <div className="state-message">检查尚未产生建议。</div>
              ) : (
                <div className="recommendation-list compact">
                  {resource.data.recommendations.map((recommendation) => (
                    <article key={recommendation.id}>
                      <header>
                        <Badge variant="outline">
                          {recommendation.status}
                        </Badge>
                      </header>
                      <h3>{recommendation.title}</h3>
                      <p>{recommendation.rationale}</p>
                      <footer>
                        <ShieldCheck size={13} />
                        execution_supported=false
                      </footer>
                      <RecommendationActions
                        recommendation={recommendation}
                        onChanged={resource.reload}
                      />
                    </article>
                  ))}
                </div>
              )}
            </DataSurface>
          </div>
          {((resource.data.pack_diffs?.length ?? 0) > 0 || report) && (
            <div className="phase1-two-column">
              <DataSurface
                title="相邻检查变化"
                description="仅展示 DiagnosticPack 的有界结构化 diff。"
              >
                {(resource.data.pack_diffs?.length ?? 0) === 0 ? (
                  <div className="state-message">暂无历史 Pack diff。</div>
                ) : (
                  <div className="pack-diff-list">
                    {resource.data.pack_diffs?.map((item) => (
                      <article key={`${item.pack_id}-${item.pack_version}`}>
                        <strong>{item.pack_id}</strong>
                        <span>{item.pack_version}</span>
                        <pre>{boundedJson(item.diff)}</pre>
                      </article>
                    ))}
                  </div>
                )}
              </DataSurface>
              <DataSurface
                title="报告预览"
                description="HTML 仅作为文件下载，预览始终按纯文本显示。"
                meta={
                  report ? <Badge variant="outline">{report.media_type}</Badge> : null
                }
              >
                {report ? (
                  <pre className="report-preview">{report.content}</pre>
                ) : (
                  <div className="state-message">
                    选择 Markdown 或 HTML 生成报告。
                  </div>
                )}
              </DataSurface>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function RecommendationActions({
  recommendation,
  onChanged,
}: {
  recommendation: Recommendation;
  onChanged: () => void;
}) {
  const { api } = useSreData();
  const [status, setStatus] =
    useState<Exclude<RecommendationStatus, "open">>("acknowledged");
  const [assignee, setAssignee] = useState("");
  const [promoteTo, setPromoteTo] = useState<
    "investigation" | "incident"
  >("incident");
  const [reason, setReason] = useState("已完成人工复核并记录处置结果。");
  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState<string>();
  const terminal = ["dismissed", "resolved", "promoted"].includes(
    recommendation.status,
  );
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setMessage(undefined);
    try {
      const updated = await api.dispositionRecommendation(
        recommendation.id,
        {
          status,
          assignee:
            status === "assigned" ? assignee.trim() || undefined : undefined,
          reason: reason.trim(),
          promote_to: status === "promoted" ? promoteTo : undefined,
        },
      );
      setMessage(`处置已更新为 ${updated.status}。`);
      onChanged();
    } catch {
      setMessage("处置更新失败；建议状态保持不变。");
    } finally {
      setSubmitting(false);
    }
  };

  if (terminal) {
    return (
      <div className="recommendation-terminal">
        <ShieldCheck size={13} />
        终态处置 · {recommendation.status}
        {recommendation.incident_id && (
          <Link to={`/incidents/${recommendation.incident_id}`}>
            查看 Incident
          </Link>
        )}
        {recommendation.investigation_id &&
          !recommendation.incident_id && (
            <Link
              to={`/investigations/${recommendation.investigation_id}`}
            >
              查看调查
            </Link>
          )}
      </div>
    );
  }

  return (
    <form
      className="recommendation-actions"
      onSubmit={(event) => void submit(event)}
    >
      <select
        aria-label="建议处置"
        className="native-select"
        onChange={(event) =>
          setStatus(
            event.target.value as Exclude<
              RecommendationStatus,
              "open"
            >,
          )
        }
        value={status}
      >
        <option value="acknowledged">确认</option>
        <option value="assigned">分派</option>
        <option value="dismissed">忽略</option>
        <option value="resolved">解决</option>
        <option value="promoted">升级</option>
      </select>
      {status === "assigned" && (
        <input
          aria-label="分派对象"
          className="text-input"
          maxLength={256}
          onChange={(event) => setAssignee(event.target.value)}
          placeholder="assignee subject"
          required
          value={assignee}
        />
      )}
      {status === "promoted" && (
        <select
          aria-label="升级目标"
          className="native-select"
          onChange={(event) =>
            setPromoteTo(
              event.target.value as "investigation" | "incident",
            )
          }
          value={promoteTo}
        >
          <option value="incident">升级到 Incident</option>
          <option value="investigation">升级到调查</option>
        </select>
      )}
      <input
        aria-label="处置原因"
        className="text-input recommendation-reason"
        maxLength={2048}
        onChange={(event) => setReason(event.target.value)}
        placeholder="处置原因"
        required
        value={reason}
      />
      <Button
        disabled={
          submitting ||
          !reason.trim() ||
          (status === "assigned" && !assignee.trim())
        }
        size="sm"
        type="submit"
        variant="outline"
      >
        {submitting ? "保存中…" : "保存处置"}
      </Button>
      {message && <span className="recommendation-message">{message}</span>}
    </form>
  );
}

function workflowEventSummary(event: WorkflowStreamEvent) {
  const status =
    typeof event.payload.status === "string"
      ? ` · ${event.payload.status}`
      : "";
  return `${event.event_type}${status}`;
}

function boundedJson(value: unknown) {
  const serialized = JSON.stringify(value, null, 2) ?? String(value);
  return serialized.length > 4_000
    ? `${serialized.slice(0, 4_000)}\n… truncated`
    : serialized;
}

function Summary({
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
