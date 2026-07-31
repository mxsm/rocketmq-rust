import {
  ArrowLeft,
  BookCheck,
  CheckCircle2,
  ClipboardList,
  FileClock,
  Save,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import type {
  ActionItem,
  ActionItemStatus,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  ClusterScopeSelect,
  DataState,
  DataSurface,
  DefinitionGrid,
  formatTime,
} from "@/components/Phase1Primitives";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";

const statusLabels: Record<string, string> = {
  draft: "AI 草稿",
  in_review: "人工编辑中",
  confirmed: "已人工确认",
  published: "已发布知识",
  archived: "已归档",
};

const actionStatusLabels: Record<ActionItemStatus, string> = {
  open: "待处理",
  assigned: "已分派",
  in_progress: "进行中",
  blocked: "受阻",
  completed: "已完成",
  reopened: "重新打开",
  cancelled: "已取消",
};

export function IncidentPostmortemPage() {
  const { incidentId = "" } = useParams();
  const { api } = useSreData();
  const navigate = useNavigate();
  const [creating, setCreating] = useState(false);
  const [message, setMessage] = useState<string>();
  const load = useCallback(
    (signal: AbortSignal) => api.getIncident(incidentId, signal),
    [api, incidentId],
  );
  const resource = useAsyncResource(load);

  const create = async () => {
    setCreating(true);
    setMessage(undefined);
    try {
      const postmortem = await api.createPostmortem(incidentId, {
        operator_notes: [],
      });
      navigate(`/postmortems/${postmortem.postmortem.id}`);
    } catch {
      setMessage("复盘草稿生成失败；没有执行任何 RocketMQ 变更。");
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="page">
      <Button asChild className="back-link" variant="ghost">
        <Link to={`/incidents/${incidentId}`}>
          <ArrowLeft size={15} />
          返回 Incident
        </Link>
      </Button>
      <PageHeader
        eyebrow="POSTMORTEM"
        title="生成证据化复盘草稿"
        description="AI 只整理 Incident、Evidence、诊断 Revision 和时间线；草稿必须经人工确认才能发布为知识。"
      />
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <div className="phase1-two-column detail-balance">
          <DataSurface
            title={resource.data.incident.title}
            description="确定性输入将在生成前进行边界和敏感字段校验。"
          >
            <DefinitionGrid
              items={[
                {
                  label: "Incident",
                  value: resource.data.incident.id,
                  mono: true,
                },
                {
                  label: "状态",
                  value: resource.data.incident.status,
                },
                {
                  label: "Evidence",
                  value: String(
                    new Set(
                      resource.data.diagnosis_revisions.flatMap(
                        (revision) => revision.evidence_ids,
                      ),
                    ).size,
                  ),
                },
                {
                  label: "诊断 Revision",
                  value: String(
                    resource.data.diagnosis_revisions.length,
                  ),
                },
              ]}
            />
          </DataSurface>
          <DataSurface
            title="生成边界"
            description="固定 Schema、证据引用 fail closed、模型失败自动回退到确定性草稿。"
          >
            <ReadOnlyBoundary compact />
            <Button
              disabled={creating}
              onClick={() => void create()}
              type="button"
            >
              <Sparkles size={15} />
              {creating ? "正在生成…" : "生成或打开复盘草稿"}
            </Button>
            {message && <div className="inline-alert warning">{message}</div>}
          </DataSurface>
        </div>
      )}
    </div>
  );
}

export function PostmortemDetailPage() {
  const { postmortemId = "" } = useParams();
  const { api } = useSreData();
  const [summary, setSummary] = useState("");
  const [recovery, setRecovery] = useState("");
  const [owner, setOwner] = useState("rocketmq-sre");
  const [component, setComponent] = useState("cluster");
  const [message, setMessage] = useState<string>();
  const [saving, setSaving] = useState(false);
  const load = useCallback(
    (signal: AbortSignal) =>
      api.getPostmortem(postmortemId, signal),
    [api, postmortemId],
  );
  const resource = useAsyncResource(load);
  const latest = resource.data?.revisions.at(-1);

  useEffect(() => {
    if (latest) {
      setSummary(latest.summary);
      setRecovery(latest.recovery);
    }
  }, [latest]);

  const appendRevision = async (humanConfirmed: boolean) => {
    setSaving(true);
    setMessage(undefined);
    try {
      await api.patchPostmortem(postmortemId, {
        summary,
        recovery,
        human_confirmed: humanConfirmed,
      });
      setMessage(
        humanConfirmed
          ? "已追加人工确认 Revision；现在可以发布为已验证知识。"
          : "已追加新的人工编辑 Revision，旧 Revision 保持可查询。",
      );
      resource.reload();
    } catch {
      setMessage("Revision 保存失败；请检查 Evidence 引用和字段长度。");
    } finally {
      setSaving(false);
    }
  };

  const publish = async () => {
    setSaving(true);
    setMessage(undefined);
    try {
      const reviewDue = new Date();
      reviewDue.setDate(reviewDue.getDate() + 90);
      await api.publishPostmortem(postmortemId, {
        human_confirmed: true,
        owner,
        component,
        rocketmq_version_range: "*",
        review_due_at: reviewDue.toISOString(),
      });
      setMessage("复盘已由人工发布，并创建 validated KnowledgeItem。");
      resource.reload();
    } catch {
      setMessage("只有当前人工确认的 Revision 才能发布。");
    } finally {
      setSaving(false);
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
        eyebrow="POSTMORTEM WORKSPACE"
        title={latest?.summary ?? "复盘工作区"}
        description="AI 草稿、人工 Revision、Action Item、复发关联和知识发布位于独立 SRE 元数据边界。"
        actions={
          resource.data ? (
            <Badge
              variant={
                resource.data.postmortem.status === "published"
                  ? "success"
                  : "warning"
              }
            >
              {statusLabels[resource.data.postmortem.status]}
            </Badge>
          ) : undefined
        }
      />
      {message && <div className="inline-alert warning">{message}</div>}
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && latest && (
        <>
          <section className="summary-strip phase1-summary incident-summary">
            <SummaryCard
              label="当前 Revision"
              value={String(resource.data.postmortem.current_revision)}
            />
            <SummaryCard
              label="状态"
              value={statusLabels[resource.data.postmortem.status]}
            />
            <SummaryCard
              label="Evidence 引用"
              value={String(latest.evidence_ids.length)}
            />
            <SummaryCard
              label="Action Items"
              value={String(resource.data.action_items.length)}
            />
            <SummaryCard
              label="复发关联"
              value={String(resource.data.recurrences.length)}
            />
            <SummaryCard
              label="执行日志"
              value={
                resource.data.execution_journal_empty ? "empty" : "blocked"
              }
              safe
            />
          </section>

          <div className="phase1-two-column detail-balance">
            <DataSurface
              title="人工编辑与确认"
              description="每次保存都会追加不可变 Revision；不会覆盖历史。"
            >
              <div className="phase1-form postmortem-editor">
                <label className="form-field">
                  <span>摘要</span>
                  <textarea
                    onChange={(event) => setSummary(event.target.value)}
                    rows={5}
                    value={summary}
                  />
                </label>
                <label className="form-field">
                  <span>恢复过程</span>
                  <textarea
                    onChange={(event) => setRecovery(event.target.value)}
                    rows={5}
                    value={recovery}
                  />
                </label>
                <div className="postmortem-actions">
                  <Button
                    disabled={saving}
                    onClick={() => void appendRevision(false)}
                    type="button"
                    variant="outline"
                  >
                    <Save size={15} />
                    保存 Revision
                  </Button>
                  <Button
                    disabled={saving}
                    onClick={() => void appendRevision(true)}
                    type="button"
                  >
                    <CheckCircle2 size={15} />
                    人工确认
                  </Button>
                </div>
              </div>
            </DataSurface>

            <DataSurface
              title="发布为已验证知识"
              description="只允许人工确认后发布；到期扫描只生成 Todo，不改写知识。"
            >
              <div className="phase1-form postmortem-editor">
                <label className="form-field">
                  <span>知识 Owner</span>
                  <input
                    onChange={(event) => setOwner(event.target.value)}
                    value={owner}
                  />
                </label>
                <label className="form-field">
                  <span>组件</span>
                  <input
                    onChange={(event) => setComponent(event.target.value)}
                    value={component}
                  />
                </label>
                <Button
                  disabled={
                    saving ||
                    resource.data.postmortem.status !== "confirmed"
                  }
                  onClick={() => void publish()}
                  type="button"
                >
                  <BookCheck size={15} />
                  人工发布
                </Button>
                {resource.data.knowledge_item && (
                  <div className="knowledge-published">
                    <ShieldCheck size={16} />
                    <span>
                      validated ·{" "}
                      {resource.data.knowledge_item.source_version}
                    </span>
                  </div>
                )}
              </div>
            </DataSurface>
          </div>

          <div className="phase1-two-column detail-balance">
            <DataSurface
              title="根因与关键结论"
              description="每条重要结论都必须引用 Incident 范围内的 Evidence。"
            >
              <ConclusionList
                conclusions={[
                  ...latest.root_causes,
                  ...latest.contributing_factors,
                  ...latest.conclusions,
                ]}
              />
            </DataSurface>
            <DataSurface
              title="Action Items"
              description="Owner、到期日、验证文本与 Evidence 独立跟踪。"
              meta={
                <Button asChild size="sm" variant="outline">
                  <Link to="/action-items">
                    <ClipboardList size={14} />
                    打开工作台
                  </Link>
                </Button>
              }
            >
              <ActionSummary items={resource.data.action_items} />
            </DataSurface>
          </div>

          <DataSurface
            title="不可变 Revision 历史"
            description="旧 Revision 始终可查询；模型调用只保存引用和用量元数据。"
          >
            <div className="revision-history">
              {resource.data.revisions
                .slice()
                .reverse()
                .map((revision) => (
                  <article key={revision.id}>
                    <FileClock size={16} />
                    <div>
                      <strong>Revision {revision.revision}</strong>
                      <span>{revision.summary}</span>
                      <small>
                        {revision.edited_by} ·{" "}
                        {formatTime(revision.created_at)} ·{" "}
                        {revision.human_confirmed
                          ? "human confirmed"
                          : "draft"}
                      </small>
                    </div>
                  </article>
                ))}
            </div>
          </DataSurface>
        </>
      )}
    </div>
  );
}

export function ActionItemsPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listActionItems(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);

  return (
    <div className="page">
      <PageHeader
        eyebrow="CONTINUOUS IMPROVEMENT"
        title="Action Items"
        description="分派、开始、阻塞、完成和重新打开均记录为 SRE 元数据事件；完成必须提供验证或 Evidence。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            onChange={scope.setClusterId}
            value={scope.clusterId}
          />
        }
      />
      <ReadOnlyBoundary />
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={!resource.loading && (resource.data?.items.length ?? 0) === 0}
        onRetry={resource.reload}
        emptyTitle="当前没有 Action Item"
        emptyDescription="从 Incident 复盘生成 Action Item 后会显示在这里。"
      />
      <div className="action-item-grid">
        {resource.data?.items.map((item) => (
          <ActionItemEditor
            api={api}
            item={item}
            key={item.id}
            onSaved={resource.reload}
          />
        ))}
      </div>
    </div>
  );
}

function ActionItemEditor({
  api,
  item,
  onSaved,
}: {
  api: ReturnType<typeof useSreData>["api"];
  item: ActionItem;
  onSaved: () => void;
}) {
  const [status, setStatus] = useState<ActionItemStatus>(item.status);
  const [owner, setOwner] = useState(item.owner ?? "");
  const [verification, setVerification] = useState(
    item.verification ?? "",
  );
  const [message, setMessage] = useState<string>();
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    setMessage(undefined);
    try {
      await api.patchActionItem(item.id, {
        status,
        owner: owner || undefined,
        verification: verification || undefined,
        evidence_ids: item.evidence_ids,
      });
      setMessage("已记录 Action Item 状态事件。");
      onSaved();
    } catch {
      setMessage("状态转换不合法，或完成项缺少验证/Evidence。");
    } finally {
      setSaving(false);
    }
  };

  return (
    <article className="action-item-card">
      <div className="action-item-heading">
        <div>
          <small>{item.id.slice(0, 8)}</small>
          <h2>{item.title}</h2>
        </div>
        <Badge
          variant={item.status === "completed" ? "success" : "warning"}
        >
          {actionStatusLabels[item.status]}
        </Badge>
      </div>
      <div className="action-item-form">
        <label className="form-field">
          <span>状态</span>
          <select
            onChange={(event) =>
              setStatus(event.target.value as ActionItemStatus)
            }
            value={status}
          >
            {Object.entries(actionStatusLabels).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label className="form-field">
          <span>Owner</span>
          <input
            onChange={(event) => setOwner(event.target.value)}
            placeholder="sre@example.com"
            value={owner}
          />
        </label>
        <label className="form-field action-verification">
          <span>验证说明</span>
          <input
            onChange={(event) => setVerification(event.target.value)}
            placeholder="完成时填写验证结果，或保留 Evidence 引用"
            value={verification}
          />
        </label>
      </div>
      <div className="action-item-footer">
        <span>
          {item.evidence_ids.length} Evidence · due{" "}
          {formatTime(item.due_at)}
        </span>
        <Button disabled={saving} onClick={() => void save()} size="sm">
          <Save size={14} />
          保存
        </Button>
      </div>
      {message && <small className="action-item-message">{message}</small>}
    </article>
  );
}

function ConclusionList({
  conclusions,
}: {
  conclusions: Array<{
    code: string;
    statement: string;
    evidence_ids: string[];
  }>;
}) {
  if (conclusions.length === 0) {
    return <div className="state-message">暂无已验证结论。</div>;
  }
  return (
    <div className="conclusion-list">
      {conclusions.map((conclusion) => (
        <article key={`${conclusion.code}:${conclusion.statement}`}>
          <strong>{conclusion.code}</strong>
          <p>{conclusion.statement}</p>
          <div>
            {conclusion.evidence_ids.map((id) => (
              <code key={id}>{id.slice(0, 8)}</code>
            ))}
          </div>
        </article>
      ))}
    </div>
  );
}

function ActionSummary({ items }: { items: ActionItem[] }) {
  if (items.length === 0) {
    return <div className="state-message">当前 Revision 未生成 Action Item。</div>;
  }
  return (
    <div className="action-summary">
      {items.map((item) => (
        <article key={item.id}>
          <CheckCircle2 size={15} />
          <span>
            <strong>{item.title}</strong>
            <small>
              {actionStatusLabels[item.status]} ·{" "}
              {item.owner ?? "未分派"}
            </small>
          </span>
        </article>
      ))}
    </div>
  );
}

function SummaryCard({
  label,
  value,
  safe = false,
}: {
  label: string;
  value: string;
  safe?: boolean;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong className={safe ? "safe-value" : undefined}>{value}</strong>
    </div>
  );
}
