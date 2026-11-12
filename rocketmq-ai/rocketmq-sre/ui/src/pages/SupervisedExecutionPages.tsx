import {
  Activity,
  ArchiveX,
  BookOpenCheck,
  CalendarRange,
  FileCheck2,
  Fingerprint,
  Play,
  RefreshCw,
  Route,
  ShieldAlert,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";

import { createSupervisedSreApi } from "@/api/supervisedClient";
import type {
  ActionPlanView,
  AuditPage,
  ExecutionSubmissionView,
  QuarantinePage,
  ResourceQuarantine,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { resourcePath } from "@/context/workspaceScope";
import { ApprovalPanel } from "@/features/approvals/ApprovalPanel";
import { AuditTimeline } from "@/features/audit/AuditTimeline";
import { ChangeWorkspaceNav } from "@/features/change-management/ChangeWorkspace";
import { ExecutionTimeline } from "@/features/executions/ExecutionTimeline";
import { PlanDetail } from "@/features/plans/PlanDetail";
import { ObjectScopeBoundary } from "@/features/scope/ObjectScopeBoundary";
import {
  formatTimestamp,
  shortDigest,
} from "@/features/plans/planPresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";

function useSupervisedApi() {
  const auth = useAuth();
  const api = useMemo(
    () =>
      auth.requestContext
        ? createSupervisedSreApi(auth.requestContext)
        : undefined,
    [auth.requestContext],
  );
  return { auth, api };
}

export function ChangeCenterPage() {
  const navigate = useNavigate();
  const scope = useClusterScope();
  const [planId, setPlanId] = useState("");
  const [executionId, setExecutionId] = useState("");
  const [correlationId, setCorrelationId] = useState("");
  const clusterId = scope.clusterId;

  const go = (event: FormEvent, path: string) => {
    event.preventDefault();
    navigate(path);
  };

  return (
    <div className="page">
      <PageHeader
        description="从不可变计划开始，完成异构 Critic、人工审批、受围栏执行、验证回滚和审计追踪。"
        eyebrow="SUPERVISED CHANGE CONTROL"
        title="变更中心"
      />
      <ChangeWorkspaceNav />
      <section className="change-guardrail">
        <ShieldAlert size={18} />
        <div>
          <strong>所有变更都经过 Control Plane 与独立 Executor</strong>
          <p>这里没有 shell、raw RequestCode 或任意 JSON Patch；外部系统也不能绕过审批与围栏。</p>
        </div>
        <code>R1/R2 · HUMAN APPROVAL</code>
      </section>
      <section className="change-entry-grid">
        <article className="change-entry-card workspace-entry">
          <header>
            <BookOpenCheck size={18} />
            <div>
              <h2>Runbook 库</h2>
              <p>检查不可变版本、结构化步骤以及版本之间的可读差异。</p>
            </div>
          </header>
          <Button onClick={() => navigate("/changes/runbooks")}>
            打开 Runbook
          </Button>
        </article>
        <article className="change-entry-card workspace-entry">
          <header>
            <CalendarRange size={18} />
            <div>
              <h2>变更日历</h2>
              <p>维护绝对时间窗口、冻结期、黑名单和资源并发约束。</p>
            </div>
          </header>
          <Button onClick={() => navigate("/changes/calendar")}>
            打开变更日历
          </Button>
        </article>
        <article className="change-entry-card workspace-entry">
          <header>
            <Route size={18} />
            <div>
              <h2>Runbook 排程</h2>
              <p>先预演窗口与资源冲突，再创建、暂停或人工接管排程。</p>
            </div>
          </header>
          <Button onClick={() => navigate("/changes/schedules")}>
            打开排程队列
          </Button>
        </article>
        <EntryCard
          description="查看步骤、脱敏 diff、Evidence、风险、Critic 和回滚方式。"
          icon={<FileCheck2 size={18} />}
          label="打开 Plan"
          onChange={setPlanId}
          onSubmit={(event) => go(event, `/changes/plans/${encodeURIComponent(planId)}`)}
          placeholder="Action Plan UUID"
          value={planId}
        />
        <EntryCard
          description="实时查看 precheck、intent、apply、verify、rollback 与人工接管状态。"
          icon={<Play size={18} />}
          label="跟踪 Execution"
          onChange={setExecutionId}
          onSubmit={(event) =>
            go(event, `/changes/executions/${encodeURIComponent(executionId)}`)
          }
          placeholder="Execution UUID"
          value={executionId}
        />
        <EntryCard
          description="按 correlation ID 查询发起人、审批人、策略、证据和错误。"
          icon={<Fingerprint size={18} />}
          label="查询 Audit"
          onChange={setCorrelationId}
          onSubmit={(event) =>
            go(event, `/changes/audit/${encodeURIComponent(correlationId)}`)
          }
          placeholder="Correlation UUID"
          value={correlationId}
        />
        <article className="change-entry-card">
          <header>
            <ArchiveX size={18} />
            <div>
              <h2>资源隔离</h2>
              <p>查看回滚失败后的持久 quarantine，并提交 Evidence 驱动的解除请求。</p>
            </div>
          </header>
          <Button
            disabled={!clusterId}
            onClick={() =>
              navigate(
                `/changes/quarantines${clusterId ? `?cluster_id=${encodeURIComponent(clusterId)}` : ""}`,
              )
            }
          >
            打开隔离清单
          </Button>
        </article>
      </section>
    </div>
  );
}

export function PlanPage() {
  const { planId = "" } = useParams();
  const navigate = useNavigate();
  const { auth, api } = useSupervisedApi();
  const [view, setView] = useState<ActionPlanView>();
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [busy, setBusy] = useState(false);
  const pending = useRef<AbortController | undefined>(undefined);
  useEffect(() => () => pending.current?.abort(), []);

  const load = useCallback(async () => {
    if (!api || !planId) return;
    setError(undefined);
    try {
      setView(await api.getPlan(planId));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "计划加载失败");
    }
  }, [api, planId]);

  useEffect(() => {
    void load();
  }, [load]);

  const mutate = async (operation: (signal: AbortSignal) => Promise<void>) => {
    if (busy) return;
    const controller = new AbortController();
    pending.current = controller;
    setBusy(true);
    setMessage(undefined);
    try {
      await operation(controller.signal);
      if (controller.signal.aborted) return;
      await load();
    } catch (reason) {
      if (controller.signal.aborted) return;
      setMessage(reason instanceof Error ? reason.message : "请求失败");
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  if (!view) {
    return <LoadState title="计划详情" error={error} onRetry={() => void load()} />;
  }

  const roles = auth.session?.roles ?? [];
  const canApprove =
    roles.includes("approver") || roles.includes("rocketmq:approve");
  const canExecute =
    roles.includes("operator") || roles.includes("rocketmq:operate");

  return (
    <ObjectScopeBoundary object={view.plan}>
    <div className="page">
      <PageHeader
        actions={
          <Button onClick={() => void load()} size="sm" variant="outline">
            <RefreshCw size={14} />
            刷新
          </Button>
        }
        description={`Plan ${shortDigest(view.plan.id)} · 绑定 Incident ${shortDigest(view.plan.incident_id)}`}
        eyebrow="PLAN / APPROVAL"
        title="变更计划详情"
      />
      <div className="plan-approval-layout">
        <main>
          <PlanDetail view={view} />
        </main>
        <ApprovalPanel
          busy={busy}
          canApprove={canApprove}
          canExecute={canExecute}
          message={message}
          onCriticReview={() =>
            mutate(async (signal) => {
              await api?.reviewPlanWithCritic(planId, {
                plan_hash: view.plan.plan_hash,
              }, signal);
              if (signal.aborted) return;
              setMessage("Critic review 已刷新");
            })
          }
          onDecision={(decision, request) =>
            mutate(async (signal) => {
              if (decision === "approve") {
                await api?.approvePlan(planId, request, signal);
              } else {
                await api?.rejectPlan(planId, request, signal);
              }
              if (signal.aborted) return;
              setMessage(decision === "approve" ? "计划已批准" : "计划已拒绝");
            })
          }
          onExecute={(preconditionHash) =>
            mutate(async (signal) => {
              const result = await api?.submitExecution({
                plan_id: view.plan.id,
                plan_hash: view.plan.plan_hash,
                precondition_hash: preconditionHash,
                idempotency_key: crypto.randomUUID(),
              }, signal);
              if (result && !signal.aborted) {
                navigate(`/changes/executions/${result.execution.id}`);
              }
            })
          }
          view={view}
        />
      </div>
    </div>
    </ObjectScopeBoundary>
  );
}

export function ExecutionPage() {
  const { executionId = "" } = useParams();
  const { api } = useSupervisedApi();
  const [execution, setExecution] = useState<ExecutionSubmissionView>();
  const [error, setError] = useState<string>();

  const load = useCallback(async () => {
    if (!api || !executionId) return;
    try {
      setExecution(await api.getExecution(executionId));
      setError(undefined);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "执行加载失败");
    }
  }, [api, executionId]);

  useEffect(() => {
    void load();
  }, [load]);

  const progress = useWorkflowProgress(
    execution?.execution.cluster_id ?? "",
    load,
  );
  if (!execution) {
    return <LoadState title="执行时间线" error={error} onRetry={() => void load()} />;
  }
  return (
    <ObjectScopeBoundary object={execution.execution}>
    <div className="page">
      <PageHeader
        actions={
          <Button onClick={() => void load()} size="sm" variant="outline">
            <RefreshCw size={14} />
            刷新
          </Button>
        }
        description="围栏状态、类型化 Agent 效果与验证 Evidence 会持续汇聚到同一条执行时间线。"
        eyebrow="EXECUTION / VERIFICATION"
        title="执行与回滚"
      />
      <ExecutionTimeline
        events={progress.events}
        execution={execution}
        transport={progress.transport}
      />
    </div>
    </ObjectScopeBoundary>
  );
}

export function AuditPageView() {
  const { correlationId = "" } = useParams();
  const { api, auth } = useSupervisedApi();
  const scope = useClusterScope();
  const [searchParams] = useSearchParams();
  const [audit, setAudit] = useState<AuditPage>();
  const [error, setError] = useState<string>();

  const load = useCallback(async () => {
    if (!api || !correlationId) return;
    try {
      setAudit(await api.getAudit(correlationId));
      setError(undefined);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "审计加载失败");
    }
  }, [api, correlationId]);
  useEffect(() => {
    void load();
  }, [load]);

  if (!audit) {
    return <LoadState title="审计详情" error={error} onRetry={() => void load()} />;
  }
  const authorizedItems = audit.items.filter((item) => item.tenant_id === auth.session?.tenantId
    && auth.hasClusterScope(item.cluster_id)
    && scope.clusters.some((cluster) => cluster.id === item.cluster_id && cluster.tenant_id === item.tenant_id));
  const selectedCluster = searchParams.has("cluster_id") ? scope.clusterId : authorizedItems[0]?.cluster_id;
  const items = authorizedItems.filter((item) => item.cluster_id === selectedCluster);
  const auditScope = items[0] ?? authorizedItems[0] ?? audit.items[0];
  const filtered = items.length !== audit.items.length;
  return (
    <ObjectScopeBoundary object={auditScope}>
    <div className="page">
      <PageHeader
        description="完整展示人、策略、计划、Evidence、执行结果与人工隔离操作，不显示凭据或目标配置全文。"
        eyebrow="IMMUTABLE AUDIT"
        title="审计详情"
      />
      {filtered && <p className="inline-alert warning" role="status">仅展示当前授权租户和集群内的审计记录；其他范围的记录已过滤。</p>}
      <AuditTimeline audit={{ ...audit, items, partial: audit.partial || filtered }} />
    </div>
    </ObjectScopeBoundary>
  );
}

export function QuarantinePageView() {
  const { api, auth } = useSupervisedApi();
  const scope = useClusterScope();
  const [searchParams, setSearchParams] = useSearchParams();
  const availableClusters = scope.clusters.filter((cluster) => auth.hasClusterScope(cluster.id)
    && cluster.tenant_id === auth.session?.tenantId);
  const explicitCluster = searchParams.has("cluster_id");
  const legacyValues = searchParams.getAll("cluster");
  const legacyCluster = legacyValues[0]?.trim();
  const validLegacyCluster = legacyValues.length === 1 && legacyCluster !== undefined
    && auth.hasClusterScope(legacyCluster)
    && availableClusters.some((cluster) => cluster.id === legacyCluster);
  const invalidLegacyCluster = !explicitCluster && legacyValues.length > 0 && !validLegacyCluster;
  const clusterId = invalidLegacyCluster ? ""
    : !explicitCluster && validLegacyCluster ? legacyCluster : scope.clusterId;
  const [selection, setSelection] = useState<{ owner: typeof api; clusterId: string; value: ResourceQuarantine }>();
  const selected = selection && selection.owner === api && selection.clusterId === clusterId ? selection.value : undefined;
  const [reason, setReason] = useState("");
  const [evidence, setEvidence] = useState("");
  const [message, setMessage] = useState<string>();

  useEffect(() => {
    if (!searchParams.has("cluster") || invalidLegacyCluster) return;
    const next = new URLSearchParams(searchParams);
    next.delete("cluster");
    if (!explicitCluster && validLegacyCluster) {
      next.set("cluster_id", legacyCluster);
      next.delete("resource_kind");
      next.delete("resource_key");
    }
    setSearchParams(next, { replace: true });
  }, [explicitCluster, invalidLegacyCluster, legacyCluster, searchParams, setSearchParams, validLegacyCluster]);

  const load = useCallback((signal: AbortSignal) => {
    if (!api || !clusterId) return Promise.resolve<QuarantinePage>({
      schema_version: "rocketmq-sre.resource-quarantine-page.v1", items: [], partial: false,
    });
    return api.listQuarantines(clusterId, true, signal);
  }, [api, clusterId]);
  const resource = useAsyncResource(load);
  const requestedResource = scope.urlContext?.resourceKind !== "cluster" && scope.urlContext ? resourcePath(scope.urlContext) : undefined;
  const items = resource.data?.items.filter((item) => item.cluster_id === clusterId
    && item.tenant_id === auth.session?.tenantId
    && (!requestedResource || item.resource_key === requestedResource)) ?? [];

  useEffect(() => {
    setSelection(undefined);
    setReason("");
    setEvidence("");
    setMessage(undefined);
  }, [clusterId]);

  const clear = async () => {
    if (!api || !selected || reason.trim().length < 8) return;
    try {
      await api.clearQuarantine(selected.id, {
        reason: reason.trim(),
        evidence_ids: evidence
          .split(/[,\s]+/)
          .map((item) => item.trim())
          .filter(Boolean),
      });
      setMessage("隔离解除请求已审计并完成");
      setSelection(undefined);
      setReason("");
      setEvidence("");
      resource.reload();
    } catch (failure) {
      setMessage(failure instanceof Error ? failure.message : "隔离解除失败");
    }
  };

  return (
    <div className="page">
      <PageHeader
        actions={
          <select
            aria-label="隔离集群"
            className="native-select"
            onChange={(event) => {
              const value = event.target.value;
              const next = new URLSearchParams(searchParams);
              next.delete("cluster");
              next.set("cluster_id", value);
              next.delete("resource_kind");
              next.delete("resource_key");
              setSearchParams(next);
            }}
            value={clusterId}
          >
            <option disabled value="">选择授权集群</option>
            {availableClusters.map((cluster) => (
              <option key={cluster.id} value={cluster.id}>
                {cluster.external_cluster_key}
              </option>
            ))}
          </select>
        }
        description="Quarantine 独立于临时锁持久化；解除必须提交原因和验证 Evidence。"
        eyebrow="MANUAL TAKEOVER"
        title="资源隔离与人工接管"
      />
      {invalidLegacyCluster && <p className="inline-alert warning" role="alert">旧链接中的集群范围无效或未授权，请选择授权集群。</p>}
      {Boolean(resource.error) && <p className="inline-alert warning" role="alert">隔离清单加载失败，请刷新后重试。</p>}
      {message && <p className="inline-alert warning">{message}</p>}
      <div className="quarantine-layout">
        <section className="data-surface">
          <header className="surface-heading">
            <div>
              <h2>隔离资源</h2>
              <p>回滚失败后，即使临时 lock 已释放，新的 Plan/Execution 仍会被阻断。</p>
            </div>
            <span>{resource.loading && !resource.data ? "正在读取…" : resource.error ? "不可用" : `${items.length} items`}</span>
          </header>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>资源</th>
                  <th>原因</th>
                  <th>创建时间</th>
                  <th>状态</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr key={item.id}>
                    <td>
                      <strong>{item.resource_key}</strong>
                      <code className="table-subline">{item.action_id ?? "unknown action"}</code>
                    </td>
                    <td>{item.reason_code}</td>
                    <td>{formatTimestamp(item.created_at)}</td>
                    <td>
                      <Badge variant={item.cleared_at ? "secondary" : "destructive"}>
                        {item.cleared_at ? "已解除" : "隔离中"}
                      </Badge>
                    </td>
                    <td>
                      <Button
                        disabled={Boolean(item.cleared_at)}
                        onClick={() => {
                          setSelection({ owner: api, clusterId, value: item });
                          setReason("");
                          setEvidence("");
                          setMessage(undefined);
                        }}
                        size="sm"
                        variant="outline"
                      >
                        人工接管
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!resource.loading && !resource.error && !invalidLegacyCluster && items.length === 0 && (
            <div className="compact-empty">当前集群没有资源隔离记录。</div>
          )}
        </section>

        <aside className="data-surface quarantine-clear-panel">
          <header className="surface-heading">
            <div>
              <h2>解除隔离</h2>
              <p>仅接受明确原因和 Evidence ID，不提供命令输入框。</p>
            </div>
          </header>
          {selected ? (
            <div className="approval-body">
              <code>{selected.resource_key}</code>
              <label className="form-field">
                <span>解除原因</span>
                <textarea
                  onChange={(event) => setReason(event.target.value)}
                  placeholder="说明人工修复、验证结果与风险"
                  value={reason}
                />
              </label>
              <label className="form-field">
                <span>验证 Evidence IDs（逗号分隔）</span>
                <input
                  className="text-input"
                  onChange={(event) => setEvidence(event.target.value)}
                  value={evidence}
                />
              </label>
              <Button
                disabled={reason.trim().length < 8 || evidence.trim().length === 0}
                onClick={() => void clear()}
                variant="destructive"
              >
                确认解除隔离
              </Button>
            </div>
          ) : (
            <div className="compact-empty">选择一条未解除记录开始人工接管。</div>
          )}
        </aside>
      </div>
    </div>
  );
}

function EntryCard({
  icon,
  label,
  description,
  placeholder,
  value,
  onChange,
  onSubmit,
}: {
  icon: React.ReactNode;
  label: string;
  description: string;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
  onSubmit: (event: FormEvent) => void;
}) {
  return (
    <article className="change-entry-card">
      <header>
        {icon}
        <div>
          <h2>{label}</h2>
          <p>{description}</p>
        </div>
      </header>
      <form onSubmit={onSubmit}>
        <input
          className="text-input"
          onChange={(event) => onChange(event.target.value)}
          placeholder={placeholder}
          required
          value={value}
        />
        <Button size="sm" type="submit">
          打开
        </Button>
      </form>
    </article>
  );
}

function LoadState({
  title,
  error,
  onRetry,
}: {
  title: string;
  error?: string;
  onRetry: () => void;
}) {
  return (
    <div className="page">
      <PageHeader
        description="正在从 Control Plane 读取受 scope 保护的数据。"
        eyebrow="SUPERVISED CHANGE CONTROL"
        title={title}
      />
      <div className="state-panel">
        {error ? <ShieldAlert size={20} /> : <Activity size={20} />}
        <div>
          <strong>{error ?? "正在加载…"}</strong>
          <span>{error ? "检查 ID、角色、租户和集群 scope。" : "不会读取任何目标凭据。"}</span>
        </div>
        {error && (
          <Button onClick={onRetry} size="sm" variant="outline">
            重试
          </Button>
        )}
      </div>
    </div>
  );
}
