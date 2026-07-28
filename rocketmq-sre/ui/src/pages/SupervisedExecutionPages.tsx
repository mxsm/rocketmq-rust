import {
  Activity,
  ArchiveX,
  FileCheck2,
  Fingerprint,
  Play,
  RefreshCw,
  ShieldAlert,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
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
import { ApprovalPanel } from "@/features/approvals/ApprovalPanel";
import { AuditTimeline } from "@/features/audit/AuditTimeline";
import { ExecutionTimeline } from "@/features/executions/ExecutionTimeline";
import { PlanDetail } from "@/features/plans/PlanDetail";
import {
  formatTimestamp,
  shortDigest,
} from "@/features/plans/planPresentation";
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
  const { auth } = useSupervisedApi();
  const [planId, setPlanId] = useState("");
  const [executionId, setExecutionId] = useState("");
  const [correlationId, setCorrelationId] = useState("");
  const clusterId = auth.session?.clusterIds[0];

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
      <section className="change-guardrail">
        <ShieldAlert size={18} />
        <div>
          <strong>所有变更都经过 Control Plane 与独立 Executor</strong>
          <p>这里没有 shell、raw RequestCode 或任意 JSON Patch；外部系统也不能绕过审批与围栏。</p>
        </div>
        <code>R1/R2 · HUMAN APPROVAL</code>
      </section>
      <section className="change-entry-grid">
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
                `/changes/quarantines${clusterId ? `?cluster=${encodeURIComponent(clusterId)}` : ""}`,
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

  const mutate = async (operation: () => Promise<void>) => {
    setBusy(true);
    setMessage(undefined);
    try {
      await operation();
      await load();
    } catch (reason) {
      setMessage(reason instanceof Error ? reason.message : "请求失败");
    } finally {
      setBusy(false);
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
            mutate(async () => {
              await api?.reviewPlanWithCritic(planId, {
                plan_hash: view.plan.plan_hash,
              });
              setMessage("Critic review 已刷新");
            })
          }
          onDecision={(decision, request) =>
            mutate(async () => {
              if (decision === "approve") {
                await api?.approvePlan(planId, request);
              } else {
                await api?.rejectPlan(planId, request);
              }
              setMessage(decision === "approve" ? "计划已批准" : "计划已拒绝");
            })
          }
          onExecute={(preconditionHash) =>
            mutate(async () => {
              const result = await api?.submitExecution({
                plan_id: view.plan.id,
                plan_hash: view.plan.plan_hash,
                precondition_hash: preconditionHash,
                idempotency_key: crypto.randomUUID(),
              });
              if (result) {
                navigate(`/changes/executions/${result.execution.id}`);
              }
            })
          }
          view={view}
        />
      </div>
    </div>
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
  );
}

export function AuditPageView() {
  const { correlationId = "" } = useParams();
  const { api } = useSupervisedApi();
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
  return (
    <div className="page">
      <PageHeader
        description="完整展示人、策略、计划、Evidence、执行结果与人工隔离操作，不显示凭据或目标配置全文。"
        eyebrow="IMMUTABLE AUDIT"
        title="审计详情"
      />
      <AuditTimeline audit={audit} />
    </div>
  );
}

export function QuarantinePageView() {
  const { api, auth } = useSupervisedApi();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialCluster =
    searchParams.get("cluster") ?? auth.session?.clusterIds[0] ?? "";
  const [clusterId, setClusterId] = useState(initialCluster);
  const [page, setPage] = useState<QuarantinePage>();
  const [selected, setSelected] = useState<ResourceQuarantine>();
  const [reason, setReason] = useState("");
  const [evidence, setEvidence] = useState("");
  const [message, setMessage] = useState<string>();

  const load = useCallback(async () => {
    if (!api || !clusterId) return;
    setPage(await api.listQuarantines(clusterId, true));
  }, [api, clusterId]);
  useEffect(() => {
    void load().catch((reason) =>
      setMessage(reason instanceof Error ? reason.message : "隔离清单加载失败"),
    );
  }, [load]);

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
      setSelected(undefined);
      setReason("");
      setEvidence("");
      await load();
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
              setClusterId(value);
              setSearchParams({ cluster: value });
            }}
            value={clusterId}
          >
            {(auth.session?.clusterIds ?? []).map((id) => (
              <option key={id} value={id}>
                {shortDigest(id)}
              </option>
            ))}
          </select>
        }
        description="Quarantine 独立于临时锁持久化；解除必须提交原因和验证 Evidence。"
        eyebrow="MANUAL TAKEOVER"
        title="资源隔离与人工接管"
      />
      {message && <p className="inline-alert warning">{message}</p>}
      <div className="quarantine-layout">
        <section className="data-surface">
          <header className="surface-heading">
            <div>
              <h2>隔离资源</h2>
              <p>回滚失败后，即使临时 lock 已释放，新的 Plan/Execution 仍会被阻断。</p>
            </div>
            <span>{page?.items.length ?? 0} items</span>
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
                {(page?.items ?? []).map((item) => (
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
                        onClick={() => setSelected(item)}
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
          {(page?.items.length ?? 0) === 0 && (
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
