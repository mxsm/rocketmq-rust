import {
  ArrowLeft,
  Check,
  CircleDashed,
  Clock3,
  FileCheck2,
  Pause,
  Play,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
  Square,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { createChangeManagementApi } from "@/api/changeManagementClient";
import type {
  ChangeSchedule,
  RunbookDefinition,
  RunbookStepPlanBinding,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { createMockChangeManagementApi } from "@/data/phase3ChangeDemo";
import {
  ChangeStatePanel,
  ChangeWorkspaceNav,
  ScheduleStatusBadge,
} from "@/features/change-management/ChangeWorkspace";
import { formatChangeTimestamp } from "@/features/change-management/changePresentation";

const liveStatuses = new Set<ChangeSchedule["status"]>([
  "scheduled",
  "running",
  "safe_stopping",
  "reconciling",
]);

export function ChangeScheduleDetailPage() {
  const { scheduleId = "" } = useParams();
  const auth = useAuth();
  const { demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [schedule, setSchedule] = useState<ChangeSchedule>();
  const [runbook, setRunbook] = useState<RunbookDefinition>();
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState<string>();
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();

  const load = useCallback(async () => {
    if (!api || !scheduleId) {
      return;
    }
    try {
      const next = await api.getSchedule(scheduleId);
      const definition = await api.getRunbook(
        next.cluster_id,
        next.runbook_id,
        next.runbook_version,
      );
      setSchedule(next);
      setRunbook(definition);
      setError(undefined);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "排程详情加载失败",
      );
    }
  }, [api, scheduleId]);

  useEffect(() => {
    void load();
  }, [load]);

  const scheduleStatus = schedule?.status;

  useEffect(() => {
    if (!scheduleStatus || !liveStatuses.has(scheduleStatus)) {
      return;
    }
    const timer = window.setInterval(() => void load(), 4_000);
    return () => window.clearInterval(timer);
  }, [load, scheduleStatus]);

  const transition = async (
    operation: string,
    invoke: () => Promise<ChangeSchedule>,
  ) => {
    if (!reason.trim()) {
      setMessage("请输入不含敏感信息的操作原因。");
      return;
    }
    setBusy(operation);
    setMessage(undefined);
    try {
      const next = await invoke();
      setSchedule(next);
      setReason("");
      setMessage("状态已持久化并写入审计时间线。");
    } catch (cause) {
      setMessage(
        cause instanceof Error ? cause.message : "排程操作失败",
      );
    } finally {
      setBusy(undefined);
    }
  };

  if (!schedule || !runbook) {
    return (
      <div className="page change-page">
        <ChangeStatePanel
          state={error ? "error" : "loading"}
          title={error ? "排程详情加载失败" : "正在加载排程详情"}
          detail={error}
          action={
            error ? (
              <Button onClick={() => void load()} variant="outline">
                重试
              </Button>
            ) : undefined
          }
        />
      </div>
    );
  }

  const roles = auth.session?.roles ?? [];
  const canOperate = roles.includes("operator");
  const canApprove = roles.includes("approver");
  const isCreator = auth.session?.subject === schedule.created_by;
  const waitingStep = schedule.waiting_manual_gate;
  const request = { reason: reason.trim() };

  return (
    <div className="page change-page schedule-detail-page">
      <PageHeader
        eyebrow="P3-11 · SCHEDULE DETAIL"
        title={runbook.name}
        description={`${runbook.version} · ${schedule.resource_keys.join(" · ")}`}
        actions={
          <>
            <ScheduleStatusBadge status={schedule.status} />
            <Button asChild variant="outline">
              <Link to="/changes/schedules">
                <ArrowLeft size={15} /> 返回排程
              </Link>
            </Button>
          </>
        }
      />
      <ChangeWorkspaceNav />

      <section className="schedule-detail-summary">
        <SummaryFact label="计划开始" value={formatChangeTimestamp(schedule.scheduled_start)} />
        <SummaryFact label="计划结束" value={formatChangeTimestamp(schedule.scheduled_end)} />
        <SummaryFact label="创建人" value={schedule.created_by} />
        <SummaryFact label="下一步骤" value={String(schedule.next_step_sequence)} />
        <SummaryFact
          label="已完成"
          value={`${schedule.completed_steps.length} / ${runbook.steps.length}`}
        />
        <SummaryFact
          label="Intent"
          value={schedule.intent_persisted ? "已持久化" : "尚未持久化"}
        />
      </section>

      <div className="schedule-detail-grid">
        <section className="schedule-step-panel">
          <header>
            <div>
              <span className="section-kicker">RUNBOOK PROGRESS</span>
              <h2>类型化步骤</h2>
            </div>
            {schedule.active_execution_id && (
              <Button asChild size="sm" variant="outline">
                <Link
                  to={`/changes/executions/${encodeURIComponent(schedule.active_execution_id)}`}
                >
                  <Play size={14} /> 查看当前 Execution
                </Link>
              </Button>
            )}
          </header>
          <ol className="schedule-step-rail">
            {runbook.steps.map((step) => (
              <ScheduleStep
                binding={schedule.plan_bindings.find(
                  (item) => item.step_id === step.id,
                )}
                completed={schedule.completed_steps.includes(step.id)}
                current={step.sequence === schedule.next_step_sequence}
                key={step.id}
                step={step}
                waiting={waitingStep === step.id}
              />
            ))}
          </ol>
        </section>

        <aside className="schedule-control-panel">
          <header>
            <span className="composer-icon">
              <ShieldAlert aria-hidden="true" size={18} />
            </span>
            <div>
              <span className="section-kicker">OPERATOR CONTROL</span>
              <h2>暂停、取消与人工门</h2>
              <p>暂停只阻止下一步；Intent 后取消会进入安全停止与对账。</p>
            </div>
          </header>

          <label className="control-reason">
            <span>操作原因</span>
            <textarea
              maxLength={2048}
              onChange={(event) => setReason(event.target.value)}
              placeholder="说明证据与值班判断；不要填写 token、密码或消息正文。"
              value={reason}
            />
          </label>

          <div className="schedule-control-actions">
            {canOperate &&
              ["scheduled", "running", "awaiting_manual_gate"].includes(
                schedule.status,
              ) && (
                <Button
                  disabled={busy !== undefined}
                  onClick={() =>
                    void transition("pause", () =>
                      api!.pauseSchedule(schedule.id, request),
                    )
                  }
                  variant="outline"
                >
                  <Pause size={15} /> 暂停下一步
                </Button>
              )}
            {canOperate && schedule.status === "paused" && (
              <Button
                disabled={busy !== undefined}
                onClick={() =>
                  void transition("resume", () =>
                    api!.resumeSchedule(schedule.id, request),
                  )
                }
              >
                <Play size={15} /> 恢复排程
              </Button>
            )}
            {canOperate &&
              !["completed", "cancelled", "rejected", "safe_stopping", "reconciling"].includes(
                schedule.status,
              ) && (
                <Button
                  disabled={busy !== undefined}
                  onClick={() =>
                    void transition("cancel", () =>
                      api!.cancelSchedule(schedule.id, request),
                    )
                  }
                  variant="destructive"
                >
                  <Square size={15} /> 取消 / 安全停止
                </Button>
              )}
            {canOperate && schedule.status === "safe_stopping" && (
              <Button
                disabled={busy !== undefined}
                onClick={() =>
                  void transition("reconcile", () =>
                    api!.reconcileSchedule(schedule.id, request),
                  )
                }
                variant="outline"
              >
                <RotateCcw size={15} /> 开始人工对账
              </Button>
            )}
            <Button
              disabled={busy !== undefined}
              onClick={() => void load()}
              variant="ghost"
            >
              <RefreshCw size={15} /> 刷新
            </Button>
          </div>

          {waitingStep && (
            <section className="manual-gate-panel">
              <header>
                <FileCheck2 aria-hidden="true" size={17} />
                <div>
                  <strong>等待人工门决策</strong>
                  <span>{waitingStep}</span>
                </div>
              </header>
              {isCreator && (
                <p className="inline-alert">
                  创建排程的人不能批准自己的人工门。
                </p>
              )}
              <div>
                <Button
                  disabled={!canApprove || isCreator || busy !== undefined}
                  onClick={() =>
                    void transition("approve-gate", () =>
                      api!.approveManualGate(
                        schedule.id,
                        waitingStep,
                        request,
                      ),
                    )
                  }
                >
                  <Check size={15} /> 批准继续
                </Button>
                <Button
                  disabled={!canApprove || isCreator || busy !== undefined}
                  onClick={() =>
                    void transition("reject-gate", () =>
                      api!.rejectManualGate(
                        schedule.id,
                        waitingStep,
                        request,
                      ),
                    )
                  }
                  variant="destructive"
                >
                  <X size={15} /> 拒绝排程
                </Button>
              </div>
            </section>
          )}

          {message && <p className="form-message">{message}</p>}
          <dl className="schedule-control-metadata">
            <div>
              <dt>Correlation ID</dt>
              <dd>
                <code>{schedule.correlation_id}</code>
              </dd>
            </div>
            <div>
              <dt>更新时间</dt>
              <dd>{formatChangeTimestamp(schedule.updated_at)}</dd>
            </div>
          </dl>
        </aside>
      </div>
    </div>
  );
}

function ScheduleStep({
  step,
  binding,
  completed,
  current,
  waiting,
}: {
  step: RunbookDefinition["steps"][number];
  binding?: RunbookStepPlanBinding;
  completed: boolean;
  current: boolean;
  waiting: boolean;
}) {
  const Icon = completed
    ? Check
    : waiting
      ? Clock3
      : current
        ? Play
        : CircleDashed;
  const state = completed
    ? "completed"
    : waiting
      ? "waiting"
      : current
        ? "current"
        : "upcoming";
  return (
    <li className={state}>
      <span className="schedule-step-state">
        <Icon aria-hidden="true" size={15} />
      </span>
      <div>
        <header>
          <strong>
            {step.sequence}. {step.name}
          </strong>
          <Badge variant={completed ? "success" : waiting ? "warning" : "outline"}>
            {step.body.kind === "manual_gate" ? "人工门" : "类型化动作"}
          </Badge>
        </header>
        {step.body.kind === "action" ? (
          <>
            <code>{step.body.action}</code>
            <span>{step.body.resource}</span>
            {binding && <small>Plan {binding.plan_id}</small>}
          </>
        ) : (
          <>
            <span>{step.body.gate.instructions}</span>
            <small>需要 {step.body.gate.required_role} 角色</small>
          </>
        )}
      </div>
    </li>
  );
}

function SummaryFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
