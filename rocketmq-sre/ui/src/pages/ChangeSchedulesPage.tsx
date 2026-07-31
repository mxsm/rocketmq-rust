import {
  AlertTriangle,
  ArrowRight,
  CalendarClock,
  Route,
  ShieldCheck,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { createChangeManagementApi } from "@/api/changeManagementClient";
import type {
  ChangeSchedule,
  ChangeScheduleStatus,
  RunbookDefinition,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSreData } from "@/data/SreDataContext";
import {
  ChangeClusterSelect,
  ChangeStatePanel,
  ChangeWorkspaceNav,
  ScheduleStatusBadge,
} from "@/features/change-management/ChangeWorkspace";
import { ScheduleComposer } from "@/features/change-management/ScheduleComposer";
import { formatChangeTimestamp } from "@/features/change-management/changePresentation";
import { createMockChangeManagementApi } from "@/data/phase3ChangeDemo";

const statuses: ChangeScheduleStatus[] = [
  "scheduled",
  "running",
  "awaiting_manual_gate",
  "paused",
  "safe_stopping",
  "reconciling",
  "completed",
  "cancelled",
  "rejected",
];

export function ChangeSchedulesPage() {
  const auth = useAuth();
  const { clusters, demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [clusterId, setClusterId] = useState("");
  const [status, setStatus] = useState<ChangeScheduleStatus | "all">("all");
  const [schedules, setSchedules] = useState<ChangeSchedule[]>([]);
  const [runbooks, setRunbooks] = useState<RunbookDefinition[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    if (!clusterId && clusters[0]) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  useEffect(() => {
    if (!api || !clusterId) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void Promise.all([
      api.listSchedules(
        clusterId,
        status === "all" ? undefined : status,
        256,
        controller.signal,
      ),
      api.listRunbooks(clusterId, 256, controller.signal),
    ])
      .then(([schedulePage, runbookPage]) => {
        setSchedules(schedulePage.items);
        setRunbooks(runbookPage.items);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "变更排程暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, clusterId, refresh, status]);

  const canCreate = auth.session?.roles.includes("operator") ?? false;

  return (
    <div className="page change-page">
      <PageHeader
        eyebrow="P3-11 · CONTROLLED SCHEDULING"
        title="变更排程与冲突预演"
        description="每个动作步骤必须绑定同集群、未过期且已批准的不可变 Plan。排程器只负责编排，实际写入仍经过 Executor 与 Agent。"
        actions={
          <ChangeClusterSelect
            clusters={clusters}
            value={clusterId}
            onValueChange={setClusterId}
          />
        }
      />
      <ChangeWorkspaceNav />

      <section className="schedule-safety-banner">
        <ShieldCheck aria-hidden="true" size={18} />
        <div>
          <strong>排程不会绕过审批链</strong>
          <span>
            Plan hash、前置条件摘要、维护窗口、资源冲突与并发上限在创建时再次校验。
          </span>
        </div>
        <code>PLAN → APPROVAL → EXECUTOR → AGENT</code>
      </section>

      <div className="schedule-workspace">
        <section className="schedule-list-panel">
          <header>
            <div>
              <span className="section-kicker">SCHEDULE QUEUE</span>
              <h2>排程队列</h2>
            </div>
            <Select
              value={status}
              onValueChange={(value) =>
                setStatus(value as ChangeScheduleStatus | "all")
              }
            >
              <SelectTrigger aria-label="排程状态筛选">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部状态</SelectItem>
                {statuses.map((item) => (
                  <SelectItem key={item} value={item}>
                    {scheduleFilterLabel(item)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </header>

          {loading && schedules.length === 0 ? (
            <ChangeStatePanel state="loading" title="正在加载排程" />
          ) : error && schedules.length === 0 ? (
            <ChangeStatePanel
              state="error"
              title="排程加载失败"
              detail={error}
            />
          ) : schedules.length === 0 ? (
            <ChangeStatePanel
              state="empty"
              title="当前筛选条件没有排程"
              detail="在右侧选择 Runbook 并绑定已批准 Plan 后进行预演。"
            />
          ) : (
            <div className="schedule-list">
              {schedules.map((schedule) => (
                <ScheduleRow key={schedule.id} schedule={schedule} />
              ))}
            </div>
          )}
        </section>

        <ScheduleComposer
          canCreate={canCreate}
          clusterId={clusterId}
          demoMode={demoMode}
          onCreated={() => setRefresh((value) => value + 1)}
          runbooks={runbooks}
        />
      </div>
    </div>
  );
}

function ScheduleRow({ schedule }: { schedule: ChangeSchedule }) {
  return (
    <Link
      className="schedule-row"
      to={`/changes/schedules/${encodeURIComponent(schedule.id)}`}
    >
      <span className="schedule-row-icon">
        <Route aria-hidden="true" size={17} />
      </span>
      <div className="schedule-row-primary">
        <header>
          <strong>
            {schedule.runbook_id} · {schedule.runbook_version}
          </strong>
          <ScheduleStatusBadge status={schedule.status} />
        </header>
        <span>
          {schedule.resource_keys.join(" · ")} · 创建人 {schedule.created_by}
        </span>
      </div>
      <div className="schedule-row-time">
        <CalendarClock aria-hidden="true" size={14} />
        <span>{formatChangeTimestamp(schedule.scheduled_start)}</span>
        <small>至 {formatChangeTimestamp(schedule.scheduled_end)}</small>
      </div>
      <div className="schedule-row-progress">
        <span>下一步</span>
        <strong>{schedule.next_step_sequence}</strong>
        {schedule.waiting_manual_gate && (
          <Badge variant="warning">
            <AlertTriangle size={12} /> 人工门
          </Badge>
        )}
      </div>
      <ArrowRight aria-hidden="true" size={16} />
    </Link>
  );
}

function scheduleFilterLabel(status: ChangeScheduleStatus) {
  return {
    scheduled: "已排程",
    running: "执行中",
    awaiting_manual_gate: "等待人工门",
    paused: "已暂停",
    safe_stopping: "安全停止中",
    reconciling: "对账中",
    completed: "已完成",
    cancelled: "已取消",
    rejected: "已拒绝",
  }[status];
}
