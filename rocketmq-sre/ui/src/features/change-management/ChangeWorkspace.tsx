import {
  AlertTriangle,
  CalendarRange,
  Cable,
  CheckCircle2,
  GitCompareArrows,
  ListTree,
  LoaderCircle,
  Rocket,
  Route,
} from "lucide-react";
import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";

import type {
  ChangeConflict,
  ChangeScheduleStatus,
  ClusterSummary,
  RunbookDefinition,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import {
  conflictCodeLabel,
  diffRunbooks,
  formatChangeTimestamp,
  scheduleStatusLabel,
  scheduleStatusTone,
} from "./changePresentation";

const workspaceLinks = [
  { to: "/changes", label: "变更中心", icon: ListTree, end: true },
  {
    to: "/changes/runbooks",
    label: "Runbook",
    icon: GitCompareArrows,
  },
  { to: "/changes/calendar", label: "变更日历", icon: CalendarRange },
  { to: "/changes/schedules", label: "排程", icon: Route },
  { to: "/changes/releases", label: "发布护航", icon: Rocket },
  { to: "/changes/integrations", label: "外部集成", icon: Cable },
];

export function ChangeWorkspaceNav() {
  return (
    <nav className="change-workspace-nav" aria-label="变更工作台">
      {workspaceLinks.map(({ to, label, icon: Icon, end }) => (
        <NavLink
          className={({ isActive }) =>
            `change-workspace-link${isActive ? " active" : ""}`
          }
          end={end}
          key={to}
          to={to}
        >
          <Icon aria-hidden="true" size={15} />
          {label}
        </NavLink>
      ))}
    </nav>
  );
}

export function ChangeClusterSelect({
  clusters,
  value,
  onValueChange,
}: {
  clusters: ClusterSummary[];
  value: string;
  onValueChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger
        aria-label="变更集群范围"
        className="cluster-select"
      >
        <SelectValue placeholder="选择授权集群" />
      </SelectTrigger>
      <SelectContent>
        {clusters.map((cluster) => (
          <SelectItem key={cluster.id} value={cluster.id}>
            {cluster.external_cluster_key} · {cluster.region}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export function ScheduleStatusBadge({
  status,
}: {
  status: ChangeScheduleStatus;
}) {
  return (
    <Badge variant={scheduleStatusTone(status)}>
      {scheduleStatusLabel(status)}
    </Badge>
  );
}

export function ChangeStatePanel({
  state,
  title,
  detail,
  action,
}: {
  state: "loading" | "error" | "empty";
  title: string;
  detail?: string;
  action?: ReactNode;
}) {
  const Icon = state === "loading" ? LoaderCircle : AlertTriangle;
  return (
    <div
      className={`state-panel${state === "error" ? " unavailable" : ""}`}
      role={state === "error" ? "alert" : "status"}
    >
      <Icon
        aria-hidden="true"
        className={state === "loading" ? "spin" : undefined}
        size={22}
      />
      <div>
        <strong>{title}</strong>
        {detail && <span>{detail}</span>}
      </div>
      {action}
    </div>
  );
}

export function RunbookDiff({
  before,
  after,
}: {
  before: RunbookDefinition;
  after: RunbookDefinition;
}) {
  const rows = diffRunbooks(before, after);
  const changes = rows.filter((row) => row.status !== "unchanged").length;
  return (
    <section className="runbook-diff-panel" aria-label="Runbook 版本差异">
      <header>
        <div>
          <span className="section-kicker">VERSION DIFF</span>
          <h2>
            {before.version} → {after.version}
          </h2>
        </div>
        <Badge variant={changes === 0 ? "secondary" : "warning"}>
          {changes} 项变化
        </Badge>
      </header>
      <div className="runbook-diff-head" aria-hidden="true">
        <span>字段</span>
        <span>{before.version}</span>
        <span>{after.version}</span>
      </div>
      <div className="runbook-diff-rows">
        {rows.map((row) => (
          <article
            className={`runbook-diff-row ${row.status}`}
            key={row.key}
          >
            <div>
              <strong>{row.label}</strong>
              <small>{diffStatusLabel(row.status)}</small>
            </div>
            <code>{row.before}</code>
            <code>{row.after}</code>
          </article>
        ))}
      </div>
    </section>
  );
}

export function ConflictPanel({
  conflicts,
  schedulable,
}: {
  conflicts: ChangeConflict[];
  schedulable: boolean;
}) {
  return (
    <section
      className={`conflict-panel ${schedulable ? "clear" : "blocked"}`}
      aria-live="polite"
    >
      <header>
        {schedulable ? (
          <CheckCircle2 aria-hidden="true" size={18} />
        ) : (
          <AlertTriangle aria-hidden="true" size={18} />
        )}
        <div>
          <strong>
            {schedulable ? "排程预演通过" : "排程存在阻断冲突"}
          </strong>
          <span>
            {schedulable
              ? "维护窗口、资源重叠与并发上限均满足。"
              : "修正以下冲突后才能创建排程。"}
          </span>
        </div>
      </header>
      {conflicts.length > 0 && (
        <div className="conflict-list">
          {conflicts.map((conflict, index) => (
            <article
              key={`${conflict.code}-${conflict.window_id ?? conflict.conflicting_schedule_id ?? index}`}
            >
              <Badge variant="destructive">
                {conflictCodeLabel(conflict.code)}
              </Badge>
              <div>
                <strong>{conflict.message}</strong>
                <span>
                  {formatChangeTimestamp(conflict.starts_at)} —{" "}
                  {formatChangeTimestamp(conflict.ends_at)}
                  {conflict.resource_key
                    ? ` · ${conflict.resource_key}`
                    : ""}
                </span>
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function diffStatusLabel(
  status: "added" | "removed" | "changed" | "unchanged",
) {
  return {
    added: "新增",
    removed: "删除",
    changed: "已变更",
    unchanged: "无变化",
  }[status];
}
