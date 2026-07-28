import type {
  ChangeConflict,
  ChangeScheduleStatus,
  ChangeWindowKind,
  RunbookDefinition,
} from "@/api/types";

export type ChangeTone =
  | "success"
  | "warning"
  | "destructive"
  | "secondary"
  | "info";

const scheduleLabels: Record<ChangeScheduleStatus, string> = {
  scheduled: "已排程",
  running: "执行中",
  awaiting_manual_gate: "等待人工门",
  paused: "已暂停",
  safe_stopping: "安全停止中",
  reconciling: "对账中",
  completed: "已完成",
  cancelled: "已取消",
  rejected: "已拒绝",
};

const scheduleTones: Record<ChangeScheduleStatus, ChangeTone> = {
  scheduled: "info",
  running: "warning",
  awaiting_manual_gate: "warning",
  paused: "secondary",
  safe_stopping: "warning",
  reconciling: "warning",
  completed: "success",
  cancelled: "secondary",
  rejected: "destructive",
};

const windowLabels: Record<ChangeWindowKind, string> = {
  maintenance: "维护窗口",
  freeze: "冻结期",
  blackout: "禁止变更",
};

const conflictLabels: Record<ChangeConflict["code"], string> = {
  outside_maintenance_window: "不在维护窗口",
  freeze_window: "命中冻结期",
  blackout_window: "命中禁止变更期",
  resource_overlap: "资源排程重叠",
  parallelism_exceeded: "并发上限冲突",
};

export function scheduleStatusLabel(status: ChangeScheduleStatus): string {
  return scheduleLabels[status];
}

export function scheduleStatusTone(status: ChangeScheduleStatus): ChangeTone {
  return scheduleTones[status];
}

export function changeWindowKindLabel(kind: ChangeWindowKind): string {
  return windowLabels[kind];
}

export function conflictCodeLabel(code: ChangeConflict["code"]): string {
  return conflictLabels[code];
}

export function formatChangeTimestamp(value?: string | null): string {
  if (!value) {
    return "—";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function toDateTimeLocal(value: Date): string {
  const offset = value.getTimezoneOffset() * 60_000;
  return new Date(value.getTime() - offset).toISOString().slice(0, 16);
}

export function dateTimeLocalToIso(value: string): string {
  return new Date(value).toISOString();
}

export interface RunbookDiffRow {
  key: string;
  label: string;
  status: "added" | "removed" | "changed" | "unchanged";
  before: string;
  after: string;
}

export function diffRunbooks(
  before: RunbookDefinition,
  after: RunbookDefinition,
): RunbookDiffRow[] {
  const rows: RunbookDiffRow[] = [
    diffValue("risk", "组合风险", before.risk, after.risk),
    diffValue(
      "max_parallelism",
      "最大并发",
      String(before.max_parallelism),
      String(after.max_parallelism),
    ),
    diffValue("owner", "Owner", before.owner, after.owner),
  ];
  const steps = new Set([
    ...before.steps.map((step) => step.sequence),
    ...after.steps.map((step) => step.sequence),
  ]);
  for (const sequence of [...steps].sort((left, right) => left - right)) {
    const previous = before.steps.find((step) => step.sequence === sequence);
    const next = after.steps.find((step) => step.sequence === sequence);
    rows.push(
      diffValue(
        `step-${sequence}`,
        `步骤 ${sequence}`,
        previous ? summarizeStep(previous) : "",
        next ? summarizeStep(next) : "",
      ),
    );
  }
  rows.push(
    diffValue(
      "compensation_edges",
      "补偿边",
      String(before.compensation_edges.length),
      String(after.compensation_edges.length),
    ),
  );
  return rows;
}

function diffValue(
  key: string,
  label: string,
  before: string,
  after: string,
): RunbookDiffRow {
  const status =
    before === after
      ? "unchanged"
      : before.length === 0
        ? "added"
        : after.length === 0
          ? "removed"
          : "changed";
  return { key, label, status, before: before || "—", after: after || "—" };
}

function summarizeStep(step: RunbookDefinition["steps"][number]): string {
  if (step.body.kind === "manual_gate") {
    return `${step.name} · 人工门 · ${step.body.gate.required_role}`;
  }
  return `${step.name} · ${step.body.action} · ${step.body.resource}`;
}
