import { ApiError } from "@/api/client";
import type { ChangeManagementApi } from "@/api/changeManagementClient";
import type {
  ChangeConflict,
  ChangeSchedule,
  ChangeWindow,
  CreateChangeScheduleRequest,
  RunbookDefinition,
} from "@/api/types";
import type { ApiRequestContext } from "@/auth/AuthContext";

const DEMO_CLUSTER_ID = "10000000-0000-4000-8000-000000000001";
const DEMO_TENANT_ID = "00000000-0000-4000-8000-000000000001";
const RUNBOOK_ID = "30000000-0000-4000-8000-000000000001";
const SCALE_STEP_ID = "31000000-0000-4000-8000-000000000001";
const GATE_STEP_ID = "31000000-0000-4000-8000-000000000002";
const RESTART_STEP_ID = "31000000-0000-4000-8000-000000000003";
const WAIT_MS = 90;

const now = Date.now();

const demoRunbooks: RunbookDefinition[] = [
  {
    schema_version: "rocketmq-sre.runbook.v1",
    id: RUNBOOK_ID,
    name: "Proxy 单副本扩容与验证",
    version: "1.0.0",
    owner: "messaging-platform",
    description: "扩容一个 Proxy 副本，经人工确认后完成流量验证。",
    risk: "r1",
    max_parallelism: 1,
    steps: [
      actionStep(
        SCALE_STEP_ID,
        1,
        "扩容一个 Proxy 副本",
        "proxy.scale_out_one.v1",
      ),
      manualGateStep([SCALE_STEP_ID]),
    ],
    compensation_edges: [],
    created_at: new Date(now - 14 * 24 * 60 * 60 * 1000).toISOString(),
  },
  {
    schema_version: "rocketmq-sre.runbook.v1",
    id: RUNBOOK_ID,
    name: "Proxy 单副本扩容与验证",
    version: "1.1.0",
    owner: "messaging-platform",
    description: "扩容一个 Proxy 副本，经人工确认后滚动重启一个旧副本。",
    risk: "r2",
    max_parallelism: 1,
    steps: [
      actionStep(
        SCALE_STEP_ID,
        1,
        "扩容一个 Proxy 副本",
        "proxy.scale_out_one.v1",
      ),
      manualGateStep([SCALE_STEP_ID]),
      actionStep(
        RESTART_STEP_ID,
        3,
        "滚动重启一个旧副本",
        "proxy.restart_one.v1",
        [GATE_STEP_ID],
      ),
    ],
    compensation_edges: [],
    created_at: new Date(now - 2 * 24 * 60 * 60 * 1000).toISOString(),
  },
];

const demoWindows: ChangeWindow[] = [
  {
    schema_version: "rocketmq-sre.change-window.v1",
    id: "32000000-0000-4000-8000-000000000001",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    name: "工作日 Proxy 维护",
    kind: "maintenance",
    timezone: "Asia/Shanghai",
    starts_at: new Date(now + 30 * 60 * 1000).toISOString(),
    ends_at: new Date(now + 6 * 60 * 60 * 1000).toISOString(),
    resource_keys: ["deployment/rocketmq/proxy"],
    max_parallelism: 1,
    reason: "扩容并验证生产 Proxy 单副本，保持容量余量。",
    created_by: "shift-lead",
    created_at: new Date(now - 24 * 60 * 60 * 1000).toISOString(),
  },
  {
    schema_version: "rocketmq-sre.change-window.v1",
    id: "32000000-0000-4000-8000-000000000002",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    name: "季度结算冻结期",
    kind: "freeze",
    timezone: "Asia/Shanghai",
    starts_at: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString(),
    ends_at: new Date(now + 3 * 24 * 60 * 60 * 1000).toISOString(),
    resource_keys: [],
    max_parallelism: 1,
    reason: "结算窗口期间仅允许紧急人工处置。",
    created_by: "change-manager",
    created_at: new Date(now - 3 * 24 * 60 * 60 * 1000).toISOString(),
  },
];

const demoSchedules: ChangeSchedule[] = [
  {
    schema_version: "rocketmq-sre.change-schedule.v1",
    id: "33000000-0000-4000-8000-000000000001",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    runbook_id: RUNBOOK_ID,
    runbook_version: "1.1.0",
    scheduled_start: new Date(now - 20 * 60 * 1000).toISOString(),
    scheduled_end: new Date(now + 70 * 60 * 1000).toISOString(),
    plan_bindings: [
      planBinding(SCALE_STEP_ID, "34000000-0000-4000-8000-000000000001"),
      planBinding(RESTART_STEP_ID, "34000000-0000-4000-8000-000000000002"),
    ],
    resource_keys: ["deployment/rocketmq/proxy"],
    status: "awaiting_manual_gate",
    next_step_sequence: 2,
    completed_steps: [SCALE_STEP_ID],
    waiting_manual_gate: GATE_STEP_ID,
    intent_persisted: true,
    active_execution_id: "35000000-0000-4000-8000-000000000001",
    created_by: "previous-shift-operator",
    correlation_id: "36000000-0000-4000-8000-000000000001",
    created_at: new Date(now - 2 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date(now - 3 * 60 * 1000).toISOString(),
  },
  {
    schema_version: "rocketmq-sre.change-schedule.v1",
    id: "33000000-0000-4000-8000-000000000002",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    runbook_id: RUNBOOK_ID,
    runbook_version: "1.0.0",
    scheduled_start: new Date(now + 90 * 60 * 1000).toISOString(),
    scheduled_end: new Date(now + 150 * 60 * 1000).toISOString(),
    plan_bindings: [
      planBinding(SCALE_STEP_ID, "34000000-0000-4000-8000-000000000003"),
    ],
    resource_keys: ["deployment/rocketmq/proxy"],
    status: "scheduled",
    next_step_sequence: 1,
    completed_steps: [],
    intent_persisted: false,
    created_by: "change-manager",
    correlation_id: "36000000-0000-4000-8000-000000000002",
    created_at: new Date(now - 40 * 60 * 1000).toISOString(),
    updated_at: new Date(now - 40 * 60 * 1000).toISOString(),
  },
];

export function createMockChangeManagementApi(
  auth: ApiRequestContext,
): ChangeManagementApi {
  const checkScope = (clusterId: string) => {
    if (!auth.clusterIds.includes(clusterId)) {
      throw new ApiError(
        403,
        "cluster_not_allowed",
        "cluster is outside the authenticated scope",
      );
    }
  };

  const findSchedule = (scheduleId: string) => {
    const schedule = demoSchedules.find((item) => item.id === scheduleId);
    if (!schedule) {
      throw new ApiError(404, "source_unavailable", "schedule is unavailable");
    }
    checkScope(schedule.cluster_id);
    return schedule;
  };

  const transition = async (
    scheduleId: string,
    status: ChangeSchedule["status"],
  ) => {
    await wait();
    const schedule = findSchedule(scheduleId);
    schedule.status = status;
    schedule.updated_at = new Date().toISOString();
    return clone(schedule);
  };

  return {
    listRunbooks: async (clusterId, limit = 256, signal) => {
      checkScope(clusterId);
      await wait(signal);
      return page(
        "rocketmq-sre.runbook-page.v1",
        demoRunbooks.slice(0, limit),
      );
    },
    getRunbook: async (clusterId, runbookId, version, signal) => {
      checkScope(clusterId);
      await wait(signal);
      const runbook = demoRunbooks.find(
        (item) => item.id === runbookId && item.version === version,
      );
      if (!runbook) {
        throw new ApiError(
          404,
          "source_unavailable",
          "runbook version is unavailable",
        );
      }
      return clone(runbook);
    },
    createRunbook: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      demoRunbooks.push(clone(input.definition));
      return clone(input.definition);
    },
    listChangeWindows: async (
      clusterId,
      from,
      to,
      limit = 256,
      signal,
    ) => {
      checkScope(clusterId);
      await wait(signal);
      const fromTime = Date.parse(from);
      const toTime = Date.parse(to);
      return page(
        "rocketmq-sre.change-window-page.v1",
        demoWindows
          .filter(
            (item) =>
              item.cluster_id === clusterId &&
              Date.parse(item.ends_at) >= fromTime &&
              Date.parse(item.starts_at) <= toTime,
          )
          .slice(0, limit),
      );
    },
    createChangeWindow: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      const created: ChangeWindow = {
        ...clone(input),
        schema_version: "rocketmq-sre.change-window.v1",
        id: crypto.randomUUID(),
        tenant_id: auth.tenantId,
        resource_keys: input.resource_keys ?? [],
        created_by: auth.subject,
        created_at: new Date().toISOString(),
      };
      demoWindows.push(created);
      return clone(created);
    },
    previewSchedule: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      const conflicts = scheduleConflicts(input);
      return {
        schema_version: "rocketmq-sre.change-schedule-preview.v1",
        schedule: candidateSchedule(input, auth),
        conflicts: clone(conflicts),
        schedulable: conflicts.length === 0,
      };
    },
    listSchedules: async (
      clusterId,
      status,
      limit = 256,
      signal,
    ) => {
      checkScope(clusterId);
      await wait(signal);
      return page(
        "rocketmq-sre.change-schedule-page.v1",
        demoSchedules
          .filter(
            (item) =>
              item.cluster_id === clusterId &&
              (!status || item.status === status),
          )
          .slice(0, limit),
      );
    },
    createSchedule: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      const conflicts = scheduleConflicts(input);
      if (conflicts.length > 0) {
        throw new ApiError(
          409,
          "capability_mismatch",
          "schedule conflicts must be resolved before creation",
        );
      }
      const schedule = candidateSchedule(input, auth);
      demoSchedules.unshift(schedule);
      return clone(schedule);
    },
    getSchedule: async (scheduleId, signal) => {
      await wait(signal);
      return clone(findSchedule(scheduleId));
    },
    pauseSchedule: (scheduleId) => transition(scheduleId, "paused"),
    resumeSchedule: (scheduleId) => transition(scheduleId, "scheduled"),
    cancelSchedule: async (scheduleId) => {
      const schedule = findSchedule(scheduleId);
      return transition(
        scheduleId,
        schedule.intent_persisted ? "safe_stopping" : "cancelled",
      );
    },
    reconcileSchedule: (scheduleId) =>
      transition(scheduleId, "reconciling"),
    approveManualGate: async (scheduleId, stepId) => {
      await wait();
      const schedule = findSchedule(scheduleId);
      if (schedule.waiting_manual_gate !== stepId) {
        throw new ApiError(
          409,
          "capability_mismatch",
          "manual gate does not match the waiting step",
        );
      }
      schedule.waiting_manual_gate = null;
      schedule.status = "running";
      schedule.next_step_sequence += 1;
      schedule.updated_at = new Date().toISOString();
      return clone(schedule);
    },
    rejectManualGate: (scheduleId) => transition(scheduleId, "rejected"),
  };
}

function actionStep(
  id: string,
  sequence: number,
  name: string,
  action: Extract<
    RunbookDefinition["steps"][number]["body"],
    { kind: "action" }
  >["action"],
  dependsOn: string[] = [],
): RunbookDefinition["steps"][number] {
  return {
    id,
    sequence,
    name,
    depends_on: dependsOn,
    body: {
      kind: "action",
      action,
      descriptor_version: "1.0.0",
      resource: "deployment/rocketmq/proxy",
      parameters: { replicas: 1 },
    },
  };
}

function manualGateStep(
  dependsOn: string[],
): RunbookDefinition["steps"][number] {
  return {
    id: GATE_STEP_ID,
    sequence: 2,
    name: "确认新副本健康",
    depends_on: dependsOn,
    body: {
      kind: "manual_gate",
      gate: {
        gate_id: "proxy-health-verification",
        title: "确认新副本流量与延迟",
        instructions: "检查错误率、P99 延迟和连接数后决定是否继续。",
        required_role: "approver",
        timeout_seconds: 900,
      },
    },
  };
}

function planBinding(stepId: string, planId: string) {
  return {
    step_id: stepId,
    plan_id: planId,
    plan_hash: `sha256:${"a".repeat(64)}`,
    precondition_hash: `sha256:${"b".repeat(64)}`,
  };
}

function candidateSchedule(
  input: CreateChangeScheduleRequest,
  auth: ApiRequestContext,
): ChangeSchedule {
  const runbook = demoRunbooks.find(
    (item) =>
      item.id === input.runbook_id && item.version === input.runbook_version,
  );
  const resources = [
    ...new Set(
      (runbook?.steps ?? [])
        .filter((step) => step.body.kind === "action")
        .map((step) =>
          step.body.kind === "action" ? step.body.resource : "",
        )
        .filter(Boolean),
    ),
  ];
  const timestamp = new Date().toISOString();
  return {
    schema_version: "rocketmq-sre.change-schedule.v1",
    id: crypto.randomUUID(),
    tenant_id: auth.tenantId,
    cluster_id: input.cluster_id,
    runbook_id: input.runbook_id,
    runbook_version: input.runbook_version,
    scheduled_start: input.scheduled_start,
    scheduled_end: input.scheduled_end,
    plan_bindings: clone(input.plan_bindings),
    resource_keys: resources,
    status: "scheduled",
    next_step_sequence: 1,
    completed_steps: [],
    intent_persisted: false,
    created_by: auth.subject,
    correlation_id: crypto.randomUUID(),
    created_at: timestamp,
    updated_at: timestamp,
  };
}

function scheduleConflicts(
  input: CreateChangeScheduleRequest,
): ChangeConflict[] {
  const start = Date.parse(input.scheduled_start);
  const end = Date.parse(input.scheduled_end);
  const blocking = demoWindows.find(
    (item) =>
      item.cluster_id === input.cluster_id &&
      item.kind !== "maintenance" &&
      Date.parse(item.starts_at) < end &&
      Date.parse(item.ends_at) > start,
  );
  if (blocking) {
    return [
      {
        blocking: true,
        code:
          blocking.kind === "freeze"
            ? "freeze_window"
            : "blackout_window",
        message: `${blocking.name} 阻断该排程。`,
        starts_at: blocking.starts_at,
        ends_at: blocking.ends_at,
        window_id: blocking.id,
      },
    ];
  }
  const maintenance = demoWindows.some(
    (item) =>
      item.cluster_id === input.cluster_id &&
      item.kind === "maintenance" &&
      Date.parse(item.starts_at) <= start &&
      Date.parse(item.ends_at) >= end,
  );
  return maintenance
    ? []
    : [
        {
          blocking: true,
          code: "outside_maintenance_window",
          message: "计划时间不在维护窗口内。",
          starts_at: input.scheduled_start,
          ends_at: input.scheduled_end,
        },
      ];
}

function page<
  T,
  V extends
    | "rocketmq-sre.runbook-page.v1"
    | "rocketmq-sre.change-window-page.v1"
    | "rocketmq-sre.change-schedule-page.v1",
>(schemaVersion: V, items: T[]) {
  return {
    schema_version: schemaVersion,
    items: clone(items),
    partial: false,
  };
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function wait(signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = window.setTimeout(resolve, WAIT_MS);
    signal?.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}
