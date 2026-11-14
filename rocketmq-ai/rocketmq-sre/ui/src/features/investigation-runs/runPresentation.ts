import type { InvestigationRun, InvestigationRunControls } from "@/api/types";
import type { WorkspaceScope } from "@/context/workspaceScope";
import { absoluteTimeKey } from "@/context/workspaceScope";

type RunScope = InvestigationRun["effective_scope"];
export type RunSelection =
  | { kind: "manual" }
  | { kind: "invalid" }
  | { kind: "run"; id: string; revisionId?: string };
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const U64_MAX = "18446744073709551615";

export class RunScopeError extends Error {}

export function decimal(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^(0|[1-9]\d{0,19})$/u.test(value) &&
    (value.length < 20 || value <= U64_MAX)
  );
}

/** Revisions are decimal u64 strings; converting them to Number loses CAS order. */
export function compareRevision(left: string, right: string) {
  if (!decimal(left) || !decimal(right)) throw new RunScopeError();
  return left.length === right.length
    ? left === right
      ? 0
      : left > right
        ? 1
        : -1
    : left.length > right.length
      ? 1
      : -1;
}

export function runSelection(search: string): RunSelection {
  const params = new URLSearchParams(search);
  const ids = params.getAll("run_id");
  const revisions = params.getAll("run_revision_id");
  if (!ids.length && !revisions.length) return { kind: "manual" };
  if (
    ids.length !== 1 ||
    !UUID.test(ids[0]) ||
    revisions.length > 1 ||
    (revisions.length === 1 && !UUID.test(revisions[0]))
  )
    return { kind: "invalid" };
  return { kind: "run", id: ids[0], revisionId: revisions[0] };
}

export function selectionSearch(
  search: string,
  id?: string,
  revisionId?: string,
) {
  const params = new URLSearchParams(search);
  ["run_id", "run_revision_id", "evidence_id"].forEach((key) =>
    params.delete(key),
  );
  if (id) {
    ["investigation_id", "revision_id"].forEach((key) => params.delete(key));
    params.set("run_id", id);
    if (revisionId) params.set("run_revision_id", revisionId);
  }
  return `?${params.toString()}`;
}

export function sameWindow(
  left: RunScope["time_range"],
  right: { start: string; end: string },
) {
  return (
    absoluteTimeKey(left.start) !== undefined &&
    absoluteTimeKey(left.start) === absoluteTimeKey(right.start) &&
    absoluteTimeKey(left.end) === absoluteTimeKey(right.end)
  );
}

export function sameRunScope(left: RunScope, right: RunScope) {
  return (
    left.tenant_id === right.tenant_id &&
    left.cluster_id === right.cluster_id &&
    left.investigation_id === right.investigation_id &&
    left.resource?.kind === right.resource?.kind &&
    left.resource?.key === right.resource?.key &&
    sameWindow(left.time_range, right.time_range)
  );
}

export function resourceForWorkspace(
  resource: RunScope["resource"],
): Pick<WorkspaceScope, "resourceKind" | "resourceKey"> {
  if (!resource || resource.kind === "cluster") return {};
  const kind =
    resource.kind === "consumer_group"
      ? "consumer"
      : resource.kind === "producer_group"
        ? "producer"
        : resource.kind;
  const supported = [
    "name_server",
    "controller",
    "broker",
    "proxy",
    "store",
    "topic",
    "queue",
    "pod",
    "node",
    "persistent_volume_claim",
    "consumer",
    "producer",
  ];
  return supported.includes(kind)
    ? {
        resourceKind: kind as WorkspaceScope["resourceKind"],
        resourceKey: resource.key,
      }
    : {};
}

export function runInWorkspace(
  run: InvestigationRun,
  workspace: WorkspaceScope,
) {
  const resource = resourceForWorkspace(run.effective_scope.resource);
  return (
    run.effective_scope.cluster_id === workspace.clusterId &&
    sameWindow(run.effective_scope.time_range, workspace.window) &&
    (!workspace.resourceKind ||
      workspace.resourceKind === "cluster" ||
      (resource.resourceKind === workspace.resourceKind &&
        resource.resourceKey === workspace.resourceKey))
  );
}

export function validateRun(
  run: InvestigationRun,
  expected: {
    id: string;
    investigationId: string;
    tenantId: string;
    clusterId: string;
  },
) {
  if (
    !run ||
    run.id !== expected.id ||
    run.schema_version !== "rocketmq-sre.investigation-run.v1" ||
    !decimal(run.revision) ||
    !decimal(run.control_epoch) ||
    [run.initial_scope, run.effective_scope].some(
      (scope) =>
        !scope ||
        scope.tenant_id !== expected.tenantId ||
        scope.cluster_id !== expected.clusterId ||
        scope.investigation_id !== expected.investigationId,
    ) ||
    !Array.isArray(run.steps) ||
    !Array.isArray(run.pack_results ?? [])
  )
    throw new RunScopeError();
  return run;
}

export function commandAvailable(
  controls: InvestigationRunControls | undefined,
  run: InvestigationRun | undefined,
  kind: "pause" | "resume" | "cancel" | "steer",
) {
  return (
    !!controls &&
    !!run &&
    validCommands(controls.commands) &&
    controls.run.revision === run.revision &&
    controls.run.control_epoch === run.control_epoch &&
    controls.commands.some(
      (command) =>
        command.kind === kind &&
        command.allowed === true &&
        command.blocked_reason == null,
    )
  );
}

export function validCommands(commands: InvestigationRunControls["commands"]) {
  const kinds = ["pause", "resume", "cancel", "steer"];
  return (
    Array.isArray(commands) &&
    commands.length === 4 &&
    new Set(commands.map((command) => command.kind)).size === 4 &&
    commands.every(
      (command) =>
        kinds.includes(command.kind) &&
        typeof command.allowed === "boolean" &&
        (!command.allowed || command.blocked_reason == null),
    )
  );
}

export const stopReasonKeys = [
  "evidence_insufficient",
  "budget_exhausted",
  "deadline_exceeded",
  "no_progress",
  "model_unavailable",
  "source_unavailable",
] as const;
export const terminalRunStatuses = new Set([
  "completed",
  "inconclusive",
  "cancelled",
  "failed",
]);

export function budgetRemaining(
  limit: number,
  settled: number,
  reserved: number,
) {
  return [limit, settled, reserved].every(
    (value) => Number.isFinite(value) && value >= 0,
  )
    ? Math.max(0, limit - settled - reserved)
    : undefined;
}
