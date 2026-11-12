import type { AssetKind } from "@/api/types";

export const RESOURCE_KINDS = [
  "cluster",
  "name_server",
  "controller",
  "broker",
  "proxy",
  "store",
  "pod",
  "node",
  "persistent_volume_claim",
  "pod_disruption_budget",
  "topic",
  "queue",
  "producer",
  "consumer",
  "connection",
] as const satisfies readonly (AssetKind | "cluster")[];

const RESOURCE_KEYS = ["cluster_id", "resource_kind", "resource_key"];
export const WORKSPACE_KEYS = [...RESOURCE_KEYS, "window_start", "window_end"];

export interface ReadOnlyUrlContext {
  clusterId: string;
  resourceKind?: AssetKind | "cluster";
  resourceKey?: string;
}

export interface AbsoluteWindow {
  start: string;
  end: string;
}

export interface WorkspaceScope extends ReadOnlyUrlContext {
  window: AbsoluteWindow;
}

export type UrlContextResult =
  | { status: "none" }
  | { status: "invalid" }
  | { status: "valid"; context: ReadOnlyUrlContext };

export function parseReadOnlyUrlContext(
  search: string,
  allowedClusterIds: readonly string[],
): UrlContextResult {
  const params = new URLSearchParams(search);
  if (!RESOURCE_KEYS.some((key) => params.has(key))) return { status: "none" };
  const clusterId = params.get("cluster_id")?.trim() ?? "";
  const resourceKind = params.get("resource_kind")?.trim() ?? "";
  const resourceKey = params.get("resource_key")?.trim() ?? "";
  const hasResource = resourceKind.length > 0 || resourceKey.length > 0;
  const validCluster =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      clusterId,
    ) && allowedClusterIds.includes(clusterId);
  const validResource =
    !hasResource ||
    (RESOURCE_KINDS.includes(resourceKind as (typeof RESOURCE_KINDS)[number]) &&
      resourceKey.length > 0 &&
      resourceKey.length <= 512 &&
      ![...resourceKey].some(
        (character) =>
          character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
      ));
  if (
    !validCluster ||
    !validResource ||
    RESOURCE_KEYS.some((key) => params.getAll(key).length > 1)
  )
    return { status: "invalid" };
  return {
    status: "valid",
    context: {
      clusterId,
      resourceKind: hasResource
        ? (resourceKind as AssetKind | "cluster")
        : undefined,
      resourceKey: hasResource ? resourceKey : undefined,
    },
  };
}

export function withoutReadOnlyUrlContext(search: string) {
  const params = new URLSearchParams(search);
  RESOURCE_KEYS.forEach((key) => params.delete(key));
  return encodeSearch(params);
}

export function parseAbsoluteWindow(
  search: string,
): AbsoluteWindow | "invalid" | undefined {
  const params = new URLSearchParams(search);
  if (!params.has("window_start") && !params.has("window_end"))
    return undefined;
  const start = params.get("window_start") ?? "";
  const end = params.get("window_end") ?? "";
  if (
    params.getAll("window_start").length !== 1 ||
    params.getAll("window_end").length !== 1 ||
    !isAbsoluteDate(start) ||
    !isAbsoluteDate(end) ||
    !Number.isFinite(Date.parse(start)) ||
    !Number.isFinite(Date.parse(end)) ||
    Date.parse(start) >= Date.parse(end)
  )
    return "invalid";
  return {
    start: new Date(start).toISOString(),
    end: new Date(end).toISOString(),
  };
}

export function projectWorkspaceSearch(search: string, scope: WorkspaceScope) {
  const params = new URLSearchParams(search);
  WORKSPACE_KEYS.forEach((key) => params.delete(key));
  if (scope.clusterId) {
    params.set("cluster_id", scope.clusterId);
    if (scope.resourceKind && scope.resourceKey) {
      params.set("resource_kind", scope.resourceKind);
      params.set("resource_key", scope.resourceKey);
    }
  }
  params.set("window_start", scope.window.start);
  params.set("window_end", scope.window.end);
  return encodeSearch(params);
}

/** Internal navigation carries scope; an explicit target cluster owns its resource. */
export function inheritWorkspaceSearch(
  currentSearch: string,
  targetSearch: string,
  targetPath?: string,
) {
  const current = new URLSearchParams(currentSearch);
  const target = new URLSearchParams(targetSearch);
  const explicitResource = RESOURCE_KEYS.some((key) => target.has(key));
  const clusterLink = /^\/clusters\/([0-9a-f-]{36})$/i.exec(targetPath ?? "");
  if (!explicitResource && clusterLink)
    target.set("cluster_id", clusterLink[1]);
  const explicitWindow = target.has("window_start") || target.has("window_end");
  for (const key of WORKSPACE_KEYS) {
    if (
      (RESOURCE_KEYS.includes(key) && (explicitResource || clusterLink)) ||
      (key.startsWith("window_") && explicitWindow)
    )
      continue;
    current.getAll(key).forEach((value) => target.append(key, value));
  }
  return encodeSearch(target);
}

export function resourcePath(scope: ReadOnlyUrlContext) {
  return scope.resourceKind && scope.resourceKey
    ? `${scope.resourceKind}/${scope.resourceKey}`
    : undefined;
}

export function withinWindow(value: string, window: AbsoluteWindow) {
  const timestamp = Date.parse(value);
  return (
    timestamp >= Date.parse(window.start) && timestamp <= Date.parse(window.end)
  );
}

function encodeSearch(params: URLSearchParams) {
  const value = params.toString();
  return value ? `?${value}` : "";
}

function isAbsoluteDate(value: string) {
  const parts =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,3})?)?(?:Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!parts) return false;
  const [, y, mo, d, h, mi, s = "0"] = parts;
  const [year, month, day, hour, minute, second] = [y, mo, d, h, mi, s].map(
    Number,
  );
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= days[month - 1] &&
    hour < 24 &&
    minute < 60 &&
    second < 60
  );
}
