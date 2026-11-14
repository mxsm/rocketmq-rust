import type { EvidenceRecord } from "@/api/types";
import { absoluteTimeKey, parseAbsoluteWindow } from "@/context/workspaceScope";

export const LATENCY_SEGMENTS = [
  "client",
  "proxy",
  "remoting",
  "broker",
  "store",
] as const;
export type LatencySegmentId = (typeof LATENCY_SEGMENTS)[number];
export const LATENCY_AVAILABILITY = [
  "available",
  "missing",
  "unsupported_scope",
  "no_traffic",
  "insufficient_data",
  "invalid",
  "stale",
  "unknown",
] as const;
export type LatencyAvailability = (typeof LATENCY_AVAILABILITY)[number];

export interface LatencyObservation {
  estimate: number;
  lower: number;
  upper: number | null;
  tailLimited: boolean;
  observedIncrements: number;
  estimatedIncrements: number;
  nodes: number;
  scrapes: number;
  resets: number;
}
export interface LatencySegment {
  id: LatencySegmentId;
  availability: LatencyAvailability;
  reason: string;
  observation?: LatencyObservation;
}
export interface SendLatencySummary {
  kind: "summary";
  topic: string;
  requested: { start: string; end: string };
  query: { start: string; end: string };
  data?: { start: string; end: string };
  collectedAt: string;
  age?: number;
  segments: LatencySegment[];
  policy: {
    status: "not_configured" | "exceeded" | "within_observed_policy" | "not_qualified";
    threshold?: number;
    reason: string;
  };
}
export type LatencyDisplay = SendLatencySummary | { kind: "invalid"; reason: "scope" | "structure" };
export type LatencyEvidence = Pick<
  EvidenceRecord,
  "tenant_id" | "cluster_id" | "resource" | "time_range" | "observed_at" | "source" | "exposure"
>;

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
function nonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
function count(value: unknown): value is number {
  return nonnegative(value) && Number.isSafeInteger(value);
}
function instant(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 40 &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}
function window(start: unknown, end: unknown) {
  if (!instant(start) || !instant(end)) return undefined;
  const validated = parseAbsoluteWindow(
    new URLSearchParams({ window_start: start, window_end: end }).toString(),
  );
  return validated && validated !== "invalid" ? { start, end } : undefined;
}
function persistedTimeMatches(original: string, persisted: string) {
  const source = absoluteTimeKey(original);
  const stored = absoluteTimeKey(persisted);
  if (!source || !stored) return false;
  // Normal SQL persistence truncates nanoseconds to microseconds. Preserve
  // exact values too; millisecond equality alone would accept a different range.
  return source === stored || `${source.slice(0, 26)}000Z` === stored;
}
function observation(value: unknown): LatencyObservation | undefined {
  const item = record(value);
  if (
    !item || item.quantile !== "p99" ||
    !nonnegative(item.estimate_millis) || !nonnegative(item.lower_bound_millis) ||
    !nonnegative(item.observed_increments) || !nonnegative(item.estimated_increments) ||
    !count(item.observed_nodes) || !count(item.scrape_samples) || !count(item.observed_resets)
  ) return undefined;
  const tailLimited = item.resolution === "tail_limited";
  if (
    (!tailLimited && item.resolution !== "finite_bucket") ||
    (tailLimited ? item.upper_bound_millis !== null : !nonnegative(item.upper_bound_millis)) ||
    item.estimate_millis < item.lower_bound_millis ||
    (!tailLimited && (item.upper_bound_millis as number) < item.estimate_millis)
  ) return undefined;
  return {
    estimate: item.estimate_millis,
    lower: item.lower_bound_millis,
    upper: tailLimited ? null : item.upper_bound_millis as number,
    tailLimited,
    observedIncrements: item.observed_increments,
    estimatedIncrements: item.estimated_increments,
    nodes: item.observed_nodes,
    scrapes: item.scrape_samples,
    resets: item.observed_resets,
  };
}

/** Reads a bounded display projection; it never calculates a quantile or grants qualification. */
export function sendLatencyDisplay(
  value: unknown,
  evidence: LatencyEvidence,
): LatencyDisplay | undefined {
  const root = record(value);
  if (root?.observation_schema !== "rocketmq.send-latency-window.v1") return undefined;
  const scope = record(root.scope);
  const ranges = record(root.window);
  const method = record(root.method);
  const requested = window(ranges?.requested_start, ranges?.requested_end);
  const query = window(ranges?.query_start, ranges?.query_end);
  if (
    evidence.source !== "prometheus" || evidence.exposure !== "prometheus_api" ||
    scope?.tenant_id !== evidence.tenant_id || scope?.cluster_id !== evidence.cluster_id ||
    scope?.canonical_resource !== evidence.resource || typeof scope?.topic !== "string" ||
    !/^[A-Za-z0-9_.%:-]{1,255}$/.test(scope.topic) || evidence.resource !== `send-latency/${scope.topic}`
  ) return { kind: "invalid", reason: "scope" };
  if (
    root.schema_version !== "rocketmq.sre-diagnostic-source.v1" ||
    root.profile !== "send_latency" || root.measurement_kind !== "classic_histogram_window_estimate" ||
    root.unit !== "milliseconds" || root.partial !== true ||
    !requested || !query || ranges?.interval !== "left_open_right_closed" || ranges.time_resolution_millis !== 1 ||
    !instant(root.collected_at) ||
    method?.name !== "classic_linear_p99_strict.v1" || method.counter_method !== "prometheus_increase_per_series" ||
    method.grid_ref !== "otel_default_explicit_ms.v1" || method.quantile !== 0.99 ||
    !Array.isArray(root.segments) || root.segments.length !== LATENCY_SEGMENTS.length
  ) return { kind: "invalid", reason: "structure" };
  // Only documented persistence normalization is accepted for this display.
  // This check is not a substitute for the server's scoped source proof.
  if (
    !persistedTimeMatches(requested.start, evidence.time_range.start) ||
    !persistedTimeMatches(requested.end, evidence.time_range.end) ||
    !persistedTimeMatches(root.collected_at, evidence.observed_at)
  ) return { kind: "invalid", reason: "scope" };
  const segments: LatencySegment[] = [];
  for (const id of LATENCY_SEGMENTS) {
    const matching = root.segments.map(record).filter((item) => item?.id === id);
    const item = matching[0];
    if (
      matching.length !== 1 || !item ||
      !LATENCY_AVAILABILITY.includes(item.availability as LatencyAvailability)
    ) return { kind: "invalid", reason: "structure" };
    const observed = item.observation == null ? undefined : observation(item.observation);
    if ((item.observation != null && !observed) ||
      (item.availability === "available" && (!observed || observed.tailLimited)) ||
      (id !== "broker" && (item.availability !== "unsupported_scope" || observed))
    ) return { kind: "invalid", reason: "structure" };
    segments.push({
      id, availability: item.availability as LatencyAvailability,
      reason: typeof item.reason_code === "string" ? item.reason_code : "unknown",
      observation: observed,
    });
  }
  const policy = record(root.policy);
  const policyScope = record(policy?.scope);
  const status = policy?.status;
  const configured = status === "exceeded" || status === "within_observed_policy";
  const validPolicy = configured && policy?.source === "operator_configuration" &&
    policy.unit === "milliseconds" && policy.comparison === "strictly_greater" &&
    policyScope?.cluster_id === evidence.cluster_id && policyScope.topic === scope.topic &&
    policyScope.canonical_resource === evidence.resource && count(policy.p99_millis) && policy.p99_millis > 0;
  let policyStatus: SendLatencySummary["policy"]["status"] = "not_qualified";
  if (validPolicy && (status === "exceeded" || status === "within_observed_policy")) policyStatus = status;
  else if (status === "not_configured") policyStatus = status;
  return {
    kind: "summary", topic: scope.topic, requested, query,
    data: window(ranges.data_first_at, ranges.data_last_at),
    collectedAt: root.collected_at,
    age: nonnegative(root.source_age_seconds) ? root.source_age_seconds : undefined,
    segments,
    policy: {
      status: policyStatus,
      threshold: validPolicy ? policy.p99_millis as number : undefined,
      reason: configured && !validPolicy ? "policy_scope_mismatch"
        : typeof policy?.reason_code === "string" ? policy.reason_code : "unknown",
    },
  };
}
