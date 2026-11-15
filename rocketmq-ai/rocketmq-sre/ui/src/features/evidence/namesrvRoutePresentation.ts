import type { EvidenceRecord } from "@/api/types";
import { absoluteTimeKey } from "@/context/workspaceScope";

const SCHEMA = "rocketmq.sre-namesrv-route-observation.v1";
export const ROUTE_STATES = [
  "compared_equal", "stable_observed_difference", "transient_change_observed", "incomparable",
] as const;
export const ROUTE_DIMENSIONS = [
  "topic_presence", "brokers", "broker_ids", "addresses", "cluster", "zone", "acting_master",
  "queues", "permissions", "topic_sys_flags", "order_config", "filter_servers", "static_mapping", "route_schema",
] as const;
export const ROUTE_REASONS = [
  "view_configuration_observed", "view_configuration_unavailable", "view_configuration_invalid",
  "external_route_fallback_enabled", "view_configuration_mismatch", "owner_cancelled", "deadline_exceeded",
  "source_byte_budget_exceeded", "source_row_budget_exceeded", "route_observed", "topic_absent",
  "route_not_attempted_view_ineligible", "route_source_unavailable", "route_permission_denied",
  "route_schema_invalid", "route_scope_mismatch", "route_response_too_large", "route_source_unsupported", "current_routes_compared_equal",
  "current_route_difference_observed", "current_route_change_observed", "current_routes_incomparable",
  "acquisition_span_exceeded", "peer_independence_unconfigured", "route_evidence_stale",
  "requested_window_not_observed", "fingerprint_domain_unavailable", "fingerprint_domain_changed", "source_unconfigured", "source_response_bounded",
] as const;
export const ROUTE_MISSING_FACTS = [
  "registration.first_registration_time", "registration.last_registration_time", "registration.last_heartbeat_time",
  "route.source_generation_time", "route.source_generation", "route.source_instance_id",
  "route.local_origin_atomic_proof", "broker.scope_bound_reachability", "historical_window_coverage",
] as const;
const CONFIG_STATES = ["available", "unavailable", "invalid", "view_mismatch"] as const;
const READ_STATES = [
  "found", "topic_absent", "not_attempted", "unavailable", "permission_denied", "unsupported", "invalid", "too_large",
] as const;
type ConfigState = (typeof CONFIG_STATES)[number];
type ReadState = (typeof READ_STATES)[number];
export type RouteState = (typeof ROUTE_STATES)[number];
export type RouteSpan = { start: string; end: string };
type ViewFlags = { cluster_test: boolean | null; order_message_enable: boolean | null; support_acting_master: boolean | null };
export interface RouteRead<T extends string = ReadState> {
  status: T;
  reason: string;
  span?: RouteSpan;
  receivedBytes?: number;
}
export interface RoutePeer {
  id: string;
  rounds: Array<{
    round: 1 | 2;
    span: RouteSpan;
    configuration: RouteRead<ConfigState> & { flags: ViewFlags };
    route: RouteRead;
  }>;
}
export interface NamesrvRouteSummaryDisplay {
  kind: "summary";
  topic: string;
  requested: RouteSpan;
  collection: RouteSpan;
  age: number;
  state: RouteState;
  reason: string;
  compared: string[];
  different: string[];
  unknown: string[];
  missing: string[];
  peers: RoutePeer[];
  statistics: {
    plannedConfig: number; startedConfig: number; plannedRoute: number; startedRoute: number;
    receivedBytes: number; unknownBytes: number; acceptedBytes: number; rows: number | null; bounded: boolean;
  };
}
export type NamesrvRouteDisplay = NamesrvRouteSummaryDisplay | { kind: "invalid"; reason: "scope" | "structure" };
type RouteEvidence = Pick<EvidenceRecord, "tenant_id" | "cluster_id" | "resource" | "source" | "exposure" | "time_range" | "observed_at">;
type JsonRecord = Record<string, unknown>;
const object = (value: unknown): JsonRecord | undefined => value !== null && typeof value === "object" && !Array.isArray(value)
  ? value as JsonRecord : undefined;
const count = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const enumValue = <T extends string>(value: unknown, values: readonly T[]): value is T =>
  typeof value === "string" && values.includes(value as T);
const reason = (value: unknown) => enumValue(value, ROUTE_REASONS) ? value : "unknown";

function instant(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 40) return false;
  const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!parts) return false;
  const [year, month, day, hour, minute, second] = parts.slice(1).map(Number);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1] &&
    hour < 24 && minute < 60 && second < 60 && absoluteTimeKey(value) !== undefined;
}
function span(start: unknown, end: unknown): RouteSpan | undefined {
  return instant(start) && instant(end) && absoluteTimeKey(start)! <= absoluteTimeKey(end)!
    ? { start, end } : undefined;
}
function timeMatches(original: string, persisted: string) {
  const source = absoluteTimeKey(original);
  const stored = absoluteTimeKey(persisted);
  return !!source && !!stored && (source === stored || `${source.slice(0, 26)}000Z` === stored);
}
function contained(inner: RouteSpan, outer: RouteSpan) {
  return absoluteTimeKey(inner.start)! >= absoluteTimeKey(outer.start)! && absoluteTimeKey(inner.end)! <= absoluteTimeKey(outer.end)!;
}
function flags(value: unknown): ViewFlags | undefined {
  if (value === null) return { cluster_test: null, order_message_enable: null, support_acting_master: null };
  const item = object(value);
  if (!item) return undefined;
  const keys = ["cluster_test", "order_message_enable", "support_acting_master"] as const;
  if (Object.keys(item).length !== keys.length ||
    !keys.every((key) => item[key] === null || typeof item[key] === "boolean")) return undefined;
  return Object.fromEntries(keys.map((key) => [key, item[key]])) as ViewFlags;
}
function digest(value: unknown): value is string {
  return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}
function read<T extends string>(value: unknown, states: readonly T[], outer: RouteSpan): RouteRead<T> | undefined {
  const item = object(value);
  if (!item || !enumValue(item.status, states)) return undefined;
  const interval = span(item.started_at, item.finished_at);
  if ((!interval && (item.started_at != null || item.finished_at != null)) ||
    (interval && !contained(interval, outer)) ||
    (item.status === "not_attempted" && interval) ||
    (item.received_body_bytes != null && !count(item.received_body_bytes)) ||
    (["available", "found", "topic_absent"].includes(item.status) && !interval)) return undefined;
  return {
    status: item.status, reason: reason(item.reason_code), span: interval,
    receivedBytes: count(item.received_body_bytes) ? item.received_body_bytes : undefined,
  };
}
function dimensions(value: unknown): string[] | undefined {
  if (!Array.isArray(value) || value.length > 32 || value.some((entry) => typeof entry !== "string")) return undefined;
  return [...new Set(value.map((entry) => enumValue(entry, ROUTE_DIMENSIONS) ? entry : "unknown_dimension"))];
}

/** Projects only saved display fields. Route equality and qualification remain server decisions. */
export function namesrvRouteDisplay(value: unknown, evidence: RouteEvidence): NamesrvRouteDisplay | undefined {
  const root = object(value);
  const scopedProfile = evidence.source === "admin-query" && evidence.resource.startsWith("namesrv-route/");
  if (root?.schema_version !== SCHEMA && root?.profile !== "nameserver_route_comparison" && !scopedProfile) return undefined;
  const invalid = { kind: "invalid", reason: "structure" } as const;
  if (!root || evidence.source !== "admin-query" || evidence.exposure !== "admin_rpc" ||
    root.tenant_id !== evidence.tenant_id || root.cluster_id !== evidence.cluster_id ||
    typeof root.topic !== "string" || !/^[A-Za-z0-9_.%:-]{1,255}$/.test(root.topic) ||
    evidence.resource !== `namesrv-route/${root.topic}`) return { kind: "invalid", reason: "scope" };
  const requested = object(root.requested_window);
  const window = span(requested?.start, requested?.end);
  const collection = span(root.collection_started_at, root.collected_at);
  if (root.schema_version !== SCHEMA || root.profile !== "nameserver_route_comparison" ||
    root.temporal_semantics !== "current_context_only" || root.historical_coverage !== "unknown" ||
    !window || !collection || !count(root.source_age_seconds)) return invalid;
  if (!timeMatches(window.start, evidence.time_range.start) || !timeMatches(window.end, evidence.time_range.end) ||
    !timeMatches(collection.end, evidence.observed_at)) return { kind: "invalid", reason: "scope" };
  const config = object(root.configuration);
  const expected = config?.peers;
  const expectedView = flags(config?.expected_view);
  if (config?.independence_source !== "operator_declared_direct_peers" || !expectedView ||
    expectedView.cluster_test !== false || Object.values(expectedView).some((flag) => flag === null) ||
    !Array.isArray(expected) || expected.length < 2 || expected.length > 4 ||
    !Array.isArray(root.rounds) || root.rounds.length !== 2) return invalid;
  const peers: RoutePeer[] = [];
  const peerIds = new Set<string>();
  const peerEndpoints = new Map<string, string>();
  const endpoints = new Set<string>();
  for (const rawPeer of expected) {
    const peer = object(rawPeer);
    if (!peer || typeof peer.peer_id !== "string" || !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(peer.peer_id) ||
      peerIds.has(peer.peer_id) || !digest(peer.peer_endpoint_id) || endpoints.has(peer.peer_endpoint_id)) return invalid;
    peerIds.add(peer.peer_id);
    endpoints.add(peer.peer_endpoint_id);
    peerEndpoints.set(peer.peer_id, peer.peer_endpoint_id);
    peers.push({ id: peer.peer_id, rounds: [] });
  }
  for (const roundNumber of [1, 2] as const) {
    const matches = root.rounds.map(object).filter((round) => round?.round === roundNumber);
    const round = matches[0];
    const interval = span(round?.started_at, round?.finished_at);
    if (matches.length !== 1 || !round || !interval || !contained(interval, collection) ||
      !Array.isArray(round.peers) || round.peers.length !== peers.length) return invalid;
    for (const peer of peers) {
      const rows = round.peers.map(object).filter((row) => row?.peer_id === peer.id);
      const row = rows[0];
      const configuration = read(row?.configuration, CONFIG_STATES, interval);
      const view = flags(object(row?.configuration)?.flags);
      const route = read(row?.route, READ_STATES, interval);
      if (rows.length !== 1 || !configuration || !view || !route || row?.peer_endpoint_id !== peerEndpoints.get(peer.id) ||
        (configuration.status === "available" && (Object.keys(expectedView) as Array<keyof ViewFlags>)
          .some((key) => view[key] !== expectedView[key])) ||
        (configuration.status !== "available" && route.status !== "not_attempted")) return invalid;
      peer.rounds.push({ round: roundNumber, span: interval, configuration: { ...configuration, flags: view }, route });
    }
  }
  const comparison = object(root.comparison);
  const compared = dimensions(comparison?.compared_dimensions);
  const different = dimensions(comparison?.differing_dimensions);
  const unknown = dimensions(comparison?.unknown_dimensions);
  const missing = root.missing_facts;
  if (!comparison || !enumValue(comparison.state, ROUTE_STATES) || !compared || !different || !unknown ||
    !Array.isArray(missing) || missing.length > 32 || missing.some((entry) => typeof entry !== "string") ||
    ROUTE_MISSING_FACTS.some((fact) => !missing.includes(fact))) return invalid;
  const statistics = object(root.read_statistics);
  const counters = ["planned_config_rpcs", "started_config_rpcs", "planned_route_rpcs", "started_route_rpcs",
    "known_received_body_bytes", "unknown_response_byte_count", "accepted_body_bytes"] as const;
  if (!statistics || !counters.every((key) => count(statistics[key])) || typeof statistics.bounded !== "boolean" ||
    (statistics.row_work !== null && !count(statistics.row_work))) return invalid;
  const numbers = Object.fromEntries(counters.map((key) => [key, statistics[key]])) as Record<(typeof counters)[number], number>;
  const startedConfig = peers.flatMap((peer) => peer.rounds).filter((round) => round.configuration.span).length;
  const startedRoute = peers.flatMap((peer) => peer.rounds).filter((round) => round.route.status !== "not_attempted" && round.route.span).length;
  if (numbers.planned_config_rpcs !== 2 * peers.length || numbers.planned_route_rpcs !== 2 * peers.length ||
    numbers.started_config_rpcs !== startedConfig || numbers.started_route_rpcs !== startedRoute) return invalid;
  return {
    kind: "summary", topic: root.topic, requested: window, collection, age: root.source_age_seconds,
    state: comparison.state, reason: reason(comparison.reason_code), compared, different, unknown,
    missing: [...new Set(missing.map((fact) => enumValue(fact, ROUTE_MISSING_FACTS) ? fact : "unknown_fact"))], peers,
    statistics: {
      plannedConfig: numbers.planned_config_rpcs, startedConfig: numbers.started_config_rpcs,
      plannedRoute: numbers.planned_route_rpcs, startedRoute: numbers.started_route_rpcs,
      receivedBytes: numbers.known_received_body_bytes, unknownBytes: numbers.unknown_response_byte_count,
      acceptedBytes: numbers.accepted_body_bytes, rows: statistics.row_work as number | null, bounded: statistics.bounded,
    },
  };
}
