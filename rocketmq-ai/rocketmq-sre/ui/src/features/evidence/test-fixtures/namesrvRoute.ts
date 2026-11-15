import { phase1Evidence } from "@/data/phase1Demo";

/** Synthetic display data. It supplies no collector, independence or production proof. */
export function namesrvRouteFixture() {
  const endpointId = (peer: string) => `sha256:${(peer === "ns-a" ? "a" : "b").repeat(64)}`;
  const evidence = {
    ...phase1Evidence.items[0],
    source: "admin-query",
    resource: "namesrv-route/orders",
    exposure: "admin_rpc" as const,
    time_range: { start: "2026-07-27T08:55:00Z", end: "2026-07-27T09:00:00Z" },
    observed_at: "2026-07-27T09:00:01.100000Z",
    freshness_seconds: 1,
    partial: true,
  };
  const read = (round: number, peer: string) => {
    const start = round === 1 ? "2026-07-27T09:00:01.000000Z" : "2026-07-27T09:00:01.050000Z";
    const end = round === 1 ? "2026-07-27T09:00:01.050000Z" : evidence.observed_at;
    return {
      peer_id: peer,
      peer_endpoint_id: endpointId(peer),
      configuration: {
        status: "available", reason_code: "view_configuration_observed",
        flags: { cluster_test: false as boolean | null, order_message_enable: false as boolean | null, support_acting_master: false as boolean | null },
        started_at: start as string | null, finished_at: end as string | null, received_body_bytes: 180,
      },
      route: {
        status: peer === "ns-a" ? "found" : "topic_absent",
        reason_code: peer === "ns-a" ? "route_observed" : "topic_absent",
        started_at: start as string | null, finished_at: end as string | null,
        received_body_bytes: peer === "ns-a" ? 350 : 0,
        normalized: null as unknown,
      },
    };
  };
  const payload = {
    schema_version: "rocketmq.sre-namesrv-route-observation.v1",
    profile: "nameserver_route_comparison",
    tenant_id: evidence.tenant_id, cluster_id: evidence.cluster_id, topic: "orders",
    requested_window: { ...evidence.time_range },
    collection_started_at: "2026-07-27T09:00:01.000000Z",
    collected_at: evidence.observed_at,
    source_age_seconds: 1,
    temporal_semantics: "current_context_only", historical_coverage: "unknown",
    configuration: {
      independence_source: "operator_declared_direct_peers",
      expected_view: { cluster_test: false, order_message_enable: false, support_acting_master: false },
      peers: ["ns-a", "ns-b"].map((peer) => ({ peer_id: peer, peer_endpoint_id: endpointId(peer) })),
    },
    rounds: [1, 2].map((round) => ({
      round,
      started_at: round === 1 ? "2026-07-27T09:00:01.000000Z" : "2026-07-27T09:00:01.050000Z",
      finished_at: round === 1 ? "2026-07-27T09:00:01.050000Z" : evidence.observed_at,
      peers: ["ns-a", "ns-b"].map((peer) => read(round, peer)),
    })),
    comparison: {
      state: "stable_observed_difference", reason_code: "current_route_difference_observed",
      compared_dimensions: ["topic_presence"], differing_dimensions: ["topic_presence"], unknown_dimensions: [] as string[],
    },
    read_statistics: {
      planned_config_rpcs: 4, started_config_rpcs: 4, planned_route_rpcs: 4, started_route_rpcs: 4,
      known_received_body_bytes: 1420, unknown_response_byte_count: 0, accepted_body_bytes: 1420, row_work: 24 as number | null, bounded: false,
    },
    missing_facts: [
      "registration.first_registration_time", "registration.last_registration_time", "registration.last_heartbeat_time",
      "route.source_generation_time", "route.source_generation", "route.source_instance_id",
      "route.local_origin_atomic_proof", "broker.scope_bound_reachability", "historical_window_coverage",
    ],
  };
  return { evidence, payload };
}
