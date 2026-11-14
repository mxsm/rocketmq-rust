import { phase1Evidence } from "@/data/phase1Demo";

/** Synthetic display fixture, without collector proof or production verification. */
export function sendLatencyFixture() {
  const evidence = {
    ...phase1Evidence.items[0],
    source: "prometheus",
    resource: "send-latency/orders",
    exposure: "prometheus_api" as const,
    time_range: { start: "2026-07-27T08:55:00Z", end: "2026-07-27T09:00:00Z" },
    observed_at: "2026-07-27T09:00:01Z",
    freshness_seconds: 6,
    partial: true,
  };
  const payload = {
    schema_version: "rocketmq.sre-diagnostic-source.v1",
    profile: "send_latency",
    observation_schema: "rocketmq.send-latency-window.v1",
    measurement_kind: "classic_histogram_window_estimate",
    unit: "milliseconds",
    scope: {
      tenant_id: evidence.tenant_id,
      cluster_id: evidence.cluster_id,
      canonical_resource: evidence.resource,
      topic: "orders",
    },
    window: {
      requested_start: evidence.time_range.start, requested_end: evidence.time_range.end,
      query_start: evidence.time_range.start, query_end: evidence.time_range.end,
      interval: "left_open_right_closed", time_resolution_millis: 1,
      data_first_at: "2026-07-27T08:55:05Z", data_last_at: "2026-07-27T08:59:55Z",
    },
    collected_at: evidence.observed_at,
    source_age_seconds: 6,
    method: {
      name: "classic_linear_p99_strict.v1",
      counter_method: "prometheus_increase_per_series",
      grid_ref: "otel_default_explicit_ms.v1", quantile: 0.99,
    },
    segments: ["client", "proxy", "remoting", "broker", "store"].map((id) => ({
      id,
      availability: id === "broker" ? "available" : "unsupported_scope",
      reason_code: id === "broker" ? "broker_histogram_window_available" : "exact_topic_scope_unavailable",
      observation: id === "broker" ? {
        quantile: "p99", estimate_millis: 248.5, lower_bound_millis: 100,
        upper_bound_millis: 250 as number | null, resolution: "finite_bucket",
        observed_increments: 200, estimated_increments: 206.89655,
        observed_nodes: 1, scrape_samples: 59, observed_resets: 0,
      } : null,
    })),
    policy: {
      status: "exceeded", reason_code: "estimate_exceeds_operator_policy",
      source: "operator_configuration", scope: {
        cluster_id: evidence.cluster_id, topic: "orders", canonical_resource: evidence.resource,
      },
      unit: "milliseconds", comparison: "strictly_greater", p99_millis: 200,
    },
    partial: true,
  };
  return { evidence, payload };
}
