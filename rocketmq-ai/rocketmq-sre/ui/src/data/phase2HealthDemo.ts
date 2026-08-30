import type {
  ClusterHealthReport,
  FleetHealthReport,
  HealthDataQuality,
  HealthStatus,
  SloDimension,
} from "@/api/types";
import { demoClusters } from "@/data/demo";

const OBSERVED_AT = "2026-07-27T08:42:10Z";
const EVIDENCE_ID = "40000000-0000-4000-8000-000000000001";
const DIMENSIONS: SloDimension[] = [
  "traffic",
  "consumer",
  "broker",
  "store",
  "ha_controller",
  "routing_proxy",
  "security",
  "platform",
];

function dimensions(
  scores: Partial<Record<SloDimension, number>>,
  quality: HealthDataQuality,
) {
  return DIMENSIONS.map((dimension) => {
    const score = scores[dimension];
    const status: HealthStatus =
      score === undefined
        ? "unknown"
        : score < 50
          ? "critical"
          : score < 80
            ? "degraded"
            : "healthy";
    return {
      dimension,
      weight:
        dimension === "consumer" || dimension === "store"
          ? 15
          : dimension === "security"
            ? 10
            : 12,
      score,
      status,
      data_quality: score === undefined ? ("missing" as const) : quality,
      triggered_sli_ids:
        status === "critical" || status === "degraded"
          ? [`${dimension}_health`]
          : [],
      evidence_ids: score === undefined ? [] : [EVIDENCE_ID],
      reason_codes:
        score === undefined
          ? ["missing_signal"]
          : status === "healthy"
            ? []
            : ["burn_rate_triggered"],
    };
  });
}

export const demoClusterHealth: Record<string, ClusterHealthReport> = {
  [demoClusters[0].id]: {
    schema_version: "rocketmq-sre.cluster-health.v1",
    id: "61000000-0000-4000-8000-000000000001",
    tenant_id: demoClusters[0].tenant_id,
    cluster_id: demoClusters[0].id,
    score: 94,
    status: "healthy",
    data_quality: "complete",
    operational_state: "normal",
    dimensions: dimensions(
      {
        traffic: 97,
        consumer: 92,
        broker: 96,
        store: 91,
        ha_controller: 95,
        routing_proxy: 94,
        security: 98,
        platform: 90,
      },
      "complete",
    ),
    slis: [
      {
        id: "delivery_ratio",
        display_name: "Delivery ratio",
        dimension: "traffic",
        objective: 0.999,
        status: "healthy",
        data_quality: "complete",
        windows: [
          {
            window_id: "fast",
            short_window_seconds: 300,
            long_window_seconds: 3600,
            short_burn_rate: 0.6,
            long_burn_rate: 0.4,
            threshold: 14.4,
            severity: "critical",
            triggered: false,
            data_quality: "complete",
            observed_at: OBSERVED_AT,
            evidence_ids: [EVIDENCE_ID],
            reason_codes: [],
          },
        ],
        evidence_ids: [EVIDENCE_ID],
        reason_codes: [],
      },
    ],
    incident_summary: {
      active_incidents: 0,
      critical_incidents: 0,
      unassigned_incidents: 0,
      last_alert_at: null,
    },
    triggered_sli_ids: [],
    evidence_ids: [EVIDENCE_ID],
    recent_changes: [],
    algorithm_version: "rocketmq-sre.health-score.v1",
    model_adjustment_supported: false,
    execution_eligible: false,
    observed_at: OBSERVED_AT,
  },
  [demoClusters[1].id]: {
    schema_version: "rocketmq-sre.cluster-health.v1",
    id: "61000000-0000-4000-8000-000000000002",
    tenant_id: demoClusters[1].tenant_id,
    cluster_id: demoClusters[1].id,
    score: 58,
    status: "critical",
    data_quality: "partial",
    operational_state: "maintenance",
    dimensions: dimensions(
      {
        traffic: 88,
        consumer: 31,
        broker: 76,
        store: 54,
        ha_controller: 83,
        routing_proxy: 71,
        security: 96,
      },
      "partial",
    ),
    slis: [
      {
        id: "consumer_lag_backlog",
        display_name: "Consumer lag backlog",
        dimension: "consumer",
        objective: 0.99,
        status: "critical",
        data_quality: "partial",
        windows: [
          {
            window_id: "fast",
            short_window_seconds: 300,
            long_window_seconds: 3600,
            short_burn_rate: 17.2,
            long_burn_rate: 15.1,
            threshold: 14.4,
            severity: "critical",
            triggered: true,
            data_quality: "partial",
            observed_at: OBSERVED_AT,
            evidence_ids: [EVIDENCE_ID],
            reason_codes: ["partial_evidence", "burn_rate_triggered"],
          },
          {
            window_id: "medium",
            short_window_seconds: 1800,
            long_window_seconds: 21600,
            short_burn_rate: 7.4,
            long_burn_rate: 6.7,
            threshold: 6,
            severity: "warning",
            triggered: true,
            data_quality: "partial",
            observed_at: OBSERVED_AT,
            evidence_ids: [EVIDENCE_ID],
            reason_codes: ["partial_evidence", "burn_rate_triggered"],
          },
          {
            window_id: "slow",
            short_window_seconds: 21600,
            long_window_seconds: 259200,
            short_burn_rate: 0.8,
            long_burn_rate: 0.6,
            threshold: 1,
            severity: "warning",
            triggered: false,
            data_quality: "partial",
            observed_at: OBSERVED_AT,
            evidence_ids: [EVIDENCE_ID],
            reason_codes: ["partial_evidence"],
          },
        ],
        evidence_ids: [EVIDENCE_ID],
        reason_codes: ["burn_rate_triggered", "partial_evidence"],
      },
    ],
    incident_summary: {
      active_incidents: 2,
      critical_incidents: 1,
      unassigned_incidents: 1,
      last_alert_at: "2026-07-27T08:40:00Z",
    },
    triggered_sli_ids: ["consumer_lag_backlog"],
    evidence_ids: [EVIDENCE_ID],
    recent_changes: [
      {
        previous_score: 86,
        current_score: 58,
        score_delta: -28,
        previous_status: "healthy",
        current_status: "critical",
        occurred_at: "2026-07-27T08:40:00Z",
      },
    ],
    algorithm_version: "rocketmq-sre.health-score.v1",
    model_adjustment_supported: false,
    execution_eligible: false,
    observed_at: OBSERVED_AT,
  },
  [demoClusters[2].id]: {
    schema_version: "rocketmq-sre.cluster-health.v1",
    id: "61000000-0000-4000-8000-000000000003",
    tenant_id: demoClusters[2].tenant_id,
    cluster_id: demoClusters[2].id,
    score: null,
    status: "unknown",
    data_quality: "missing",
    operational_state: "fault_drill",
    dimensions: dimensions({}, "missing"),
    slis: [],
    incident_summary: {
      active_incidents: 0,
      critical_incidents: 0,
      unassigned_incidents: 0,
      last_alert_at: null,
    },
    triggered_sli_ids: [],
    evidence_ids: [],
    recent_changes: [],
    algorithm_version: "rocketmq-sre.health-score.v1",
    model_adjustment_supported: false,
    execution_eligible: false,
    observed_at: OBSERVED_AT,
  },
};

export function demoFleetHealth(
  allowedClusterIds?: string[],
  region?: string,
): FleetHealthReport {
  const clusters = demoClusters
    .filter(
      (cluster) =>
        (!allowedClusterIds || allowedClusterIds.includes(cluster.id)) &&
        (!region || cluster.region === region),
    )
    .map((cluster) => {
      const health = demoClusterHealth[cluster.id];
      return {
        cluster_id: cluster.id,
        external_cluster_key: cluster.external_cluster_key,
        region: cluster.region,
        score: health?.score,
        status: health?.status ?? ("unknown" as const),
        data_quality: health?.data_quality ?? ("missing" as const),
        operational_state: health?.operational_state ?? ("normal" as const),
        critical_incidents: health?.incident_summary.critical_incidents ?? 0,
        triggered_sli_ids: health?.triggered_sli_ids ?? [],
        observed_at: health?.observed_at ?? OBSERVED_AT,
      };
    });
  const rank: Record<HealthStatus, number> = {
    healthy: 0,
    unknown: 1,
    degraded: 2,
    critical: 3,
  };
  const worst = clusters.reduce<(typeof clusters)[number] | undefined>(
    (current, cluster) =>
      !current ||
      rank[cluster.status] > rank[current.status] ||
      (rank[cluster.status] === rank[current.status] &&
        (cluster.score ?? -1) < (current.score ?? -1))
        ? cluster
        : current,
    undefined,
  );
  const hasMissing = clusters.some(
    (cluster) => cluster.data_quality === "missing",
  );
  const quality: HealthDataQuality = hasMissing
    ? "missing"
    : clusters.some((cluster) => cluster.data_quality === "stale")
      ? "stale"
      : clusters.some((cluster) => cluster.data_quality === "partial")
        ? "partial"
        : "complete";
  return {
    schema_version: "rocketmq-sre.fleet-health.v1",
    tenant_id: demoClusters[0].tenant_id,
    region: region ?? null,
    score: hasMissing ? null : worst?.score,
    status: worst?.status ?? "unknown",
    data_quality: clusters.length === 0 ? "missing" : quality,
    worst_cluster_id: worst?.cluster_id ?? null,
    cluster_count: clusters.length,
    healthy_clusters: clusters.filter((cluster) => cluster.status === "healthy")
      .length,
    degraded_clusters: clusters.filter(
      (cluster) => cluster.status === "degraded",
    ).length,
    critical_clusters: clusters
      .filter((cluster) => cluster.status === "critical")
      .map((cluster) => cluster.cluster_id),
    unknown_clusters: clusters
      .filter((cluster) => cluster.status === "unknown")
      .map((cluster) => cluster.cluster_id),
    maintenance_clusters: clusters
      .filter((cluster) => cluster.operational_state === "maintenance")
      .map((cluster) => cluster.cluster_id),
    fault_drill_clusters: clusters
      .filter((cluster) => cluster.operational_state === "fault_drill")
      .map((cluster) => cluster.cluster_id),
    clusters,
    aggregation: "worst_cluster_no_average_masking",
    observed_at: OBSERVED_AT,
  };
}
