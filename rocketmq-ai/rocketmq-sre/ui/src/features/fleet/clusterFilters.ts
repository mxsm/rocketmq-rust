import type {
  ClusterSummary,
  FleetHealthReport,
  HealthDataQuality,
  HealthStatus,
} from "@/api/types";

export interface ClusterFilters {
  environment: string;
  region: string;
  tenant: string;
  version: string;
}

export function filterAndSortClusters(
  clusters: ClusterSummary[],
  filters: ClusterFilters,
  fleet?: FleetHealthReport,
): ClusterSummary[] {
  const healthByCluster = new Map(
    fleet?.clusters.map((cluster) => [cluster.cluster_id, cluster]),
  );

  return clusters
    .filter(
      (cluster) =>
        matches(filters.environment, cluster.environment) &&
        matches(filters.region, cluster.region) &&
        matches(filters.tenant, cluster.tenant_id) &&
        matches(filters.version, cluster.rocketmq_version),
    )
    .sort((left, right) => {
      const leftHealth = healthByCluster.get(left.id);
      const rightHealth = healthByCluster.get(right.id);
      return (
        healthRank(rightHealth?.status) -
          healthRank(leftHealth?.status) ||
        (leftHealth?.score ?? -1) - (rightHealth?.score ?? -1) ||
        left.external_cluster_key.localeCompare(
          right.external_cluster_key,
        )
      );
    });
}

export function projectFleetHealth(
  fleet: FleetHealthReport | undefined,
  clusters: ClusterSummary[],
): FleetHealthReport | undefined {
  if (!fleet) {
    return undefined;
  }

  const clusterIds = new Set(clusters.map((cluster) => cluster.id));
  const visible = fleet.clusters.filter((cluster) =>
    clusterIds.has(cluster.cluster_id),
  );
  const worst = [...visible].sort(
    (left, right) =>
      healthRank(right.status) - healthRank(left.status) ||
      (left.score ?? -1) - (right.score ?? -1),
  )[0];
  const lowestQuality = [...visible].sort(
    (left, right) =>
      dataQualityRank(right.data_quality) -
      dataQualityRank(left.data_quality),
  )[0]?.data_quality;

  return {
    ...fleet,
    cluster_count: visible.length,
    clusters: visible,
    critical_clusters: visible
      .filter((cluster) => cluster.status === "critical")
      .map((cluster) => cluster.cluster_id),
    degraded_clusters: visible.filter(
      (cluster) => cluster.status === "degraded",
    ).length,
    healthy_clusters: visible.filter(
      (cluster) => cluster.status === "healthy",
    ).length,
    unknown_clusters: visible
      .filter((cluster) => cluster.status === "unknown")
      .map((cluster) => cluster.cluster_id),
    maintenance_clusters: fleet.maintenance_clusters.filter((id) =>
      clusterIds.has(id),
    ),
    fault_drill_clusters: fleet.fault_drill_clusters.filter((id) =>
      clusterIds.has(id),
    ),
    status: worst?.status ?? "unknown",
    score: worst?.score ?? null,
    data_quality: lowestQuality ?? "missing",
    worst_cluster_id: worst?.cluster_id ?? null,
  };
}

export function uniqueClusterValues(
  clusters: ClusterSummary[],
  select: (cluster: ClusterSummary) => string,
): string[] {
  return [...new Set(clusters.map(select).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b),
  );
}

function matches(filter: string, value: string) {
  return filter === "all" || filter === value;
}

function healthRank(status?: HealthStatus) {
  return {
    healthy: 0,
    unknown: 1,
    degraded: 2,
    critical: 3,
  }[status ?? "unknown"];
}

function dataQualityRank(quality: HealthDataQuality) {
  return {
    complete: 0,
    partial: 1,
    stale: 2,
    missing: 3,
  }[quality];
}

export function dataQualityLabel(quality?: HealthDataQuality) {
  return {
    complete: "完整",
    partial: "部分",
    stale: "过期",
    missing: "缺失",
  }[quality ?? "missing"];
}
