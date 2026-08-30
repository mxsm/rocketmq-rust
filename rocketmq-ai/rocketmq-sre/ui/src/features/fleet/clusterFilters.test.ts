import { describe, expect, it } from "vitest";

import type {
  ClusterSummary,
  FleetHealthReport,
} from "@/api/types";

import {
  filterAndSortClusters,
  projectFleetHealth,
  uniqueClusterValues,
} from "./clusterFilters";

const clusters = [
  cluster("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "orders", "cn-east", "5.3.0"),
  cluster("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "payments", "cn-west", "5.2.0"),
  cluster("cccccccc-cccc-4ccc-8ccc-cccccccccccc", "orders", "cn-east", "5.2.0"),
];

describe("fleet cluster filters", () => {
  it("combines tenant, region and version filters", () => {
    expect(
      filterAndSortClusters(
        clusters,
        {
          environment: "production",
          region: "cn-east",
          tenant: "orders",
          version: "5.2.0",
        },
        fleet(),
      ).map((item) => item.id),
    ).toEqual(["cccccccc-cccc-4ccc-8ccc-cccccccccccc"]);
  });

  it("orders critical and lower-scoring clusters first", () => {
    expect(
      filterAndSortClusters(
        clusters,
        {
          environment: "all",
          region: "all",
          tenant: "all",
          version: "all",
        },
        fleet(),
      ).map((item) => item.id),
    ).toEqual([
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    ]);
  });

  it("projects fleet totals without averaging away the worst cluster", () => {
    const projected = projectFleetHealth(fleet(), [
      clusters[0]!,
      clusters[2]!,
    ]);

    expect(projected).toMatchObject({
      cluster_count: 2,
      status: "degraded",
      score: 72,
      data_quality: "stale",
      worst_cluster_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
      healthy_clusters: 1,
      degraded_clusters: 1,
    });
    expect(projected?.clusters).toHaveLength(2);
  });

  it("returns stable unique filter values", () => {
    expect(
      uniqueClusterValues(clusters, (item) => item.rocketmq_version),
    ).toEqual(["5.2.0", "5.3.0"]);
  });
});

function cluster(
  id: string,
  tenant: string,
  region: string,
  version: string,
): ClusterSummary {
  return {
    id,
    tenant_id: tenant,
    external_cluster_key: `${tenant}-${region}`,
    environment: "production",
    region,
    rocketmq_version: version,
    deployment_mode: "bare-metal",
    owner: "messaging",
    state: "ready_read_only",
    effective_access_profile: "read_only",
    updated_at: "2026-07-27T08:00:00Z",
  };
}

function fleet(): FleetHealthReport {
  return {
    schema_version: "rocketmq-sre.fleet-health.v1",
    tenant_id: "orders",
    region: null,
    observed_at: "2026-07-27T08:00:00Z",
    status: "critical",
    score: 41,
    data_quality: "partial",
    aggregation: "worst_cluster",
    cluster_count: 3,
    healthy_clusters: 1,
    degraded_clusters: 1,
    critical_clusters: [
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ],
    unknown_clusters: [],
    maintenance_clusters: [],
    fault_drill_clusters: [],
    worst_cluster_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    clusters: [
      {
        cluster_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        external_cluster_key: "orders-cn-east",
        region: "cn-east",
        status: "healthy",
        score: 96,
        data_quality: "complete",
        operational_state: "normal",
        critical_incidents: 0,
        triggered_sli_ids: [],
        observed_at: "2026-07-27T08:00:00Z",
      },
      {
        cluster_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        external_cluster_key: "payments-cn-west",
        region: "cn-west",
        status: "critical",
        score: 41,
        data_quality: "partial",
        operational_state: "normal",
        critical_incidents: 2,
        triggered_sli_ids: ["store-availability"],
        observed_at: "2026-07-27T08:00:00Z",
      },
      {
        cluster_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        external_cluster_key: "orders-cn-east-legacy",
        region: "cn-east",
        status: "degraded",
        score: 72,
        data_quality: "stale",
        operational_state: "maintenance",
        critical_incidents: 0,
        triggered_sli_ids: ["consumer-lag"],
        observed_at: "2026-07-27T08:00:00Z",
      },
    ],
  };
}
