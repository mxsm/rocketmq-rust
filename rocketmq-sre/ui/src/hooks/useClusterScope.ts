import { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";

import type { AssetKind } from "@/api/types";
import { useSreData } from "@/data/SreDataContext";

const RESOURCE_KINDS = new Set<AssetKind | "cluster">([
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
]);
const CONTEXT_KEYS = ["cluster_id", "resource_kind", "resource_key"];

export interface ReadOnlyUrlContext {
  clusterId: string;
  resourceKind?: AssetKind | "cluster";
  resourceKey?: string;
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
  if (!CONTEXT_KEYS.some((key) => params.has(key))) {
    return { status: "none" };
  }
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
    (RESOURCE_KINDS.has(resourceKind as AssetKind | "cluster") &&
      resourceKey.length > 0 &&
      resourceKey.length <= 512 &&
      ![...resourceKey].some((character) => {
        const code = character.charCodeAt(0);
        return code <= 31 || code === 127;
      }));
  if (!validCluster || !validResource) {
    return { status: "invalid" };
  }
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
  CONTEXT_KEYS.forEach((key) => params.delete(key));
  const value = params.toString();
  return value ? `?${value}` : "";
}

export function useClusterScope() {
  const { clusters } = useSreData();
  const location = useLocation();
  const [clusterId, setClusterId] = useState("");
  const urlContext = useMemo(
    () =>
      parseReadOnlyUrlContext(
        location.search,
        clusters.map((cluster) => cluster.id),
      ),
    [clusters, location.search],
  );

  useEffect(() => {
    if (clusters.length === 0) {
      setClusterId("");
      return;
    }
    const requested =
      urlContext.status === "valid"
        ? urlContext.context.clusterId
        : undefined;
    if (requested && requested !== clusterId) {
      setClusterId(requested);
    } else if (!clusters.some((cluster) => cluster.id === clusterId)) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters, urlContext]);

  return useMemo(
    () => ({
      clusterId,
      setClusterId,
      cluster: clusters.find((item) => item.id === clusterId),
      clusters,
      urlContext:
        urlContext.status === "valid" ? urlContext.context : undefined,
    }),
    [clusterId, clusters, urlContext],
  );
}
