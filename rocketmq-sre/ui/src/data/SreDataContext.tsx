import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

import { createHttpSreApi, type SreApi } from "@/api/client";
import type {
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  CoverageMatrix,
  ServiceStatus,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { createMockSreApi } from "@/data/mockApi";

interface SreData {
  clusters: ClusterSummary[];
  health: ServiceStatus["status"];
  readiness: ServiceStatus["status"];
  loading: boolean;
  error?: string;
  demoMode: boolean;
  api: SreApi;
  refresh: () => Promise<void>;
  capability: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CapabilitySnapshot>;
  coverage: (signal?: AbortSignal) => Promise<CoverageMatrix>;
  catalog: (signal?: AbortSignal) => Promise<CapabilityCatalogResponse>;
}

const SreDataContext = createContext<SreData | undefined>(undefined);

function isDemoMode() {
  if (!import.meta.env.DEV) {
    return false;
  }
  if (import.meta.env.VITE_SRE_API_MODE === "mock") {
    return true;
  }
  const requested =
    import.meta.env.VITE_SRE_DEMO_MODE === "true" ||
    new URLSearchParams(window.location.search).get("demo") === "1";
  if (requested) {
    window.sessionStorage.setItem("rocketmq-sre-demo", "1");
  }
  return requested || window.sessionStorage.getItem("rocketmq-sre-demo") === "1";
}

export function SreDataProvider({ children }: PropsWithChildren) {
  const auth = useAuth();
  const demoMode = useMemo(isDemoMode, []);
  const api = useMemo(
    () =>
      demoMode
        ? createMockSreApi(auth.requestContext)
        : createHttpSreApi(auth.requestContext),
    [auth.requestContext, demoMode],
  );
  const [clusters, setClusters] = useState<ClusterSummary[]>([]);
  const [health, setHealth] =
    useState<ServiceStatus["status"]>("unavailable");
  const [readiness, setReadiness] =
    useState<ServiceStatus["status"]>("unavailable");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    const controller = new AbortController();
    const results = await Promise.allSettled([
      api.listClusters(controller.signal),
      api.getHealth(controller.signal),
      api.getReadiness(controller.signal),
    ]);
    const [clusterResult, healthResult, readinessResult] = results;
    if (clusterResult.status === "fulfilled") {
      setClusters(
        clusterResult.value.filter((cluster) =>
          auth.hasClusterScope(cluster.id),
        ),
      );
    }
    if (healthResult.status === "fulfilled") {
      setHealth(healthResult.value.status);
    }
    if (readinessResult.status === "fulfilled") {
      setReadiness(readinessResult.value.status);
    }
    if (results.some((result) => result.status === "rejected")) {
      setError("部分只读数据源暂不可用；未返回的数据不会按 0 处理。");
    }
    setLoading(false);
  }, [api, auth]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const value = useMemo<SreData>(
    () => ({
      clusters,
      health,
      readiness,
      loading,
      error,
      demoMode,
      api,
      refresh,
      capability: (clusterId, signal) =>
        api.getClusterCapabilities(clusterId, signal),
      coverage: (signal) => api.getCoverage(signal),
      catalog: (signal) => api.getCapabilities(signal),
    }),
    [
      clusters,
      api,
      demoMode,
      error,
      health,
      loading,
      readiness,
      refresh,
    ],
  );

  return (
    <SreDataContext.Provider value={value}>{children}</SreDataContext.Provider>
  );
}

export function useSreData() {
  const value = useContext(SreDataContext);
  if (!value) {
    throw new Error("useSreData must be used within SreDataProvider");
  }
  return value;
}
