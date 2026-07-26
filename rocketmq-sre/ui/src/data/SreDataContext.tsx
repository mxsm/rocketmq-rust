import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getCapabilities,
  getClusterCapabilities,
  getCoverage,
  getHealth,
  getReadiness,
  listClusters,
} from "@/api/client";
import type {
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  CoverageMatrix,
  ServiceStatus,
} from "@/api/types";
import {
  demoCapabilities,
  demoCatalog,
  demoClusters,
  demoCoverage,
} from "@/data/demo";

interface SreData {
  clusters: ClusterSummary[];
  health: ServiceStatus["status"];
  readiness: ServiceStatus["status"];
  loading: boolean;
  error?: string;
  demoMode: boolean;
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
  const requested =
    import.meta.env.VITE_SRE_DEMO_MODE === "true" ||
    new URLSearchParams(window.location.search).get("demo") === "1";
  if (requested) {
    window.sessionStorage.setItem("rocketmq-sre-demo", "1");
  }
  return requested || window.sessionStorage.getItem("rocketmq-sre-demo") === "1";
}

export function SreDataProvider({ children }: PropsWithChildren) {
  const demoMode = useMemo(isDemoMode, []);
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
    if (demoMode) {
      setClusters(demoClusters);
      setHealth("healthy");
      setReadiness("ready");
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    const results = await Promise.allSettled([
      listClusters(controller.signal),
      getHealth(controller.signal),
      getReadiness(controller.signal),
    ]);
    const [clusterResult, healthResult, readinessResult] = results;
    if (clusterResult.status === "fulfilled") {
      setClusters(clusterResult.value);
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
  }, [demoMode]);

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
      refresh,
      capability: async (clusterId, signal) => {
        if (demoMode) {
          const capability = demoCapabilities[clusterId];
          if (!capability) {
            throw new Error("该集群没有可用的 capability snapshot");
          }
          return capability;
        }
        return getClusterCapabilities(clusterId, signal);
      },
      coverage: (signal) =>
        demoMode ? Promise.resolve(demoCoverage) : getCoverage(signal),
      catalog: (signal) =>
        demoMode ? Promise.resolve(demoCatalog) : getCapabilities(signal),
    }),
    [
      clusters,
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
