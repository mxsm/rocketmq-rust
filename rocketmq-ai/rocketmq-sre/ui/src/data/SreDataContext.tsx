import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
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
import { createApiRelay } from "./apiRelay";

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
const EMPTY_CLUSTERS: ClusterSummary[] = [];

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
  return (
    requested || window.sessionStorage.getItem("rocketmq-sre-demo") === "1"
  );
}

export function SreDataProvider({ children }: PropsWithChildren) {
  const auth = useAuth();
  const demoMode = useMemo(isDemoMode, []);
  const requestApi = useMemo(
    () =>
      demoMode
        ? createMockSreApi(auth.requestContext)
        : createHttpSreApi(auth.requestContext),
    [auth.requestContext, demoMode],
  );
  const identityKey = JSON.stringify([
    auth.session?.tenantId,
    auth.session?.subject,
    [...(auth.session?.clusterIds ?? [])].sort(),
    [...(auth.session?.roles ?? [])].sort(),
    demoMode,
  ]);
  const relay = useMemo(
    () => ({ identityKey, ...createApiRelay() }),
    [identityKey],
  );
  useLayoutEffect(() => {
    relay.replace(requestApi);
    return () => relay.replace(undefined);
  }, [relay, requestApi]);
  const api = relay.api;
  const [snapshot, setSnapshot] = useState<{
    owner: string;
    clusters: ClusterSummary[];
    health: ServiceStatus["status"];
    readiness: ServiceStatus["status"];
    loading: boolean;
    error?: string;
  }>();
  const request = useRef<{ generation: number; controller?: AbortController }>({
    generation: 0,
  });

  const refresh = useCallback(async () => {
    request.current.controller?.abort();
    const generation = ++request.current.generation;
    const controller = new AbortController();
    request.current.controller = controller;
    setSnapshot((previous) =>
      previous?.owner === identityKey
        ? { ...previous, loading: true, error: undefined }
        : {
            owner: identityKey,
            clusters: [],
            health: "unavailable",
            readiness: "unavailable",
            loading: true,
          },
    );
    const results = await Promise.allSettled([
      requestApi.listClusters(controller.signal),
      requestApi.getHealth(controller.signal),
      requestApi.getReadiness(controller.signal),
    ]);
    const [clusterResult, healthResult, readinessResult] = results;
    if (controller.signal.aborted || request.current.generation !== generation)
      return;
    setSnapshot({
      owner: identityKey,
      clusters:
        clusterResult.status === "fulfilled"
          ? clusterResult.value.filter(
              (cluster) =>
                auth.hasClusterScope(cluster.id) &&
                cluster.tenant_id === auth.session?.tenantId,
            )
          : [],
      health:
        healthResult.status === "fulfilled"
          ? healthResult.value.status
          : "unavailable",
      readiness:
        readinessResult.status === "fulfilled"
          ? readinessResult.value.status
          : "unavailable",
      loading: false,
      error: results.some((result) => result.status === "rejected")
        ? "部分只读数据源暂不可用；未返回的数据不会按 0 处理。"
        : undefined,
    });
  }, [auth, identityKey, requestApi]);

  useEffect(() => {
    const ownedRequest = request.current;
    void refresh();
    return () => {
      ownedRequest.controller?.abort();
      ownedRequest.generation += 1;
    };
  }, [refresh]);

  // Principal boundaries hide old snapshots synchronously; renewing a token for
  // the same principal preserves the workspace and uses the latest request API.
  const current = snapshot?.owner === identityKey ? snapshot : undefined;
  const clusters = current?.clusters ?? EMPTY_CLUSTERS;
  const health = current?.health ?? "unavailable";
  const readiness = current?.readiness ?? "unavailable";
  const loading = current?.loading ?? true;
  const error = current?.error;

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
    [clusters, api, demoMode, error, health, loading, readiness, refresh],
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
