import type {
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  CoverageMatrix,
  ServiceStatus,
} from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
    signal,
  });

  if (!response.ok) {
    let code = "source_unavailable";
    try {
      const body = (await response.json()) as { code?: string };
      code = body.code ?? code;
    } catch {
      // The stable status still gives callers a bounded failure signal.
    }
    throw new ApiError(
      response.status,
      code,
      `API request failed with ${response.status}`,
    );
  }

  return (await response.json()) as T;
}

export function listClusters(signal?: AbortSignal): Promise<ClusterSummary[]> {
  return request<ClusterSummary[]>("/v1/clusters", signal);
}

export function getCluster(
  clusterId: string,
  signal?: AbortSignal,
): Promise<ClusterSummary> {
  return request<ClusterSummary>(
    `/v1/clusters/${encodeURIComponent(clusterId)}`,
    signal,
  );
}

export function getClusterCapabilities(
  clusterId: string,
  signal?: AbortSignal,
): Promise<CapabilitySnapshot> {
  return request<CapabilitySnapshot>(
    `/v1/clusters/${encodeURIComponent(clusterId)}/capabilities`,
    signal,
  );
}

export function getCapabilities(
  signal?: AbortSignal,
): Promise<CapabilityCatalogResponse> {
  return request<CapabilityCatalogResponse>("/v1/capabilities", signal);
}

export function getCoverage(signal?: AbortSignal): Promise<CoverageMatrix> {
  return request<CoverageMatrix>("/v1/capabilities/coverage", signal);
}

export function getHealth(signal?: AbortSignal): Promise<ServiceStatus> {
  return request<ServiceStatus>("/healthz", signal);
}

export function getReadiness(signal?: AbortSignal): Promise<ServiceStatus> {
  return request<ServiceStatus>("/readyz", signal);
}

export function stateLabel(state: ClusterSummary["state"]): string {
  const labels: Record<ClusterSummary["state"], string> = {
    pending: "待接入",
    handshaking: "握手中",
    ready_read_only: "只读就绪",
    read_only_degraded: "只读降级",
    rejected: "已拒绝",
    offboarded: "已下线",
  };
  return labels[state];
}
