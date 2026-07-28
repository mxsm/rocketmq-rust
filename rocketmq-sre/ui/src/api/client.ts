import type { ApiRequestContext } from "@/auth/AuthContext";

import type {
  AssetSnapshot,
  ActionItem,
  ActionItemPage,
  ActionItemPatchRequest,
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  ClusterForecastReport,
  CollectionEnvelope,
  ConversationView,
  CoverageMatrix,
  CreateConversationRequest,
  CreatePostmortemRequest,
  CreateInspectionRequest,
  DiagnosisDispatch,
  DrReadinessReport,
  EvidenceRecord,
  FleetHealthReport,
  IncidentTopologyView,
  IncidentOperationRequest,
  IncidentOperationResult,
  IncidentOperationsState,
  IncidentView,
  InspectionReport,
  InspectionView,
  InvestigationView,
  KnowledgeItem,
  MessageJourney,
  ModelCapabilitiesResponse,
  ModelProfileLifecyclePage,
  ModelProfileLifecycleTransitionRequest,
  ModelProfileLifecycleView,
  ModelProfileRollbackRequest,
  OnboardClusterRequest,
  OnboardOutcome,
  OperationsReport,
  OperationsReportWindow,
  Phase2ContractManifest,
  PostmortemPatchRequest,
  PostmortemPublishRequest,
  PostmortemView,
  ClusterHealthReport,
  PromoteInvestigationRequest,
  Recommendation,
  RecommendationDispositionRequest,
  ProviderSmokeResult,
  ServiceStatus,
  ShiftHandoffSummary,
  TopologySnapshot,
  UpgradeReadinessReport,
  WhatIfSimulation,
  WhatIfSimulationRequest,
  WorkflowStreamEvent,
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

export interface RequestOptions {
  signal?: AbortSignal;
  auth?: ApiRequestContext;
  method?: "GET" | "POST" | "PATCH";
  body?: unknown;
}

function requestHeaders(options: RequestOptions): Headers {
  const headers = new Headers({ Accept: "application/json" });
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json");
  }
  if (options.auth) {
    headers.set("Authorization", `Bearer ${options.auth.token}`);
    headers.set("X-RocketMQ-Tenant", options.auth.tenantId);
    headers.set("X-RocketMQ-Clusters", options.auth.clusterIds.join(","));
    headers.set("X-RocketMQ-Subject", options.auth.subject);
  }
  return headers;
}

export async function apiRequest<T>(
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const response = await fetch(path, {
    method: options.method ?? "GET",
    headers: requestHeaders(options),
    credentials: "same-origin",
    body:
      options.body === undefined ? undefined : JSON.stringify(options.body),
    signal: options.signal,
  });

  if (!response.ok) {
    let code = "source_unavailable";
    let message = `API request failed with ${response.status}`;
    try {
      const body = (await response.json()) as {
        code?: string;
        message?: string;
      };
      code = body.code ?? code;
      message = body.message ?? message;
    } catch {
      // The stable status still gives callers a bounded failure signal.
    }
    throw new ApiError(response.status, code, message);
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

async function download(
  path: string,
  auth?: ApiRequestContext,
  signal?: AbortSignal,
): Promise<Blob> {
  const response = await fetch(path, {
    headers: requestHeaders({ auth }),
    credentials: "same-origin",
    signal,
  });
  if (!response.ok) {
    throw new ApiError(
      response.status,
      response.status === 403 ? "cluster_not_allowed" : "source_unavailable",
      "operations report download is unavailable",
    );
  }
  return response.blob();
}

export function apiQuery(
  path: string,
  values: Record<string, string | undefined>,
): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value) {
      params.set(key, value);
    }
  }
  const suffix = params.toString();
  return suffix ? `${path}?${suffix}` : path;
}

function collection<T>(
  value: CollectionEnvelope<T> | T[],
): CollectionEnvelope<T> {
  return Array.isArray(value)
    ? {
        items: value,
        partial: false,
        warnings: [],
        observed_at: new Date().toISOString(),
      }
    : value;
}

export interface SreApi {
  listClusters: (signal?: AbortSignal) => Promise<ClusterSummary[]>;
  getCluster: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<ClusterSummary>;
  getClusterCapabilities: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CapabilitySnapshot>;
  getCapabilities: (
    signal?: AbortSignal,
  ) => Promise<CapabilityCatalogResponse>;
  getPhase2Contract: (
    signal?: AbortSignal,
  ) => Promise<Phase2ContractManifest>;
  getCoverage: (signal?: AbortSignal) => Promise<CoverageMatrix>;
  getHealth: (signal?: AbortSignal) => Promise<ServiceStatus>;
  getReadiness: (signal?: AbortSignal) => Promise<ServiceStatus>;
  getClusterSlo: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<ClusterHealthReport>;
  getClusterHealth: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<ClusterHealthReport>;
  getFleetHealth: (
    region?: string,
    signal?: AbortSignal,
  ) => Promise<FleetHealthReport>;
  getClusterForecasts: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<ClusterForecastReport>;
  runSimulation: (
    input: WhatIfSimulationRequest,
    signal?: AbortSignal,
  ) => Promise<WhatIfSimulation>;
  getUpgradeReadiness: (
    clusterId: string,
    targetVersion: string,
    signal?: AbortSignal,
  ) => Promise<UpgradeReadinessReport>;
  getDrReadiness: (
    clusterId: string,
    targetRegion?: string,
    signal?: AbortSignal,
  ) => Promise<DrReadinessReport>;
  onboardCluster: (
    input: OnboardClusterRequest,
    signal?: AbortSignal,
  ) => Promise<OnboardOutcome>;
  listAssets: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<AssetSnapshot>>;
  getTopology: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<TopologySnapshot>;
  listConversations: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<ConversationView>>;
  getConversation: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<ConversationView>;
  createConversation: (
    input: CreateConversationRequest,
    signal?: AbortSignal,
  ) => Promise<ConversationView>;
  listInvestigations: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<InvestigationView>>;
  getInvestigation: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<InvestigationView>;
  promoteInvestigation: (
    id: string,
    input: PromoteInvestigationRequest,
    signal?: AbortSignal,
  ) => Promise<IncidentView>;
  listIncidents: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<IncidentView>>;
  getIncident: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<IncidentView>;
  getIncidentTopology: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<IncidentTopologyView>;
  getIncidentOperations: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<IncidentOperationsState>;
  applyIncidentOperation: (
    id: string,
    input: IncidentOperationRequest,
    signal?: AbortSignal,
  ) => Promise<IncidentOperationResult>;
  getShiftHandoff: (
    clusterId?: string,
    signal?: AbortSignal,
  ) => Promise<ShiftHandoffSummary>;
  getOperationsReport: (
    window: OperationsReportWindow,
    clusterId?: string,
    signal?: AbortSignal,
  ) => Promise<OperationsReport>;
  downloadOperationsReport: (
    window: OperationsReportWindow,
    format: "markdown" | "html",
    clusterId?: string,
    signal?: AbortSignal,
  ) => Promise<Blob>;
  createPostmortem: (
    incidentId: string,
    input?: CreatePostmortemRequest,
    signal?: AbortSignal,
  ) => Promise<PostmortemView>;
  getPostmortem: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<PostmortemView>;
  patchPostmortem: (
    id: string,
    input: PostmortemPatchRequest,
    signal?: AbortSignal,
  ) => Promise<PostmortemView>;
  publishPostmortem: (
    id: string,
    input: PostmortemPublishRequest,
    signal?: AbortSignal,
  ) => Promise<PostmortemView>;
  listActionItems: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<ActionItemPage>;
  patchActionItem: (
    id: string,
    input: ActionItemPatchRequest,
    signal?: AbortSignal,
  ) => Promise<ActionItem>;
  diagnoseIncident: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<DiagnosisDispatch>;
  listInspections: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<InspectionView>>;
  getInspection: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<InspectionView>;
  createInspection: (
    input: CreateInspectionRequest,
    signal?: AbortSignal,
  ) => Promise<InspectionView>;
  runInspection: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<InspectionView>;
  getInspectionReport: (
    id: string,
    format: "markdown" | "html",
    signal?: AbortSignal,
  ) => Promise<InspectionReport>;
  listRecommendations: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<Recommendation>>;
  dispositionRecommendation: (
    id: string,
    input: RecommendationDispositionRequest,
    signal?: AbortSignal,
  ) => Promise<Recommendation>;
  listEvidence: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<EvidenceRecord>>;
  getEvidence: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<EvidenceRecord>;
  getEvidenceContent: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<unknown>;
  getMessageJourney: (
    clusterId: string,
    traceOrMessageId: string,
    signal?: AbortSignal,
  ) => Promise<MessageJourney>;
  listKnowledge: (
    clusterId: string,
    signal?: AbortSignal,
  ) => Promise<CollectionEnvelope<KnowledgeItem>>;
  getModelCapabilities: (
    signal?: AbortSignal,
  ) => Promise<ModelCapabilitiesResponse>;
  listModelProfileLifecycles: (
    signal?: AbortSignal,
  ) => Promise<ModelProfileLifecyclePage>;
  transitionModelProfileLifecycle: (
    id: string,
    input: ModelProfileLifecycleTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ModelProfileLifecycleView>;
  rollbackModelProfile: (
    id: string,
    input: ModelProfileRollbackRequest,
    signal?: AbortSignal,
  ) => Promise<ModelProfileLifecycleView>;
  runModelProfileSmoke: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<ProviderSmokeResult>;
  subscribeWorkflowEvents: (
    onEvent: (event: WorkflowStreamEvent) => void,
    signal: AbortSignal,
  ) => Promise<void>;
}

export function createHttpSreApi(auth?: ApiRequestContext): SreApi {
  const get = <T>(path: string, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, signal });
  const post = <T>(path: string, body: unknown, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, body, method: "POST", signal });
  const patch = <T>(path: string, body: unknown, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, body, method: "PATCH", signal });

  return {
    listClusters: (signal) => get<ClusterSummary[]>("/v1/clusters", signal),
    getCluster: (clusterId, signal) =>
      get<ClusterSummary>(
        `/v1/clusters/${encodeURIComponent(clusterId)}`,
        signal,
      ),
    getClusterCapabilities: (clusterId, signal) =>
      get<CapabilitySnapshot>(
        `/v1/clusters/${encodeURIComponent(clusterId)}/capabilities`,
        signal,
      ),
    getCapabilities: (signal) =>
      get<CapabilityCatalogResponse>("/v1/capabilities", signal),
    getPhase2Contract: (signal) =>
      get<Phase2ContractManifest>(
        "/v1/capabilities/phase2-contract",
        signal,
      ),
    getCoverage: (signal) =>
      get<CoverageMatrix>("/v1/capabilities/coverage", signal),
    getHealth: (signal) => get<ServiceStatus>("/healthz", signal),
    getReadiness: (signal) => get<ServiceStatus>("/readyz", signal),
    getClusterSlo: (clusterId, signal) =>
      get<ClusterHealthReport>(
        `/v1/clusters/${encodeURIComponent(clusterId)}/slo`,
        signal,
      ),
    getClusterHealth: (clusterId, signal) =>
      get<ClusterHealthReport>(
        `/v1/clusters/${encodeURIComponent(clusterId)}/health`,
        signal,
      ),
    getFleetHealth: (region, signal) =>
      get<FleetHealthReport>(
        apiQuery("/v1/fleet/health", { region }),
        signal,
      ),
    getClusterForecasts: (clusterId, signal) =>
      get<ClusterForecastReport>(
        `/v1/clusters/${encodeURIComponent(clusterId)}/forecasts`,
        signal,
      ),
    runSimulation: (input, signal) =>
      post<WhatIfSimulation>("/v1/simulations", input, signal),
    getUpgradeReadiness: (clusterId, targetVersion, signal) =>
      get<UpgradeReadinessReport>(
        apiQuery(
          `/v1/clusters/${encodeURIComponent(clusterId)}/readiness/upgrade`,
          { target_version: targetVersion },
        ),
        signal,
      ),
    getDrReadiness: (clusterId, targetRegion, signal) =>
      get<DrReadinessReport>(
        apiQuery(
          `/v1/clusters/${encodeURIComponent(clusterId)}/readiness/dr`,
          { target_region: targetRegion },
        ),
        signal,
      ),
    onboardCluster: (input, signal) =>
      post<OnboardOutcome>("/v1/clusters/onboard", input, signal),
    listAssets: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<AssetSnapshot> | AssetSnapshot[]>(
          apiQuery("/v1/assets", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getTopology: (clusterId, signal) =>
      get<TopologySnapshot>(
        apiQuery("/v1/topology", { cluster_id: clusterId }),
        signal,
      ),
    listConversations: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<ConversationView> | ConversationView[]>(
          apiQuery("/v1/conversations", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getConversation: (id, signal) =>
      get<ConversationView>(
        `/v1/conversations/${encodeURIComponent(id)}`,
        signal,
      ),
    createConversation: (input, signal) =>
      post<ConversationView>("/v1/conversations", input, signal),
    listInvestigations: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<InvestigationView> | InvestigationView[]>(
          apiQuery("/v1/investigations", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getInvestigation: (id, signal) =>
      get<InvestigationView>(
        `/v1/investigations/${encodeURIComponent(id)}`,
        signal,
      ),
    promoteInvestigation: (id, input, signal) =>
      post<IncidentView>(
        `/v1/investigations/${encodeURIComponent(id)}/promote`,
        input,
        signal,
      ),
    listIncidents: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<IncidentView> | IncidentView[]>(
          apiQuery("/v1/incidents", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getIncident: (id, signal) =>
      get<IncidentView>(`/v1/incidents/${encodeURIComponent(id)}`, signal),
    getIncidentTopology: (id, signal) =>
      get<IncidentTopologyView>(
        `/v1/incidents/${encodeURIComponent(id)}/topology`,
        signal,
      ),
    getIncidentOperations: (id, signal) =>
      get<IncidentOperationsState>(
        `/v1/incidents/${encodeURIComponent(id)}/operations`,
        signal,
      ),
    applyIncidentOperation: (id, input, signal) =>
      post<IncidentOperationResult>(
        `/v1/incidents/${encodeURIComponent(id)}/operations`,
        input,
        signal,
      ),
    getShiftHandoff: (clusterId, signal) =>
      get<ShiftHandoffSummary>(
        apiQuery("/v1/operations/shift-handoff", {
          cluster_id: clusterId,
        }),
        signal,
      ),
    getOperationsReport: (window, clusterId, signal) =>
      get<OperationsReport>(
        apiQuery("/v1/operations/reports", {
          cluster_id: clusterId,
          window,
          format: "json",
        }),
        signal,
      ),
    downloadOperationsReport: (window, format, clusterId, signal) =>
      download(
        apiQuery("/v1/operations/reports", {
          cluster_id: clusterId,
          window,
          format,
        }),
        auth,
        signal,
      ),
    createPostmortem: (incidentId, input = {}, signal) =>
      post<PostmortemView>(
        `/v1/incidents/${encodeURIComponent(incidentId)}/postmortems`,
        input,
        signal,
      ),
    getPostmortem: (id, signal) =>
      get<PostmortemView>(
        `/v1/postmortems/${encodeURIComponent(id)}`,
        signal,
      ),
    patchPostmortem: (id, input, signal) =>
      patch<PostmortemView>(
        `/v1/postmortems/${encodeURIComponent(id)}`,
        input,
        signal,
      ),
    publishPostmortem: (id, input, signal) =>
      post<PostmortemView>(
        `/v1/postmortems/${encodeURIComponent(id)}/publish`,
        input,
        signal,
      ),
    listActionItems: (clusterId, signal) =>
      get<ActionItemPage>(
        apiQuery("/v1/action-items", { cluster_id: clusterId }),
        signal,
      ),
    patchActionItem: (id, input, signal) =>
      patch<ActionItem>(
        `/v1/action-items/${encodeURIComponent(id)}`,
        input,
        signal,
      ),
    diagnoseIncident: (id, signal) =>
      post<DiagnosisDispatch>(
        `/v1/incidents/${encodeURIComponent(id)}/diagnose`,
        {},
        signal,
      ),
    listInspections: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<InspectionView> | InspectionView[]>(
          apiQuery("/v1/inspections", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getInspection: (id, signal) =>
      get<InspectionView>(
        `/v1/inspections/${encodeURIComponent(id)}`,
        signal,
      ),
    createInspection: (input, signal) =>
      post<InspectionView>("/v1/inspections", input, signal),
    runInspection: (id, signal) =>
      post<InspectionView>(
        `/v1/inspections/${encodeURIComponent(id)}/run`,
        {},
        signal,
      ),
    getInspectionReport: (id, format, signal) =>
      get<InspectionReport>(
        apiQuery(`/v1/inspections/${encodeURIComponent(id)}/report`, {
          format,
        }),
        signal,
      ),
    listRecommendations: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<Recommendation> | Recommendation[]>(
          apiQuery("/v1/recommendations", { cluster_id: clusterId }),
          signal,
        ),
      ),
    dispositionRecommendation: (id, input, signal) =>
      post<Recommendation>(
        `/v1/recommendations/${encodeURIComponent(id)}/disposition`,
        input,
        signal,
      ),
    listEvidence: async (clusterId, signal) =>
      evidenceCollection(
        await get<EvidencePage | EvidenceRecord[]>(
          apiQuery("/v1/evidence", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getEvidence: (id, signal) =>
      get<EvidenceRecord>(
        `/v1/evidence/${encodeURIComponent(id)}`,
        signal,
      ),
    getEvidenceContent: (id, signal) =>
      get<unknown>(
        `/v1/evidence/${encodeURIComponent(id)}/content`,
        signal,
      ),
    getMessageJourney: (clusterId, traceOrMessageId, signal) =>
      get<MessageJourney>(
        apiQuery("/v1/message-journeys", {
          cluster_id: clusterId,
          query: traceOrMessageId,
        }),
        signal,
      ),
    listKnowledge: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<KnowledgeItem> | KnowledgeItem[]>(
          apiQuery("/v1/knowledge", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getModelCapabilities: (signal) =>
      get<ModelCapabilitiesResponse>("/v1/models/capabilities", signal),
    listModelProfileLifecycles: (signal) =>
      get<ModelProfileLifecyclePage>(
        "/v1/models/profiles/lifecycle",
        signal,
      ),
    transitionModelProfileLifecycle: (id, input, signal) =>
      post<ModelProfileLifecycleView>(
        `/v1/models/profiles/${encodeURIComponent(id)}/lifecycle`,
        input,
        signal,
      ),
    rollbackModelProfile: (id, input, signal) =>
      post<ModelProfileLifecycleView>(
        `/v1/models/profiles/${encodeURIComponent(id)}/rollback`,
        input,
        signal,
      ),
    runModelProfileSmoke: (id, signal) =>
      post<ProviderSmokeResult>(
        `/v1/models/profiles/${encodeURIComponent(id)}/smoke`,
        undefined,
        signal,
      ),
    subscribeWorkflowEvents: (onEvent, signal) =>
      streamWorkflowEvents(auth, onEvent, signal),
  };
}

async function streamWorkflowEvents(
  auth: ApiRequestContext | undefined,
  onEvent: (event: WorkflowStreamEvent) => void,
  signal: AbortSignal,
) {
  const response = await fetch("/v1/events/stream", {
    headers: new Headers({
      ...Object.fromEntries(requestHeaders({ auth }).entries()),
      Accept: "text/event-stream",
    }),
    credentials: "same-origin",
    signal,
  });
  if (!response.ok || !response.body) {
    throw new ApiError(
      response.status,
      response.status === 403 ? "cluster_not_allowed" : "source_unavailable",
      "workflow event stream is unavailable",
    );
  }

  const reader = response.body
    .pipeThrough(new TextDecoderStream())
    .getReader();
  let buffer = "";
  while (!signal.aborted) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += value;
    const frames = buffer.split(/\r?\n\r?\n/);
    buffer = frames.pop() ?? "";
    for (const frame of frames) {
      const event = parseWorkflowSseFrame(frame);
      if (event) {
        onEvent(event);
      }
    }
  }
}

export function parseWorkflowSseFrame(
  frame: string,
): WorkflowStreamEvent | undefined {
  const lines = frame.split(/\r?\n/);
  const eventId = lines
    .find((line) => line.startsWith("id:"))
    ?.slice(3)
    .trim();
  const data = lines
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trim())
    .join("\n");
  if (!data) {
    return undefined;
  }
  const event = JSON.parse(data) as WorkflowStreamEvent;
  return eventId ? { ...event, event_id: eventId } : event;
}

interface EvidencePage {
  items: EvidenceRecord[];
  next_cursor?: string;
  partial: boolean;
}

function evidenceCollection(
  value: EvidencePage | EvidenceRecord[],
): CollectionEnvelope<EvidenceRecord> {
  if (Array.isArray(value)) {
    return collection(value);
  }
  return {
    ...value,
    warnings: value.partial ? ["evidence_page_partial"] : [],
    observed_at:
      value.items
        .map((item) => item.observed_at)
        .sort()
        .at(-1) ?? new Date().toISOString(),
  };
}

export function listClusters(
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<ClusterSummary[]> {
  return createHttpSreApi(auth).listClusters(signal);
}

export function getCluster(
  clusterId: string,
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<ClusterSummary> {
  return createHttpSreApi(auth).getCluster(clusterId, signal);
}

export function getClusterCapabilities(
  clusterId: string,
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<CapabilitySnapshot> {
  return createHttpSreApi(auth).getClusterCapabilities(clusterId, signal);
}

export function getCapabilities(
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<CapabilityCatalogResponse> {
  return createHttpSreApi(auth).getCapabilities(signal);
}

export function getCoverage(
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<CoverageMatrix> {
  return createHttpSreApi(auth).getCoverage(signal);
}

export function getHealth(
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<ServiceStatus> {
  return createHttpSreApi(auth).getHealth(signal);
}

export function getReadiness(
  signal?: AbortSignal,
  auth?: ApiRequestContext,
): Promise<ServiceStatus> {
  return createHttpSreApi(auth).getReadiness(signal);
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
