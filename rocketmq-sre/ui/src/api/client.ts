import type { ApiRequestContext } from "@/auth/AuthContext";

import type {
  AssetSnapshot,
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  CollectionEnvelope,
  ConversationView,
  CoverageMatrix,
  CreateConversationRequest,
  CreateInspectionRequest,
  DiagnosisDispatch,
  EvidenceRecord,
  IncidentView,
  InspectionReport,
  InspectionView,
  InvestigationView,
  KnowledgeItem,
  MessageJourney,
  ModelCapabilitiesResponse,
  OnboardClusterRequest,
  OnboardOutcome,
  PromoteInvestigationRequest,
  Recommendation,
  RecommendationDispositionRequest,
  ServiceStatus,
  TopologySnapshot,
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

interface RequestOptions {
  signal?: AbortSignal;
  auth?: ApiRequestContext;
  method?: "GET" | "POST";
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

async function request<T>(
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

function query(
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
  getCoverage: (signal?: AbortSignal) => Promise<CoverageMatrix>;
  getHealth: (signal?: AbortSignal) => Promise<ServiceStatus>;
  getReadiness: (signal?: AbortSignal) => Promise<ServiceStatus>;
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
  subscribeWorkflowEvents: (
    onEvent: (event: WorkflowStreamEvent) => void,
    signal: AbortSignal,
  ) => Promise<void>;
}

export function createHttpSreApi(auth?: ApiRequestContext): SreApi {
  const get = <T>(path: string, signal?: AbortSignal) =>
    request<T>(path, { auth, signal });
  const post = <T>(path: string, body: unknown, signal?: AbortSignal) =>
    request<T>(path, { auth, body, method: "POST", signal });

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
    getCoverage: (signal) =>
      get<CoverageMatrix>("/v1/capabilities/coverage", signal),
    getHealth: (signal) => get<ServiceStatus>("/healthz", signal),
    getReadiness: (signal) => get<ServiceStatus>("/readyz", signal),
    onboardCluster: (input, signal) =>
      post<OnboardOutcome>("/v1/clusters/onboard", input, signal),
    listAssets: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<AssetSnapshot> | AssetSnapshot[]>(
          query("/v1/assets", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getTopology: (clusterId, signal) =>
      get<TopologySnapshot>(
        query("/v1/topology", { cluster_id: clusterId }),
        signal,
      ),
    listConversations: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<ConversationView> | ConversationView[]>(
          query("/v1/conversations", { cluster_id: clusterId }),
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
          query("/v1/investigations", { cluster_id: clusterId }),
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
          query("/v1/incidents", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getIncident: (id, signal) =>
      get<IncidentView>(`/v1/incidents/${encodeURIComponent(id)}`, signal),
    diagnoseIncident: (id, signal) =>
      post<DiagnosisDispatch>(
        `/v1/incidents/${encodeURIComponent(id)}/diagnose`,
        {},
        signal,
      ),
    listInspections: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<InspectionView> | InspectionView[]>(
          query("/v1/inspections", { cluster_id: clusterId }),
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
        query(`/v1/inspections/${encodeURIComponent(id)}/report`, {
          format,
        }),
        signal,
      ),
    listRecommendations: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<Recommendation> | Recommendation[]>(
          query("/v1/recommendations", { cluster_id: clusterId }),
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
          query("/v1/evidence", { cluster_id: clusterId }),
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
        query("/v1/message-journeys", {
          cluster_id: clusterId,
          query: traceOrMessageId,
        }),
        signal,
      ),
    listKnowledge: async (clusterId, signal) =>
      collection(
        await get<CollectionEnvelope<KnowledgeItem> | KnowledgeItem[]>(
          query("/v1/knowledge", { cluster_id: clusterId }),
          signal,
        ),
      ),
    getModelCapabilities: (signal) =>
      get<ModelCapabilitiesResponse>("/v1/models/capabilities", signal),
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
