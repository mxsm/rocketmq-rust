import type { components as GeneratedComponents } from "./generated";

type ApiSchemas = GeneratedComponents["schemas"];

export type Phase2ContractManifest =
  ApiSchemas["Phase2ContractManifest"];
export type ClusterHealthReport = ApiSchemas["ClusterHealthReport"];
export type FleetHealthReport = ApiSchemas["FleetHealthReport"];
export type ClusterForecastReport =
  ApiSchemas["ClusterForecastReport"];
export type WhatIfSimulationRequest =
  ApiSchemas["WhatIfSimulationRequest"];
export type WhatIfSimulation = ApiSchemas["WhatIfSimulation"];
export type UpgradeReadinessReport =
  ApiSchemas["UpgradeReadinessReport"];
export type DrReadinessReport = ApiSchemas["DrReadinessReport"];
export type IncidentTopologyView = ApiSchemas["IncidentTopologyView"];
export type HealthStatus =
  ApiSchemas["ClusterHealthReport__HealthStatus"];
export type HealthDataQuality =
  ApiSchemas["ClusterHealthReport__HealthDataQuality"];
export type HealthOperationalState =
  ApiSchemas["ClusterHealthReport__HealthOperationalState"];
export type SloDimension =
  ApiSchemas["ClusterHealthReport__SloDimension"];

export type OnboardingState =
  | "pending"
  | "handshaking"
  | "ready_read_only"
  | "read_only_degraded"
  | "rejected"
  | "offboarded";

export type DataSourceAvailability =
  | "existing"
  | "missing_instrumentation"
  | "in_process_only"
  | "queryable"
  | "not_production_verified";

export interface ClusterSummary {
  id: string;
  tenant_id: string;
  external_cluster_key: string;
  environment: string;
  region: string;
  rocketmq_version: string;
  deployment_mode: string;
  owner: string;
  state: OnboardingState;
  effective_access_profile: "read_only";
  created_at?: string;
  updated_at: string;
  offboarded_at?: string;
}

export interface DataSourceStatus {
  id: string;
  availability: DataSourceAvailability;
  freshness_ms?: number;
  detail?: string;
}

export interface CapabilitySnapshot {
  cluster_id: string;
  digest: string;
  protocol_version: string;
  schema_version: string;
  mutation_supported: false;
  observed_at: string;
  data_sources: DataSourceStatus[];
  manifest?: {
    tool_surface_digest?: string;
    resource_surface_digest?: string;
    visible_tools?: string[];
    visible_resources?: string[];
  };
}

export interface ServiceStatus {
  status: "healthy" | "ready" | "not_ready" | "unavailable";
}

export type EvidenceCollectionStatus =
  | "complete"
  | "partial"
  | "unavailable";

export interface EvidenceRow {
  id: string;
  source: string;
  sourceLabel: string;
  status: EvidenceCollectionStatus;
  observedAt: string;
  freshnessSeconds?: number;
  coveragePercent?: number;
  hash?: string;
  warning?: string;
  errorCode?: string;
}

export type CoverageCellStatus =
  | "queryable"
  | "implemented_local"
  | "in_process_only"
  | "missing_instrumentation"
  | "not_production_verified";

export interface CoverageRequirement {
  id: string;
  signalType: "metric" | "span" | "log" | "resource";
  registryReference: string;
  freshness: string;
  expectedAttributes: string[];
  sensitivity: string;
  missingBehavior: "missing" | "not_production_verified";
  evidenceField: string;
  owner: string;
  purpose: string;
}

export interface CoverageMatrixRow {
  component: string;
  cells: Record<string, CoverageCellStatus>;
}

export interface CoverageMatrix {
  generatedAt: string;
  semanticSignalCount: number;
  semanticOwnerCount: number;
  packs: Array<{ id: string; label: string }>;
  rows: CoverageMatrixRow[];
  selected: {
    component: string;
    pack: string;
    status: CoverageCellStatus;
    requirements: CoverageRequirement[];
  };
}

export interface CapabilityCatalogResponse {
  schema_version: string;
  phase: string;
  effective_access_profile: "read_only";
  execution_supported: false;
  approval_supported: false;
  provider_network_calls_supported: true;
  providers: Array<{
    id: string;
    protocols: string[];
    supports_streaming: boolean;
    supports_tools: boolean;
    supports_structured_output: boolean;
    supports_embeddings: boolean;
  }>;
}

export interface CollectionEnvelope<T> {
  items: T[];
  partial: boolean;
  warnings: string[];
  observed_at: string;
  next_cursor?: string;
}

export interface WorkflowActor {
  subject: string;
  display_name?: string;
}

export type ConversationStatus = "active" | "promoted" | "closed";

export interface Conversation {
  id: string;
  tenant_id: string;
  cluster_id: string;
  question: string;
  resource?: string;
  status: ConversationStatus;
  investigation_id?: string;
  created_by: WorkflowActor;
  created_at: string;
  updated_at: string;
}

export interface ConversationView {
  conversation: Conversation;
  investigation?: Investigation;
}

export type InvestigationStatus =
  | "open"
  | "collecting"
  | "diagnosing"
  | "needs_evidence"
  | "monitoring"
  | "promoted"
  | "closed";

export interface Investigation {
  id: string;
  tenant_id: string;
  cluster_id: string;
  conversation_id?: string;
  incident_id?: string;
  title: string;
  resource?: string;
  symptom_family: string;
  fingerprint: string;
  status: InvestigationStatus;
  created_by: WorkflowActor;
  created_at: string;
  updated_at: string;
}

export interface TimelineEvent {
  id: string;
  tenant_id: string;
  cluster_id: string;
  investigation_id?: string;
  incident_id?: string;
  event_type: string;
  summary: string;
  details: Record<string, unknown>;
  correlation_id: string;
  actor: WorkflowActor;
  occurred_at: string;
}

export interface InvestigationView {
  investigation: Investigation;
  timeline: TimelineEvent[];
}

export type IncidentStatus =
  | "new"
  | "collecting"
  | "diagnosing"
  | "needs_evidence"
  | "monitoring"
  | "resolved"
  | "escalated";

export interface Incident {
  id: string;
  tenant_id: string;
  cluster_id: string;
  title: string;
  resource?: string;
  symptom_family?: string;
  fingerprint?: string;
  status: IncidentStatus;
  summary?: string;
  severity?: "info" | "warning" | "error" | "critical";
  owner?: string;
  occurrence_count: number;
  last_alert_at?: string;
  reopened_from_incident_id?: string;
  created_at: string;
  updated_at: string;
}

export interface DiagnosisRevision {
  id: string;
  incident_id: string;
  revision: number;
  status: IncidentStatus;
  rule_result: Record<string, unknown>;
  hypotheses: Array<{
    title: string;
    confidence: number;
    status: "supported" | "contradicted" | "unknown";
  }>;
  evidence_ids: string[];
  primary_model_invocation_id?: string;
  execution_eligible: false;
  partial: boolean;
  created_at: string;
}

export interface IncidentView {
  incident: Incident;
  investigation?: Investigation;
  timeline: TimelineEvent[];
  diagnosis_revisions: DiagnosisRevision[];
}

export type InspectionTemplate =
  | "cluster_health"
  | "consumer"
  | "broker"
  | "telemetry";

export type InspectionStatus =
  | "scheduled"
  | "running"
  | "needs_evidence"
  | "completed"
  | "failed"
  | "cancelled";

export interface InspectionRun {
  id: string;
  tenant_id: string;
  cluster_id: string;
  template: InspectionTemplate;
  status: InspectionStatus;
  schedule?: string | null;
  finding_count: number;
  partial: boolean;
  started_at?: string | null;
  completed_at?: string | null;
  created_at: string;
}

export type RecommendationStatus =
  | "open"
  | "acknowledged"
  | "assigned"
  | "dismissed"
  | "resolved"
  | "promoted";

export interface Recommendation {
  id: string;
  inspection_run_id: string;
  tenant_id: string;
  cluster_id: string;
  severity: "info" | "warning" | "critical";
  title: string;
  rationale: string;
  evidence_ids: string[];
  status: RecommendationStatus;
  assignee?: string | null;
  investigation_id?: string | null;
  incident_id?: string | null;
  created_at: string;
  updated_at: string;
}

export interface InspectionView {
  run: InspectionRun;
  recommendations: Recommendation[];
  pack_diffs?: Array<{
    pack_id: string;
    pack_version: string;
    diff: unknown;
  }>;
}

export type AssetKind =
  | "name_server"
  | "controller"
  | "broker"
  | "proxy"
  | "store"
  | "pod"
  | "node"
  | "persistent_volume_claim"
  | "pod_disruption_budget"
  | "topic"
  | "queue"
  | "producer"
  | "consumer"
  | "connection";

export interface AssetSnapshot {
  id: string;
  tenant_id: string;
  cluster_id: string;
  kind: AssetKind;
  external_key: string;
  display_name: string;
  source: string;
  attributes: Record<string, string | number | boolean | null>;
  observed_at: string;
  freshness_seconds: number;
  partial: boolean;
  content_hash: string;
}

export interface TopologyEdge {
  id: string;
  tenant_id: string;
  cluster_id: string;
  from_key: string;
  to_key: string;
  relation:
    | "contains"
    | "routes_to"
    | "stores_on"
    | "runs_on"
    | "connects_to"
    | "consumes_from"
    | "produces_to"
    | "replicates_to";
  source: string;
  observed_at: string;
  freshness_seconds: number;
  partial: boolean;
  content_hash: string;
}

export interface TopologySnapshot {
  assets: AssetSnapshot[];
  edges: TopologyEdge[];
  observed_at: string;
  partial: boolean;
  warnings: string[];
}

export type EvidenceSchemaVersion = ApiSchemas["SchemaVersion"];
export type EvidenceCoverage =
  ApiSchemas["EvidenceSnapshot"]["coverage"];
export type EvidenceContent = ApiSchemas["EvidenceContent"];
export type EvidenceRecord = ApiSchemas["EvidenceSnapshot"];
export type MessageJourneyHop = ApiSchemas["MessageJourneyHop"];
export type MessageJourney = ApiSchemas["MessageJourney"];

export type KnowledgeReviewStatus =
  | "draft"
  | "in_review"
  | "validated"
  | "deprecated"
  | "expired";

export interface KnowledgeItem {
  id: string;
  tenant_id: string;
  cluster_id?: string;
  title: string;
  component: string;
  rocketmq_version_range: string;
  source_uri: string;
  source_version: string;
  owner: string;
  review_status: KnowledgeReviewStatus;
  review_due_at: string;
  sensitivity: string;
  content_hash: string;
  conflict: boolean;
  summary: string;
  updated_at: string;
}

export interface ModelProfile {
  id: string;
  profile_name: string;
  provider_family: string;
  protocol_family: string;
  model_family: string;
  model_name: string;
  model_revision: string;
  endpoint_instance: string;
  region: string;
  data_residency: string;
  capabilities: string[];
  enabled: boolean;
  health: "unknown" | "healthy" | "degraded" | "quarantined" | "disabled";
  credential_present: boolean;
}

export interface ModelCapabilitiesResponse {
  schema_version: string;
  network_calls_enabled: boolean;
  rules_only_available: boolean;
  providers: CapabilityCatalogResponse["providers"];
  profiles?: ModelProfile[];
}

export type WorkflowStreamEvent = ApiSchemas["WorkflowStreamEvent"] & {
  event_id?: string;
};

export interface OnboardClusterRequest {
  tenant_id: string;
  external_cluster_key: string;
  environment: string;
  region: string;
  rocketmq_version: string;
  deployment_mode: string;
  owner: string;
  actor_subject: string;
}

export interface OnboardOutcome {
  cluster: ClusterSummary;
  created: boolean;
}

export type CreateConversationRequest =
  ApiSchemas["CreateConversationRequest"];
export type CreateInspectionRequest =
  ApiSchemas["CreateInspectionRequest"];
export type PromoteInvestigationRequest =
  ApiSchemas["PromoteInvestigationRequest"];
export type RecommendationDispositionRequest =
  ApiSchemas["RecommendationDispositionRequest"];
export type InspectionReport = ApiSchemas["InspectionReport"];
export type DiagnosisDispatch = ApiSchemas["DiagnosisDispatch"];
