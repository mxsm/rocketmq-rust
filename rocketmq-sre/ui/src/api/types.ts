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
  provider_network_calls_supported: false;
  providers: Array<{
    id: string;
    protocols: string[];
    supports_streaming: boolean;
    supports_tools: boolean;
    supports_structured_output: boolean;
    supports_embeddings: boolean;
  }>;
}
