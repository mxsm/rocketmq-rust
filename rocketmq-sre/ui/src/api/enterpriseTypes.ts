export type EnterpriseHealth =
  | "healthy"
  | "degraded"
  | "critical"
  | "disconnected"
  | "unknown";

export interface FleetRegion {
  id: string;
  fleet_id: string;
  key: string;
  display_name: string;
  owner: string;
  residency_tags: string[];
  active: boolean;
  updated_at: string;
}

export interface FleetRegistration {
  cluster_id: string;
  fleet_id: string;
  tenant_id: string;
  region_id: string;
  external_cluster_key: string;
  environment: string;
  owner: string;
  state:
    | "pending"
    | "onboarding"
    | "active"
    | "read_only_degraded"
    | "offboarding"
    | "retired";
  residency_tags: string[];
  lifecycle_revision: number;
  updated_at: string;
}

export interface FleetOverview {
  schema_version: string;
  fleet: {
    id: string;
    name: string;
    owner: string;
  };
  tenant: {
    id: string;
    fleet_id: string;
    name: string;
    owner: string;
    active: boolean;
  };
  regions: FleetRegion[];
  registrations: FleetRegistration[];
  observed_at: string;
}

export interface FleetAsset {
  cluster_id: string;
  fleet_id: string;
  tenant_id: string;
  region_id: string;
  environment: string;
  owner: string;
  component: string;
  component_version: string;
  image_digest?: string;
  feature_digest?: string;
  configuration_digest?: string;
  health: EnterpriseHealth | string;
  attributes: Record<string, string>;
  observed_at: string;
}

export interface FleetAssetPage {
  schema_version: string;
  items: FleetAsset[];
  total: number;
  limit: number;
  offset: number;
  health_distribution: Record<string, number>;
  worst_health?: string;
}

export interface ComplianceFinding {
  id: string;
  fleet_id: string;
  tenant_id: string;
  region_id: string;
  cluster_id: string;
  category: string;
  expected_digest: string;
  live_digest: string;
  evidence_ids: string[];
  severity: "info" | "warning" | "error" | "critical";
  owner: string;
  recommendation: string;
  state: "open" | "acknowledged" | "resolved" | "accepted_exception";
  observed_at: string;
}

export interface ComplianceFindingPage {
  schema_version: string;
  items: ComplianceFinding[];
  total: number;
  limit: number;
  offset: number;
}

export interface FleetInspectionRun {
  id: string;
  fleet_id: string;
  tenant_id: string;
  region_ids: string[];
  cluster_ids: string[];
  pack_ids: string[];
  max_concurrency: number;
  timeout_seconds: number;
  model_token_budget: number;
  evidence_byte_budget: number;
  state:
    | "pending"
    | "running"
    | "completed"
    | "partially_completed"
    | "failed"
    | "cancelled";
  completed_clusters: number;
  failed_clusters: number;
  created_at: string;
  completed_at?: string;
}

export interface FleetInspectionPage {
  schema_version: string;
  items: FleetInspectionRun[];
  truncated: boolean;
}

export interface DrPlan {
  id: string;
  fleet_id: string;
  tenant_id: string;
  region_id: string;
  cluster_id?: string;
  subject: string;
  name: string;
  version: number;
  owner: string;
  target: {
    rto_seconds: number;
    rpo_seconds: number;
  };
  allowed_modes: string[];
  required_sources: string[];
  checkpoints: Array<{
    key: string;
    title: string;
    expected_duration_seconds: number;
    manual_confirmation_required: boolean;
    cleanup_required: boolean;
    required_evidence_kinds: string[];
  }>;
  active: boolean;
  updated_at: string;
}

export interface DrPlanPage {
  schema_version: string;
  items: DrPlan[];
  truncated: boolean;
}

export interface DrExercise {
  id: string;
  plan_id: string;
  tenant_id: string;
  region_id: string;
  cluster_id?: string;
  mode: "readiness" | "tabletop" | "supervised_test";
  boundary: string;
  state:
    | "planned"
    | "running"
    | "awaiting_manual_confirmation"
    | "completed"
    | "failed"
    | "cancelled";
  target: {
    rto_seconds: number;
    rpo_seconds: number;
  };
  actual_rto_seconds?: number;
  actual_rpo_seconds?: number;
  manual_checkpoint_count: number;
  cleanup_complete: boolean;
  evidence_ids: string[];
  created_by: string;
  started_at?: string;
  completed_at?: string;
  updated_at: string;
}

export interface DrExercisePage {
  schema_version: string;
  items: DrExercise[];
  truncated: boolean;
}

export interface DrActionItem {
  id: string;
  finding_id: string;
  tenant_id: string;
  cluster_id?: string;
  title: string;
  owner?: string;
  due_at?: string;
  status: "open" | "in_progress" | "blocked" | "done" | "cancelled";
  verification?: string;
  evidence_ids: string[];
  updated_at: string;
}

export interface DrActionItemPage {
  schema_version: string;
  items: DrActionItem[];
  truncated: boolean;
}

export type GovernanceObjectKind =
  | "data_policy"
  | "evidence_policy"
  | "prompt"
  | "knowledge"
  | "model"
  | "provider"
  | "diagnostic_pack"
  | "policy"
  | "action"
  | "runbook"
  | "integration";

export interface GovernanceArtifact {
  id: string;
  tenant_id: string;
  kind: GovernanceObjectKind;
  logical_key: string;
  owner: string;
  reviewer: string;
  current_version_id?: string;
  updated_at: string;
}

export interface GovernanceArtifactPage {
  schema_version: string;
  items: GovernanceArtifact[];
  truncated: boolean;
}

export interface GovernanceCompliance {
  schema_version: string;
  state_counts: Record<string, number>;
  unsigned_active: number;
  expired_active: number;
  overdue_review: number;
  quarantined: number;
  compliant: boolean;
  observed_at: string;
}

export interface FinOpsShowbackRow {
  dimensions: Record<string, string>;
  request_count: number;
  input_tokens: number;
  output_tokens: number;
  error_count: number;
  average_latency_millis?: number;
  cost_micros: number;
  successful_outcomes: number;
  slo_compliant_outcomes: number;
  estimated_minutes_saved: number;
}

export interface FinOpsForecast {
  budget_id: string;
  period_start: string;
  period_end: string;
  observed_cost_micros: number;
  projected_cost_micros: number;
  hard_limit_micros: number;
  sample_count: number;
  coverage_basis_points: number;
  projected_over_budget: boolean;
  generated_at: string;
}

export interface FinOpsAnomaly {
  scope_kind: string;
  scope_key: string;
  current_cost_micros: number;
  baseline_cost_micros: number;
  change_basis_points?: number;
  reason_code: string;
}

export interface FinOpsReport {
  schema_version: string;
  tenant_id: string;
  from: string;
  to: string;
  allocation_mode: "showback" | "chargeback";
  chargeback_enabled: boolean;
  rows: FinOpsShowbackRow[];
  total_cost_micros: number;
  ledger_entries: number;
  entries_missing_cost: number;
  cost_coverage_basis_points?: number;
  forecasts: FinOpsForecast[];
  anomalies: FinOpsAnomaly[];
  warnings: string[];
  generated_at: string;
}

export interface EnterpriseSnapshot {
  fleet: FleetOverview;
  assets: FleetAssetPage;
  compliance: ComplianceFindingPage;
  inspections: FleetInspectionPage;
  drPlans: DrPlanPage;
  drExercises: DrExercisePage;
  drActionItems: DrActionItemPage;
  governanceArtifacts: GovernanceArtifactPage;
  governanceCompliance: GovernanceCompliance;
  finops: FinOpsReport;
}
