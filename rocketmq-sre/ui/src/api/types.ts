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
export type CreatePostmortemRequest =
  ApiSchemas["CreatePostmortemRequest"];
export type PostmortemPatchRequest =
  ApiSchemas["PostmortemPatchRequest"];
export type PostmortemPublishRequest =
  ApiSchemas["PostmortemPublishRequest"];
export type PostmortemView = ApiSchemas["PostmortemView"];
export type ActionItem = ApiSchemas["ActionItem"];
export type ActionItemStatus =
  ApiSchemas["ActionItem__ActionItemStatus"];
export type ActionItemPatchRequest =
  ApiSchemas["ActionItemPatchRequest"];
export type ActionItemPage = ApiSchemas["ActionItemPage"];
export type IncidentOperationRequest =
  ApiSchemas["IncidentOperationRequest"];
export type IncidentOperationResult =
  ApiSchemas["IncidentOperationResult"];
export type IncidentOperationsState =
  ApiSchemas["IncidentOperationsState"];
export type ShiftHandoffSummary =
  ApiSchemas["ShiftHandoffSummary"];
export type OperationsReport = ApiSchemas["OperationsReport"];
export type OperationsReportWindow =
  ApiSchemas["OperationsReport__OperationsReportWindow"];
export type HealthStatus =
  ApiSchemas["ClusterHealthReport__HealthStatus"];
export type HealthDataQuality =
  ApiSchemas["ClusterHealthReport__HealthDataQuality"];
export type HealthOperationalState =
  ApiSchemas["ClusterHealthReport__HealthOperationalState"];
export type SloDimension =
  ApiSchemas["ClusterHealthReport__SloDimension"];
export type CreatePlanRequest = ApiSchemas["CreatePlanRequest"];
export type CreatePlanResponse = ApiSchemas["CreatePlanResponse"];
export type ActionPlanView = ApiSchemas["ActionPlanView"];
export type CriticReviewRequest = ApiSchemas["CriticReviewRequest"];
export type CriticReviewResponse = ApiSchemas["CriticReviewResponse"];
export type ApprovalDecisionRequest =
  ApiSchemas["ApprovalDecisionRequest"];
export type ApprovalDecisionResponse =
  ApiSchemas["ApprovalDecisionResponse"];
export type SubmitExecutionRequest =
  ApiSchemas["SubmitExecutionRequest"];
export type ExecutionSubmissionView =
  ApiSchemas["ExecutionSubmissionView"];
export type AuditPage = ApiSchemas["AuditPage"];
export type QuarantinePage = ApiSchemas["QuarantinePage"];
export type ClearQuarantineRequest =
  ApiSchemas["ClearQuarantineRequest"];
export type ResourceQuarantine = ApiSchemas["ResourceQuarantine"];
export type RunbookDefinition = ApiSchemas["RunbookDefinition"];
export type RunbookPage = ApiSchemas["RunbookPage"];
export type CreateRunbookRequest = ApiSchemas["CreateRunbookRequest"];
export type ChangeWindow = ApiSchemas["ChangeWindow"];
export type ChangeWindowKind = ApiSchemas["ChangeWindowKind"];
export type ChangeWindowPage = ApiSchemas["ChangeWindowPage"];
export type CreateChangeWindowRequest =
  ApiSchemas["CreateChangeWindowRequest"];
export type ChangeSchedule = ApiSchemas["ChangeSchedule"];
export type ChangeScheduleStatus = ApiSchemas["ChangeScheduleStatus"];
export type ChangeSchedulePage = ApiSchemas["ChangeSchedulePage"];
export type ChangeSchedulePreview =
  ApiSchemas["ChangeSchedulePreview"];
export type CreateChangeScheduleRequest =
  ApiSchemas["CreateChangeScheduleRequest"];
export type ScheduleTransitionRequest =
  ApiSchemas["ScheduleTransitionRequest"];
export type ManualGateDecisionRequest =
  ApiSchemas["ManualGateDecisionRequest"];
export type RunbookStepPlanBinding =
  ApiSchemas["RunbookStepPlanBinding"];
export type ChangeConflict = ApiSchemas["ChangeConflict"];
export type IntegrationAdapterKind =
  ApiSchemas["IntegrationAdapterKind"];
export type IntegrationEventKind =
  ApiSchemas["IntegrationEventKind"];
export type IntegrationDeliveryStatus =
  ApiSchemas["IntegrationDeliveryStatus"];
export type IntegrationDescriptor =
  ApiSchemas["IntegrationDescriptor"];
export type IntegrationTargetView =
  ApiSchemas["IntegrationTargetView"];
export type IntegrationTargetPage =
  ApiSchemas["IntegrationTargetPage"];
export type IntegrationDelivery =
  ApiSchemas["IntegrationDelivery"];
export type IntegrationDeliveryPage =
  ApiSchemas["IntegrationDeliveryPage"];
export type RegisterIntegrationTargetRequest =
  ApiSchemas["RegisterIntegrationTargetRequest"];
export type SetIntegrationTargetStateRequest =
  ApiSchemas["SetIntegrationTargetStateRequest"];
export type ExternalApprovalInput =
  ApiSchemas["ExternalApprovalInput"];
export type ExternalApprovalView =
  ApiSchemas["ExternalApprovalView"];
export type ReleaseStatus = ApiSchemas["ReleaseStatus"];
export type ReleaseObservationPhase =
  ApiSchemas["ReleaseObservationPhase"];
export type ReleaseReadinessSnapshot =
  ApiSchemas["ReleaseReadinessSnapshot"];
export type ReleaseObservation =
  ApiSchemas["ReleaseObservation"];
export type ReleaseWorkflow = ApiSchemas["ReleaseWorkflow"];
export type ReleaseReport = ApiSchemas["ReleaseReport"];
export type CreateReleaseRequest =
  ApiSchemas["CreateReleaseRequest"];
export type PrepareReleaseRequest =
  ApiSchemas["PrepareReleaseRequest"];
export type ReleaseExecutionRequest =
  ApiSchemas["ReleaseExecutionRequest"];
export type RecordReleaseObservationRequest =
  ApiSchemas["RecordReleaseObservationRequest"];
export type ReleaseTransitionRequest =
  ApiSchemas["ReleaseTransitionRequest"];
export type CompleteRollbackRequest =
  ApiSchemas["CompleteRollbackRequest"];
export type ReleasePage = ApiSchemas["ReleasePage"];
export type ReleaseDetail = ApiSchemas["ReleaseDetail"];
export type ReleasePreparationView =
  ApiSchemas["ReleasePreparationView"];
export type ReleaseExecutionView =
  ApiSchemas["ReleaseExecutionView"];

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
  effective_access_profile: "read_only" | "human_approved_supervised";
  execution_supported: boolean;
  execution_submission_supported?: boolean;
  approval_supported: boolean;
  unattended_execution_supported?: false;
  arbitrary_mutation_supported?: false;
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
  | "telemetry"
  | "full_cluster"
  | "producer_consumer"
  | "store_ha"
  | "routing_proxy"
  | "security"
  | "upgrade"
  | "disaster_recovery";

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

export type ModelProfileLifecycleState =
  | "draft"
  | "certified"
  | "promoted"
  | "quarantined"
  | "retired";

export interface ProviderSmokeResult {
  id: string;
  profile_id: string;
  connectivity_ok: boolean;
  structured_output_ok: boolean;
  tool_arguments_ok: boolean;
  evidence_citation_ok: boolean;
  overall_ok: boolean;
  latency_ms?: number;
  failure_codes: string[];
  result_snapshot: Record<string, unknown>;
  observed_at: string;
}

export interface ModelProfileLifecycleView {
  profile_id: string;
  profile_name: string;
  provider_family: string;
  model_family: string;
  model_revision: string;
  state: ModelProfileLifecycleState;
  revision: number;
  rollback_profile_id?: string;
  reason_code: string;
  operator_confirmed: boolean;
  updated_by: string;
  updated_at: string;
  latest_smoke?: ProviderSmokeResult;
  automation_eligible: boolean;
}

export interface ModelProfileLifecyclePage {
  schema_version: string;
  items: ModelProfileLifecycleView[];
  observed_at: string;
}

export interface ModelProfileLifecycleTransitionRequest {
  target_state: ModelProfileLifecycleState;
  expected_revision: number;
  rollback_profile_id?: string;
  reason_code: string;
  operator_confirmed: boolean;
}

export interface ModelProfileRollbackRequest {
  expected_revision: number;
  reason_code: string;
  operator_confirmed: boolean;
}

export type AutonomyOutcomeClass =
  | "expected_deny"
  | "success"
  | "autonomous_execution_failure";

export type AutonomousExecutionFailure =
  | "apply_failed"
  | "verification_failed"
  | "unknown_effect"
  | "compensation_started"
  | "rolled_back"
  | "escalated"
  | "safety_invalidated_during_execution"
  | "operator_stopped"
  | "critic_unavailable"
  | "critic_invalid"
  | "critic_conflict"
  | "evidence_degraded";

export interface AutonomyOutcome {
  id: string;
  tenant_id: string;
  cluster_id: string;
  action: string;
  action_version: string;
  incident_id: string;
  plan_id: string;
  plan_hash: string;
  execution_id: string | null;
  cohort_id: string | null;
  class: AutonomyOutcomeClass;
  failure: AutonomousExecutionFailure | null;
  reason_codes: string[];
  first_positive_intent_persisted: boolean;
  occurred_at: string;
  reconciled_at: string;
}

export interface AutonomyOutcomePage {
  schema_version: string;
  items: AutonomyOutcome[];
  truncated: boolean;
  observed_at: string;
}

export interface AutonomyOutcomeQuery {
  clusterId?: string;
  action?: string;
  class?: AutonomyOutcomeClass;
  from?: string;
  until?: string;
  limit?: number;
}

export type AutonomyReportPeriod = "weekly" | "monthly";

export interface AutonomyOperationalReportQuery {
  period: AutonomyReportPeriod;
  anchor?: string;
  clusterId?: string;
}

export interface AutonomyReportWindow {
  period: AutonomyReportPeriod;
  start: string;
  end: string;
  complete: boolean;
}

export interface AutonomyOutcomeMetrics {
  candidates: number;
  eligible: number;
  denied: number;
  successes: number;
  execution_failures: number;
  rollbacks: number;
  unknown_effects: number;
  human_handoffs: number;
}

export interface AutonomyDurationMetrics {
  mean_time_to_acknowledge_seconds: number | null;
  mean_time_to_resolve_seconds: number | null;
  average_diagnosis_seconds: number | null;
  average_execution_seconds: number | null;
  average_recovery_seconds: number | null;
  acknowledged_incidents: number;
  resolved_incidents: number;
  diagnosed_incidents: number;
  completed_executions: number;
}

export interface AutonomyQualityMetrics {
  raw_alert_occurrences: number;
  correlated_alerts: number;
  noise_reduction_basis_points: number | null;
  routed_incidents: number;
  owner_routing_hit_basis_points: number | null;
  terminal_incidents: number;
  recurrent_incidents: number;
  recurrence_basis_points: number | null;
  overdue_action_items: number;
  post_close_recurrences: number;
  health_score_delta: number | null;
}

export interface AutonomyFeedbackMetrics {
  total: number;
  adopted: number;
  modified: number;
  rejected: number;
  adoption_basis_points: number | null;
  modification_basis_points: number | null;
  rejection_basis_points: number | null;
}

export interface AutomationSavingsMetrics {
  successful_no_side_effect_runs: number;
  successful_preventive_runs: number;
  estimated_minutes_saved: number;
  estimate_method: string;
}

export interface ModelUsageMetrics {
  calls: number;
  input_tokens: number;
  output_tokens: number;
  cost_micros: number;
  calls_missing_tokens: number;
  calls_missing_cost: number;
  failed_calls: number;
  fallback_calls: number;
  usage_coverage_basis_points: number | null;
  cost_coverage_basis_points: number | null;
}

export interface ActionOutcomeBreakdown {
  cluster_id: string;
  action_id: string;
  action_version: string;
  outcomes: AutonomyOutcomeMetrics;
  average_execution_seconds: number | null;
}

export interface ModelCostBreakdown {
  provider_family: string;
  model_family: string;
  model_revision: string;
  actual_profile_id: string;
  usage: ModelUsageMetrics;
}

export interface IncidentModelCost {
  incident_id: string;
  usage: ModelUsageMetrics;
}

export interface VersionEffectComparison {
  dimension: string;
  version: string;
  samples: number;
  successes: number;
  success_basis_points: number | null;
  cost_micros: number;
}

export interface CostBudgetAlert {
  scope_kind: string;
  scope_id: string;
  observed_cost_micros: number;
  budget_micros: number;
  reason_code: string;
  recommended_degradation: string;
  automatic_provider_mutation: boolean;
}

export interface OptimizationCandidate {
  id: string;
  category: string;
  scope: string;
  reason_code: string;
  evidence_summary: string;
  review_status: string;
  requires_human_review: boolean;
  publication_allowed: boolean;
}

export interface AutonomyOperationalReport {
  schema_version: string;
  tenant_id: string;
  cluster_ids: string[];
  window: AutonomyReportWindow;
  outcomes: AutonomyOutcomeMetrics;
  durations: AutonomyDurationMetrics;
  quality: AutonomyQualityMetrics;
  feedback: AutonomyFeedbackMetrics;
  savings: AutomationSavingsMetrics;
  model_usage: ModelUsageMetrics;
  action_breakdown: ActionOutcomeBreakdown[];
  model_breakdown: ModelCostBreakdown[];
  incident_costs: IncidentModelCost[];
  version_effects: VersionEffectComparison[];
  budget_alerts: CostBudgetAlert[];
  optimization_candidates: OptimizationCandidate[];
  warnings: string[];
  generated_at: string;
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
