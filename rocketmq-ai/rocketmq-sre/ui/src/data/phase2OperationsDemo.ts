import type {
  IncidentOperationsState,
  OperationsReport,
  ShiftHandoffSummary,
} from "@/api/types";

import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "./phase1Demo";

const INCIDENT_ID = "32000000-0000-4000-8000-000000000001";
const SECOND_INCIDENT_ID = "32000000-0000-4000-8000-000000000002";

export const demoIncidentOperations: Record<
  string,
  IncidentOperationsState
> = {
  [INCIDENT_ID]: {
    schema_version: "rocketmq-sre.incident-operations-state.v1",
    incident_id: INCIDENT_ID,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    owner: "messaging-platform",
    acknowledged_by: "sre.li",
    split_incident_ids: [],
    sla: {
      acknowledgement_due_at: "2026-07-27T08:46:00Z",
      resolution_due_at: "2026-07-27T12:31:00Z",
      acknowledged_at: "2026-07-27T08:39:00Z",
      acknowledgement_breached: false,
      resolution_breached: false,
    },
    updated_at: "2026-07-27T08:42:00Z",
  },
  [SECOND_INCIDENT_ID]: {
    schema_version: "rocketmq-sre.incident-operations-state.v1",
    incident_id: SECOND_INCIDENT_ID,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    owner: "controller-team",
    split_incident_ids: [],
    sla: {
      acknowledgement_due_at: "2026-07-27T08:15:00Z",
      resolution_due_at: "2026-07-28T06:15:00Z",
      acknowledgement_breached: true,
      resolution_breached: false,
    },
    updated_at: "2026-07-27T08:10:00Z",
  },
};

const incidentFinding = {
  category: "incident",
  severity: "critical",
  title: "orders / order-worker 消费堆积",
  cluster_id: DEMO_CLUSTER_ID,
  incident_id: INCIDENT_ID,
  resource: "consumer-group:order-worker",
  detail:
    "status=diagnosing; SLA 正常；Lag 持续上升，当前主假设为消费实例处理能力下降。",
  suggested_owner: "messaging-platform",
  observed_at: "2026-07-27T08:42:00Z",
  deep_link: `/incidents/${INCIDENT_ID}`,
};

const capacityFinding = {
  category: "capacity_risk",
  severity: "warning",
  title: "commitlog_disk_ratio capacity runway is constrained",
  cluster_id: DEMO_CLUSTER_ID,
  resource: "commitlog_disk_ratio",
  detail: "status=ready; exhaustion=2026-08-09T10:00:00Z; coverage=96%",
  suggested_owner: "platform-sre",
  observed_at: "2026-07-27T08:30:00Z",
  deep_link: "/forecasts",
};

const sourceGap = {
  category: "source_gap",
  severity: "info",
  title: "controller-runtime is not remotely queryable",
  cluster_id: DEMO_CLUSTER_ID,
  resource: "controller-runtime",
  detail: "availability=in_process_only",
  suggested_owner: "messaging-observability",
  observed_at: "2026-07-27T08:34:00Z",
  deep_link: `/clusters/${DEMO_CLUSTER_ID}`,
};

export const demoShiftHandoff: ShiftHandoffSummary = {
  schema_version: "rocketmq-sre.shift-handoff.v1",
  tenant_id: DEMO_TENANT_ID,
  window_start: "2026-07-26T20:45:00Z",
  generated_at: "2026-07-27T08:45:00Z",
  new_incidents: [incidentFinding],
  unresolved_incidents: [
    incidentFinding,
    {
      ...incidentFinding,
      severity: "warning",
      title: "Controller heartbeat 短时抖动",
      incident_id: SECOND_INCIDENT_ID,
      resource: "controller:controller-0",
      detail: "status=monitoring; acknowledgement SLA breached",
      suggested_owner: "controller-team",
      deep_link: `/incidents/${SECOND_INCIDENT_ID}`,
    },
  ],
  risk_trends: [
    {
      ...capacityFinding,
      category: "risk_trend",
      title: "consume_tps trend changed",
      detail: "value changed from 5820.000 to 3317.000",
    },
  ],
  recent_changes: [
    {
      ...capacityFinding,
      category: "recent_change",
      severity: "info",
      title: "order-worker deployment rollout recorded",
      detail: "Deployment revision changed 24 minutes before the alert.",
      deep_link: `/incidents/${INCIDENT_ID}`,
    },
  ],
  expiring_certificates: [
    {
      ...capacityFinding,
      category: "expiring_certificate",
      title: "proxy certificate requires rotation review",
      resource: "certificate_expiry",
      detail: "expires in 18 days; coverage=100%",
    },
  ],
  capacity_risks: [capacityFinding],
  overdue_action_items: [
    {
      ...incidentFinding,
      category: "overdue_action_item",
      severity: "warning",
      title: "补齐 Controller 远程诊断查询",
      detail: "Action Item 已逾期 2 天。",
      deep_link: `/incidents/${SECOND_INCIDENT_ID}/postmortem`,
    },
  ],
  source_gaps: [sourceGap],
  partial: false,
  warnings: [],
};

export const demoOperationsReport: OperationsReport = {
  schema_version: "rocketmq-sre.operations-report.v1",
  tenant_id: DEMO_TENANT_ID,
  window: "daily",
  window_start: "2026-07-26T08:45:00Z",
  window_end: "2026-07-27T08:45:00Z",
  generated_at: "2026-07-27T08:45:00Z",
  worst_clusters: [
    {
      ...capacityFinding,
      category: "cluster_health",
      severity: "warning",
      title: "rmq-prod-cn health is degraded",
      detail: "score=72; data_quality=partial",
      deep_link: `/clusters/${DEMO_CLUSTER_ID}`,
    },
  ],
  slo_burns: [
    {
      ...incidentFinding,
      category: "slo_burn",
      title: "consumer_delivery SLO burn triggered",
      detail: "window=fast; short_rate=16.400; long_rate=7.800",
      deep_link: `/clusters/${DEMO_CLUSTER_ID}/slo`,
    },
  ],
  diagnostic_pack_findings: [
    {
      ...incidentFinding,
      category: "diagnostic_pack",
      severity: "warning",
      title: "Consumer processing capacity declined",
      detail: "pack=consumer-lag.v2; reason_code=CONSUMER_PROCESSING_SLOW",
    },
  ],
  repeat_incidents: [
    {
      ...incidentFinding,
      category: "repeat_incident",
      severity: "warning",
      detail: "occurrence_count=6; previous_incident=known",
    },
  ],
  forecast_mean_absolute_error: 0.037,
  forecast_errors: [
    {
      ...capacityFinding,
      category: "forecast_error",
      severity: "info",
      title: "commitlog_disk_ratio forecast error 0.037",
      detail: "window=seven_days; predicted=0.721; actual=0.758; mae_sample=0.037",
    },
  ],
  source_gaps: [sourceGap],
  partial: false,
  warnings: [],
  cluster_mutation_count: 0,
};
