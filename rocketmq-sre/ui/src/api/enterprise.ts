import type { ApiRequestContext } from "@/auth/AuthContext";

import { apiQuery, apiRequest } from "./client";
import type {
  ComplianceFinding,
  DrActionItem,
  DrExercise,
  DrPlan,
  EnterpriseSnapshot,
  FinOpsShowbackRow,
  FleetAsset,
  FleetRegistration,
  FleetRegion,
  GovernanceArtifact,
  GovernanceObjectKind,
  GovernanceVersionPage,
} from "./enterpriseTypes";

export interface EnterpriseQuery {
  regionId?: string;
  environment?: string;
  owner?: string;
  health?: string;
  from?: string;
  to?: string;
}

export async function loadEnterpriseSnapshot(
  auth: ApiRequestContext | undefined,
  query: EnterpriseQuery,
  demoMode: boolean,
  signal?: AbortSignal,
): Promise<EnterpriseSnapshot> {
  if (demoMode) {
    return enterpriseDemoSnapshot();
  }

  const now = new Date();
  const to = query.to ?? now.toISOString();
  const from =
    query.from ??
    new Date(now.getTime() - 30 * 24 * 60 * 60 * 1_000).toISOString();
  const request = <T>(path: string) =>
    apiRequest<T>(path, { auth, signal });
  const fleetQuery = {
    region_id: query.regionId,
    environment: query.environment,
    owner: query.owner,
    health: query.health,
    limit: "200",
    offset: "0",
  };

  const [
    fleet,
    assets,
    compliance,
    inspections,
    drPlans,
    drExercises,
    drActionItems,
    governanceArtifacts,
    governanceCompliance,
    finops,
  ] = await Promise.all([
    request<EnterpriseSnapshot["fleet"]>(
      apiQuery("/v1/fleet/overview", fleetQuery),
    ),
    request<EnterpriseSnapshot["assets"]>(
      apiQuery("/v1/fleet/assets", fleetQuery),
    ),
    request<EnterpriseSnapshot["compliance"]>(
      apiQuery("/v1/fleet/compliance", {
        region_id: query.regionId,
        limit: "200",
        offset: "0",
      }),
    ),
    request<EnterpriseSnapshot["inspections"]>(
      apiQuery("/v1/fleet/inspections", { limit: "200" }),
    ),
    request<EnterpriseSnapshot["drPlans"]>(
      apiQuery("/v1/dr/plans", { limit: "200" }),
    ),
    request<EnterpriseSnapshot["drExercises"]>(
      apiQuery("/v1/dr/exercises", { limit: "200" }),
    ),
    request<EnterpriseSnapshot["drActionItems"]>(
      apiQuery("/v1/dr/action-items", { limit: "200" }),
    ),
    request<EnterpriseSnapshot["governanceArtifacts"]>(
      apiQuery("/v1/governance/artifacts", { limit: "200" }),
    ),
    request<EnterpriseSnapshot["governanceCompliance"]>(
      "/v1/governance/compliance",
    ),
    request<EnterpriseSnapshot["finops"]>(
      apiQuery("/v1/finops/report", {
        from,
        to,
        limit: "200",
      }),
    ),
  ]);

  return {
    fleet,
    assets,
    compliance,
    inspections,
    drPlans,
    drExercises,
    drActionItems,
    governanceArtifacts,
    governanceCompliance,
    finops,
  };
}

export async function loadGovernanceVersions(
  artifactId: string,
  auth: ApiRequestContext | undefined,
  demoMode: boolean,
  signal?: AbortSignal,
): Promise<GovernanceVersionPage> {
  if (demoMode) {
    const artifact = enterpriseDemoSnapshot().governanceArtifacts.items.find(
      (item) => item.id === artifactId,
    );
    if (!artifact) {
      return {
        schema_version: "rocketmq-sre.governance-api.v1",
        items: [],
        truncated: false,
      };
    }
    const index =
      enterpriseDemoSnapshot().governanceArtifacts.items.indexOf(artifact);
    const states = [
      "active",
      "active",
      "review",
      "deprecated",
      "quarantined",
    ] as const;
    const state = states[index % states.length];
    return {
      schema_version: "rocketmq-sre.governance-api.v1",
      items: [
        {
          id:
            artifact.current_version_id ??
            stableId("governance-version", index + 1),
          artifact_id: artifact.id,
          tenant_id: artifact.tenant_id,
          version: `${1 + (index % 3)}.${index % 5}.0`,
          content_digest: `sha256:${String(index + 11).padStart(64, "f")}`,
          signature:
            state === "review"
              ? undefined
              : {
                  algorithm: "ed25519",
                  key_id: "governance-signing-2026-q3",
                  value: `sig:${String(index + 1).padStart(48, "a")}`,
                },
          state,
          applicable_components: [
            index % 2 === 0 ? "broker" : "control-plane",
          ],
          applicable_version_range: ">=1.95,<2.0",
          dependencies:
            index > 1
              ? [
                  {
                    kind: "evidence_policy",
                    logical_key: "evidence_policy.enterprise.v1",
                    version: "1.2.0",
                  },
                ]
              : [],
          review_due_at:
            index === 7
              ? "2026-07-20T00:00:00.000Z"
              : "2026-10-30T00:00:00.000Z",
          expires_at:
            state === "deprecated"
              ? "2026-08-31T00:00:00.000Z"
              : undefined,
          rollback_version_id:
            index % 2 === 0
              ? stableId("governance-version-rollback", index + 1)
              : undefined,
          created_by: "governance.operator",
          created_at: "2026-06-01T00:00:00.000Z",
          updated_at: artifact.updated_at,
        },
      ],
      truncated: false,
    };
  }

  return apiRequest<GovernanceVersionPage>(
    apiQuery(
      `/v1/governance/artifacts/${encodeURIComponent(artifactId)}/versions`,
      { limit: "200" },
    ),
    { auth, signal },
  );
}

let cachedDemo: EnterpriseSnapshot | undefined;

export function enterpriseDemoSnapshot(): EnterpriseSnapshot {
  cachedDemo ??= buildEnterpriseDemoSnapshot();
  return cachedDemo;
}

function buildEnterpriseDemoSnapshot(): EnterpriseSnapshot {
  const now = new Date("2026-07-29T09:30:00.000Z");
  const tenantId = "50000000-0000-4000-8000-000000000001";
  const fleetId = "51000000-0000-4000-8000-000000000001";
  const regions: FleetRegion[] = [
    demoRegion(fleetId, 1, "cn-hangzhou", "华东 · 杭州", [
      "cn-mainland",
      "region-local",
    ]),
    demoRegion(fleetId, 2, "cn-beijing", "华北 · 北京", [
      "cn-mainland",
      "region-local",
    ]),
    demoRegion(fleetId, 3, "sg-singapore", "亚太 · 新加坡", [
      "apac",
      "aggregated-only",
    ]),
  ];
  const registrations = Array.from({ length: 100 }, (_, index) =>
    demoRegistration(fleetId, tenantId, regions[index % regions.length], index),
  );
  const assets = registrations.flatMap((registration, index) =>
    ["broker", "nameserver", "controller", "proxy"].map((component, componentIndex) =>
      demoAsset(registration, component, index, componentIndex),
    ),
  );
  const findings = registrations
    .filter((_, index) => index % 4 === 0)
    .map((registration, index) => demoCompliance(registration, index));
  const inspections = [
    demoInspection(fleetId, tenantId, regions, registrations, 1, "running"),
    demoInspection(fleetId, tenantId, regions, registrations, 2, "completed"),
    demoInspection(
      fleetId,
      tenantId,
      regions,
      registrations,
      3,
      "partially_completed",
    ),
  ];
  const drPlans = [
    demoDrPlan(fleetId, tenantId, regions[0], registrations[0], 1, "AI SRE 控制面恢复"),
    demoDrPlan(fleetId, tenantId, regions[1], registrations[1], 2, "RocketMQ 路由与存储恢复"),
  ];
  const drExercises = [
    demoDrExercise(drPlans[0], 1, "completed", 840, 92),
    demoDrExercise(drPlans[1], 2, "awaiting_manual_confirmation"),
    demoDrExercise(drPlans[0], 3, "planned"),
  ];
  const drActionItems = [
    demoDrActionItem(drExercises[0], 1, "done"),
    demoDrActionItem(drExercises[1], 2, "in_progress"),
    demoDrActionItem(drExercises[1], 3, "open"),
  ];
  const governanceArtifacts = demoGovernanceArtifacts(tenantId, now);
  const finopsRows = demoFinOpsRows(registrations);
  const totalCost = finopsRows.reduce((sum, row) => sum + row.cost_micros, 0);

  return {
    fleet: {
      schema_version: "rocketmq-sre.fleet-api.v1",
      fleet: {
        id: fleetId,
        name: "RocketMQ Production Fleet",
        owner: "messaging-platform",
      },
      tenant: {
        id: tenantId,
        fleet_id: fleetId,
        name: "platform-shared-services",
        owner: "sre-platform",
        active: true,
      },
      regions,
      registrations,
      observed_at: now.toISOString(),
    },
    assets: {
      schema_version: "rocketmq-sre.fleet-api.v1",
      items: assets,
      total: assets.length,
      limit: 200,
      offset: 0,
      health_distribution: countBy(assets.map((asset) => asset.health)),
      worst_health: "critical",
    },
    compliance: {
      schema_version: "rocketmq-sre.fleet-api.v1",
      items: findings,
      total: findings.length,
      limit: 200,
      offset: 0,
    },
    inspections: {
      schema_version: "rocketmq-sre.fleet-api.v1",
      items: inspections,
      truncated: false,
    },
    drPlans: {
      schema_version: "rocketmq-sre.dr-api.v1",
      items: drPlans,
      truncated: false,
    },
    drExercises: {
      schema_version: "rocketmq-sre.dr-api.v1",
      items: drExercises,
      truncated: false,
    },
    drActionItems: {
      schema_version: "rocketmq-sre.dr-api.v1",
      items: drActionItems,
      truncated: false,
    },
    governanceArtifacts: {
      schema_version: "rocketmq-sre.governance-api.v1",
      items: governanceArtifacts,
      truncated: false,
    },
    governanceCompliance: {
      schema_version: "rocketmq-sre.governance-api.v1",
      state_counts: {
        active: 8,
        review: 1,
        deprecated: 1,
        quarantined: 1,
      },
      unsigned_active: 0,
      expired_active: 0,
      overdue_review: 1,
      quarantined: 1,
      compliant: false,
      observed_at: now.toISOString(),
    },
    finops: {
      schema_version: "rocketmq-sre.finops.v1",
      tenant_id: tenantId,
      from: "2026-07-01T00:00:00.000Z",
      to: now.toISOString(),
      allocation_mode: "showback",
      chargeback_enabled: false,
      rows: finopsRows,
      total_cost_micros: totalCost,
      ledger_entries: 18_642,
      entries_missing_cost: 31,
      cost_coverage_basis_points: 9_983,
      forecasts: [
        {
          budget_id: stableId("budget", 1),
          period_start: "2026-07-01T00:00:00.000Z",
          period_end: "2026-08-01T00:00:00.000Z",
          observed_cost_micros: totalCost,
          projected_cost_micros: Math.round(totalCost * 1.17),
          hard_limit_micros: 98_000_000,
          sample_count: 18_642,
          coverage_basis_points: 9_983,
          projected_over_budget: false,
          generated_at: now.toISOString(),
        },
      ],
      anomalies: [
        {
          scope_kind: "cluster",
          scope_key: registrations[22].cluster_id,
          current_cost_micros: 3_820_000,
          baseline_cost_micros: 1_420_000,
          change_basis_points: 16_901,
          reason_code: "model_retry_spike",
        },
      ],
      warnings: [
        "SLO outcome attribution is unavailable for 31 ledger entries.",
      ],
      generated_at: now.toISOString(),
    },
  };
}

function demoRegion(
  fleetId: string,
  index: number,
  key: string,
  displayName: string,
  residencyTags: string[],
): FleetRegion {
  return {
    id: stableId("region", index),
    fleet_id: fleetId,
    key,
    display_name: displayName,
    owner: `${key}-sre`,
    residency_tags: residencyTags,
    active: true,
    updated_at: "2026-07-29T09:29:00.000Z",
  };
}

function demoRegistration(
  fleetId: string,
  tenantId: string,
  region: FleetRegion,
  index: number,
): FleetRegistration {
  const state =
    index % 29 === 0
      ? "read_only_degraded"
      : index % 47 === 0
        ? "onboarding"
        : "active";
  return {
    cluster_id: stableId("cluster", index + 1),
    fleet_id: fleetId,
    tenant_id: tenantId,
    region_id: region.id,
    external_cluster_key: `rmq-${region.key}-${String(index + 1).padStart(3, "0")}`,
    environment: index % 9 === 0 ? "staging" : "production",
    owner: index % 3 === 0 ? "payments-sre" : "messaging-platform",
    state,
    residency_tags: region.residency_tags,
    lifecycle_revision: 4 + (index % 7),
    updated_at: new Date(
      Date.parse("2026-07-29T09:29:00.000Z") - index * 11_000,
    ).toISOString(),
  };
}

function demoAsset(
  registration: FleetRegistration,
  component: string,
  registrationIndex: number,
  componentIndex: number,
): FleetAsset {
  const health =
    registration.state === "read_only_degraded" && componentIndex === 0
      ? "critical"
      : (registrationIndex + componentIndex) % 17 === 0
        ? "degraded"
        : "healthy";
  return {
    cluster_id: registration.cluster_id,
    fleet_id: registration.fleet_id,
    tenant_id: registration.tenant_id,
    region_id: registration.region_id,
    environment: registration.environment,
    owner: registration.owner,
    component,
    component_version:
      component === "proxy" ? "5.3.1" : `5.3.${registrationIndex % 3}`,
    image_digest: `sha256:${String(registrationIndex + 31).padStart(64, "a")}`,
    feature_digest: `sha256:${String(componentIndex + 7).padStart(64, "b")}`,
    configuration_digest: `sha256:${String(registrationIndex + 9).padStart(64, "c")}`,
    health,
    attributes: {
      replicas: String(component === "broker" ? 3 : 2),
      zone: `az-${(registrationIndex % 3) + 1}`,
    },
    observed_at: registration.updated_at,
  };
}

function demoCompliance(
  registration: FleetRegistration,
  index: number,
): ComplianceFinding {
  const severities: ComplianceFinding["severity"][] = [
    "critical",
    "error",
    "warning",
    "info",
  ];
  const states: ComplianceFinding["state"][] = [
    "open",
    "acknowledged",
    "open",
    "accepted_exception",
  ];
  return {
    id: stableId("finding", index + 1),
    fleet_id: registration.fleet_id,
    tenant_id: registration.tenant_id,
    region_id: registration.region_id,
    cluster_id: registration.cluster_id,
    category: index % 2 === 0 ? "broker-runtime-template" : "tls-policy",
    expected_digest: `sha256:${String(index + 3).padStart(64, "d")}`,
    live_digest: `sha256:${String(index + 5).padStart(64, "e")}`,
    evidence_ids: [stableId("evidence", index + 1)],
    severity: severities[index % severities.length],
    owner: registration.owner,
    recommendation:
      index % 2 === 0
        ? "在下一个维护窗口创建类型化配置计划。"
        : "复核证书轮换策略并保留验证证据。",
    state: states[index % states.length],
    observed_at: registration.updated_at,
  };
}

function demoInspection(
  fleetId: string,
  tenantId: string,
  regions: FleetRegion[],
  registrations: FleetRegistration[],
  index: number,
  state: EnterpriseSnapshot["inspections"]["items"][number]["state"],
) {
  const targetCount = index === 1 ? 100 : index === 2 ? 40 : 72;
  return {
    id: stableId("inspection", index),
    fleet_id: fleetId,
    tenant_id: tenantId,
    region_ids: regions.map((region) => region.id),
    cluster_ids: registrations
      .slice(0, targetCount)
      .map((registration) => registration.cluster_id),
    pack_ids: ["fleet-health.v1", "upgrade-readiness.v1"],
    max_concurrency: 8,
    timeout_seconds: 1_800,
    model_token_budget: 240_000,
    evidence_byte_budget: 64_000_000,
    state,
    completed_clusters:
      state === "running" ? 63 : state === "partially_completed" ? 68 : targetCount,
    failed_clusters: state === "partially_completed" ? 4 : 0,
    created_at: `2026-07-${String(28 - index).padStart(2, "0")}T08:00:00.000Z`,
    completed_at:
      state === "completed" ? "2026-07-27T08:18:00.000Z" : undefined,
  };
}

function demoDrPlan(
  fleetId: string,
  tenantId: string,
  region: FleetRegion,
  registration: FleetRegistration,
  index: number,
  name: string,
): DrPlan {
  return {
    id: stableId("dr-plan", index),
    fleet_id: fleetId,
    tenant_id: tenantId,
    region_id: region.id,
    cluster_id: registration.cluster_id,
    subject: index === 1 ? "ai_sre_control_plane" : "rocket_mq_cluster",
    name,
    version: 3,
    owner: "resilience-sre",
    target: {
      rto_seconds: index === 1 ? 1_800 : 900,
      rpo_seconds: index === 1 ? 300 : 60,
    },
    allowed_modes: ["readiness", "tabletop", "supervised_test"],
    required_sources: ["postgresql", "object_storage", "audit_ledger"],
    checkpoints: [
      {
        key: "restore-authority",
        title: "恢复权威状态与身份",
        expected_duration_seconds: 240,
        manual_confirmation_required: true,
        cleanup_required: false,
        required_evidence_kinds: ["backup-manifest", "identity-check"],
      },
      {
        key: "verify-data-plane",
        title: "验证消息数据面",
        expected_duration_seconds: 360,
        manual_confirmation_required: false,
        cleanup_required: true,
        required_evidence_kinds: ["synthetic-probe", "slo-window"],
      },
    ],
    active: true,
    updated_at: "2026-07-28T10:00:00.000Z",
  };
}

function demoDrExercise(
  plan: DrPlan,
  index: number,
  state: DrExercise["state"],
  actualRto?: number,
  actualRpo?: number,
): DrExercise {
  return {
    id: stableId("dr-exercise", index),
    plan_id: plan.id,
    tenant_id: plan.tenant_id,
    region_id: plan.region_id,
    cluster_id: plan.cluster_id,
    mode: index === 3 ? "tabletop" : "supervised_test",
    boundary: "test_resources_only",
    state,
    target: plan.target,
    actual_rto_seconds: actualRto,
    actual_rpo_seconds: actualRpo,
    manual_checkpoint_count: 1,
    cleanup_complete: state === "completed",
    evidence_ids:
      state === "completed" ? [stableId("dr-evidence", index)] : [],
    created_by: "resilience.operator",
    started_at: state === "planned" ? undefined : "2026-07-28T02:00:00.000Z",
    completed_at:
      state === "completed" ? "2026-07-28T02:14:00.000Z" : undefined,
    updated_at: "2026-07-28T02:14:00.000Z",
  };
}

function demoDrActionItem(
  exercise: DrExercise,
  index: number,
  status: DrActionItem["status"],
): DrActionItem {
  return {
    id: stableId("dr-action", index),
    finding_id: stableId("dr-finding", index),
    tenant_id: exercise.tenant_id,
    cluster_id: exercise.cluster_id,
    title:
      index === 1
        ? "补充 PostgreSQL 恢复证据"
        : "缩短跨区域路由恢复确认时间",
    owner: "resilience-sre",
    due_at: `2026-08-${String(index + 2).padStart(2, "0")}T10:00:00.000Z`,
    status,
    verification:
      status === "done" ? "restore manifest and checksum verified" : undefined,
    evidence_ids: status === "done" ? [stableId("dr-evidence", index)] : [],
    updated_at: "2026-07-29T08:30:00.000Z",
  };
}

function demoGovernanceArtifacts(
  tenantId: string,
  now: Date,
): GovernanceArtifact[] {
  const kinds: GovernanceObjectKind[] = [
    "data_policy",
    "evidence_policy",
    "prompt",
    "knowledge",
    "model",
    "provider",
    "diagnostic_pack",
    "policy",
    "action",
    "runbook",
    "integration",
  ];
  return kinds.map((kind, index) => ({
    id: stableId("governance-artifact", index + 1),
    tenant_id: tenantId,
    kind,
    logical_key: `${kind}.enterprise.v${index % 3}`,
    owner: index % 2 === 0 ? "sre-governance" : "messaging-platform",
    reviewer: "risk-review-board",
    current_version_id:
      index === 8 ? undefined : stableId("governance-version", index + 1),
    updated_at: new Date(now.getTime() - index * 3_600_000).toISOString(),
  }));
}

function demoFinOpsRows(
  registrations: FleetRegistration[],
): FinOpsShowbackRow[] {
  return registrations.slice(0, 12).map((registration, index) => ({
    dimensions: {
      region_id: registration.region_id,
      cluster_id: registration.cluster_id,
      provider: index % 3 === 0 ? "deepseek" : "openai-compatible",
      model: index % 3 === 0 ? "deepseek-chat" : "gpt-5-mini",
      workload: index % 2 === 0 ? "incident" : "inspection",
    },
    request_count: 420 + index * 37,
    input_tokens: 84_000 + index * 4_900,
    output_tokens: 17_000 + index * 1_350,
    error_count: index % 5,
    average_latency_millis: 720 + index * 31,
    cost_micros: 2_400_000 + index * 390_000,
    successful_outcomes: 32 + index,
    slo_compliant_outcomes: 29 + index,
    estimated_minutes_saved: 180 + index * 24,
  }));
}

function countBy(values: string[]): Record<string, number> {
  return values.reduce<Record<string, number>>((counts, value) => {
    counts[value] = (counts[value] ?? 0) + 1;
    return counts;
  }, {});
}

function stableId(prefix: string, index: number): string {
  const prefixCode = [...prefix].reduce(
    (value, character) => (value * 31 + character.charCodeAt(0)) >>> 0,
    17,
  );
  return `${prefixCode.toString(16).padStart(8, "0")}-0000-4000-8000-${String(index).padStart(12, "0")}`;
}
