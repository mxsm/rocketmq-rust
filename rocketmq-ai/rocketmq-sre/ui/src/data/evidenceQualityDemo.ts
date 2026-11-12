import type {
  CapabilityFact,
  CapabilityQualityProjection,
  DiagnosticPackQuality,
  DiagnosticRequirementQuality,
  EvidenceRecord,
  InvestigationView,
} from "@/api/types";
import {
  DEMO_CLUSTER_ID,
  DEMO_TENANT_ID,
  phase1Evidence,
  phase1Investigations,
} from "./phase1Demo";

type Field = DiagnosticRequirementQuality["fields"][number];
interface Template {
  pack: string;
  id: string;
  source: string;
  prefix: string;
  maxAge: number;
  fields: Field[];
}
const fields = (kind: Field["kind"], paths: string[]): Field[] =>
  paths.map((path) => ({ path, kind }));

// Synthetic development samples follow the compiled field names, without
// claiming connector, target-environment or production verification.
const TEMPLATES: Template[] = [
  {
    pack: "consumer-lag.v2",
    id: "consumer-lag",
    source: "rocketmq-mcp",
    prefix: "consumer-lag/",
    maxAge: 300,
    fields: fields("number", [
      "total_lag",
      "lag_slope_per_min",
      "queue_skew_ratio",
      "consume_rate_per_sec",
      "produce_rate_per_sec",
    ]),
  },
  {
    pack: "store-pressure.v1",
    id: "store-pressure",
    source: "admin-query",
    prefix: "store-pressure/",
    maxAge: 300,
    fields: fields("boolean", [
      "disk_pressure",
      "flush_or_dispatch_stalled",
      "tiered_wal_pressure",
    ]),
  },
  {
    pack: "send-latency.v1",
    id: "send-latency",
    source: "prometheus",
    prefix: "send-latency/",
    maxAge: 180,
    fields: fields("boolean", [
      "client_or_proxy_slow",
      "remoting_or_broker_slow",
      "store_slow",
    ]),
  },
  {
    pack: "broker-ha.v1",
    id: "broker-ha",
    source: "admin-query",
    prefix: "broker-ha/",
    maxAge: 180,
    fields: fields("boolean", [
      "replica_lag_high",
      "sync_state_set_sufficient",
      "replicas_healthy",
    ]),
  },
  {
    pack: "namesrv-route.v1",
    id: "namesrv-route",
    source: "rocketmq-mcp",
    prefix: "namesrv-route/",
    maxAge: 180,
    fields: fields("boolean", [
      "route_divergent",
      "registration_stale",
      "broker_unreachable",
    ]),
  },
  {
    pack: "producer-connectivity.v1",
    id: "producer",
    source: "admin-query",
    prefix: "producer-connectivity/",
    maxAge: 300,
    fields: [
      ...fields("boolean", [
        "connected",
        "backpressure",
        "client_version_supported",
      ]),
      ...fields("number", [
        "route_age_seconds",
        "send_error_rate_percent",
        "retry_rate_percent",
        "timeout_rate_percent",
        "queue_selection_failures",
      ]),
    ],
  },
];
export const QUALITY_SCENARIOS = [
  { id: "backlog_store", pack_ids: ["consumer-lag.v2", "store-pressure.v1"] },
  { id: "send_latency_ha", pack_ids: ["send-latency.v1", "broker-ha.v1"] },
  {
    id: "routing_disconnect",
    pack_ids: ["namesrv-route.v1", "producer-connectivity.v1"],
  },
];

function fact(
  status: CapabilityFact["status"],
  reason: string,
  at?: string,
): CapabilityFact {
  return {
    status,
    reason_code: reason,
    source_ref: "development_fixture",
    observed_at: at ?? null,
  };
}

export function demoCapabilityQuality(
  clusterId: string | undefined,
  tenantId: string,
  evidence: EvidenceRecord[],
  now = new Date().toISOString(),
): CapabilityQualityProjection {
  const packs: DiagnosticPackQuality[] = TEMPLATES.map((template) => {
    const sample = evidence
      .filter(
        (item) =>
          item.tenant_id === tenantId &&
          item.cluster_id === clusterId &&
          item.source === template.source &&
          item.resource.startsWith(template.prefix),
      )
      .sort((a, b) => Date.parse(b.observed_at) - Date.parse(a.observed_at))[0];
    const value =
      sample?.content.storage === "inline" ? sample.content.value : undefined;
    const missing = template.fields
      .filter(
        (field) =>
          !value ||
          typeof value !== "object" ||
          Array.isArray(value) ||
          typeof value[field.path as keyof typeof value] !== field.kind,
      )
      .map((field) => field.path);
    const age = sample
      ? Math.max(
          sample.freshness_seconds,
          (Date.parse(now) - Date.parse(sample.observed_at)) / 1000,
        )
      : 0;
    const quality: DiagnosticRequirementQuality["quality"] = !clusterId
      ? "unknown"
      : !sample
        ? "missing"
        : age > template.maxAge
          ? "stale"
          : sample.coverage === "missing"
            ? "missing"
            : sample.partial || missing.length
              ? "partial"
              : "available";
    const requirement: DiagnosticRequirementQuality = {
      id: template.id,
      source: template.source,
      resource_prefix: template.prefix,
      required: true,
      max_age_seconds: template.maxAge,
      fields: template.fields,
      quality,
      reason_code: `development_fixture_${quality}`,
      observed_at: sample?.observed_at ?? null,
      evidence_ids: sample ? [sample.evidence_id] : [],
      missing_fields: sample
        ? missing
        : template.fields.map((field) => field.path),
    };
    return {
      pack_id: template.pack,
      requirements: [requirement],
      layers: {
        registered: fact("present", "synthetic_descriptor_fixture", now),
        handler: fact("present", "synthetic_handler_fixture", now),
        configured: fact("unknown", "no_connector_verification_record"),
        target_observed: fact(
          quality === "available"
            ? "present"
            : quality === "unknown"
              ? "unknown"
              : "absent",
          `synthetic_sample_${quality}`,
          sample?.observed_at,
        ),
        test_verified: fact("unknown", "no_deployment_test_record"),
        production_verified: fact(
          "unknown",
          "no_production_verification_record",
        ),
      },
    };
  });
  return {
    schema_version: "rocketmq-sre.capability-quality.v1",
    tenant_id: tenantId,
    cluster_id: clusterId ?? null,
    investigation_id: null,
    revision_id: null,
    observed_at: now,
    execution_eligible: false,
    partial: false,
    packs,
    scenarios: QUALITY_SCENARIOS.map((scenario) => ({
      ...scenario,
      required_signal_ids: TEMPLATES.filter((item) =>
        scenario.pack_ids.includes(item.pack),
      ).map((item) => `${item.pack}:${item.id}`),
    })),
  };
}

export function createEvidenceQualityDemo(now = new Date()) {
  const at = new Date(now.getTime() - 60_000).toISOString();
  const values: Array<Record<string, number | boolean>> = [
    {
      total_lag: 1284,
      lag_slope_per_min: 18,
      queue_skew_ratio: 1.4,
      consume_rate_per_sec: 221,
      produce_rate_per_sec: 642,
    },
    {
      disk_pressure: true,
      flush_or_dispatch_stalled: false,
      tiered_wal_pressure: false,
    },
    { client_or_proxy_slow: false, remoting_or_broker_slow: true },
    {
      replica_lag_high: true,
      sync_state_set_sufficient: true,
      replicas_healthy: true,
    },
    {
      route_divergent: true,
      registration_stale: false,
      broker_unreachable: false,
    },
  ];
  const evidence: EvidenceRecord[] = values.map((value, index) => ({
    ...structuredClone(phase1Evidence.items[0]),
    evidence_id: `42000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
    source: TEMPLATES[index].source,
    resource: `${TEMPLATES[index].prefix}demo`,
    exposure:
      TEMPLATES[index].source === "rocketmq-mcp"
        ? "mcp_tool"
        : TEMPLATES[index].source === "prometheus"
          ? "prometheus_api"
          : "admin_rpc",
    observed_at:
      index === 3 ? new Date(now.getTime() - 900_000).toISOString() : at,
    time_range: {
      start: new Date(now.getTime() - 900_000).toISOString(),
      end: at,
    },
    freshness_seconds: index === 3 ? 900 : 0,
    partial: index === 2,
    coverage: index === 2 ? "partial" : "available",
    warnings: ["development_fixture_not_target_verification"],
    content: { storage: "inline", value: { ...value, demo: true } },
  }));
  const context: EvidenceRecord = {
    ...structuredClone(evidence[0]),
    evidence_id: "42000000-0000-4000-8000-000000000006",
    source: "kubernetes",
    resource: "deployment/another-service",
    exposure: "kubernetes_api",
    content: {
      storage: "inline",
      value: { demo: true, rollout_observed: true },
    },
  };
  evidence.push(context);
  const titles = [
    "[演示] 积压 + Store 压力 / 完整样本",
    "[演示] 发送延迟 + HA lag / 缺失与过期",
    "[演示] 路由变更 + 断连 / 缺源与同期发布",
  ];
  const ids = [
    [evidence[0].evidence_id, evidence[1].evidence_id],
    [evidence[2].evidence_id, evidence[3].evidence_id],
    [evidence[4].evidence_id, context.evidence_id],
  ];
  const investigations: InvestigationView[] = QUALITY_SCENARIOS.map(
    (scenario, index) => {
      const id = `32000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`;
      return {
        investigation: {
          ...structuredClone(phase1Investigations.items[1].investigation),
          id,
          tenant_id: DEMO_TENANT_ID,
          cluster_id: DEMO_CLUSTER_ID,
          title: titles[index],
          status: "needs_evidence",
          resource: undefined,
          symptom_family: scenario.id,
          created_at: at,
          updated_at: at,
        },
        diagnosis_revisions: [
          {
            id: `35000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
            investigation_id: id,
            conversation_id: "",
            turn_id: `demo-turn-${index}`,
            answer_revision_id: `demo-answer-${index}`,
            revision: 1,
            pack_id: scenario.pack_ids[0],
            pack_version: "1.0.0",
            status: "inconclusive",
            rule_result: {
              missing_required_evidence:
                index === 2
                  ? ["producer connectivity snapshot"]
                  : index === 1
                    ? ["store_slow", "fresh HA sample"]
                    : [],
            },
            hypotheses: [],
            evidence_ids: ids[index],
            primary_model_invocation_id: null,
            execution_eligible: false,
            partial: index > 0,
            correlation_id:
              evidence[index * 2]?.correlation_id ?? context.correlation_id,
            created_at: at,
          },
        ],
        timeline:
          index === 2
            ? [
                {
                  id: "33000000-0000-4000-8000-000000000099",
                  tenant_id: DEMO_TENANT_ID,
                  cluster_id: DEMO_CLUSTER_ID,
                  investigation_id: id,
                  event_type: "deployment_observed",
                  summary: "[演示] 另一服务同期发布，尚未建立支持关系",
                  details: {},
                  correlation_id: context.correlation_id,
                  actor: { subject: "development-fixture" },
                  occurred_at: at,
                },
              ]
            : [],
      };
    },
  );
  return { evidence, investigations };
}
