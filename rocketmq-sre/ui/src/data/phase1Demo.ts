import type {
  AssetSnapshot,
  CollectionEnvelope,
  ConversationView,
  EvidenceRecord,
  IncidentView,
  InspectionView,
  InvestigationView,
  KnowledgeItem,
  MessageJourney,
  ModelCapabilitiesResponse,
  Recommendation,
  TimelineEvent,
  TopologySnapshot,
  WorkflowStreamEvent,
} from "@/api/types";

export const DEMO_CLUSTER_ID = "10000000-0000-4000-8000-000000000001";
export const DEMO_TENANT_ID = "00000000-0000-4000-8000-000000000001";
const STAGING_CLUSTER_ID = "10000000-0000-4000-8000-000000000002";
const ACTOR = {
  subject: "sre.li",
  display_name: "李明 · Platform SRE",
};

function envelope<T>(
  items: T[],
  options: { partial?: boolean; warnings?: string[] } = {},
): CollectionEnvelope<T> {
  return {
    items,
    partial: options.partial ?? false,
    warnings: options.warnings ?? [],
    observed_at: "2026-07-27T08:42:00Z",
  };
}

function event(
  id: string,
  aggregate: "investigation" | "incident",
  aggregateId: string,
  type: string,
  summary: string,
  at: string,
): TimelineEvent {
  return {
    id,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    investigation_id:
      aggregate === "investigation" ? aggregateId : undefined,
    incident_id: aggregate === "incident" ? aggregateId : undefined,
    event_type: type,
    summary,
    details: {},
    correlation_id: "91000000-0000-4000-8000-000000000001",
    actor: ACTOR,
    occurred_at: at,
  };
}

export const phase1Assets = envelope<AssetSnapshot>(
  [
    ["name_server", "namesrv/0", "NameServer · rmq-namesrv-0", "queryable"],
    ["controller", "controller/0", "Controller · controller-0", "leader"],
    ["broker", "broker-a/0", "Broker A · master", "active"],
    ["broker", "broker-a/1", "Broker A · replica", "active"],
    ["proxy", "proxy/0", "Proxy · proxy-0", "ready"],
    ["proxy", "proxy/1", "Proxy · proxy-1", "ready"],
    ["store", "broker-a/store", "CommitLog Store · broker-a", "healthy"],
    ["topic", "topic/orders", "orders", "8 queues"],
    ["consumer", "group/order-worker", "order-worker", "lag 1,284"],
    ["pod", "pod/proxy-0", "Pod · rmq-proxy-0", "Running"],
  ].map(([kind, externalKey, displayName, state], index) => ({
    id: `20000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    kind: kind as AssetSnapshot["kind"],
    external_key: externalKey,
    display_name: displayName,
    source: index > 8 ? "kubernetes" : "rocketmq-mcp.v2",
    attributes: {
      state,
      owner: index > 8 ? "platform-runtime" : "messaging-platform",
      zone: index % 2 === 0 ? "cn-shanghai-a" : "cn-shanghai-b",
    },
    observed_at: "2026-07-27T08:40:00Z",
    freshness_seconds: index === 8 ? 185 : 34 + index,
    partial: index === 8,
    content_hash: `sha256:${String(index + 1).repeat(64).slice(0, 64)}`,
  })),
  {
    partial: true,
    warnings: ["consumer connection inventory reached the configured row bound"],
  },
);

export const phase1Topology: TopologySnapshot = {
  assets: phase1Assets.items,
  observed_at: phase1Assets.observed_at,
  partial: true,
  warnings: phase1Assets.warnings,
  edges: [
    ["namesrv/0", "broker-a/0", "routes_to"],
    ["controller/0", "broker-a/0", "contains"],
    ["broker-a/0", "broker-a/1", "replicates_to"],
    ["broker-a/0", "broker-a/store", "stores_on"],
    ["proxy/0", "broker-a/0", "connects_to"],
    ["proxy/1", "broker-a/0", "connects_to"],
    ["group/order-worker", "topic/orders", "consumes_from"],
    ["pod/proxy-0", "proxy/0", "runs_on"],
  ].map(([from, to, relation], index) => ({
    id: `21000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    from_key: from,
    to_key: to,
    relation: relation as TopologySnapshot["edges"][number]["relation"],
    source: relation === "runs_on" ? "kubernetes" : "rocketmq-mcp.v2",
    observed_at: "2026-07-27T08:40:00Z",
    freshness_seconds: 48,
    partial: false,
    content_hash: `sha256:${String(index + 2).repeat(64).slice(0, 64)}`,
  })),
};

const investigationId = "30000000-0000-4000-8000-000000000001";
const conversationId = "31000000-0000-4000-8000-000000000001";
const incidentId = "32000000-0000-4000-8000-000000000001";
const secondIncidentId = "32000000-0000-4000-8000-000000000002";

const investigation = {
  id: investigationId,
  tenant_id: DEMO_TENANT_ID,
  cluster_id: DEMO_CLUSTER_ID,
  conversation_id: conversationId,
  incident_id: incidentId,
  title: "orders 消费延迟持续上升",
  resource: "consumer-groups/order-worker",
  symptom_family: "consumer_lag",
  fingerprint: "consumer_lag:orders:order-worker",
  status: "promoted" as const,
  created_by: ACTOR,
  created_at: "2026-07-27T08:02:00Z",
  updated_at: "2026-07-27T08:31:00Z",
};

const investigationTimeline = [
  event(
    "33000000-0000-4000-8000-000000000001",
    "investigation",
    investigationId,
    "investigation.created",
    "已建立只读调查并锁定 Topic/Group 范围",
    "2026-07-27T08:02:00Z",
  ),
  event(
    "33000000-0000-4000-8000-000000000002",
    "investigation",
    investigationId,
    "evidence.partial",
    "Consumer connection 清单触发行数边界，结果标记为 partial",
    "2026-07-27T08:06:00Z",
  ),
  event(
    "33000000-0000-4000-8000-000000000003",
    "investigation",
    investigationId,
    "investigation.promoted",
    "调查已升级为 Incident，仍保持只读",
    "2026-07-27T08:31:00Z",
  ),
];

export const phase1Conversations = envelope<ConversationView>([
  {
    conversation: {
      id: conversationId,
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      question:
        "orders Topic 的 order-worker 消费组为什么在过去 30 分钟持续积压？",
      resource: "consumer-groups/order-worker",
      status: "promoted",
      investigation_id: investigationId,
      created_by: ACTOR,
      created_at: "2026-07-27T08:01:00Z",
      updated_at: "2026-07-27T08:31:00Z",
    },
    investigation,
  },
  {
    conversation: {
      id: "31000000-0000-4000-8000-000000000002",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      question: "Broker A 的磁盘恢复时间是否正在恶化？",
      resource: "brokers/broker-a",
      status: "active",
      created_by: {
        subject: "sre.wang",
        display_name: "王芳 · Messaging",
      },
      created_at: "2026-07-27T07:24:00Z",
      updated_at: "2026-07-27T07:42:00Z",
    },
  },
]);

export const phase1Investigations = envelope<InvestigationView>([
  { investigation, timeline: investigationTimeline },
  {
    investigation: {
      id: "30000000-0000-4000-8000-000000000002",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      title: "Broker A 磁盘恢复时间评估",
      resource: "brokers/broker-a",
      symptom_family: "store_recovery",
      fingerprint: "store_recovery:broker-a",
      status: "monitoring",
      created_by: {
        subject: "sre.wang",
        display_name: "王芳 · Messaging",
      },
      created_at: "2026-07-27T07:24:00Z",
      updated_at: "2026-07-27T07:42:00Z",
    },
    timeline: [
      event(
        "33000000-0000-4000-8000-000000000004",
        "investigation",
        "30000000-0000-4000-8000-000000000002",
        "investigation.monitoring",
        "证据未显示当前故障，进入监测状态",
        "2026-07-27T07:42:00Z",
      ),
    ],
  },
]);

export const phase1Incidents = envelope<IncidentView>([
  {
    incident: {
      id: incidentId,
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      title: "orders / order-worker 消费堆积",
      resource: "consumer-group:order-worker",
      symptom_family: "consumer_lag",
      status: "diagnosing",
      severity: "critical",
      owner: "messaging-platform",
      occurrence_count: 6,
      last_alert_at: "2026-07-27T08:42:00Z",
      summary: "Lag 已达 1,284，Broker 与路由健康，消费速率下降是当前主假设。",
      created_at: "2026-07-27T08:31:00Z",
      updated_at: "2026-07-27T08:42:00Z",
    },
    investigation,
    timeline: [
      ...investigationTimeline,
      event(
        "33000000-0000-4000-8000-000000000005",
        "incident",
        incidentId,
        "diagnosis.revision",
        "规则诊断 revision 2 已生成；模型网络调用仍禁用",
        "2026-07-27T08:42:00Z",
      ),
    ],
    diagnosis_revisions: [
      {
        id: "34000000-0000-4000-8000-000000000001",
        incident_id: incidentId,
        revision: 2,
        status: "diagnosing",
        rule_result: { pack: "consumer-lag.v1", outcome: "supported" },
        hypotheses: [
          {
            title: "Consumer 实例处理能力下降",
            confidence: 0.82,
            status: "supported",
          },
          {
            title: "Broker 路由或存储故障",
            confidence: 0.18,
            status: "contradicted",
          },
        ],
        evidence_ids: [
          "40000000-0000-4000-8000-000000000001",
          "40000000-0000-4000-8000-000000000002",
        ],
        execution_eligible: false,
        partial: true,
        created_at: "2026-07-27T08:42:00Z",
      },
    ],
  },
  {
    incident: {
      id: secondIncidentId,
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      title: "Controller heartbeat 短时抖动",
      resource: "controller:controller-0",
      symptom_family: "controller_heartbeat",
      status: "monitoring",
      severity: "warning",
      owner: "controller-team",
      occurrence_count: 2,
      last_alert_at: "2026-07-27T08:10:00Z",
      summary: "单次 heartbeat age 峰值，quorum 保持健康。",
      created_at: "2026-07-27T06:15:00Z",
      updated_at: "2026-07-27T08:10:00Z",
    },
    timeline: [
      event(
        "33000000-0000-4000-8000-000000000006",
        "incident",
        secondIncidentId,
        "incident.monitoring",
        "进入 2 小时稳定窗口监测",
        "2026-07-27T08:10:00Z",
      ),
    ],
    diagnosis_revisions: [],
  },
]);

const recommendations: Recommendation[] = [
  {
    id: "51000000-0000-4000-8000-000000000001",
    inspection_run_id: "50000000-0000-4000-8000-000000000001",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    severity: "warning",
    title: "核查 order-worker 实例处理耗时",
    rationale:
      "过去 30 分钟消费 TPS 下降 43%，Broker 投递与 Store 延迟保持稳定。",
    evidence_ids: ["40000000-0000-4000-8000-000000000001"],
    status: "open",
    created_at: "2026-07-27T08:30:00Z",
    updated_at: "2026-07-27T08:30:00Z",
  },
  {
    id: "51000000-0000-4000-8000-000000000002",
    inspection_run_id: "50000000-0000-4000-8000-000000000002",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    severity: "info",
    title: "补齐 Controller 生产查询验证",
    rationale: "本地指标已实现，但该来源尚未完成生产远程查询验证。",
    evidence_ids: [],
    status: "assigned",
    assignee: "messaging-observability",
    created_at: "2026-07-27T07:00:00Z",
    updated_at: "2026-07-27T07:15:00Z",
  },
];

export const phase1Inspections = envelope<InspectionView>(
  [
    {
      run: {
        id: "50000000-0000-4000-8000-000000000001",
        tenant_id: DEMO_TENANT_ID,
        cluster_id: DEMO_CLUSTER_ID,
        template: "consumer",
        status: "completed",
        finding_count: 1,
        partial: true,
        started_at: "2026-07-27T08:25:00Z",
        completed_at: "2026-07-27T08:30:00Z",
        created_at: "2026-07-27T08:25:00Z",
      },
      recommendations: [recommendations[0]],
    },
    {
      run: {
        id: "50000000-0000-4000-8000-000000000002",
        tenant_id: DEMO_TENANT_ID,
        cluster_id: DEMO_CLUSTER_ID,
        template: "cluster_health",
        status: "completed",
        schedule: "@hourly",
        finding_count: 1,
        partial: false,
        started_at: "2026-07-27T07:00:00Z",
        completed_at: "2026-07-27T07:02:00Z",
        created_at: "2026-07-26T00:00:00Z",
      },
      recommendations: [recommendations[1]],
    },
    {
      run: {
        id: "50000000-0000-4000-8000-000000000003",
        tenant_id: DEMO_TENANT_ID,
        cluster_id: DEMO_CLUSTER_ID,
        template: "telemetry",
        status: "running",
        finding_count: 0,
        partial: false,
        started_at: "2026-07-27T08:40:00Z",
        created_at: "2026-07-27T08:40:00Z",
      },
      recommendations: [],
    },
  ],
  { partial: true, warnings: ["one consumer source returned bounded output"] },
);

export const phase1Recommendations = envelope(recommendations);

export const phase1Evidence = envelope<EvidenceRecord>(
  [
    {
      schema: {
        family: "rocketmq-sre.evidence",
        major: 1,
        minor: 0,
      },
      evidence_id: "40000000-0000-4000-8000-000000000001",
      query_id: "41000000-0000-4000-8000-000000000001",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      source: "consumer.lag",
      resource: "consumer-groups/order-worker/topics/orders",
      correlation_id: "91000000-0000-4000-8000-000000000001",
      sensitivity: "internal",
      exposure: "admin_rpc",
      time_range: {
        start: "2026-07-27T08:11:40Z",
        end: "2026-07-27T08:41:40Z",
      },
      observed_at: "2026-07-27T08:41:40Z",
      freshness_seconds: 20,
      partial: false,
      warnings: [],
      coverage: "available",
      content: {
        storage: "inline",
        value: { lag: 1284, queues: 8, consumer_tps: 221 },
      },
      content_hash:
        "sha256:6c3b9f2e9a8c0b7da1b5c8f6e3d2a9f9e2a6c1d0b67f15590000000000000",
    },
    {
      schema: {
        family: "rocketmq-sre.evidence",
        major: 1,
        minor: 0,
      },
      evidence_id: "40000000-0000-4000-8000-000000000002",
      query_id: "41000000-0000-4000-8000-000000000002",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      source: "broker.runtime",
      resource: "brokers/broker-a",
      correlation_id: "91000000-0000-4000-8000-000000000001",
      sensitivity: "internal",
      exposure: "admin_rpc",
      time_range: {
        start: "2026-07-27T08:11:38Z",
        end: "2026-07-27T08:41:38Z",
      },
      observed_at: "2026-07-27T08:41:38Z",
      freshness_seconds: 22,
      partial: false,
      warnings: [],
      coverage: "available",
      content: {
        storage: "inline",
        value: {
          broker_up: true,
          put_tps: 642,
          page_cache_busy: false,
        },
      },
      content_hash:
        "sha256:7d4c9f2e9a8c0b7da1b5c8f6e3d2a9f9e2a6c1d0b67f15590000000000001",
    },
    {
      schema: {
        family: "rocketmq-sre.evidence",
        major: 1,
        minor: 0,
      },
      evidence_id: "40000000-0000-4000-8000-000000000003",
      query_id: "41000000-0000-4000-8000-000000000003",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      source: "consumer.connections",
      resource: "consumer-groups/order-worker",
      correlation_id: "91000000-0000-4000-8000-000000000001",
      sensitivity: "restricted",
      exposure: "admin_rpc",
      time_range: {
        start: "2026-07-27T08:11:35Z",
        end: "2026-07-27T08:41:35Z",
      },
      observed_at: "2026-07-27T08:41:35Z",
      freshness_seconds: 25,
      partial: true,
      warnings: [
        "bounded_output: client identifiers were pseudonymized",
      ],
      coverage: "partial",
      content: {
        storage: "reference",
        value: {
          uri: "s3://rocketmq-sre-evidence/demo/consumer-connections.json",
          digest:
            "sha256:8e5c9f2e9a8c0b7da1b5c8f6e3d2a9f9e2a6c1d0b67f15590000000000002",
          media_type: "application/json",
          size_bytes: 84,
        },
      },
      content_hash:
        "sha256:8e5c9f2e9a8c0b7da1b5c8f6e3d2a9f9e2a6c1d0b67f15590000000000002",
    },
  ],
  {
    partial: true,
    warnings: ["restricted client fields were sanitized and bounded"],
  },
);

export const phase1MessageJourney: MessageJourney = {
  schema_version: "rocketmq-sre.message-journey.v1",
  cluster_id: DEMO_CLUSTER_ID,
  trace_fingerprint: "sha256:af21…e09c",
  topic: "orders",
  queue_id: 3,
  message_body_available: false,
  partial: true,
  warnings: [
    "consumer receipt evidence is outside the configured retention window",
  ],
  hops: [
    {
      stage: "producer",
      component: "producer/order-api",
      observed_at: "2026-07-27T08:20:10.110Z",
      status: "observed",
      latency_ms: 4,
      evidence_id: phase1Evidence.items[0].evidence_id,
      detail: "Send result observed; producer address and payload removed.",
    },
    {
      stage: "proxy",
      component: "proxy/proxy-0",
      observed_at: "2026-07-27T08:20:10.114Z",
      status: "observed",
      latency_ms: 3,
      evidence_id: phase1Evidence.items[0].evidence_id,
      detail: "gRPC request completed with bounded outcome=ok.",
    },
    {
      stage: "broker",
      component: "broker-a",
      observed_at: "2026-07-27T08:20:10.117Z",
      status: "observed",
      latency_ms: 7,
      evidence_id: phase1Evidence.items[1].evidence_id,
      detail: "Queue 3 append confirmed.",
    },
    {
      stage: "store",
      component: "broker-a/store",
      observed_at: "2026-07-27T08:20:10.124Z",
      status: "observed",
      evidence_id: phase1Evidence.items[1].evidence_id,
      detail: "CommitLog offset is represented by a one-way fingerprint.",
    },
    {
      stage: "consumer",
      component: "group/order-worker",
      observed_at: "2026-07-27T08:20:11.000Z",
      status: "missing",
      evidence_id: phase1Evidence.items[2].evidence_id,
      detail: "Receipt evidence is unavailable; this is not treated as success.",
    },
  ],
};

export const phase1Knowledge = envelope<KnowledgeItem>([
  {
    id: "60000000-0000-4000-8000-000000000001",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    title: "Consumer Lag 诊断手册",
    component: "Consumer",
    rocketmq_version_range: ">=5.3.0-rust",
    source_uri: "rocketmq-doc://runbooks/consumer-lag-v3",
    source_version: "3.2.0",
    owner: "messaging-sre",
    review_status: "validated",
    review_due_at: "2026-10-01T00:00:00Z",
    sensitivity: "internal",
    content_hash: "sha256:knowledge-consumer-lag",
    conflict: false,
    summary: "通过 Lag、消费 TPS、连接、重平衡和 Broker 投递证据定位积压。",
    updated_at: "2026-07-20T06:00:00Z",
  },
  {
    id: "60000000-0000-4000-8000-000000000002",
    tenant_id: DEMO_TENANT_ID,
    title: "Controller Quorum 排障检查表",
    component: "Controller",
    rocketmq_version_range: ">=5.3.0-rust",
    source_uri: "rocketmq-doc://runbooks/controller-quorum-v1",
    source_version: "1.4.1",
    owner: "controller-team",
    review_status: "in_review",
    review_due_at: "2026-08-15T00:00:00Z",
    sensitivity: "internal",
    content_hash: "sha256:knowledge-controller-quorum",
    conflict: true,
    summary: "聚合 quorum health、heartbeat age、election span 与结构化日志。",
    updated_at: "2026-07-25T06:00:00Z",
  },
  {
    id: "60000000-0000-4000-8000-000000000003",
    tenant_id: DEMO_TENANT_ID,
    title: "Broker Store 恢复基线",
    component: "Store",
    rocketmq_version_range: ">=5.2.0-rust",
    source_uri: "rocketmq-doc://baselines/store-recovery",
    source_version: "2.0.0",
    owner: "storage-team",
    review_status: "validated",
    review_due_at: "2026-12-01T00:00:00Z",
    sensitivity: "internal",
    content_hash: "sha256:knowledge-store-recovery",
    conflict: false,
    summary: "Store 启动、恢复时间和 flush-behind 的生产基线。",
    updated_at: "2026-07-18T06:00:00Z",
  },
]);

export const phase1Models: ModelCapabilitiesResponse = {
  schema_version: "rocketmq-sre.model-capabilities.v1",
  network_calls_enabled: false,
  rules_only_available: true,
  providers: [
    "openai-compatible",
    "anthropic",
    "gemini",
    "bedrock",
    "deepseek",
    "zhipu-glm",
    "kimi-moonshot",
    "local-openai-compatible",
  ].map((id) => ({
    id,
    protocols:
      id === "anthropic"
        ? ["anthropic-messages"]
        : id === "gemini"
          ? ["gemini-generate-content"]
          : id === "bedrock"
            ? ["aws-bedrock-converse"]
            : ["openai-compatible"],
    supports_streaming: true,
    supports_tools: id !== "local-openai-compatible",
    supports_structured_output: true,
    supports_embeddings: ["openai-compatible", "local-openai-compatible"].includes(
      id,
    ),
  })),
  profiles: [
    {
      id: "70000000-0000-4000-8000-000000000001",
      profile_name: "rules-only",
      provider_family: "rules",
      protocol_family: "local",
      model_family: "deterministic-rules",
      model_name: "diagnostic-pack-evaluator",
      model_revision: "v1",
      endpoint_instance: "control-plane",
      region: "local",
      data_residency: "in-cluster",
      capabilities: ["structured_output"],
      enabled: true,
      health: "healthy",
      credential_present: false,
    },
    {
      id: "70000000-0000-4000-8000-000000000002",
      profile_name: "deepseek-prod",
      provider_family: "deepseek",
      protocol_family: "openai-compatible",
      model_family: "deepseek-reasoner",
      model_name: "deepseek-reasoner",
      model_revision: "configured-not-verified",
      endpoint_instance: "cn-primary",
      region: "cn",
      data_residency: "cn",
      capabilities: ["structured_output", "tools"],
      enabled: false,
      health: "unknown",
      credential_present: false,
    },
    {
      id: "70000000-0000-4000-8000-000000000003",
      profile_name: "zhipu-glm-prod",
      provider_family: "zhipu-glm",
      protocol_family: "openai-compatible",
      model_family: "glm",
      model_name: "glm-4-plus",
      model_revision: "configured-not-verified",
      endpoint_instance: "cn-primary",
      region: "cn",
      data_residency: "cn",
      capabilities: ["structured_output", "tools"],
      enabled: false,
      health: "unknown",
      credential_present: false,
    },
    {
      id: "70000000-0000-4000-8000-000000000004",
      profile_name: "kimi-prod",
      provider_family: "kimi-moonshot",
      protocol_family: "openai-compatible",
      model_family: "moonshot",
      model_name: "moonshot-v1",
      model_revision: "configured-not-verified",
      endpoint_instance: "cn-primary",
      region: "cn",
      data_residency: "cn",
      capabilities: ["structured_output", "tools"],
      enabled: false,
      health: "unknown",
      credential_present: false,
    },
  ],
};

export const phase1WorkflowEvents: WorkflowStreamEvent[] = [
  {
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    aggregate_type: "inspection",
    aggregate_id: "50000000-0000-4000-8000-000000000003",
    event_type: "inspection.progress",
    payload: {
      status: "running",
      progress: "5/6 required sources",
    },
    correlation_id: "91000000-0000-4000-8000-000000000002",
    occurred_at: "2026-07-27T08:42:10Z",
  },
  {
    tenant_id: DEMO_TENANT_ID,
    cluster_id: STAGING_CLUSTER_ID,
    aggregate_type: "investigation",
    aggregate_id: "30000000-0000-4000-8000-000000000010",
    event_type: "evidence.partial",
    payload: {
      status: "needs_evidence",
      source: "controller.quorum",
    },
    correlation_id: "91000000-0000-4000-8000-000000000003",
    occurred_at: "2026-07-27T08:42:20Z",
  },
];

export {
  envelope,
  recommendations,
};
