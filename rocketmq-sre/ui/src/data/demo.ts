import type {
  CapabilityCatalogResponse,
  CapabilitySnapshot,
  ClusterSummary,
  CoverageMatrix,
  EvidenceRow,
} from "@/api/types";

export const demoClusters: ClusterSummary[] = [
  {
    id: "10000000-0000-4000-8000-000000000001",
    tenant_id: "platform-core",
    external_cluster_key: "rmq-prod-cn",
    environment: "生产",
    region: "cn-shanghai",
    rocketmq_version: "5.3.2-rust",
    deployment_mode: "controller",
    owner: "platform-sre",
    state: "ready_read_only",
    effective_access_profile: "read_only",
    created_at: "2026-07-20T08:00:00Z",
    updated_at: "2026-07-26T07:34:00Z",
  },
  {
    id: "10000000-0000-4000-8000-000000000002",
    tenant_id: "messaging-dev",
    external_cluster_key: "rmq-staging",
    environment: "预发",
    region: "cn-hangzhou",
    rocketmq_version: "5.3.2-rust",
    deployment_mode: "controller",
    owner: "middleware",
    state: "read_only_degraded",
    effective_access_profile: "read_only",
    created_at: "2026-07-22T03:00:00Z",
    updated_at: "2026-07-26T07:06:00Z",
  },
  {
    id: "10000000-0000-4000-8000-000000000003",
    tenant_id: "disaster-recovery",
    external_cluster_key: "rmq-dr",
    environment: "容灾",
    region: "cn-beijing",
    rocketmq_version: "5.3.1-rust",
    deployment_mode: "controller",
    owner: "platform-sre",
    state: "rejected",
    effective_access_profile: "read_only",
    created_at: "2026-07-24T05:00:00Z",
    updated_at: "2026-07-26T07:18:00Z",
  },
];

const baseSources: CapabilitySnapshot["data_sources"] = [
  {
    id: "broker",
    availability: "queryable",
    freshness_ms: 8 * 60 * 1000,
    detail: "required signals verified",
  },
  {
    id: "nameserver",
    availability: "queryable",
    freshness_ms: 8 * 60 * 1000,
    detail: "route freshness verified",
  },
  {
    id: "controller",
    availability: "queryable",
    freshness_ms: 9 * 60 * 1000,
    detail: "quorum and election evidence verified",
  },
  {
    id: "proxy",
    availability: "queryable",
    freshness_ms: 7 * 60 * 1000,
    detail: "bounded result telemetry",
  },
  {
    id: "mcp",
    availability: "queryable",
    freshness_ms: 8 * 60 * 1000,
    detail: "system resources authenticated",
  },
  {
    id: "runtime",
    availability: "queryable",
    freshness_ms: 10 * 60 * 1000,
    detail: "diagnostics view v1",
  },
];

export const demoCapabilities: Record<string, CapabilitySnapshot> = {
  [demoClusters[0].id]: {
    cluster_id: demoClusters[0].id,
    digest:
      "sha256:6c3b9f2e9a8c0b7da1b5c8f6e3d2a9f9e2a6c1d0b67f1559",
    protocol_version: "2025-11-25",
    schema_version: "rocketmq-mcp.v2",
    mutation_supported: false,
    observed_at: "2026-07-26T07:34:00Z",
    data_sources: baseSources,
    manifest: {
      tool_surface_digest: "sha256:c7d9e8f1a2b3",
      resource_surface_digest: "sha256:f1e2d3c4b5a6",
      visible_tools: [
        "rocketmq_get_cluster_overview",
        "rocketmq_list_topics",
        "rocketmq_describe_topic",
        "rocketmq_describe_broker",
        "rocketmq_get_consumer_lag",
      ],
      visible_resources: [
        "rocketmq://clusters/rmq-prod-cn/capabilities",
        "rocketmq://system/runtime/v1",
        "rocketmq://system/observability/v1",
      ],
    },
  },
  [demoClusters[1].id]: {
    cluster_id: demoClusters[1].id,
    digest: "sha256:aa1bb2cc3dd4ee5ff6",
    protocol_version: "2025-11-25",
    schema_version: "rocketmq-mcp.v2",
    mutation_supported: false,
    observed_at: "2026-07-26T07:06:00Z",
    data_sources: baseSources.map((source) =>
      source.id === "controller"
        ? {
            ...source,
            availability: "not_production_verified",
            detail: "controller evidence has not completed production verification",
          }
        : source,
    ),
    manifest: {
      tool_surface_digest: "sha256:c7d9e8f1a2b3",
      resource_surface_digest: "sha256:f1e2d3c4b5a6",
    },
  },
  [demoClusters[2].id]: {
    cluster_id: demoClusters[2].id,
    digest: "sha256:degraded01aa22bb33cc44",
    protocol_version: "2025-11-25",
    schema_version: "rocketmq-mcp.v2",
    mutation_supported: false,
    observed_at: "2026-07-26T07:18:00Z",
    data_sources: baseSources.map((source) => ({
      ...source,
      availability:
        source.id === "mcp"
          ? "missing_instrumentation"
          : "not_production_verified",
      detail:
        source.id === "mcp"
          ? "MCP connection unavailable"
          : "handshake rejected before production verification",
    })),
    manifest: {
      tool_surface_digest: "sha256:unverified",
      resource_surface_digest: "sha256:unverified",
    },
  },
};

export const demoEvidence: EvidenceRow[] = [
  {
    id: "cluster-overview",
    source: "cluster.overview",
    sourceLabel: "集群概览",
    status: "complete",
    observedAt: "2026-07-26T07:10:21Z",
    freshnessSeconds: 120,
    coveragePercent: 100,
    hash: "a1b2c3d4e5f6",
  },
  {
    id: "topic-inventory",
    source: "topic.inventory",
    sourceLabel: "Topic 清单",
    status: "complete",
    observedAt: "2026-07-26T07:10:25Z",
    freshnessSeconds: 120,
    coveragePercent: 98,
    hash: "b2c3d4e5f6a7",
  },
  {
    id: "broker-runtime",
    source: "broker.runtime",
    sourceLabel: "Broker 运行时",
    status: "partial",
    observedAt: "2026-07-26T07:10:28Z",
    freshnessSeconds: 120,
    coveragePercent: 95,
    hash: "c3d4e5f6a7b8",
    warning: "bounded_output: 结果已按字节上限截断",
  },
  {
    id: "consumer-lag",
    source: "consumer.lag",
    sourceLabel: "消费延迟",
    status: "unavailable",
    observedAt: "2026-07-26T07:10:31Z",
    errorCode: "source_unavailable",
  },
  {
    id: "mcp-runtime",
    source: "mcp.runtime",
    sourceLabel: "MCP 运行时",
    status: "complete",
    observedAt: "2026-07-26T07:10:35Z",
    freshnessSeconds: 60,
    coveragePercent: 100,
    hash: "e5f6a7b8c9d0",
  },
];

export const demoCoverage: CoverageMatrix = {
  generatedAt: "2026-07-26T05:00:00Z",
  semanticSignalCount: 169,
  semanticOwnerCount: 16,
  packs: [
    { id: "cluster_health", label: "集群健康" },
    { id: "route_health", label: "路由异常" },
    { id: "consumer_lag", label: "消费堆积" },
    { id: "broker_runtime", label: "Broker 运行态" },
    { id: "controller_stability", label: "控制器选举" },
    { id: "mcp_runtime", label: "MCP 自身状态" },
  ],
  rows: [
    {
      component: "Broker",
      cells: {
        cluster_health: "implemented_local",
        route_health: "not_production_verified",
        consumer_lag: "implemented_local",
        broker_runtime: "implemented_local",
        controller_stability: "not_production_verified",
        mcp_runtime: "not_production_verified",
      },
    },
    {
      component: "NameServer",
      cells: {
        cluster_health: "implemented_local",
        route_health: "implemented_local",
        consumer_lag: "not_production_verified",
        broker_runtime: "not_production_verified",
        controller_stability: "not_production_verified",
        mcp_runtime: "not_production_verified",
      },
    },
    {
      component: "Controller",
      cells: {
        cluster_health: "implemented_local",
        route_health: "not_production_verified",
        consumer_lag: "not_production_verified",
        broker_runtime: "not_production_verified",
        controller_stability: "implemented_local",
        mcp_runtime: "not_production_verified",
      },
    },
    {
      component: "Proxy",
      cells: {
        cluster_health: "implemented_local",
        route_health: "not_production_verified",
        consumer_lag: "not_production_verified",
        broker_runtime: "not_production_verified",
        controller_stability: "not_production_verified",
        mcp_runtime: "not_production_verified",
      },
    },
    {
      component: "MCP",
      cells: {
        cluster_health: "not_production_verified",
        route_health: "not_production_verified",
        consumer_lag: "not_production_verified",
        broker_runtime: "not_production_verified",
        controller_stability: "not_production_verified",
        mcp_runtime: "queryable",
      },
    },
    {
      component: "Runtime",
      cells: {
        cluster_health: "not_production_verified",
        route_health: "not_production_verified",
        consumer_lag: "not_production_verified",
        broker_runtime: "in_process_only",
        controller_stability: "not_production_verified",
        mcp_runtime: "in_process_only",
      },
    },
  ],
  selected: {
    component: "Controller",
    pack: "controller_stability",
    status: "implemented_local",
    requirements: [
      {
        id: "controller.quorum_health",
        signalType: "metric",
        registryReference: "rocketmq_controller_quorum_health",
        freshness: "≤ 30s",
        expectedAttributes: [],
        sensitivity: "operational",
        missingBehavior: "missing",
        evidenceField: "controller.quorum_health",
        owner: "controller",
        purpose: "报告已观测到 Leader 且已提交状态的仲裁健康度。",
      },
      {
        id: "controller.elections",
        signalType: "metric",
        registryReference: "rocketmq_controller_election_total",
        freshness: "≤ 60s",
        expectedAttributes: [],
        sensitivity: "operational",
        missingBehavior: "missing",
        evidenceField: "controller.election_rate",
        owner: "controller",
        purpose: "检测控制器领导权不稳定和重复选举。",
      },
      {
        id: "controller.heartbeat_age",
        signalType: "metric",
        registryReference: "rocketmq_controller_heartbeat_age",
        freshness: "≤ 60s",
        expectedAttributes: [],
        sensitivity: "operational",
        missingBehavior: "missing",
        evidenceField: "controller.heartbeat_age_p99_ms",
        owner: "controller",
        purpose: "量化 Broker 心跳陈旧程度。",
      },
      {
        id: "controller.stale_brokers",
        signalType: "metric",
        registryReference: "rocketmq_controller_stale_brokers",
        freshness: "≤ 30s",
        expectedAttributes: [],
        sensitivity: "operational",
        missingBehavior: "missing",
        evidenceField: "controller.stale_brokers",
        owner: "controller",
        purpose: "检测因心跳过期而移除的 Broker 数量。",
      },
      {
        id: "controller.heartbeat_trace",
        signalType: "span",
        registryReference: "RocketMQ CONTROLLER HEARTBEAT_SCAN",
        freshness: "≤ 300s",
        expectedAttributes: ["result", "stale_count"],
        sensitivity: "operational",
        missingBehavior: "not_production_verified",
        evidenceField: "controller.heartbeat_trace",
        owner: "controller",
        purpose: "解释心跳扫描结果，不暴露 Broker 身份。",
      },
    ],
  },
};

export const demoCatalog: CapabilityCatalogResponse = {
  schema_version: "rocketmq-sre.capabilities.v1",
  phase: "00",
  effective_access_profile: "read_only",
  execution_supported: false,
  approval_supported: false,
  provider_network_calls_supported: true,
  providers: [
    "openai",
    "anthropic",
    "google-gemini",
    "aws-bedrock",
    "deepseek",
    "zhipu-glm",
    "kimi-moonshot",
    "local-openai-compatible",
  ].map((id) => ({
    id,
    protocols: [
      id === "anthropic"
        ? "anthropic-messages"
        : id === "google-gemini"
          ? "gemini-generate-content"
          : id === "aws-bedrock"
            ? "bedrock-converse"
            : "openai-compatible",
    ],
    supports_streaming: true,
    supports_tools: true,
    supports_structured_output: id !== "local-openai-compatible",
    supports_embeddings: [
      "openai",
      "google-gemini",
      "aws-bedrock",
      "zhipu-glm",
    ].includes(id),
  })),
};
