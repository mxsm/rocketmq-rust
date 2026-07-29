// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

const dateTime = { type: "string", format: "date-time" };
const boundedText = (maxLength = 512) => ({
  type: "string",
  minLength: 1,
  maxLength,
});
const nullable = (schema) => ({ oneOf: [schema, { type: "null" }] });
const ref = (name) => ({ $ref: `#/components/schemas/${name}` });
const page = (schemaVersion, itemSchema, maximum = 200) => ({
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "items", "truncated"],
  properties: {
    schema_version: { const: schemaVersion },
    items: {
      type: "array",
      maxItems: maximum,
      items: ref(itemSchema),
    },
    truncated: { type: "boolean" },
  },
});
const queryParameter = (name, schema, required = false) => ({
  name,
  in: "query",
  required,
  schema,
});

function addFleetSchemas({ schemas, uuid }) {
  schemas.FleetEnvironment = {
    type: "string",
    enum: ["development", "test", "staging", "production", "other"],
  };
  schemas.ClusterRegistrationState = {
    type: "string",
    enum: [
      "pending",
      "onboarding",
      "active",
      "read_only_degraded",
      "offboarding",
      "retired",
    ],
  };
  schemas.FleetAccessProfile = {
    type: "string",
    enum: ["read_only", "supervised", "bounded_autonomy"],
  };
  schemas.Fleet = {
    type: "object",
    additionalProperties: false,
    required: ["id", "name", "owner", "created_at", "updated_at"],
    properties: {
      id: uuid,
      name: boundedText(128),
      owner: boundedText(256),
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.FleetTenant = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "fleet_id",
      "name",
      "owner",
      "active",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      fleet_id: uuid,
      name: boundedText(128),
      owner: boundedText(256),
      active: { type: "boolean" },
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.FleetRegion = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "fleet_id",
      "key",
      "display_name",
      "owner",
      "residency_tags",
      "active",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      fleet_id: uuid,
      key: boundedText(64),
      display_name: boundedText(128),
      owner: boundedText(256),
      residency_tags: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(128),
      },
      active: { type: "boolean" },
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.ClusterRegistration = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "fleet_id",
      "tenant_id",
      "region_id",
      "external_cluster_key",
      "environment",
      "owner",
      "state",
      "residency_tags",
      "lifecycle_revision",
      "created_at",
      "updated_at",
    ],
    properties: {
      cluster_id: uuid,
      fleet_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      external_cluster_key: boundedText(256),
      environment: ref("FleetEnvironment"),
      owner: boundedText(256),
      state: ref("ClusterRegistrationState"),
      residency_tags: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(128),
      },
      lifecycle_revision: { type: "integer", format: "uint64", minimum: 0 },
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.FleetOverview = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "fleet",
      "tenant",
      "regions",
      "registrations",
      "observed_at",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      fleet: ref("Fleet"),
      tenant: ref("FleetTenant"),
      regions: {
        type: "array",
        maxItems: 64,
        items: ref("FleetRegion"),
      },
      registrations: {
        type: "array",
        maxItems: 200,
        items: ref("ClusterRegistration"),
      },
      observed_at: dateTime,
    },
  };
  schemas.ClusterRegistrationPage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "total", "limit", "offset"],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      items: {
        type: "array",
        maxItems: 200,
        items: ref("ClusterRegistration"),
      },
      total: { type: "integer", format: "uint64", minimum: 0 },
      limit: { type: "integer", minimum: 1, maximum: 200 },
      offset: { type: "integer", format: "uint32", minimum: 0 },
    },
  };
  schemas.FleetOnboardingRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "fleet_id",
      "region_id",
      "environment",
      "owner",
      "requested_access",
      "connector_tls_verified",
    ],
    properties: {
      cluster_id: uuid,
      fleet_id: uuid,
      region_id: uuid,
      environment: ref("FleetEnvironment"),
      owner: boundedText(256),
      residency_tags: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(128),
        default: [],
      },
      requested_access: ref("FleetAccessProfile"),
      connector_tls_verified: { type: "boolean" },
      oauth_scopes: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(128),
        default: [],
      },
      required_capabilities: {
        type: "array",
        uniqueItems: true,
        maxItems: 128,
        items: boundedText(256),
        default: [],
      },
      required_data_sources: {
        type: "array",
        uniqueItems: true,
        maxItems: 128,
        items: boundedText(256),
        default: [],
      },
    },
  };
  schemas.FleetOnboardingView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "assessment", "registration"],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      assessment: { type: "object" },
      registration: nullable(ref("ClusterRegistration")),
    },
  };
  schemas.FleetOffboardRequest = {
    type: "object",
    additionalProperties: false,
    required: ["reason"],
    properties: {
      reason: boundedText(2048),
      correlation_id: nullable(uuid),
    },
  };
  schemas.FleetAssetIndex = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "fleet_id",
      "tenant_id",
      "region_id",
      "environment",
      "owner",
      "component",
      "component_version",
      "image_digest",
      "feature_digest",
      "configuration_digest",
      "health",
      "attributes",
      "observed_at",
    ],
    properties: {
      cluster_id: uuid,
      fleet_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      environment: ref("FleetEnvironment"),
      owner: boundedText(256),
      component: boundedText(128),
      component_version: boundedText(128),
      image_digest: nullable(boundedText(256)),
      feature_digest: nullable(boundedText(256)),
      configuration_digest: nullable(boundedText(256)),
      health: boundedText(64),
      attributes: {
        type: "object",
        maxProperties: 64,
        additionalProperties: { type: "string", maxLength: 512 },
      },
      observed_at: dateTime,
    },
  };
  schemas.UpsertFleetAssetRequest = {
    type: "object",
    additionalProperties: false,
    required: ["asset"],
    properties: { asset: ref("FleetAssetIndex") },
  };
  schemas.FleetAssetPage = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "items",
      "total",
      "limit",
      "offset",
      "health_distribution",
      "worst_health",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      items: {
        type: "array",
        maxItems: 200,
        items: ref("FleetAssetIndex"),
      },
      total: { type: "integer", format: "uint64", minimum: 0 },
      limit: { type: "integer", minimum: 1, maximum: 200 },
      offset: { type: "integer", format: "uint32", minimum: 0 },
      health_distribution: {
        type: "object",
        additionalProperties: {
          type: "integer",
          format: "uint64",
          minimum: 0,
        },
      },
      worst_health: nullable(boundedText(64)),
    },
  };
  schemas.ComplianceSeverity = {
    type: "string",
    enum: ["info", "warning", "error", "critical"],
  };
  schemas.ComplianceFindingState = {
    type: "string",
    enum: ["open", "acknowledged", "resolved", "accepted_exception"],
  };
  schemas.ComplianceFinding = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "fleet_id",
      "tenant_id",
      "region_id",
      "cluster_id",
      "category",
      "expected_digest",
      "live_digest",
      "evidence_ids",
      "severity",
      "owner",
      "recommendation",
      "state",
      "observed_at",
    ],
    properties: {
      id: uuid,
      fleet_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      cluster_id: uuid,
      category: boundedText(128),
      expected_digest: boundedText(256),
      live_digest: boundedText(256),
      evidence_ids: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: uuid,
      },
      severity: ref("ComplianceSeverity"),
      owner: boundedText(256),
      recommendation: boundedText(2048),
      state: ref("ComplianceFindingState"),
      observed_at: dateTime,
    },
  };
  schemas.ComplianceFindingPage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "total", "limit", "offset"],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      items: {
        type: "array",
        maxItems: 200,
        items: ref("ComplianceFinding"),
      },
      total: { type: "integer", format: "uint64", minimum: 0 },
      limit: { type: "integer", minimum: 1, maximum: 200 },
      offset: { type: "integer", format: "uint32", minimum: 0 },
    },
  };
  schemas.EvaluateComplianceRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "fleet_id",
      "region_id",
      "cluster_id",
      "category",
      "expected_digest",
      "live_digest",
      "severity",
      "owner",
      "recommendation",
    ],
    properties: {
      fleet_id: uuid,
      region_id: uuid,
      cluster_id: uuid,
      category: boundedText(128),
      expected_digest: boundedText(256),
      live_digest: boundedText(256),
      evidence_ids: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: uuid,
        default: [],
      },
      severity: ref("ComplianceSeverity"),
      owner: boundedText(256),
      recommendation: boundedText(2048),
    },
  };
  schemas.ComplianceEvaluationView = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "compliant",
      "finding",
      "resolved_findings",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      compliant: { type: "boolean" },
      finding: nullable(ref("ComplianceFinding")),
      resolved_findings: { type: "integer", format: "uint64", minimum: 0 },
    },
  };
  schemas.FleetInspectionState = {
    type: "string",
    enum: [
      "pending",
      "running",
      "completed",
      "partially_completed",
      "failed",
      "cancelled",
    ],
  };
  schemas.FleetInspectionRun = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "fleet_id",
      "tenant_id",
      "region_ids",
      "cluster_ids",
      "pack_ids",
      "max_concurrency",
      "timeout_seconds",
      "model_token_budget",
      "evidence_byte_budget",
      "state",
      "completed_clusters",
      "failed_clusters",
      "created_at",
      "completed_at",
    ],
    properties: {
      id: uuid,
      fleet_id: uuid,
      tenant_id: uuid,
      region_ids: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: uuid,
      },
      cluster_ids: {
        type: "array",
        uniqueItems: true,
        maxItems: 200,
        items: uuid,
      },
      pack_ids: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(256),
      },
      max_concurrency: { type: "integer", minimum: 1, maximum: 64 },
      timeout_seconds: { type: "integer", minimum: 1, maximum: 86400 },
      model_token_budget: { type: "integer", format: "uint64", minimum: 0 },
      evidence_byte_budget: { type: "integer", format: "uint64", minimum: 0 },
      state: ref("FleetInspectionState"),
      completed_clusters: { type: "integer", format: "uint32", minimum: 0 },
      failed_clusters: { type: "integer", format: "uint32", minimum: 0 },
      created_at: dateTime,
      completed_at: nullable(dateTime),
    },
  };
  schemas.CreateFleetInspectionRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "fleet_id",
      "region_ids",
      "cluster_ids",
      "pack_ids",
      "max_concurrency",
      "timeout_seconds",
      "model_token_budget",
      "evidence_byte_budget",
    ],
    properties: {
      fleet_id: uuid,
      region_ids: {
        type: "array",
        minItems: 1,
        maxItems: 64,
        uniqueItems: true,
        items: uuid,
      },
      cluster_ids: {
        type: "array",
        minItems: 1,
        maxItems: 200,
        uniqueItems: true,
        items: uuid,
      },
      pack_ids: {
        type: "array",
        minItems: 1,
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(256),
      },
      max_concurrency: { type: "integer", minimum: 1, maximum: 64 },
      timeout_seconds: { type: "integer", minimum: 1, maximum: 86400 },
      model_token_budget: { type: "integer", format: "uint64", minimum: 0 },
      evidence_byte_budget: { type: "integer", format: "uint64", minimum: 0 },
    },
  };
  schemas.UpdateFleetInspectionRequest = {
    type: "object",
    additionalProperties: false,
    required: ["completed_clusters", "failed_clusters", "terminal"],
    properties: {
      completed_clusters: { type: "integer", format: "uint32", minimum: 0 },
      failed_clusters: { type: "integer", format: "uint32", minimum: 0 },
      terminal: { type: "boolean" },
    },
  };
  schemas.FleetInspectionPage = page(
    "rocketmq-sre.fleet-api.v1",
    "FleetInspectionRun",
  );

  for (const [name, shape] of Object.entries({
    QuotaPolicyView: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      policy: { type: "object" },
      usage: { type: "object" },
    },
    FleetQuotaDecisionView: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      decision: { type: "object" },
    },
    RegionalRouteDecision: {
      schema_version: { const: "rocketmq-sre.fleet-api.v1" },
      mode: {
        type: "string",
        enum: ["full", "read_only_degraded", "denied"],
      },
      endpoint: nullable({ type: "object" }),
      reason_codes: {
        type: "array",
        maxItems: 64,
        items: boundedText(128),
      },
      observed_at: dateTime,
    },
  })) {
    schemas[name] = {
      type: "object",
      additionalProperties: false,
      required: Object.keys(shape),
      properties: shape,
    };
  }
  schemas.CreateQuotaPolicyRequest = { type: "object" };
  schemas.EvaluateFleetQuotaRequest = { type: "object" };
  schemas.RegisterRegionalEndpointRequest = { type: "object" };
  schemas.RegionalRouteRequest = { type: "object" };
  schemas.FleetQuotaDecisionPage = page(
    "rocketmq-sre.fleet-api.v1",
    "FleetQuotaDecisionRecord",
  );
  schemas.FleetQuotaDecisionRecord = { type: "object" };
  schemas.RegionalEndpointPage = page(
    "rocketmq-sre.fleet-api.v1",
    "RegionalEndpoint",
  );
  schemas.RegionalEndpoint = { type: "object" };
}

function addDrSchemas({ schemas, uuid }) {
  schemas.DrSubject = {
    type: "string",
    enum: ["sre_control_plane", "rocket_mq_cluster"],
  };
  schemas.DrExerciseMode = {
    type: "string",
    enum: ["readiness", "tabletop", "supervised_test"],
  };
  schemas.DrExecutionBoundary = {
    type: "string",
    enum: ["read_only", "test_cluster_supervised"],
  };
  schemas.DrExerciseState = {
    type: "string",
    enum: [
      "planned",
      "running",
      "awaiting_manual_confirmation",
      "completed",
      "failed",
      "cancelled",
    ],
  };
  schemas.RtoRpoTarget = {
    type: "object",
    additionalProperties: false,
    required: ["rto_seconds", "rpo_seconds"],
    properties: {
      rto_seconds: { type: "integer", format: "uint64", minimum: 0 },
      rpo_seconds: { type: "integer", format: "uint64", minimum: 0 },
    },
  };
  schemas.RecoveryCheckpointDefinition = {
    type: "object",
    additionalProperties: false,
    required: [
      "key",
      "title",
      "expected_duration_seconds",
      "manual_confirmation_required",
      "cleanup_required",
      "required_evidence_kinds",
    ],
    properties: {
      key: boundedText(128),
      title: boundedText(256),
      expected_duration_seconds: {
        type: "integer",
        format: "uint64",
        minimum: 0,
      },
      manual_confirmation_required: { type: "boolean" },
      cleanup_required: { type: "boolean" },
      required_evidence_kinds: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(128),
      },
    },
  };
  schemas.DrPlan = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "fleet_id",
      "tenant_id",
      "region_id",
      "cluster_id",
      "subject",
      "name",
      "version",
      "owner",
      "target",
      "allowed_modes",
      "required_sources",
      "checkpoints",
      "active",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      fleet_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      cluster_id: nullable(uuid),
      subject: ref("DrSubject"),
      name: boundedText(256),
      version: { type: "integer", format: "uint32", minimum: 1 },
      owner: boundedText(256),
      target: ref("RtoRpoTarget"),
      allowed_modes: {
        type: "array",
        minItems: 1,
        maxItems: 3,
        uniqueItems: true,
        items: ref("DrExerciseMode"),
      },
      required_sources: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(256),
      },
      checkpoints: {
        type: "array",
        minItems: 1,
        maxItems: 128,
        items: ref("RecoveryCheckpointDefinition"),
      },
      active: { type: "boolean" },
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.CreateDrPlanRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "fleet_id",
      "region_id",
      "subject",
      "name",
      "owner",
      "target",
      "allowed_modes",
      "required_sources",
      "checkpoints",
    ],
    properties: {
      fleet_id: uuid,
      region_id: uuid,
      cluster_id: nullable(uuid),
      subject: ref("DrSubject"),
      name: boundedText(256),
      owner: boundedText(256),
      target: ref("RtoRpoTarget"),
      allowed_modes: {
        type: "array",
        minItems: 1,
        maxItems: 3,
        uniqueItems: true,
        items: ref("DrExerciseMode"),
      },
      required_sources: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(256),
      },
      checkpoints: {
        type: "array",
        minItems: 1,
        maxItems: 128,
        items: ref("RecoveryCheckpointDefinition"),
      },
    },
  };
  schemas.DrPlanPage = page("rocketmq-sre.dr-api.v1", "DrPlan");
  schemas.DrBackupAsset = { type: "object" };
  schemas.UpsertDrBackupAssetRequest = { type: "object" };
  schemas.DrBackupAssetPage = page(
    "rocketmq-sre.dr-api.v1",
    "DrBackupAsset",
  );
  schemas.DrExercise = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "plan_id",
      "tenant_id",
      "region_id",
      "cluster_id",
      "mode",
      "boundary",
      "state",
      "target",
      "actual_rto_seconds",
      "actual_rpo_seconds",
      "manual_checkpoint_count",
      "cleanup_complete",
      "evidence_ids",
      "created_by",
      "started_at",
      "completed_at",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      plan_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      cluster_id: nullable(uuid),
      mode: ref("DrExerciseMode"),
      boundary: ref("DrExecutionBoundary"),
      state: ref("DrExerciseState"),
      target: ref("RtoRpoTarget"),
      actual_rto_seconds: nullable({
        type: "integer",
        format: "uint64",
        minimum: 0,
      }),
      actual_rpo_seconds: nullable({
        type: "integer",
        format: "uint64",
        minimum: 0,
      }),
      manual_checkpoint_count: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
      cleanup_complete: { type: "boolean" },
      evidence_ids: {
        type: "array",
        maxItems: 256,
        uniqueItems: true,
        items: uuid,
      },
      created_by: boundedText(256),
      started_at: nullable(dateTime),
      completed_at: nullable(dateTime),
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.StartDrExerciseRequest = {
    type: "object",
    additionalProperties: false,
    required: ["plan_id", "mode"],
    properties: {
      plan_id: uuid,
      mode: ref("DrExerciseMode"),
    },
  };
  schemas.TransitionDrExerciseRequest = {
    type: "object",
    additionalProperties: false,
    required: ["state", "reason"],
    properties: {
      state: ref("DrExerciseState"),
      reason: boundedText(2048),
      actual_rto_seconds: nullable({
        type: "integer",
        format: "uint64",
        minimum: 0,
      }),
      actual_rpo_seconds: nullable({
        type: "integer",
        format: "uint64",
        minimum: 0,
      }),
      cleanup_complete: { type: "boolean" },
    },
  };
  schemas.DrExercisePage = page("rocketmq-sre.dr-api.v1", "DrExercise");
  schemas.RecoveryCheckpoint = { type: "object" };
  schemas.RecordRecoveryCheckpointRequest = { type: "object" };
  schemas.RecoveryCheckpointPage = page(
    "rocketmq-sre.dr-api.v1",
    "RecoveryCheckpoint",
    512,
  );
  schemas.DrFinding = { type: "object" };
  schemas.RecordDrFindingRequest = { type: "object" };
  schemas.DrFindingPage = page(
    "rocketmq-sre.dr-api.v1",
    "DrFinding",
    512,
  );
  schemas.DrActionItem = { type: "object" };
  schemas.UpdateDrActionItemRequest = { type: "object" };
  schemas.DrActionItemPage = page(
    "rocketmq-sre.dr-api.v1",
    "DrActionItem",
  );
}

function addFleetPaths({ document, operation, pathParameter, uuid }) {
  const scopeParameters = [
    queryParameter("region_id", uuid),
    queryParameter("environment", ref("FleetEnvironment")),
    queryParameter("owner", boundedText(256)),
    queryParameter("component_version", boundedText(128)),
    queryParameter("health", boundedText(64)),
    queryParameter("limit", {
      type: "integer",
      minimum: 1,
      maximum: 200,
      default: 50,
    }),
    queryParameter("offset", {
      type: "integer",
      format: "uint32",
      minimum: 0,
      default: 0,
    }),
  ];
  document.paths["/v1/fleet/overview"] = {
    get: operation({
      operationId: "getFleetOverviewV1",
      summary: "Read the authorized enterprise Fleet projection",
      responseSchema: "FleetOverview",
    }),
  };
  document.paths["/v1/fleet/clusters"] = {
    get: operation({
      operationId: "listFleetClustersV1",
      summary: "List scoped Fleet cluster registrations",
      responseSchema: "ClusterRegistrationPage",
      parameters: scopeParameters,
    }),
  };
  document.paths["/v1/fleet/onboarding/assess"] = {
    post: operation({
      operationId: "assessFleetOnboardingV1",
      summary: "Assess Fleet onboarding without registering the cluster",
      bodySchema: "FleetOnboardingRequest",
      responseSchema: "FleetOnboardingView",
    }),
  };
  document.paths["/v1/fleet/onboarding/register"] = {
    post: operation({
      operationId: "registerFleetClusterV1",
      summary: "Register a cluster after fail-closed capability assessment",
      bodySchema: "FleetOnboardingRequest",
      responseSchema: "FleetOnboardingView",
    }),
  };
  document.paths["/v1/fleet/clusters/{id}/offboard"] = {
    post: operation({
      operationId: "offboardFleetClusterV1",
      summary: "Tombstone one cluster registration and revoke new work",
      bodySchema: "FleetOffboardRequest",
      responseSchema: "ClusterRegistration",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/fleet/quotas"] = {
    get: operation({
      operationId: "getFleetQuotaPolicyV1",
      summary: "Read the effective bounded Fleet quota policy",
      responseSchema: "QuotaPolicyView",
      parameters: [queryParameter("cluster_id", uuid)],
    }),
    post: operation({
      operationId: "createFleetQuotaPolicyV1",
      summary: "Create a versioned Fleet quota policy",
      bodySchema: "CreateQuotaPolicyRequest",
      responseSchema: "QuotaPolicyView",
    }),
  };
  document.paths["/v1/fleet/quotas/evaluate"] = {
    post: operation({
      operationId: "evaluateFleetQuotaV1",
      summary: "Evaluate bounded work against the current Fleet quota",
      bodySchema: "EvaluateFleetQuotaRequest",
      responseSchema: "FleetQuotaDecisionView",
    }),
  };
  document.paths["/v1/fleet/quotas/decisions"] = {
    get: operation({
      operationId: "listFleetQuotaDecisionsV1",
      summary: "List immutable Fleet quota decisions",
      responseSchema: "FleetQuotaDecisionPage",
      parameters: [
        queryParameter("cluster_id", uuid),
        queryParameter("allowed", { type: "boolean" }),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
  };
  document.paths["/v1/fleet/regional-endpoints"] = {
    get: operation({
      operationId: "listFleetRegionalEndpointsV1",
      summary: "List compatible region-local runtime endpoints",
      responseSchema: "RegionalEndpointPage",
      parameters: [
        queryParameter("region_id", uuid),
        queryParameter("cluster_id", uuid),
        queryParameter("kind", boundedText(64)),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
    post: operation({
      operationId: "registerFleetRegionalEndpointV1",
      summary: "Register a versioned region-local runtime endpoint",
      bodySchema: "RegisterRegionalEndpointRequest",
      responseSchema: "RegionalEndpoint",
    }),
  };
  document.paths["/v1/fleet/regional-route"] = {
    post: operation({
      operationId: "resolveFleetRegionalRouteV1",
      summary: "Resolve a residency-safe current or N-1 regional route",
      bodySchema: "RegionalRouteRequest",
      responseSchema: "RegionalRouteDecision",
    }),
  };
  document.paths["/v1/fleet/assets"] = {
    get: operation({
      operationId: "listFleetAssetsV1",
      summary: "List the bounded enterprise asset index",
      responseSchema: "FleetAssetPage",
      parameters: scopeParameters,
    }),
    post: operation({
      operationId: "upsertFleetAssetV1",
      summary: "Ingest one sanitized Fleet asset projection",
      bodySchema: "UpsertFleetAssetRequest",
      responseSchema: "FleetAssetIndex",
    }),
  };
  document.paths["/v1/fleet/compliance"] = {
    get: operation({
      operationId: "listFleetComplianceFindingsV1",
      summary: "List scoped Fleet compliance findings",
      responseSchema: "ComplianceFindingPage",
      parameters: [
        queryParameter("region_id", uuid),
        queryParameter("cluster_id", uuid),
        queryParameter("severity", ref("ComplianceSeverity")),
        queryParameter("state", ref("ComplianceFindingState")),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
        queryParameter("offset", {
          type: "integer",
          format: "uint32",
          minimum: 0,
          default: 0,
        }),
      ],
    }),
    post: operation({
      operationId: "evaluateFleetComplianceV1",
      summary: "Record a read-only expected-versus-live compliance result",
      bodySchema: "EvaluateComplianceRequest",
      responseSchema: "ComplianceEvaluationView",
    }),
  };
  document.paths["/v1/fleet/inspections"] = {
    get: operation({
      operationId: "listFleetInspectionsV1",
      summary: "List bounded multi-cluster inspection runs",
      responseSchema: "FleetInspectionPage",
      parameters: [
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
    post: operation({
      operationId: "createFleetInspectionV1",
      summary: "Create a budgeted and concurrency-bounded Fleet inspection",
      bodySchema: "CreateFleetInspectionRequest",
      responseSchema: "FleetInspectionRun",
    }),
  };
  document.paths["/v1/fleet/inspections/{id}/progress"] = {
    post: operation({
      operationId: "updateFleetInspectionProgressV1",
      summary: "Advance bounded Fleet inspection progress",
      bodySchema: "UpdateFleetInspectionRequest",
      responseSchema: "FleetInspectionRun",
      parameters: [pathParameter("id")],
    }),
  };
}

function addDrPaths({ document, operation, pathParameter, uuid }) {
  document.paths["/v1/dr/plans"] = {
    get: operation({
      operationId: "listDrPlansV1",
      summary: "List scoped versioned disaster-recovery plans",
      responseSchema: "DrPlanPage",
      parameters: [
        queryParameter("cluster_id", uuid),
        queryParameter("subject", ref("DrSubject")),
        queryParameter("active", { type: "boolean" }),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
    post: operation({
      operationId: "createDrPlanV1",
      summary: "Create a bounded versioned disaster-recovery plan",
      bodySchema: "CreateDrPlanRequest",
      responseSchema: "DrPlan",
    }),
  };
  document.paths["/v1/dr/plans/{id}/backup-assets"] = {
    get: operation({
      operationId: "listDrBackupAssetsV1",
      summary: "List backup and deterministic rebuild surfaces for a plan",
      responseSchema: "DrBackupAssetPage",
      parameters: [pathParameter("id")],
    }),
    post: operation({
      operationId: "upsertDrBackupAssetV1",
      summary: "Record a sanitized backup or restore-verification surface",
      bodySchema: "UpsertDrBackupAssetRequest",
      responseSchema: "DrBackupAsset",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/dr/exercises"] = {
    get: operation({
      operationId: "listDrExercisesV1",
      summary: "List readiness, tabletop, and supervised test exercises",
      responseSchema: "DrExercisePage",
      parameters: [
        queryParameter("cluster_id", uuid),
        queryParameter("state", ref("DrExerciseState")),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
    post: operation({
      operationId: "startDrExerciseV1",
      summary: "Start a non-production-cutover DR exercise",
      bodySchema: "StartDrExerciseRequest",
      responseSchema: "DrExercise",
    }),
  };
  document.paths["/v1/dr/exercises/{id}/state"] = {
    post: operation({
      operationId: "transitionDrExerciseV1",
      summary: "Advance a DR exercise through its bounded lifecycle",
      bodySchema: "TransitionDrExerciseRequest",
      responseSchema: "DrExercise",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/dr/exercises/{id}/checkpoints"] = {
    get: operation({
      operationId: "listRecoveryCheckpointsV1",
      summary: "List append-only recovery checkpoint observations",
      responseSchema: "RecoveryCheckpointPage",
      parameters: [pathParameter("id")],
    }),
    post: operation({
      operationId: "recordRecoveryCheckpointV1",
      summary: "Record one bounded recovery checkpoint result",
      bodySchema: "RecordRecoveryCheckpointRequest",
      responseSchema: "RecoveryCheckpoint",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/dr/exercises/{id}/findings"] = {
    get: operation({
      operationId: "listDrFindingsV1",
      summary: "List exercise findings and their action-item linkage",
      responseSchema: "DrFindingPage",
      parameters: [pathParameter("id")],
    }),
    post: operation({
      operationId: "recordDrFindingV1",
      summary: "Record one DR finding and create its action item",
      bodySchema: "RecordDrFindingRequest",
      responseSchema: "DrFinding",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/dr/action-items"] = {
    get: operation({
      operationId: "listDrActionItemsV1",
      summary: "List DR remediation action items",
      responseSchema: "DrActionItemPage",
      parameters: [
        queryParameter("cluster_id", uuid),
        queryParameter("status", boundedText(64)),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
          default: 50,
        }),
      ],
    }),
  };
  document.paths["/v1/dr/action-items/{id}"] = {
    post: operation({
      operationId: "updateDrActionItemV1",
      summary: "Advance and verify one DR action item",
      bodySchema: "UpdateDrActionItemRequest",
      responseSchema: "DrActionItem",
      parameters: [pathParameter("id")],
    }),
  };
}

export function extendPhase5FleetAndDr({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
}) {
  addFleetSchemas({ schemas, uuid });
  addDrSchemas({ schemas, uuid });
  addFleetPaths({ document, operation, pathParameter, uuid });
  addDrPaths({ document, operation, pathParameter, uuid });
  document["x-rocketmq-fleet-schema"] = "rocketmq-sre.fleet.v1";
  document["x-rocketmq-dr-schema"] = "rocketmq-sre.dr.v1";
}
