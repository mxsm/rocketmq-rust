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
const queryParameter = (name, schema, required = false) => ({
  name,
  in: "query",
  required,
  schema,
});
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
const unsigned = {
  type: "integer",
  format: "uint64",
  minimum: 0,
};

function addGovernanceSchemas({ schemas, uuid, digest }) {
  schemas.GovernanceObjectKind = {
    type: "string",
    enum: [
      "data_policy",
      "evidence_policy",
      "prompt",
      "knowledge",
      "model_profile",
      "provider_profile",
      "diagnostic_pack",
      "policy_bundle",
      "action_descriptor",
      "runbook",
      "integration_adapter",
    ],
  };
  schemas.GovernanceLifecycleState = {
    type: "string",
    enum: [
      "draft",
      "review",
      "active",
      "deprecated",
      "quarantined",
      "retired",
    ],
  };
  schemas.GovernanceActorKind = {
    type: "string",
    enum: ["human", "service", "model"],
  };
  schemas.GovernanceAccessPath = {
    type: "string",
    enum: ["read_only", "high_privilege"],
  };
  schemas.GovernanceImpactKind = {
    type: "string",
    enum: [
      "cluster",
      "diagnostic_pack",
      "action_plan",
      "action",
      "incident",
      "model_route",
      "integration",
    ],
  };
  schemas.GovernanceArtifact = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "kind",
      "logical_key",
      "owner",
      "reviewer",
      "current_version_id",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      kind: ref("GovernanceObjectKind"),
      logical_key: boundedText(256),
      owner: boundedText(256),
      reviewer: boundedText(256),
      current_version_id: nullable(uuid),
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.CreateGovernanceArtifactRequest = {
    type: "object",
    additionalProperties: false,
    required: ["kind", "logical_key", "owner", "reviewer"],
    properties: {
      kind: ref("GovernanceObjectKind"),
      logical_key: boundedText(256),
      owner: boundedText(256),
      reviewer: boundedText(256),
    },
  };
  schemas.GovernanceArtifactPage = page(
    "rocketmq-sre.governance-api.v1",
    "GovernanceArtifact",
  );
  schemas.GovernanceDependency = {
    type: "object",
    additionalProperties: false,
    required: ["kind", "logical_key", "version"],
    properties: {
      kind: ref("GovernanceObjectKind"),
      logical_key: boundedText(256),
      version: boundedText(64),
    },
  };
  schemas.GovernanceSignature = {
    type: "object",
    additionalProperties: false,
    required: ["algorithm", "key_id", "value"],
    properties: {
      algorithm: boundedText(64),
      key_id: boundedText(256),
      value: boundedText(2048),
    },
  };
  schemas.GovernanceVersion = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "artifact_id",
      "tenant_id",
      "version",
      "content_digest",
      "signature",
      "state",
      "applicable_components",
      "applicable_version_range",
      "dependencies",
      "review_due_at",
      "expires_at",
      "replacement_version_id",
      "rollback_version_id",
      "created_by",
      "created_at",
      "updated_at",
    ],
    properties: {
      id: uuid,
      artifact_id: uuid,
      tenant_id: uuid,
      version: boundedText(64),
      content_digest: digest,
      signature: nullable(ref("GovernanceSignature")),
      state: ref("GovernanceLifecycleState"),
      applicable_components: {
        type: "array",
        maxItems: 128,
        uniqueItems: true,
        items: boundedText(128),
      },
      applicable_version_range: boundedText(256),
      dependencies: {
        type: "array",
        maxItems: 128,
        uniqueItems: true,
        items: ref("GovernanceDependency"),
      },
      review_due_at: dateTime,
      expires_at: nullable(dateTime),
      replacement_version_id: nullable(uuid),
      rollback_version_id: nullable(uuid),
      created_by: boundedText(256),
      created_at: dateTime,
      updated_at: dateTime,
    },
  };
  schemas.CreateGovernanceVersionRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "version",
      "content_digest",
      "applicable_version_range",
      "review_due_at",
      "expires_at",
      "rollback_version_id",
    ],
    properties: {
      version: boundedText(64),
      content_digest: digest,
      applicable_components: {
        type: "array",
        maxItems: 128,
        uniqueItems: true,
        items: boundedText(128),
        default: [],
      },
      applicable_version_range: boundedText(256),
      dependencies: {
        type: "array",
        maxItems: 128,
        uniqueItems: true,
        items: ref("GovernanceDependency"),
        default: [],
      },
      review_due_at: dateTime,
      expires_at: nullable(dateTime),
      rollback_version_id: nullable(uuid),
    },
  };
  schemas.TransitionGovernanceVersionRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "state",
      "reason",
      "replacement_version_id",
      "rollback_version_id",
    ],
    properties: {
      state: ref("GovernanceLifecycleState"),
      reason: boundedText(2048),
      replacement_version_id: nullable(uuid),
      rollback_version_id: nullable(uuid),
    },
  };
  schemas.GovernanceVersionPage = page(
    "rocketmq-sre.governance-api.v1",
    "GovernanceVersion",
  );
  schemas.GovernanceImpact = {
    type: "object",
    additionalProperties: false,
    required: [
      "version_id",
      "tenant_id",
      "cluster_id",
      "kind",
      "reference_id",
      "label",
      "observed_at",
    ],
    properties: {
      version_id: uuid,
      tenant_id: uuid,
      cluster_id: nullable(uuid),
      kind: ref("GovernanceImpactKind"),
      reference_id: boundedText(256),
      label: boundedText(512),
      observed_at: dateTime,
    },
  };
  schemas.RecordGovernanceImpactRequest = {
    type: "object",
    additionalProperties: false,
    required: ["cluster_id", "kind", "reference_id", "label"],
    properties: {
      cluster_id: nullable(uuid),
      kind: ref("GovernanceImpactKind"),
      reference_id: boundedText(256),
      label: boundedText(512),
    },
  };
  schemas.GovernanceImpactPage = page(
    "rocketmq-sre.governance-api.v1",
    "GovernanceImpact",
  );
  schemas.GovernanceAdmission = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "cluster_id",
      "access_path",
      "required_version_ids",
      "allowed",
      "degraded",
      "reason_codes",
      "evaluated_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      cluster_id: nullable(uuid),
      access_path: ref("GovernanceAccessPath"),
      required_version_ids: {
        type: "array",
        minItems: 1,
        maxItems: 128,
        uniqueItems: true,
        items: uuid,
      },
      allowed: { type: "boolean" },
      degraded: { type: "boolean" },
      reason_codes: {
        type: "array",
        maxItems: 64,
        items: boundedText(128),
      },
      evaluated_at: dateTime,
    },
  };
  schemas.EvaluateGovernanceAdmissionRequest = {
    type: "object",
    additionalProperties: false,
    required: ["cluster_id", "access_path", "required_version_ids"],
    properties: {
      cluster_id: nullable(uuid),
      access_path: ref("GovernanceAccessPath"),
      required_version_ids: {
        type: "array",
        minItems: 1,
        maxItems: 128,
        uniqueItems: true,
        items: uuid,
      },
    },
  };
  schemas.GovernanceAdmissionView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "decision"],
    properties: {
      schema_version: { const: "rocketmq-sre.governance-api.v1" },
      decision: ref("GovernanceAdmission"),
    },
  };
  schemas.GovernanceEvent = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "artifact_id",
      "version_id",
      "from_state",
      "to_state",
      "actor",
      "actor_kind",
      "reason",
      "occurred_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      artifact_id: uuid,
      version_id: uuid,
      from_state: nullable(ref("GovernanceLifecycleState")),
      to_state: ref("GovernanceLifecycleState"),
      actor: boundedText(256),
      actor_kind: ref("GovernanceActorKind"),
      reason: boundedText(2048),
      occurred_at: dateTime,
    },
  };
  schemas.GovernanceAuditExport = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "truncated", "exported_at"],
    properties: {
      schema_version: { const: "rocketmq-sre.governance-api.v1" },
      items: {
        type: "array",
        maxItems: 1000,
        items: ref("GovernanceEvent"),
      },
      truncated: { type: "boolean" },
      exported_at: dateTime,
    },
  };
  schemas.GovernanceComplianceReport = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "state_counts",
      "unsigned_active",
      "expired_active",
      "overdue_review",
      "quarantined",
      "compliant",
      "observed_at",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.governance-api.v1" },
      state_counts: {
        type: "object",
        additionalProperties: unsigned,
      },
      unsigned_active: unsigned,
      expired_active: unsigned,
      overdue_review: unsigned,
      quarantined: unsigned,
      compliant: { type: "boolean" },
      observed_at: dateTime,
    },
  };
}

function addFinOpsSchemas({ schemas, uuid }) {
  schemas.FinOpsCostSource = {
    type: "string",
    enum: [
      "model_invocation",
      "control_plane",
      "connector",
      "execution_agent",
      "observability",
      "object_storage",
      "synthetic_probe",
    ],
  };
  schemas.FinOpsWorkloadKind = {
    type: "string",
    enum: [
      "incident",
      "diagnostic_pack",
      "workflow",
      "inspection",
      "verification",
      "rollback",
      "audit",
      "system",
    ],
  };
  schemas.FinOpsBudgetScopeKind = {
    type: "string",
    enum: [
      "tenant",
      "provider",
      "model",
      "region",
      "cluster",
      "incident",
      "diagnostic_pack",
      "workflow",
    ],
  };
  schemas.FinOpsBudgetPeriod = {
    type: "string",
    enum: ["hourly", "daily", "monthly"],
  };
  schemas.FinOpsWorkClass = {
    type: "string",
    enum: [
      "safety_check",
      "audit",
      "verification",
      "rollback",
      "active_incident",
      "interactive",
      "background",
    ],
  };
  schemas.FinOpsDegradation = {
    type: "string",
    enum: [
      "none",
      "prefer_lower_cost_model",
      "reduce_sampling",
      "defer_low_priority",
      "deny_low_priority",
    ],
  };
  schemas.FinOpsAllocationMode = {
    type: "string",
    enum: ["showback", "chargeback"],
  };
  schemas.FinOpsCostEntry = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "idempotency_key",
      "fleet_id",
      "tenant_id",
      "region_id",
      "cluster_id",
      "source",
      "workload_kind",
      "provider_profile",
      "model_family",
      "incident_id",
      "pack_id",
      "workflow_id",
      "request_count",
      "input_tokens",
      "output_tokens",
      "latency_millis",
      "error_count",
      "quantity_millis",
      "cost_micros",
      "occurred_at",
      "recorded_at",
    ],
    properties: {
      id: uuid,
      idempotency_key: boundedText(256),
      fleet_id: uuid,
      tenant_id: uuid,
      region_id: uuid,
      cluster_id: nullable(uuid),
      source: ref("FinOpsCostSource"),
      workload_kind: ref("FinOpsWorkloadKind"),
      provider_profile: nullable(boundedText(256)),
      model_family: nullable(boundedText(256)),
      incident_id: nullable(uuid),
      pack_id: nullable(boundedText(256)),
      workflow_id: nullable(boundedText(256)),
      request_count: unsigned,
      input_tokens: unsigned,
      output_tokens: unsigned,
      latency_millis: unsigned,
      error_count: unsigned,
      quantity_millis: unsigned,
      cost_micros: unsigned,
      occurred_at: dateTime,
      recorded_at: dateTime,
    },
  };
  schemas.RecordFinOpsCostRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "idempotency_key",
      "fleet_id",
      "region_id",
      "cluster_id",
      "source",
      "workload_kind",
      "provider_profile",
      "model_family",
      "incident_id",
      "pack_id",
      "workflow_id",
      "occurred_at",
    ],
    properties: {
      idempotency_key: boundedText(256),
      fleet_id: uuid,
      region_id: uuid,
      cluster_id: nullable(uuid),
      source: ref("FinOpsCostSource"),
      workload_kind: ref("FinOpsWorkloadKind"),
      provider_profile: nullable(boundedText(256)),
      model_family: nullable(boundedText(256)),
      incident_id: nullable(uuid),
      pack_id: nullable(boundedText(256)),
      workflow_id: nullable(boundedText(256)),
      request_count: { ...unsigned, default: 0 },
      input_tokens: { ...unsigned, default: 0 },
      output_tokens: { ...unsigned, default: 0 },
      latency_millis: { ...unsigned, default: 0 },
      error_count: { ...unsigned, default: 0 },
      quantity_millis: { ...unsigned, default: 0 },
      cost_micros: { ...unsigned, default: 0 },
      occurred_at: dateTime,
    },
  };
  schemas.FinOpsLedgerPage = page(
    "rocketmq-sre.finops-api.v1",
    "FinOpsCostEntry",
  );
  schemas.FinOpsBudget = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "scope_kind",
      "scope_key",
      "version",
      "period",
      "soft_limit_micros",
      "hard_limit_micros",
      "owner",
      "active",
      "created_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      scope_kind: ref("FinOpsBudgetScopeKind"),
      scope_key: boundedText(256),
      version: unsigned,
      period: ref("FinOpsBudgetPeriod"),
      soft_limit_micros: unsigned,
      hard_limit_micros: unsigned,
      owner: boundedText(256),
      active: { type: "boolean" },
      created_at: dateTime,
    },
  };
  schemas.CreateFinOpsBudgetRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "scope_kind",
      "scope_key",
      "period",
      "soft_limit_micros",
      "hard_limit_micros",
      "owner",
    ],
    properties: {
      scope_kind: ref("FinOpsBudgetScopeKind"),
      scope_key: boundedText(256),
      period: ref("FinOpsBudgetPeriod"),
      soft_limit_micros: unsigned,
      hard_limit_micros: unsigned,
      owner: boundedText(256),
    },
  };
  schemas.FinOpsBudgetPage = page(
    "rocketmq-sre.finops-api.v1",
    "FinOpsBudget",
  );
  schemas.FinOpsBudgetDecision = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "cluster_id",
      "budget_id",
      "work_class",
      "requested_cost_micros",
      "observed_cost_micros",
      "projected_cost_micros",
      "soft_limit_micros",
      "hard_limit_micros",
      "allowed",
      "degradation",
      "reason_code",
      "protected_controls",
      "evaluated_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      cluster_id: nullable(uuid),
      budget_id: uuid,
      work_class: ref("FinOpsWorkClass"),
      requested_cost_micros: unsigned,
      observed_cost_micros: unsigned,
      projected_cost_micros: unsigned,
      soft_limit_micros: unsigned,
      hard_limit_micros: unsigned,
      allowed: { type: "boolean" },
      degradation: ref("FinOpsDegradation"),
      reason_code: boundedText(128),
      protected_controls: {
        type: "array",
        minItems: 4,
        maxItems: 7,
        uniqueItems: true,
        items: ref("FinOpsWorkClass"),
      },
      evaluated_at: dateTime,
    },
  };
  schemas.EvaluateFinOpsBudgetRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "budget_id",
      "cluster_id",
      "work_class",
      "requested_cost_micros",
    ],
    properties: {
      budget_id: uuid,
      cluster_id: nullable(uuid),
      work_class: ref("FinOpsWorkClass"),
      requested_cost_micros: unsigned,
    },
  };
  schemas.FinOpsBudgetDecisionView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "decision"],
    properties: {
      schema_version: { const: "rocketmq-sre.finops-api.v1" },
      decision: ref("FinOpsBudgetDecision"),
    },
  };
  schemas.FinOpsAllocationPolicy = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "version",
      "mode",
      "allocation_keys",
      "organization_confirmed",
      "owner",
      "active",
      "created_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      version: unsigned,
      mode: ref("FinOpsAllocationMode"),
      allocation_keys: {
        type: "array",
        maxItems: 32,
        uniqueItems: true,
        items: boundedText(128),
      },
      organization_confirmed: { type: "boolean" },
      owner: boundedText(256),
      active: { type: "boolean" },
      created_at: dateTime,
    },
  };
  schemas.CreateFinOpsAllocationPolicyRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "mode",
      "allocation_keys",
      "organization_confirmed",
      "owner",
    ],
    properties: {
      mode: ref("FinOpsAllocationMode"),
      allocation_keys: {
        type: "array",
        minItems: 1,
        maxItems: 32,
        uniqueItems: true,
        items: boundedText(128),
      },
      organization_confirmed: { type: "boolean" },
      owner: boundedText(256),
    },
  };
  schemas.FinOpsAllocationPolicyView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "policy"],
    properties: {
      schema_version: { const: "rocketmq-sre.finops-api.v1" },
      policy: ref("FinOpsAllocationPolicy"),
    },
  };
  schemas.FinOpsForecast = {
    type: "object",
    additionalProperties: false,
    required: [
      "budget_id",
      "period_start",
      "period_end",
      "observed_cost_micros",
      "projected_cost_micros",
      "hard_limit_micros",
      "sample_count",
      "coverage_basis_points",
      "projected_over_budget",
      "generated_at",
    ],
    properties: {
      budget_id: uuid,
      period_start: dateTime,
      period_end: dateTime,
      observed_cost_micros: unsigned,
      projected_cost_micros: unsigned,
      hard_limit_micros: unsigned,
      sample_count: unsigned,
      coverage_basis_points: {
        type: "integer",
        format: "uint32",
        minimum: 0,
        maximum: 10000,
      },
      projected_over_budget: { type: "boolean" },
      generated_at: dateTime,
    },
  };
  schemas.FinOpsAnomaly = {
    type: "object",
    additionalProperties: false,
    required: [
      "scope_kind",
      "scope_key",
      "current_cost_micros",
      "baseline_cost_micros",
      "change_basis_points",
      "reason_code",
    ],
    properties: {
      scope_kind: ref("FinOpsBudgetScopeKind"),
      scope_key: boundedText(256),
      current_cost_micros: unsigned,
      baseline_cost_micros: unsigned,
      change_basis_points: nullable({
        type: "integer",
        format: "uint32",
        minimum: 0,
      }),
      reason_code: boundedText(128),
    },
  };
  schemas.FinOpsShowbackRow = {
    type: "object",
    additionalProperties: false,
    required: [
      "dimensions",
      "request_count",
      "input_tokens",
      "output_tokens",
      "error_count",
      "average_latency_millis",
      "cost_micros",
      "successful_outcomes",
      "slo_compliant_outcomes",
      "estimated_minutes_saved",
    ],
    properties: {
      dimensions: {
        type: "object",
        maxProperties: 16,
        additionalProperties: boundedText(256),
      },
      request_count: unsigned,
      input_tokens: unsigned,
      output_tokens: unsigned,
      error_count: unsigned,
      average_latency_millis: nullable(unsigned),
      cost_micros: unsigned,
      successful_outcomes: unsigned,
      slo_compliant_outcomes: unsigned,
      estimated_minutes_saved: unsigned,
    },
  };
  schemas.FinOpsReport = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "tenant_id",
      "from",
      "to",
      "allocation_mode",
      "chargeback_enabled",
      "rows",
      "total_cost_micros",
      "ledger_entries",
      "entries_missing_cost",
      "cost_coverage_basis_points",
      "forecasts",
      "anomalies",
      "warnings",
      "generated_at",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.finops.v1" },
      tenant_id: uuid,
      from: dateTime,
      to: dateTime,
      allocation_mode: ref("FinOpsAllocationMode"),
      chargeback_enabled: { type: "boolean" },
      rows: {
        type: "array",
        maxItems: 500,
        items: ref("FinOpsShowbackRow"),
      },
      total_cost_micros: unsigned,
      ledger_entries: unsigned,
      entries_missing_cost: unsigned,
      cost_coverage_basis_points: nullable({
        type: "integer",
        format: "uint32",
        minimum: 0,
        maximum: 10000,
      }),
      forecasts: {
        type: "array",
        maxItems: 200,
        items: ref("FinOpsForecast"),
      },
      anomalies: {
        type: "array",
        maxItems: 200,
        items: ref("FinOpsAnomaly"),
      },
      warnings: {
        type: "array",
        maxItems: 64,
        items: boundedText(512),
      },
      generated_at: dateTime,
    },
  };
}

function addGovernancePaths({
  document,
  operation,
  pathParameter,
  uuid,
}) {
  const boundedLimit = (maximum = 200, defaultValue = 100) =>
    queryParameter("limit", {
      type: "integer",
      minimum: 1,
      maximum,
      default: defaultValue,
    });
  document.paths["/v1/governance/artifacts"] = {
    get: operation({
      operationId: "listGovernanceArtifactsV1",
      summary: "List version-governed SRE artifacts",
      responseSchema: "GovernanceArtifactPage",
      parameters: [
        queryParameter("kind", ref("GovernanceObjectKind")),
        queryParameter("logical_key", boundedText(256)),
        boundedLimit(),
      ],
    }),
    post: operation({
      operationId: "createGovernanceArtifactV1",
      summary: "Create a logical governed artifact head",
      bodySchema: "CreateGovernanceArtifactRequest",
      responseSchema: "GovernanceArtifact",
    }),
  };
  document.paths["/v1/governance/artifacts/{id}/versions"] = {
    get: operation({
      operationId: "listGovernanceVersionsV1",
      summary: "List immutable versions for one governed artifact",
      responseSchema: "GovernanceVersionPage",
      parameters: [
        pathParameter("id"),
        queryParameter("state", ref("GovernanceLifecycleState")),
        boundedLimit(),
      ],
    }),
    post: operation({
      operationId: "createGovernanceVersionV1",
      summary: "Create an unsigned draft governed artifact version",
      bodySchema: "CreateGovernanceVersionRequest",
      responseSchema: "GovernanceVersion",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/governance/versions/{id}/transition"] = {
    post: operation({
      operationId: "transitionGovernanceVersionV1",
      summary: "Apply a separated-duty governance lifecycle transition",
      bodySchema: "TransitionGovernanceVersionRequest",
      responseSchema: "GovernanceVersion",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/governance/versions/{id}/impacts"] = {
    get: operation({
      operationId: "listGovernanceImpactsV1",
      summary: "List bounded reverse references for a governed version",
      responseSchema: "GovernanceImpactPage",
      parameters: [
        pathParameter("id"),
        queryParameter("cluster_id", uuid),
        queryParameter("kind", ref("GovernanceImpactKind")),
        boundedLimit(),
      ],
    }),
    post: operation({
      operationId: "recordGovernanceImpactV1",
      summary: "Record one exact dependency on a governed version",
      bodySchema: "RecordGovernanceImpactRequest",
      responseSchema: "GovernanceImpact",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/governance/admissions/evaluate"] = {
    post: operation({
      operationId: "evaluateGovernanceAdmissionV1",
      summary: "Fail closed on unsigned, expired, or quarantined versions",
      bodySchema: "EvaluateGovernanceAdmissionRequest",
      responseSchema: "GovernanceAdmissionView",
    }),
  };
  document.paths["/v1/governance/audit/export"] = {
    get: operation({
      operationId: "exportGovernanceAuditV1",
      summary: "Export a bounded append-only governance audit view",
      responseSchema: "GovernanceAuditExport",
      parameters: [
        queryParameter("artifact_id", uuid),
        queryParameter("version_id", uuid),
        queryParameter("from", dateTime),
        queryParameter("to", dateTime),
        boundedLimit(1000, 500),
      ],
    }),
  };
  document.paths["/v1/governance/compliance"] = {
    get: operation({
      operationId: "getGovernanceComplianceV1",
      summary: "Read signature, expiry, review, and quarantine compliance",
      responseSchema: "GovernanceComplianceReport",
    }),
  };
}

function addFinOpsPaths({ document, operation, uuid }) {
  const limit = queryParameter("limit", {
    type: "integer",
    minimum: 1,
    maximum: 200,
    default: 100,
  });
  document.paths["/v1/finops/ledger"] = {
    get: operation({
      operationId: "listFinOpsLedgerV1",
      summary: "List append-only model and infrastructure cost entries",
      responseSchema: "FinOpsLedgerPage",
      parameters: [
        queryParameter("cluster_id", uuid),
        queryParameter("source", ref("FinOpsCostSource")),
        queryParameter("from", dateTime),
        queryParameter("to", dateTime),
        limit,
      ],
    }),
    post: operation({
      operationId: "recordFinOpsCostV1",
      summary: "Record one idempotent sanitized usage and cost entry",
      bodySchema: "RecordFinOpsCostRequest",
      responseSchema: "FinOpsCostEntry",
    }),
  };
  document.paths["/v1/finops/budgets"] = {
    get: operation({
      operationId: "listFinOpsBudgetsV1",
      summary: "List scoped soft and hard cost budgets",
      responseSchema: "FinOpsBudgetPage",
      parameters: [
        queryParameter("scope_kind", ref("FinOpsBudgetScopeKind")),
        queryParameter("active", { type: "boolean" }),
        limit,
      ],
    }),
    post: operation({
      operationId: "createFinOpsBudgetV1",
      summary: "Create a versioned cost budget",
      bodySchema: "CreateFinOpsBudgetRequest",
      responseSchema: "FinOpsBudget",
    }),
  };
  document.paths["/v1/finops/budgets/evaluate"] = {
    post: operation({
      operationId: "evaluateFinOpsBudgetV1",
      summary: "Evaluate cost pressure without weakening safety controls",
      bodySchema: "EvaluateFinOpsBudgetRequest",
      responseSchema: "FinOpsBudgetDecisionView",
    }),
  };
  document.paths["/v1/finops/allocation-policy"] = {
    get: operation({
      operationId: "getFinOpsAllocationPolicyV1",
      summary: "Read the current showback or confirmed chargeback policy",
      responseSchema: "FinOpsAllocationPolicyView",
    }),
    post: operation({
      operationId: "createFinOpsAllocationPolicyV1",
      summary: "Create a versioned cost allocation policy",
      bodySchema: "CreateFinOpsAllocationPolicyRequest",
      responseSchema: "FinOpsAllocationPolicyView",
    }),
  };
  document.paths["/v1/finops/report"] = {
    get: operation({
      operationId: "getFinOpsReportV1",
      summary: "Read bounded showback, forecasts, anomalies, and coverage",
      responseSchema: "FinOpsReport",
      parameters: [
        queryParameter("from", dateTime, true),
        queryParameter("to", dateTime, true),
        queryParameter("cluster_id", uuid),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 500,
          default: 200,
        }),
      ],
    }),
  };
}

export function extendPhase5GovernanceAndFinOps({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
  digest,
}) {
  addGovernanceSchemas({ schemas, uuid, digest });
  addFinOpsSchemas({ schemas, uuid });
  addGovernancePaths({ document, operation, pathParameter, uuid });
  addFinOpsPaths({ document, operation, uuid });
  document["x-rocketmq-governance-schema"] =
    "rocketmq-sre.governance.v1";
  document["x-rocketmq-finops-schema"] = "rocketmq-sre.finops.v1";
}
