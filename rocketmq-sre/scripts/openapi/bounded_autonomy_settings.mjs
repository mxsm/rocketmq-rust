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
const unsigned = {
  type: "integer",
  format: "uint64",
  minimum: 0,
};

const executionActions = [
  "observability.logger_level_ttl.v1",
  "proxy.scale_out_one.v1",
  "proxy.restart_one.v1",
  "broker.config.patch_allowlisted.v1",
  "topic.config.patch_allowlisted.v1",
  "subscription_group.patch_allowlisted.v1",
  "consumer.request_mode.patch_allowlisted.v1",
  "consumer.offset.reset_bounded.v1",
  "topic.queue.expand_only.v1",
  "namesrv.config.patch_allowlisted.v1",
  "controller.config.patch_allowlisted.v1",
  "proxy.rollout_image_canary.v1",
  "broker.restart_one.v1",
  "static_topic.patch_non_remap.v1",
  "tiered.cold_data_flow.patch_allowlisted.v1",
  "store.readahead.patch_allowlisted.v1",
  "security.credential_rotate_overlap.v1",
  "telemetry.collector.restart_one.v1",
  "consumer.offset.clone_or_reset_broad.v1",
  "message.direct_consume.v1",
  "message.dlq.resend.v1",
  "timer.switch.v1",
  "controller.elect.v1",
  "static_topic.remap.v1",
  "broker.container.add_remove.v1",
];

export function extendBoundedAutonomySettings({
  document,
  schemas,
  operation,
  uuid,
  digest,
}) {
  schemas.AutonomyMode = {
    type: "string",
    enum: ["disabled", "shadow", "supervised", "autonomous", "paused"],
  };
  schemas.AutonomyExecutionAction = {
    type: "string",
    enum: executionActions,
  };
  schemas.AutonomyPolicyDefinition = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "definition_version",
      "tenant_id",
      "cluster_id",
      "action",
      "action_version",
      "descriptor_digest",
      "diagnostic_pack_id",
      "diagnostic_pack_version",
      "owner",
      "minimum_evidence_freshness_seconds",
      "required_evidence_sources",
      "min_shadow_samples",
      "min_supervised_successes",
      "observation_window_days",
      "max_unresolved_unknown",
      "max_recent_rollbacks",
      "max_executions_per_hour",
      "cooldown_seconds",
      "max_concurrent_executions",
      "stable_window_seconds",
      "created_at",
    ],
    properties: {
      id: uuid,
      definition_version: { ...unsigned, minimum: 1 },
      tenant_id: uuid,
      cluster_id: uuid,
      action: ref("AutonomyExecutionAction"),
      action_version: { const: "1.0.0" },
      descriptor_digest: digest,
      diagnostic_pack_id: boundedText(128),
      diagnostic_pack_version: boundedText(32),
      owner: boundedText(128),
      minimum_evidence_freshness_seconds: { ...unsigned, minimum: 1 },
      required_evidence_sources: {
        type: "array",
        minItems: 1,
        maxItems: 32,
        uniqueItems: true,
        items: boundedText(128),
      },
      min_shadow_samples: { ...unsigned, minimum: 1 },
      min_supervised_successes: { ...unsigned, minimum: 1 },
      observation_window_days: { ...unsigned, minimum: 1, maximum: 65535 },
      max_unresolved_unknown: unsigned,
      max_recent_rollbacks: unsigned,
      max_executions_per_hour: { ...unsigned, minimum: 1, maximum: 65535 },
      cooldown_seconds: { ...unsigned, minimum: 1 },
      max_concurrent_executions: { ...unsigned, minimum: 1, maximum: 65535 },
      stable_window_seconds: { ...unsigned, minimum: 1 },
      created_at: dateTime,
    },
  };
  schemas.AutonomyLifecycleState = {
    type: "object",
    additionalProperties: false,
    required: [
      "tenant_id",
      "cluster_id",
      "action",
      "mode",
      "previous_mode",
      "owner",
      "pause_reason",
      "lifecycle_revision",
      "updated_by",
      "updated_at",
    ],
    properties: {
      tenant_id: uuid,
      cluster_id: uuid,
      action: ref("AutonomyExecutionAction"),
      mode: ref("AutonomyMode"),
      previous_mode: nullable(ref("AutonomyMode")),
      owner: boundedText(128),
      pause_reason: nullable(boundedText(512)),
      lifecycle_revision: { ...unsigned, minimum: 1 },
      updated_by: boundedText(256),
      updated_at: dateTime,
    },
  };
  schemas.AutonomyQualificationCohort = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "level",
      "tenant_id",
      "cluster_id",
      "action",
      "action_version",
      "policy_definition_version",
      "descriptor_digest",
      "diagnostic_pack_id",
      "diagnostic_pack_version",
      "primary_actual_model_identity_hash",
      "critic_actual_model_identity_hash",
      "cohort_hash",
      "created_at",
    ],
    properties: {
      id: uuid,
      level: { type: "string", enum: ["shadow", "autonomous"] },
      tenant_id: uuid,
      cluster_id: uuid,
      action: ref("AutonomyExecutionAction"),
      action_version: { const: "1.0.0" },
      policy_definition_version: { ...unsigned, minimum: 1 },
      descriptor_digest: digest,
      diagnostic_pack_id: boundedText(128),
      diagnostic_pack_version: boundedText(32),
      primary_actual_model_identity_hash: digest,
      critic_actual_model_identity_hash: nullable(digest),
      cohort_hash: digest,
      created_at: dateTime,
    },
  };
  schemas.AutonomyQualificationView = {
    type: "object",
    additionalProperties: false,
    required: [
      "shadow_cohort",
      "autonomous_cohort",
      "qualified_shadow_samples",
      "unqualified_shadow_samples",
      "qualified_supervised_successes",
      "unresolved_unknown",
      "recent_rollbacks",
      "shadow_observation_window_met",
      "autonomous_observation_window_met",
    ],
    properties: {
      shadow_cohort: nullable(ref("AutonomyQualificationCohort")),
      autonomous_cohort: nullable(ref("AutonomyQualificationCohort")),
      qualified_shadow_samples: unsigned,
      unqualified_shadow_samples: unsigned,
      qualified_supervised_successes: unsigned,
      unresolved_unknown: unsigned,
      recent_rollbacks: unsigned,
      shadow_observation_window_met: { type: "boolean" },
      autonomous_observation_window_met: { type: "boolean" },
    },
  };
  schemas.AutonomyFreezeView = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "cluster_id",
      "action",
      "action_version",
      "revision",
      "active",
      "reason",
      "starts_at",
      "expires_at",
      "updated_by",
      "updated_at",
    ],
    properties: {
      id: uuid,
      cluster_id: nullable(uuid),
      action: nullable(ref("AutonomyExecutionAction")),
      action_version: nullable({ const: "1.0.0" }),
      revision: { ...unsigned, minimum: 1 },
      active: { type: "boolean" },
      reason: boundedText(512),
      starts_at: dateTime,
      expires_at: nullable(dateTime),
      updated_by: boundedText(256),
      updated_at: dateTime,
    },
  };
  schemas.AutonomyKillSwitchView = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "action",
      "action_version",
      "revision",
      "active",
      "reason",
      "updated_by",
      "updated_at",
    ],
    properties: {
      cluster_id: uuid,
      action: ref("AutonomyExecutionAction"),
      action_version: { const: "1.0.0" },
      revision: { ...unsigned, minimum: 1 },
      active: { type: "boolean" },
      reason: boundedText(512),
      updated_by: boundedText(256),
      updated_at: dateTime,
    },
  };
  schemas.AutonomyScopeView = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "policy",
      "lifecycle",
      "qualification",
      "active_freezes",
      "kill_switch",
      "recent_outcomes",
      "reason_codes",
    ],
    properties: {
      schema_version: { const: "rocketmq-sre.autonomy.v1" },
      policy: ref("AutonomyPolicyDefinition"),
      lifecycle: ref("AutonomyLifecycleState"),
      qualification: ref("AutonomyQualificationView"),
      active_freezes: {
        type: "array",
        maxItems: 200,
        items: ref("AutonomyFreezeView"),
      },
      kill_switch: nullable(ref("AutonomyKillSwitchView")),
      recent_outcomes: {
        type: "array",
        maxItems: 200,
        items: { $ref: "#/components/schemas/JsonObject" },
      },
      reason_codes: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(128),
      },
    },
  };
  schemas.AutonomyScopePage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "truncated"],
    properties: {
      schema_version: { const: "rocketmq-sre.autonomy.v1" },
      items: {
        type: "array",
        maxItems: 200,
        items: ref("AutonomyScopeView"),
      },
      truncated: { type: "boolean" },
    },
  };
  schemas.AutonomyTransitionRequest = {
    type: "object",
    additionalProperties: false,
    required: ["target_mode"],
    properties: {
      target_mode: ref("AutonomyMode"),
      reason: nullable(boundedText(512)),
      owner_confirmed: { type: "boolean", default: false },
      owner_approval_ref: {
        type: "string",
        minLength: 14,
        maxLength: 160,
        pattern:
          "^approval://(?!.*(?:\\.\\.|//))[a-z0-9](?:[a-z0-9._/-]*[a-z0-9])$",
      },
    },
  };
  schemas.SetAutonomyFreezeRequest = {
    type: "object",
    additionalProperties: false,
    required: ["active", "reason", "starts_at"],
    properties: {
      cluster_id: nullable(uuid),
      action: nullable(ref("AutonomyExecutionAction")),
      action_version: nullable({ const: "1.0.0" }),
      active: { type: "boolean" },
      reason: boundedText(512),
      starts_at: dateTime,
      expires_at: nullable(dateTime),
    },
  };
  schemas.SetAutonomyKillSwitchRequest = {
    type: "object",
    additionalProperties: false,
    required: ["cluster_id", "action", "action_version", "active", "reason"],
    properties: {
      cluster_id: uuid,
      action: ref("AutonomyExecutionAction"),
      action_version: { const: "1.0.0" },
      active: { type: "boolean" },
      reason: boundedText(512),
    },
  };

  const scopeParameters = [
    queryParameter("cluster_id", uuid, true),
    queryParameter("action", ref("AutonomyExecutionAction"), true),
    queryParameter("action_version", { type: "string", const: "1.0.0", default: "1.0.0" }),
  ];
  const settingsPaths = {
    "/v1/autonomy/scopes": {
      get: operation({
        operationId: "listAutonomyScopes",
        summary: "List bounded autonomy scopes for one authorized cluster",
        responseSchema: "AutonomyScopePage",
        parameters: [
          queryParameter("cluster_id", uuid, true),
          queryParameter("limit", {
            type: "integer",
            format: "uint16",
            minimum: 1,
            maximum: 200,
            default: 100,
          }),
        ],
      }),
    },
    "/v1/autonomy/scope": {
      get: operation({
        operationId: "getAutonomyScope",
        summary: "Read one exact action and cluster autonomy scope",
        responseSchema: "AutonomyScopeView",
        parameters: scopeParameters,
      }),
    },
    "/v1/autonomy/transitions": {
      post: operation({
        operationId: "transitionAutonomyScope",
        summary: "Apply a human-authorized autonomy lifecycle transition",
        responseSchema: "AutonomyScopeView",
        bodySchema: "AutonomyTransitionRequest",
        parameters: scopeParameters,
      }),
    },
    "/v1/autonomy/freezes": {
      post: operation({
        operationId: "setAutonomyFreeze",
        summary: "Activate or release a bounded autonomy freeze",
        responseSchema: "AutonomyFreezeView",
        bodySchema: "SetAutonomyFreezeRequest",
      }),
    },
    "/v1/autonomy/kill-switches": {
      post: operation({
        operationId: "setAutonomyKillSwitch",
        summary: "Activate or release one action-scoped kill switch",
        responseSchema: "AutonomyKillSwitchView",
        bodySchema: "SetAutonomyKillSwitchRequest",
      }),
    },
  };
  const orderedPaths = { ...settingsPaths };
  for (const [path, pathItem] of Object.entries(document.paths)) {
    if (Object.hasOwn(settingsPaths, path)) {
      continue;
    }
    orderedPaths[path] = pathItem;
  }
  document.paths = orderedPaths;

  for (const [path, pathItem] of Object.entries(document.paths)) {
    if (!path.startsWith("/v1/autonomy/")) {
      continue;
    }
    for (const [method, value] of Object.entries(pathItem)) {
      value.tags = ["Bounded Autonomy"];
      value.security = [
        {
          oidc:
            method === "get"
              ? ["rocketmq:read"]
              : ["rocketmq:autonomy:manage"],
        },
      ];
    }
  }
}
