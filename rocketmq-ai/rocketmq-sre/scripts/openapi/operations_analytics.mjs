// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export function extendOperationsAnalytics({
  document,
  schemas,
  operation,
  uuid,
}) {
  const nullableText = (maxLength) => ({
    oneOf: [
      {
        type: "string",
        minLength: 1,
        maxLength,
      },
      { type: "null" },
    ],
  });
  const nullableMetric = {
    oneOf: [
      {
        type: "number",
        format: "double",
        minimum: 0,
      },
      { type: "null" },
    ],
  };
  const nullableBasisPoints = {
    oneOf: [
      {
        type: "integer",
        format: "uint32",
        minimum: 0,
        maximum: 10000,
      },
      { type: "null" },
    ],
  };
  const count = {
    type: "integer",
    format: "uint64",
    minimum: 0,
  };

  schemas.OperationsAnalyticsFilters = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_ids",
      "scenario",
      "provider_family",
      "model_family",
      "action_id",
    ],
    properties: {
      cluster_ids: {
        type: "array",
        minItems: 1,
        maxItems: 1000,
        uniqueItems: true,
        items: uuid,
      },
      scenario: nullableText(128),
      provider_family: nullableText(128),
      model_family: nullableText(128),
      action_id: nullableText(128),
    },
  };
  schemas.OperationsAnalyticsWindow = {
    type: "object",
    additionalProperties: false,
    required: ["period", "start", "end", "complete"],
    properties: {
      period: {
        type: "string",
        enum: ["weekly", "monthly"],
      },
      start: {
        type: "string",
        format: "date-time",
      },
      end: {
        type: "string",
        format: "date-time",
      },
      complete: {
        type: "boolean",
      },
    },
  };
  schemas.IncidentOperationsMetrics = {
    type: "object",
    additionalProperties: false,
    required: [
      "total",
      "diagnosed",
      "terminal",
      "recurrent",
      "mean_time_to_detect_seconds",
      "mean_time_to_resolve_seconds",
    ],
    properties: {
      total: count,
      diagnosed: count,
      terminal: count,
      recurrent: count,
      mean_time_to_detect_seconds: nullableMetric,
      mean_time_to_resolve_seconds: nullableMetric,
    },
  };
  schemas.OperationsAnalyticsModelUsage = {
    type: "object",
    additionalProperties: false,
    required: [
      "calls",
      "input_tokens",
      "output_tokens",
      "cost_micros",
      "calls_missing_tokens",
      "calls_missing_cost",
      "failed_calls",
      "fallback_calls",
      "usage_coverage_basis_points",
      "cost_coverage_basis_points",
    ],
    properties: {
      calls: count,
      input_tokens: count,
      output_tokens: count,
      cost_micros: count,
      calls_missing_tokens: count,
      calls_missing_cost: count,
      failed_calls: count,
      fallback_calls: count,
      usage_coverage_basis_points: nullableBasisPoints,
      cost_coverage_basis_points: nullableBasisPoints,
    },
  };
  schemas.OperationsAnalyticsFeedback = {
    type: "object",
    additionalProperties: false,
    required: [
      "total",
      "adopted",
      "modified",
      "rejected",
      "adoption_basis_points",
      "modification_basis_points",
      "rejection_basis_points",
    ],
    properties: {
      total: count,
      adopted: count,
      modified: count,
      rejected: count,
      adoption_basis_points: nullableBasisPoints,
      modification_basis_points: nullableBasisPoints,
      rejection_basis_points: nullableBasisPoints,
    },
  };
  schemas.ExecutionOperationsMetrics = {
    type: "object",
    additionalProperties: false,
    required: [
      "total",
      "terminal",
      "succeeded",
      "rolled_back",
      "escalated",
      "success_basis_points",
    ],
    properties: {
      total: count,
      terminal: count,
      succeeded: count,
      rolled_back: count,
      escalated: count,
      success_basis_points: nullableBasisPoints,
    },
  };
  schemas.AttributedAutomationSavingsMetrics = {
    type: "object",
    additionalProperties: false,
    required: [
      "successful_no_side_effect_runs",
      "successful_preventive_runs",
      "successful_autonomous_actions",
      "estimated_minutes_saved",
      "estimate_method",
    ],
    properties: {
      successful_no_side_effect_runs: count,
      successful_preventive_runs: count,
      successful_autonomous_actions: count,
      estimated_minutes_saved: count,
      estimate_method: {
        type: "string",
        minLength: 1,
        maxLength: 1024,
      },
    },
  };
  schemas.OperationsAnalyticsReport = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "tenant_id",
      "filters",
      "window",
      "incidents",
      "model_usage",
      "recommendation_feedback",
      "executions",
      "savings",
      "mttd_definition",
      "mttr_definition",
      "savings_definition",
      "warnings",
      "observed_at",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.operations-analytics.v1",
      },
      tenant_id: uuid,
      filters: {
        $ref: "#/components/schemas/OperationsAnalyticsFilters",
      },
      window: {
        $ref: "#/components/schemas/OperationsAnalyticsWindow",
      },
      incidents: {
        $ref: "#/components/schemas/IncidentOperationsMetrics",
      },
      model_usage: {
        $ref: "#/components/schemas/OperationsAnalyticsModelUsage",
      },
      recommendation_feedback: {
        $ref: "#/components/schemas/OperationsAnalyticsFeedback",
      },
      executions: {
        $ref: "#/components/schemas/ExecutionOperationsMetrics",
      },
      savings: {
        $ref: "#/components/schemas/AttributedAutomationSavingsMetrics",
      },
      mttd_definition: {
        type: "string",
        minLength: 1,
        maxLength: 1024,
      },
      mttr_definition: {
        type: "string",
        minLength: 1,
        maxLength: 1024,
      },
      savings_definition: {
        type: "string",
        minLength: 1,
        maxLength: 1024,
      },
      warnings: {
        type: "array",
        maxItems: 8,
        items: {
          type: "string",
          maxLength: 512,
        },
      },
      observed_at: {
        type: "string",
        format: "date-time",
      },
    },
  };

  document.paths["/v1/operations/analytics"] = {
    get: operation({
      operationId: "getDimensionalOperationsAnalytics",
      summary:
        "Query quality, cost, latency, adoption, execution and savings by authenticated dimensions",
      responseSchema: "OperationsAnalyticsReport",
      parameters: [
        {
          name: "cluster_id",
          in: "query",
          required: false,
          schema: uuid,
        },
        {
          name: "period",
          in: "query",
          required: false,
          schema: {
            type: "string",
            enum: ["weekly", "monthly"],
            default: "weekly",
          },
        },
        {
          name: "anchor",
          in: "query",
          required: false,
          schema: {
            type: "string",
            format: "date-time",
          },
        },
        ...["scenario", "provider_family", "model_family", "action_id"].map(
          (name) => ({
            name,
            in: "query",
            required: false,
            schema: {
              type: "string",
              minLength: 1,
              maxLength: 128,
            },
          }),
        ),
      ],
    }),
  };
}
