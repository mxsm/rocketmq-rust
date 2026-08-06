// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export function extendConversationMetrics({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
  digest,
}) {
  const nullable = (schema) => ({
    oneOf: [schema, { type: "null" }],
  });
  const timestamp = { type: "string", format: "date-time" };

  schemas.ConversationQueryIntent = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "kind",
      "source",
      "resource",
      "window_seconds",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.conversation-query-intent.v1",
      },
      kind: {
        type: "string",
        enum: [
          "cluster_overview",
          "topic_list",
          "topic_describe",
          "consumer_lag",
          "broker_runtime",
          "metric_instant",
          "metric_range",
        ],
      },
      source: {
        type: "string",
        enum: ["rocketmq-mcp", "prometheus"],
      },
      resource: { type: "string", minLength: 1, maxLength: 1024 },
      window_seconds: {
        type: "integer",
        format: "uint32",
        minimum: 60,
        maximum: 86400,
      },
    },
  };
  schemas.ConversationCitation = {
    type: "object",
    additionalProperties: false,
    required: [
      "evidence_id",
      "source",
      "resource",
      "content_hash",
      "observed_at",
      "freshness_seconds",
      "partial",
    ],
    properties: {
      evidence_id: uuid,
      source: { type: "string", minLength: 1 },
      resource: { type: "string", minLength: 1 },
      content_hash: digest,
      observed_at: timestamp,
      freshness_seconds: {
        type: "integer",
        format: "uint64",
        minimum: 0,
      },
      partial: { type: "boolean" },
    },
  };
  schemas.ConversationTurn = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "conversation_id",
      "tenant_id",
      "cluster_id",
      "sequence",
      "question",
      "resource",
      "status",
      "query_intent",
      "correlation_id",
      "created_at",
      "completed_at",
    ],
    properties: {
      id: uuid,
      conversation_id: uuid,
      tenant_id: uuid,
      cluster_id: uuid,
      sequence: { type: "integer", format: "uint32", minimum: 1 },
      question: { type: "string", minLength: 1, maxLength: 8192 },
      resource: nullable({ type: "string", minLength: 1, maxLength: 1024 }),
      status: {
        type: "string",
        enum: [
          "collecting",
          "answered",
          "needs_scope",
          "needs_evidence",
          "cancelled",
          "failed",
        ],
      },
      query_intent: nullable({
        $ref: "#/components/schemas/ConversationQueryIntent",
      }),
      correlation_id: uuid,
      created_at: timestamp,
      completed_at: nullable(timestamp),
    },
  };
  schemas.ConversationAnswerRevision = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "conversation_id",
      "turn_id",
      "revision",
      "answer",
      "mode",
      "citations",
      "evidence_ids",
      "model_invocation_id",
      "partial",
      "warnings",
      "created_at",
    ],
    properties: {
      id: uuid,
      conversation_id: uuid,
      turn_id: uuid,
      revision: { type: "integer", format: "uint32", minimum: 1 },
      answer: { type: "string", minLength: 1, maxLength: 12000 },
      mode: { type: "string", enum: ["model_assisted", "rules_only"] },
      citations: {
        type: "array",
        maxItems: 32,
        items: { $ref: "#/components/schemas/ConversationCitation" },
      },
      evidence_ids: {
        type: "array",
        maxItems: 32,
        uniqueItems: true,
        items: uuid,
      },
      model_invocation_id: nullable(uuid),
      partial: { type: "boolean" },
      warnings: {
        type: "array",
        maxItems: 16,
        items: { type: "string", maxLength: 128 },
      },
      created_at: timestamp,
    },
  };
  schemas.ConversationTurnView = {
    type: "object",
    additionalProperties: false,
    required: ["turn", "answer"],
    properties: {
      turn: { $ref: "#/components/schemas/ConversationTurn" },
      answer: nullable({
        $ref: "#/components/schemas/ConversationAnswerRevision",
      }),
    },
  };
  schemas.ConversationTurnPage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "observed_at"],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.conversation-turn-page.v1",
      },
      items: {
        type: "array",
        maxItems: 200,
        items: { $ref: "#/components/schemas/ConversationTurnView" },
      },
      observed_at: timestamp,
    },
  };
  schemas.ConversationTurnRequest = {
    type: "object",
    additionalProperties: false,
    required: ["question"],
    properties: {
      question: { type: "string", minLength: 1, maxLength: 8192 },
      resource: { type: "string", minLength: 1, maxLength: 1024 },
      window_seconds: {
        type: "integer",
        format: "uint32",
        minimum: 60,
        maximum: 86400,
      },
    },
  };
  schemas.ConversationCancelResult = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "cancellation_requested", "observed_at"],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.conversation-cancel.v1",
      },
      cancellation_requested: { type: "boolean" },
      observed_at: timestamp,
    },
  };

  const conversationId = pathParameter("id");
  document.paths["/v1/conversations/{id}/turns"] = {
    get: operation({
      operationId: "listConversationTurns",
      summary: "List bounded evidence-cited conversation turns",
      responseSchema: "ConversationTurnPage",
      parameters: [conversationId],
    }),
    post: operation({
      operationId: "submitConversationTurn",
      summary: "Run one bounded read-only conversational metric query",
      responseSchema: "ConversationTurnView",
      bodySchema: "ConversationTurnRequest",
      parameters: [conversationId],
    }),
  };
  document.paths["/v1/conversations/{id}/cancel"] = {
    post: operation({
      operationId: "cancelConversationQuery",
      summary: "Request cancellation of an in-flight read-only query",
      responseSchema: "ConversationCancelResult",
      parameters: [pathParameter("id")],
    }),
  };
  for (const path of [
    "/v1/conversations/{id}/turns",
    "/v1/conversations/{id}/cancel",
  ]) {
    for (const value of Object.values(document.paths[path])) {
      value.tags = ["Conversations"];
      value.security = [{ oidc: ["rocketmq:diagnose"] }];
    }
  }
  if (!(document.tags ?? []).some((tag) => tag.name === "Conversations")) {
    document.tags = [
      ...(document.tags ?? []),
      {
        name: "Conversations",
        description:
          "Evidence-cited natural-language queries over a fixed read-only RocketMQ and metric registry.",
      },
    ];
  }
}
