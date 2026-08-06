// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export function extendModelCapabilities({ document, schemas }) {
  for (const required of [
    "SchemaVersion",
    "DescriptorStatus",
    "Deprecation",
  ]) {
    if (!Object.hasOwn(schemas, required)) {
      throw new Error(`model capability contract requires ${required}`);
    }
  }

  const nullable = (schema) => ({
    oneOf: [schema, { type: "null" }],
  });
  const timestamp = { type: "string", format: "date-time" };
  const uuid = { type: "string", format: "uuid" };

  schemas.ModelProviderCapability = {
    type: "string",
    enum: [
      "chat",
      "text",
      "json_object",
      "json_schema",
      "tool_calling",
      "tool_choice_required",
      "tool_choice_specific",
      "strict_tools",
      "vision",
      "reasoning",
      "streaming",
      "embeddings",
      "rerank",
      "kimi_mfjs",
    ],
  };
  schemas.ModelProviderCapabilities = {
    type: "object",
    additionalProperties: false,
    required: ["supported", "max_input_tokens", "max_output_tokens"],
    properties: {
      supported: {
        type: "array",
        uniqueItems: true,
        items: { $ref: "#/components/schemas/ModelProviderCapability" },
      },
      max_input_tokens: nullable({
        type: "integer",
        format: "uint32",
        minimum: 0,
      }),
      max_output_tokens: nullable({
        type: "integer",
        format: "uint32",
        minimum: 0,
      }),
    },
  };
  schemas.ModelProviderDescriptor = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "version",
      "owner",
      "supported_versions",
      "config_schema",
      "status",
      "deprecation",
      "protocols",
      "supports_streaming",
      "supports_tools",
      "supports_structured_output",
      "supports_embeddings",
    ],
    properties: {
      id: { type: "string", minLength: 1 },
      version: { type: "string", minLength: 1 },
      owner: { type: "string", minLength: 1 },
      supported_versions: {
        type: "array",
        items: { $ref: "#/components/schemas/SchemaVersion" },
      },
      required_capabilities: {
        type: "array",
        uniqueItems: true,
        items: { type: "string", minLength: 1 },
      },
      config_schema: {
        type: "object",
        additionalProperties: true,
      },
      status: { $ref: "#/components/schemas/DescriptorStatus" },
      deprecation: nullable({ $ref: "#/components/schemas/Deprecation" }),
      protocols: {
        type: "array",
        uniqueItems: true,
        items: { type: "string", minLength: 1 },
      },
      supports_streaming: { type: "boolean" },
      supports_tools: { type: "boolean" },
      supports_structured_output: { type: "boolean" },
      supports_embeddings: { type: "boolean" },
    },
  };
  schemas.ModelProfileStatus = {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "profile_name",
      "provider_family",
      "protocol_family",
      "model_family",
      "model_name",
      "model_revision",
      "endpoint_instance",
      "region",
      "capabilities",
      "priority",
      "credential_configured",
      "credential_owner",
      "health",
      "last_health_observed_at",
    ],
    properties: {
      id: uuid,
      profile_name: { type: "string", minLength: 1 },
      provider_family: { type: "string", minLength: 1 },
      protocol_family: { type: "string", minLength: 1 },
      model_family: { type: "string", minLength: 1 },
      model_name: { type: "string", minLength: 1 },
      model_revision: { type: "string", minLength: 1 },
      endpoint_instance: {
        type: "string",
        minLength: 1,
        description: "Opaque endpoint instance identifier, never an endpoint URL.",
      },
      region: { type: "string", minLength: 1 },
      capabilities: {
        $ref: "#/components/schemas/ModelProviderCapabilities",
      },
      priority: {
        type: "integer",
        format: "uint16",
        minimum: 0,
        maximum: 65535,
      },
      credential_configured: { type: "boolean" },
      credential_owner: { type: "string", enum: ["gateway", "adapter"] },
      health: {
        type: "string",
        enum: ["unknown", "healthy", "degraded", "quarantined", "disabled"],
      },
      last_health_observed_at: nullable(timestamp),
    },
  };
  schemas.ModelCapabilitiesResponse = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "network_calls_supported",
      "network_calls_enabled",
      "rules_only_available",
      "max_fallbacks",
      "profiles",
      "fallback_order",
      "providers",
      "observed_at",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.model-capabilities.v1",
      },
      network_calls_supported: { type: "boolean", const: true },
      network_calls_enabled: { type: "boolean" },
      rules_only_available: { type: "boolean", const: true },
      max_fallbacks: { type: "integer", minimum: 0 },
      profiles: {
        type: "array",
        items: { $ref: "#/components/schemas/ModelProfileStatus" },
      },
      fallback_order: {
        type: "array",
        uniqueItems: true,
        items: uuid,
      },
      providers: {
        type: "array",
        items: { $ref: "#/components/schemas/ModelProviderDescriptor" },
      },
      observed_at: timestamp,
    },
  };

  for (const [path, summary] of [
    ["/v1/models/capabilities", "Get sanitized Model Gateway capabilities"],
    ["/v1/models/status", "Get sanitized Model Gateway runtime status"],
  ]) {
    const operation = document.paths[path]?.get;
    if (!operation) {
      throw new Error(`missing model capability operation ${path}`);
    }
    operation.summary = summary;
    operation.security = [
      { oidc: ["rocketmq:read"] },
      { oidc: ["rocketmq:diagnose"] },
    ];
    operation.responses["200"] = {
      description: "Sanitized Model Gateway capability status",
      content: {
        "application/json": {
          schema: { $ref: "#/components/schemas/ModelCapabilitiesResponse" },
        },
      },
    };
  }

  if (!(document.tags ?? []).some((tag) => tag.name === "Models")) {
    document.tags = [
      ...(document.tags ?? []),
      {
        name: "Models",
        description:
          "Sanitized Model Gateway capabilities, runtime health, lifecycle, and bounded smoke operations.",
      },
    ];
  }
  for (const path of ["/v1/models/capabilities", "/v1/models/status"]) {
    document.paths[path].get.tags = ["Models"];
  }
}
