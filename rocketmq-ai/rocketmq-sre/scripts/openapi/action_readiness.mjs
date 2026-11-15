// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export const ACTION_READINESS_SCHEMA_FILES = Object.freeze({
  ActionReadinessRequest: "action-readiness-request.schema.json",
  ActionReadinessView: "action-readiness-view.schema.json",
  ActionPreparationEvidence: "action-preparation-evidence.schema.json",
});

function rewrite(value, prefix) {
  if (Array.isArray(value)) return value.map((item) => rewrite(item, prefix));
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => key !== "$schema" && key !== "$defs")
    .map(([key, child]) => {
      if (key === "$ref" && typeof child === "string") {
        if (child.startsWith("#/$defs/")) {
          return [key, `#/components/schemas/${prefix}__${child.slice(8)}`];
        }
        if (child === "#" || child.startsWith("#/")) {
          return [key, `#/components/schemas/${prefix}${child.slice(1)}`];
        }
        throw new Error(`Action readiness schema has a non-local reference: ${prefix}`);
      }
      return [key, rewrite(child, prefix)];
    }));
}

/** Imports Rust-owned fields without changing old Plan or Agent wire schemas. */
export function extendActionReadiness({ document, readSchema }) {
  const schemas = document.components.schemas;
  if (!schemas.ExecutionPreconditionEvidenceView) {
    throw new Error("Action readiness requires the supervised execution contract");
  }
  for (const [name, filename] of Object.entries(ACTION_READINESS_SCHEMA_FILES)) {
    const source = readSchema(filename);
    for (const key of Object.keys(schemas)) {
      if (key === name || key.startsWith(`${name}__`)) delete schemas[key];
    }
    schemas[name] = rewrite(source, name);
    for (const [key, definition] of Object.entries(source.$defs ?? {})) {
      schemas[`${name}__${key}`] = rewrite(definition, name);
    }
  }
  schemas.ExecutionPreconditionEvidenceView.properties.readiness = {
    $ref: "#/components/schemas/ActionReadinessView",
  };
  const contractList = document["x-rocketmq-phase3-contracts"];
  if (Array.isArray(contractList)) {
    document["x-rocketmq-phase3-contracts"] = [...new Set([...contractList, ...Object.keys(ACTION_READINESS_SCHEMA_FILES)])];
  }
  const response = (name, description) => ({
    description,
    content: { "application/json": { schema: { $ref: `#/components/schemas/${name}` } } },
  });
  const path = "/v1/incidents/{incident_id}/action-readiness";
  document.paths[path] = {
    ...document.paths[path],
    post: {
      operationId: "readActionReadiness",
      summary: "Read exact action candidate readiness without an Agent call",
      description: "Fresh identity and Incident scope are checked. In-scope blocked and unknown facts return a safe projection. Preparation and draft affordances grant no dispatch authority. Missing action/target qualification remains Unknown; existing first execution admission checks the current facts independently.",
      parameters: [{ name: "incident_id", in: "path", required: true,
        schema: { type: "string", format: "uuid" } }],
      requestBody: { required: true, content: { "application/json": {
        schema: { $ref: "#/components/schemas/ActionReadinessRequest" },
      } } },
      responses: {
        200: response("ActionReadinessView", "Current non-authorizing candidate facts and safe blockers"),
        ...Object.fromEntries([400, 401, 403, 404, 409].map((status) => [status,
          response("ErrorEnvelope", "Sanitized request refusal"),
        ])),
      },
    },
  };
}
