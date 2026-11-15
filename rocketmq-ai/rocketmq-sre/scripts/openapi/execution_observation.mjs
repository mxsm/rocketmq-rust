// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export const EXECUTION_OBSERVATION_SCHEMA_FILES = Object.freeze({
  ExecutionObservationView: "execution-observation-view.schema.json",
  ExecutionLookupRequest: "execution-lookup-request.schema.json",
  ExecutionLookupView: "execution-lookup-view.schema.json",
  PlanExecutionPage: "plan-execution-page.schema.json",
});

function rewrite(value, prefix) {
  if (Array.isArray(value)) return value.map((item) => rewrite(item, prefix));
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => key !== "$schema" && key !== "$defs")
    .map(([key, child]) => {
      if (key === "$ref" && typeof child === "string") {
        if (child.startsWith("#/$defs/")) return [key, `#/components/schemas/${prefix}__${child.slice(8)}`];
        if (child === "#" || child.startsWith("#/")) return [key, `#/components/schemas/${prefix}${child.slice(1)}`];
        throw new Error(`Execution observation schema has a non-local reference: ${prefix}`);
      }
      return [key, rewrite(child, prefix)];
    }));
}

/** Imports only safe read projections; old signed request/receipt schemas stay unchanged. */
export function extendExecutionObservation({ document, readSchema }) {
  const schemas = document.components.schemas;
  if (!schemas.ExecutionSubmissionView || !schemas.ErrorEnvelope) {
    throw new Error("Execution observation requires the supervised execution contract");
  }
  for (const [name, filename] of Object.entries(EXECUTION_OBSERVATION_SCHEMA_FILES)) {
    const source = readSchema(filename);
    for (const key of Object.keys(schemas)) {
      if (key === name || key.startsWith(`${name}__`)) delete schemas[key];
    }
    schemas[name] = rewrite(source, name);
    for (const [key, definition] of Object.entries(source.$defs ?? {})) {
      schemas[`${name}__${key}`] = rewrite(definition, name);
    }
  }
  const contractList = document["x-rocketmq-phase3-contracts"];
  if (Array.isArray(contractList)) {
    document["x-rocketmq-phase3-contracts"] = [...new Set([...contractList, ...Object.keys(EXECUTION_OBSERVATION_SCHEMA_FILES)])];
  }
  const response = (name, description) => ({ description, content: {
    "application/json": { schema: { $ref: `#/components/schemas/${name}` } },
  } });
  const responses = (name) => ({
    200: response(name, "Bounded authorized read snapshot; no target operation is performed"),
    ...Object.fromEntries([400, 401, 403, 404, 409, 503].map((status) => [status,
      response("ErrorEnvelope", "Sanitized read refusal or unavailable retained state"),
    ])),
  });
  const id = (name) => ({ name, in: "path", required: true, schema: { type: "string", format: "uuid" } });
  document.paths["/v1/executions/{execution_id}/observation"] = { get: {
    operationId: "getExecutionObservation",
    summary: "Read exact accepted execution, recorded effects and verification provenance",
    description: "Recorded execution or policy-window success does not prove current business recovery. Missing retained or legacy proof remains explicit. This read does not reconcile or dispatch target work.",
    parameters: [id("execution_id")], responses: responses("ExecutionObservationView"),
  } };
  document.paths["/v1/executions/lookup"] = { post: {
    operationId: "lookupExecution",
    summary: "Read the original authenticated actor's exact accepted intent selector",
    description: "A not_observed result is not proof that no request was accepted or no target effect occurred. This operation never submits, retries, changes a key, acquires a lease or calls an Executor.",
    requestBody: { required: true, content: { "application/json": {
      schema: { $ref: "#/components/schemas/ExecutionLookupRequest" },
    } } }, responses: responses("ExecutionLookupView"),
  } };
  document.paths["/v1/plans/{plan_id}/executions"] = { get: {
    operationId: "getPlanExecutions",
    summary: "Read bounded accepted receipt references for one authorized immutable Plan/hash",
    parameters: [id("plan_id"), { name: "plan_hash", in: "query", required: true,
      schema: { type: "string", pattern: "^sha256:[0-9A-Fa-f]{64}$" } }], responses: responses("PlanExecutionPage"),
  } };
}
