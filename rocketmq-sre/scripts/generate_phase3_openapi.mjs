// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(root, "openapi", "rocketmq-sre-phase02.openapi.json");
const output = join(root, "openapi", "rocketmq-sre-phase03.openapi.json");
const document = JSON.parse(readFileSync(source, "utf8"));
const schemas = document.components.schemas;

const contractSchemas = {
  ActionPlan: "action-plan.schema.json",
  ApprovalGrant: "approval-grant.schema.json",
  ApprovalRecord: "approval-record.schema.json",
  AuditEvent: "audit-event.schema.json",
  CriticAssessment: "critic-assessment.schema.json",
  CriticGateState: "critic-gate-state.schema.json",
  CriticReview: "critic-review.schema.json",
  ExecutionRequest: "execution-request.schema.json",
  ManualRunbookDraft: "manual-runbook-draft.schema.json",
  PolicyDecision: "policy-decision.schema.json",
  ResourceQuarantine: "resource-quarantine.schema.json",
};

function rewriteRefs(value) {
  if (Array.isArray(value)) {
    return value.map(rewriteRefs);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  const rewritten = {};
  for (const [key, child] of Object.entries(value)) {
    if (key === "$schema" || key === "$defs") {
      continue;
    }
    if (key === "$ref" && typeof child === "string") {
      rewritten[key] = child.replace("#/$defs/", "#/components/schemas/");
    } else {
      rewritten[key] = rewriteRefs(child);
    }
  }
  return rewritten;
}

for (const [name, filename] of Object.entries(contractSchemas)) {
  const schema = JSON.parse(
    readFileSync(join(root, "config", "schema", filename), "utf8"),
  );
  for (const [definitionName, definition] of Object.entries(
    schema.$defs ?? {},
  )) {
    schemas[definitionName] = rewriteRefs(definition);
  }
  schemas[name] = rewriteRefs(schema);
}

const uuid = { type: "string", format: "uuid" };
const digest = {
  type: "string",
  pattern: "^sha256:[0-9A-Fa-f]{64}$",
};
schemas.ActionRisk = {
  type: "string",
  enum: ["read", "plan", "r1", "r2", "r3"],
};
schemas.ExecutionState = {
  type: "string",
  enum: [
    "pending",
    "prechecking",
    "intent_persisted",
    "applying",
    "unknown",
    "reconciling",
    "verifying",
    "compensating",
    "succeeded",
    "rolled_back",
    "escalated",
  ],
};
schemas.ErrorEnvelope = {
  type: "object",
  additionalProperties: false,
  required: [
    "schema_version",
    "code",
    "message",
    "retryable",
    "correlation_id",
  ],
  properties: {
    schema_version: { const: "rocketmq-sre.error.v1" },
    code: { type: "string" },
    message: { type: "string" },
    retryable: { type: "boolean" },
    correlation_id: uuid,
  },
};
const errorResponse = {
  description: "Sanitized stable error envelope",
  content: {
    "application/json": {
      schema: { $ref: "#/components/schemas/ErrorEnvelope" },
    },
  },
};
const jsonResponse = (schema, description = "Successful response") => ({
  description,
  content: {
    "application/json": {
      schema: { $ref: `#/components/schemas/${schema}` },
    },
  },
});
const requestBody = (schema) => ({
  required: true,
  content: {
    "application/json": {
      schema: { $ref: `#/components/schemas/${schema}` },
    },
  },
});
const pathParameter = (name) => ({
  name,
  in: "path",
  required: true,
  schema: uuid,
});
const operation = ({
  operationId,
  summary,
  responseSchema,
  bodySchema,
  parameters = [],
}) => ({
  operationId,
  summary,
  parameters,
  ...(bodySchema ? { requestBody: requestBody(bodySchema) } : {}),
  responses: {
    200: jsonResponse(responseSchema),
    400: errorResponse,
    401: errorResponse,
    403: errorResponse,
    404: errorResponse,
    409: errorResponse,
    503: errorResponse,
  },
});

schemas.CandidatePlanStep = {
  type: "object",
  additionalProperties: false,
  required: [
    "action_id",
    "descriptor_version",
    "resource",
    "parameters",
    "evidence_ids",
  ],
  properties: {
    action_id: { type: "string", minLength: 1, maxLength: 255 },
    descriptor_version: { type: "string", minLength: 1, maxLength: 64 },
    resource: { type: "string", minLength: 1, maxLength: 512 },
    parameters: { type: "object" },
    evidence_ids: {
      type: "array",
      minItems: 1,
      maxItems: 32,
      items: uuid,
    },
  },
};
schemas.CreatePlanRequest = {
  type: "object",
  additionalProperties: false,
  required: [
    "cluster_id",
    "incident_id",
    "diagnosis_revision_id",
    "steps",
  ],
  properties: {
    cluster_id: uuid,
    incident_id: uuid,
    diagnosis_revision_id: uuid,
    expires_at: { type: ["string", "null"], format: "date-time" },
    steps: {
      type: "array",
      minItems: 1,
      maxItems: 16,
      items: { $ref: "#/components/schemas/CandidatePlanStep" },
    },
  },
};
schemas.CreatePlanResponse = {
  oneOf: [
    {
      type: "object",
      additionalProperties: false,
      required: ["kind", "plan", "risk", "policy_decision"],
      properties: {
        kind: { const: "action_plan" },
        plan: { $ref: "#/components/schemas/ActionPlan" },
        risk: { $ref: "#/components/schemas/ActionRisk" },
        policy_decision: { $ref: "#/components/schemas/PolicyDecision" },
      },
    },
    {
      type: "object",
      additionalProperties: false,
      required: ["kind", "runbook"],
      properties: {
        kind: { const: "manual_runbook" },
        runbook: { $ref: "#/components/schemas/ManualRunbookDraft" },
      },
    },
  ],
};
schemas.ActionPlanView = {
  type: "object",
  required: [
    "plan",
    "risk",
    "critic_state",
    "latest_critic_review",
    "latest_policy_decision",
    "latest_approval",
  ],
  properties: {
    plan: { $ref: "#/components/schemas/ActionPlan" },
    risk: { $ref: "#/components/schemas/ActionRisk" },
    critic_state: { $ref: "#/components/schemas/CriticGateState" },
    latest_critic_review: {
      oneOf: [
        { $ref: "#/components/schemas/CriticReview" },
        { type: "null" },
      ],
    },
    latest_policy_decision: {
      oneOf: [
        { $ref: "#/components/schemas/PolicyDecision" },
        { type: "null" },
      ],
    },
    latest_approval: {
      oneOf: [
        { $ref: "#/components/schemas/ApprovalRecord" },
        { type: "null" },
      ],
    },
  },
};
schemas.CriticReviewRequest = {
  type: "object",
  additionalProperties: false,
  required: ["plan_hash"],
  properties: {
    plan_hash: digest,
  },
};
schemas.CriticReviewResponse = {
  type: "object",
  additionalProperties: false,
  required: ["plan", "review", "review_hash", "critic_state"],
  properties: {
    plan: { $ref: "#/components/schemas/ActionPlan" },
    review: { $ref: "#/components/schemas/CriticReview" },
    review_hash: digest,
    critic_state: { $ref: "#/components/schemas/CriticGateState" },
  },
};
schemas.ApprovalDecisionRequest = {
  type: "object",
  additionalProperties: false,
  required: ["plan_hash", "precondition_hash", "reason"],
  properties: {
    plan_hash: digest,
    precondition_hash: digest,
    reason: { type: "string", minLength: 1, maxLength: 2048 },
    validity_seconds: {
      type: ["integer", "null"],
      minimum: 1,
      maximum: 1800,
    },
  },
};
schemas.ApprovalDecisionResponse = {
  type: "object",
  required: ["plan", "approval"],
  properties: {
    plan: { $ref: "#/components/schemas/ActionPlan" },
    approval: { $ref: "#/components/schemas/ApprovalRecord" },
    grant: {
      oneOf: [
        { $ref: "#/components/schemas/ApprovalGrant" },
        { type: "null" },
      ],
    },
  },
};
schemas.SubmitExecutionRequest = {
  type: "object",
  additionalProperties: false,
  required: [
    "plan_id",
    "plan_hash",
    "precondition_hash",
    "idempotency_key",
  ],
  properties: {
    plan_id: uuid,
    plan_hash: digest,
    precondition_hash: digest,
    idempotency_key: {
      type: "string",
      minLength: 16,
      maxLength: 200,
      pattern: "^[A-Za-z0-9._:-]+$",
    },
  },
};
schemas.ExecutionSubmissionView = {
  type: "object",
  required: ["execution", "state", "submitted_at"],
  properties: {
    execution: { $ref: "#/components/schemas/ExecutionRequest" },
    state: { $ref: "#/components/schemas/ExecutionState" },
    submitted_at: { type: "string", format: "date-time" },
  },
};
schemas.AuditPage = {
  type: "object",
  required: ["schema_version", "correlation_id", "items", "partial"],
  properties: {
    schema_version: { const: "rocketmq-sre.audit-page.v1" },
    correlation_id: uuid,
    items: {
      type: "array",
      maxItems: 500,
      items: { $ref: "#/components/schemas/AuditEvent" },
    },
    partial: { type: "boolean" },
  },
};
schemas.QuarantinePage = {
  type: "object",
  required: ["schema_version", "items", "partial"],
  properties: {
    schema_version: {
      const: "rocketmq-sre.resource-quarantine-page.v1",
    },
    items: {
      type: "array",
      maxItems: 200,
      items: { $ref: "#/components/schemas/ResourceQuarantine" },
    },
    partial: { type: "boolean" },
  },
};
schemas.ClearQuarantineRequest = {
  type: "object",
  additionalProperties: false,
  required: ["reason", "evidence_ids"],
  properties: {
    reason: { type: "string", minLength: 1, maxLength: 2048 },
    evidence_ids: {
      type: "array",
      minItems: 1,
      maxItems: 16,
      items: uuid,
    },
  },
};

document.paths["/v1/plans"] = {
  post: operation({
    operationId: "createSupervisedPlanV1",
    summary: "Create an immutable supervised plan or manual-only runbook",
    bodySchema: "CreatePlanRequest",
    responseSchema: "CreatePlanResponse",
  }),
};
document.paths["/v1/plans/{id}"] = {
  get: operation({
    operationId: "getSupervisedPlanV1",
    summary: "Read a supervised plan projection",
    responseSchema: "ActionPlanView",
    parameters: [pathParameter("id")],
  }),
};
document.paths["/v1/plans/{id}/critic"] = {
  post: operation({
    operationId: "reviewSupervisedPlanWithCriticV1",
    summary: "Run and persist the required heterogeneous Critic review for an R2 plan",
    bodySchema: "CriticReviewRequest",
    responseSchema: "CriticReviewResponse",
    parameters: [pathParameter("id")],
  }),
};
for (const verb of ["approve", "reject"]) {
  document.paths[`/v1/plans/{id}/${verb}`] = {
    post: operation({
      operationId: `${verb}SupervisedPlanV1`,
      summary: `${verb === "approve" ? "Approve" : "Reject"} an exact plan hash`,
      bodySchema: "ApprovalDecisionRequest",
      responseSchema: "ApprovalDecisionResponse",
      parameters: [pathParameter("id")],
    }),
  };
}
document.paths["/v1/executions"] = {
  post: operation({
    operationId: "submitSupervisedExecutionV1",
    summary: "Submit a current human-approved plan to Change Executor",
    bodySchema: "SubmitExecutionRequest",
    responseSchema: "ExecutionSubmissionView",
  }),
};
document.paths["/v1/executions/{id}"] = {
  get: operation({
    operationId: "getSupervisedExecutionV1",
    summary: "Read a supervised execution projection",
    responseSchema: "ExecutionSubmissionView",
    parameters: [pathParameter("id")],
  }),
};
document.paths["/v1/audit/{correlation_id}"] = {
  get: operation({
    operationId: "getSupervisedAuditTimelineV1",
    summary: "Read a bounded append-only supervised audit timeline",
    responseSchema: "AuditPage",
    parameters: [pathParameter("correlation_id")],
  }),
};
document.paths["/v1/resource-quarantines"] = {
  get: operation({
    operationId: "listResourceQuarantinesV1",
    summary: "List scoped active or historical resource quarantines",
    responseSchema: "QuarantinePage",
    parameters: [
      {
        name: "cluster_id",
        in: "query",
        required: true,
        schema: uuid,
      },
      {
        name: "include_cleared",
        in: "query",
        required: false,
        schema: { type: "boolean", default: false },
      },
      {
        name: "limit",
        in: "query",
        required: false,
        schema: { type: "integer", minimum: 1, maximum: 200 },
      },
    ],
  }),
};
document.paths["/v1/resource-quarantines/{id}/clear"] = {
  post: operation({
    operationId: "clearResourceQuarantineV1",
    summary: "Clear quarantine with approver scope and current Evidence",
    bodySchema: "ClearQuarantineRequest",
    responseSchema: "ResourceQuarantine",
    parameters: [pathParameter("id")],
  }),
};

document.info.title = "RocketMQ Rust AI SRE Phase 3 API";
document.info.version = "3.0.0";
document["x-rocketmq-sre-phase"] = 3;
document["x-rocketmq-effective-access"] = "human_approved_supervised";
document["x-rocketmq-cluster-mutation-supported"] = true;
document["x-rocketmq-unattended-mutation-supported"] = false;
document["x-rocketmq-arbitrary-mutation-supported"] = false;
document["x-rocketmq-phase3-contracts"] = Object.keys(contractSchemas);

writeFileSync(output, `${JSON.stringify(document, null, 2)}\n`);
