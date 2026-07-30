// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendPhase3ReleaseManagement } from "./openapi/phase3_release_management.mjs";

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
  ChangeConflict: "change-conflict.schema.json",
  ChangeSchedule: "change-schedule.schema.json",
  ChangeWindow: "change-window.schema.json",
  CriticAssessment: "critic-assessment.schema.json",
  CriticGateState: "critic-gate-state.schema.json",
  CriticReview: "critic-review.schema.json",
  ExecutionRequest: "execution-request.schema.json",
  ExternalApprovalInput: "external-approval-input.schema.json",
  IntegrationDelivery: "integration-delivery.schema.json",
  IntegrationTarget: "integration-target.schema.json",
  ManualRunbookDraft: "manual-runbook-draft.schema.json",
  PolicyDecision: "policy-decision.schema.json",
  ReleaseObservation: "release-observation.schema.json",
  ReleaseReadinessSnapshot: "release-readiness-snapshot.schema.json",
  ReleaseReport: "release-report.schema.json",
  ReleaseWorkflow: "release-workflow.schema.json",
  ResourceQuarantine: "resource-quarantine.schema.json",
  RunbookDefinition: "runbook-definition.schema.json",
  RunbookStepPlanBinding: "runbook-step-plan-binding.schema.json",
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
const stringPathParameter = (name) => ({
  name,
  in: "path",
  required: true,
  schema: { type: "string", minLength: 1, maxLength: 128 },
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
      required: [
        "kind",
        "plan",
        "precondition_hash",
        "risk",
        "policy_decision",
      ],
      properties: {
        kind: { const: "action_plan" },
        plan: { $ref: "#/components/schemas/ActionPlan" },
        precondition_hash: digest,
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
    "precondition_hash",
    "risk",
    "critic_state",
    "latest_critic_review",
    "latest_policy_decision",
    "latest_approval",
  ],
  properties: {
    plan: { $ref: "#/components/schemas/ActionPlan" },
    precondition_hash: digest,
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
schemas.ConfirmDiagnosisExecutionRequest = {
  type: "object",
  additionalProperties: false,
  required: ["human_confirmed", "reason"],
  properties: {
    human_confirmed: { const: true },
    reason: { type: "string", minLength: 8, maxLength: 2048 },
  },
};
schemas.DiagnosisExecutionConfirmation = {
  type: "object",
  additionalProperties: false,
  required: [
    "schema_version",
    "incident_id",
    "source_revision_id",
    "confirmed_revision_id",
    "revision",
    "cluster_id",
    "primary_model_invocation_id",
    "evidence_ids",
    "execution_eligible",
    "confirmed_by",
    "reason",
    "correlation_id",
    "confirmed_at",
  ],
  properties: {
    schema_version: {
      const: "rocketmq-sre.diagnosis-execution-confirmation.v1",
    },
    incident_id: uuid,
    source_revision_id: uuid,
    confirmed_revision_id: uuid,
    revision: { type: "integer", format: "uint32", minimum: 1 },
    cluster_id: uuid,
    primary_model_invocation_id: uuid,
    evidence_ids: {
      type: "array",
      minItems: 1,
      maxItems: 32,
      items: uuid,
    },
    execution_eligible: { const: true },
    confirmed_by: { type: "string", minLength: 1, maxLength: 512 },
    reason: { type: "string", minLength: 8, maxLength: 2048 },
    correlation_id: uuid,
    confirmed_at: { type: "string", format: "date-time" },
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
schemas.CreateRunbookRequest = {
  type: "object",
  additionalProperties: false,
  required: ["cluster_id", "definition"],
  properties: {
    cluster_id: uuid,
    definition: { $ref: "#/components/schemas/RunbookDefinition" },
  },
};
schemas.RunbookPage = {
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "items", "partial"],
  properties: {
    schema_version: { const: "rocketmq-sre.runbook-page.v1" },
    items: {
      type: "array",
      maxItems: 256,
      items: { $ref: "#/components/schemas/RunbookDefinition" },
    },
    partial: { type: "boolean" },
  },
};
schemas.CreateChangeWindowRequest = {
  type: "object",
  additionalProperties: false,
  required: [
    "cluster_id",
    "name",
    "kind",
    "timezone",
    "starts_at",
    "ends_at",
    "max_parallelism",
    "reason",
  ],
  properties: {
    cluster_id: uuid,
    name: { type: "string", minLength: 1, maxLength: 128 },
    kind: { $ref: "#/components/schemas/ChangeWindowKind" },
    timezone: { type: "string", minLength: 1, maxLength: 128 },
    starts_at: { type: "string", format: "date-time" },
    ends_at: { type: "string", format: "date-time" },
    resource_keys: {
      type: "array",
      uniqueItems: true,
      maxItems: 64,
      items: { type: "string", minLength: 1, maxLength: 512 },
    },
    max_parallelism: { type: "integer", minimum: 1, maximum: 16 },
    reason: { type: "string", minLength: 1, maxLength: 2048 },
  },
};
schemas.ChangeWindowPage = {
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "items", "partial"],
  properties: {
    schema_version: { const: "rocketmq-sre.change-window-page.v1" },
    items: {
      type: "array",
      maxItems: 256,
      items: { $ref: "#/components/schemas/ChangeWindow" },
    },
    partial: { type: "boolean" },
  },
};
schemas.CreateChangeScheduleRequest = {
  type: "object",
  additionalProperties: false,
  required: [
    "cluster_id",
    "runbook_id",
    "runbook_version",
    "scheduled_start",
    "scheduled_end",
    "plan_bindings",
  ],
  properties: {
    cluster_id: uuid,
    runbook_id: uuid,
    runbook_version: { type: "string", minLength: 1, maxLength: 64 },
    scheduled_start: { type: "string", format: "date-time" },
    scheduled_end: { type: "string", format: "date-time" },
    plan_bindings: {
      type: "array",
      minItems: 1,
      maxItems: 64,
      items: { $ref: "#/components/schemas/RunbookStepPlanBinding" },
    },
  },
};
schemas.ChangeSchedulePage = {
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "items", "partial"],
  properties: {
    schema_version: { const: "rocketmq-sre.change-schedule-page.v1" },
    items: {
      type: "array",
      maxItems: 256,
      items: { $ref: "#/components/schemas/ChangeSchedule" },
    },
    partial: { type: "boolean" },
  },
};
schemas.ChangeSchedulePreview = {
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "schedule", "conflicts", "schedulable"],
  properties: {
    schema_version: {
      const: "rocketmq-sre.change-schedule-preview.v1",
    },
    schedule: { $ref: "#/components/schemas/ChangeSchedule" },
    conflicts: {
      type: "array",
      maxItems: 256,
      items: { $ref: "#/components/schemas/ChangeConflict" },
    },
    schedulable: { type: "boolean" },
  },
};
schemas.ScheduleTransitionRequest = {
  type: "object",
  additionalProperties: false,
  required: ["reason"],
  properties: {
    reason: { type: "string", minLength: 1, maxLength: 2048 },
  },
};
schemas.ManualGateDecisionRequest = schemas.ScheduleTransitionRequest;

document.paths["/v1/plans"] = {
  post: operation({
    operationId: "createSupervisedPlanV1",
    summary: "Create an immutable supervised plan or manual-only runbook",
    bodySchema: "CreatePlanRequest",
    responseSchema: "CreatePlanResponse",
  }),
};
document.paths[
  "/v1/incidents/{incident_id}/diagnosis-revisions/{revision_id}/confirm-execution"
] = {
  post: operation({
    operationId: "confirmDiagnosisForSupervisedExecutionV1",
    summary:
      "Create an immutable execution-eligible revision from the latest complete model-assisted diagnosis",
    bodySchema: "ConfirmDiagnosisExecutionRequest",
    responseSchema: "DiagnosisExecutionConfirmation",
    parameters: [
      pathParameter("incident_id"),
      pathParameter("revision_id"),
    ],
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
document.paths["/v1/runbooks"] = {
  get: operation({
    operationId: "listRunbooksV1",
    summary: "List immutable runbook versions in one cluster scope",
    responseSchema: "RunbookPage",
    parameters: [
      {
        name: "cluster_id",
        in: "query",
        required: true,
        schema: uuid,
      },
      {
        name: "limit",
        in: "query",
        required: false,
        schema: { type: "integer", minimum: 1, maximum: 256 },
      },
    ],
  }),
  post: operation({
    operationId: "createRunbookV1",
    summary: "Create one immutable typed runbook version",
    bodySchema: "CreateRunbookRequest",
    responseSchema: "RunbookDefinition",
  }),
};
document.paths["/v1/runbooks/{id}/versions/{version}"] = {
  get: operation({
    operationId: "getRunbookVersionV1",
    summary: "Read one immutable runbook version",
    responseSchema: "RunbookDefinition",
    parameters: [
      pathParameter("id"),
      stringPathParameter("version"),
      {
        name: "cluster_id",
        in: "query",
        required: true,
        schema: uuid,
      },
    ],
  }),
};
document.paths["/v1/change-windows"] = {
  get: operation({
    operationId: "listChangeWindowsV1",
    summary: "List maintenance, freeze, and blackout windows",
    responseSchema: "ChangeWindowPage",
    parameters: [
      {
        name: "cluster_id",
        in: "query",
        required: true,
        schema: uuid,
      },
      {
        name: "from",
        in: "query",
        required: true,
        schema: { type: "string", format: "date-time" },
      },
      {
        name: "to",
        in: "query",
        required: true,
        schema: { type: "string", format: "date-time" },
      },
      {
        name: "limit",
        in: "query",
        required: false,
        schema: { type: "integer", minimum: 1, maximum: 256 },
      },
    ],
  }),
  post: operation({
    operationId: "createChangeWindowV1",
    summary: "Create an immutable maintenance, freeze, or blackout window",
    bodySchema: "CreateChangeWindowRequest",
    responseSchema: "ChangeWindow",
  }),
};
document.paths["/v1/change-schedules/preview"] = {
  post: operation({
    operationId: "previewChangeScheduleV1",
    summary: "Validate plan bindings and return every blocking conflict",
    bodySchema: "CreateChangeScheduleRequest",
    responseSchema: "ChangeSchedulePreview",
  }),
};
document.paths["/v1/change-schedules"] = {
  get: operation({
    operationId: "listChangeSchedulesV1",
    summary: "List scoped runbook schedules",
    responseSchema: "ChangeSchedulePage",
    parameters: [
      {
        name: "cluster_id",
        in: "query",
        required: true,
        schema: uuid,
      },
      {
        name: "status",
        in: "query",
        required: false,
        schema: { $ref: "#/components/schemas/ChangeScheduleStatus" },
      },
      {
        name: "limit",
        in: "query",
        required: false,
        schema: { type: "integer", minimum: 1, maximum: 256 },
      },
    ],
  }),
  post: operation({
    operationId: "createChangeScheduleV1",
    summary: "Schedule a runbook whose action steps bind approved plans",
    bodySchema: "CreateChangeScheduleRequest",
    responseSchema: "ChangeSchedule",
  }),
};
document.paths["/v1/change-schedules/{id}"] = {
  get: operation({
    operationId: "getChangeScheduleV1",
    summary: "Read one durable runbook schedule projection",
    responseSchema: "ChangeSchedule",
    parameters: [pathParameter("id")],
  }),
};
for (const verb of ["pause", "resume", "cancel", "reconcile"]) {
  document.paths[`/v1/change-schedules/{id}/${verb}`] = {
    post: operation({
      operationId: `${verb}ChangeScheduleV1`,
      summary: `${verb} a durable runbook schedule`,
      bodySchema: "ScheduleTransitionRequest",
      responseSchema: "ChangeSchedule",
      parameters: [pathParameter("id")],
    }),
  };
}
for (const decision of ["approve", "reject"]) {
  document.paths[
    `/v1/change-schedules/{id}/manual-gates/{step_id}/${decision}`
  ] = {
    post: operation({
      operationId: `${decision}RunbookManualGateV1`,
      summary: `${decision} the active manual runbook gate`,
      bodySchema: "ManualGateDecisionRequest",
      responseSchema: "ChangeSchedule",
      parameters: [pathParameter("id"), pathParameter("step_id")],
    }),
  };
}

extendPhase3ReleaseManagement({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
  digest,
});

document.info.title = "RocketMQ Rust AI SRE Phase 3 API";
document.info.version = "3.0.0";
document["x-rocketmq-sre-phase"] = 3;
document["x-rocketmq-effective-access"] = "human_approved_supervised";
document["x-rocketmq-cluster-mutation-supported"] = true;
document["x-rocketmq-unattended-mutation-supported"] = false;
document["x-rocketmq-arbitrary-mutation-supported"] = false;
document["x-rocketmq-phase3-contracts"] = Object.keys(contractSchemas);

writeFileSync(output, `${JSON.stringify(document, null, 2)}\n`);
