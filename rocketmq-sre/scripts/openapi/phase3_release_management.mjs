// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

const nullableRef = (name) => ({
  oneOf: [{ $ref: `#/components/schemas/${name}` }, { type: "null" }],
});

const nullableUuid = {
  oneOf: [{ type: "string", format: "uuid" }, { type: "null" }],
};

const boundedText = (maxLength) => ({
  type: "string",
  minLength: 1,
  maxLength,
});

const queryParameter = (name, schema, required = false) => ({
  name,
  in: "query",
  required,
  schema,
});

function addIntegrationSchemas({ schemas, uuid }) {
  schemas.DescriptorStatus = {
    type: "string",
    enum: ["active", "disabled", "deprecated"],
  };
  schemas.Deprecation = {
    type: "object",
    additionalProperties: false,
    required: ["since", "replacement", "message"],
    properties: {
      since: boundedText(64),
      replacement: {
        oneOf: [boundedText(255), { type: "null" }],
      },
      message: boundedText(2048),
    },
  };
  schemas.IntegrationDescriptor = {
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
      "integration_kind",
      "inbound",
      "outbound",
    ],
    properties: {
      id: boundedText(255),
      version: boundedText(64),
      owner: boundedText(255),
      supported_versions: {
        type: "array",
        minItems: 1,
        maxItems: 16,
        items: { $ref: "#/components/schemas/SchemaVersion" },
      },
      required_capabilities: {
        type: "array",
        uniqueItems: true,
        maxItems: 64,
        items: boundedText(255),
      },
      config_schema: { type: "object" },
      status: { $ref: "#/components/schemas/DescriptorStatus" },
      deprecation: nullableRef("Deprecation"),
      integration_kind: boundedText(128),
      inbound: { type: "boolean" },
      outbound: { type: "boolean" },
    },
  };
  schemas.IntegrationDescriptorList = {
    type: "array",
    maxItems: 32,
    items: { $ref: "#/components/schemas/IntegrationDescriptor" },
  };
  schemas.RegisterIntegrationTargetRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "descriptor_id",
      "descriptor_version",
      "name",
      "adapter_kind",
      "endpoint",
    ],
    properties: {
      cluster_id: uuid,
      descriptor_id: boundedText(255),
      descriptor_version: boundedText(64),
      name: boundedText(128),
      adapter_kind: {
        $ref: "#/components/schemas/IntegrationAdapterKind",
      },
      endpoint: boundedText(2048),
      secret_reference: {
        oneOf: [boundedText(512), { type: "null" }],
      },
      notification_target_id: nullableUuid,
      enabled: { type: "boolean", default: true },
      inbound_approval: { type: "boolean", default: false },
      outbound_events: {
        type: "array",
        uniqueItems: true,
        maxItems: 16,
        items: { $ref: "#/components/schemas/IntegrationEventKind" },
        default: [],
      },
    },
  };
  schemas.SetIntegrationTargetStateRequest = {
    type: "object",
    additionalProperties: false,
    required: ["enabled"],
    properties: {
      enabled: { type: "boolean" },
    },
  };
  schemas.IntegrationTargetView = {
    ...schemas.IntegrationTarget,
    required: [
      ...schemas.IntegrationTarget.required,
      "notification_target_id",
    ],
    properties: {
      ...schemas.IntegrationTarget.properties,
      notification_target_id: nullableUuid,
    },
  };
  schemas.IntegrationTargetPage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "partial"],
    properties: {
      schema_version: {
        const: "rocketmq-sre.integration-target-page.v1",
      },
      items: {
        type: "array",
        maxItems: 200,
        items: { $ref: "#/components/schemas/IntegrationTargetView" },
      },
      partial: { type: "boolean" },
    },
  };
  schemas.IntegrationDeliveryPage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "partial"],
    properties: {
      schema_version: {
        const: "rocketmq-sre.integration-delivery-page.v1",
      },
      items: {
        type: "array",
        maxItems: 200,
        items: { $ref: "#/components/schemas/IntegrationDelivery" },
      },
      partial: { type: "boolean" },
    },
  };
  schemas.ExternalApprovalRequest = {
    $ref: "#/components/schemas/ExternalApprovalInput",
  };
  schemas.ExternalApprovalView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "duplicate", "approval", "plan_status"],
    properties: {
      schema_version: {
        const: "rocketmq-sre.external-approval-view.v1",
      },
      duplicate: { type: "boolean" },
      approval: { $ref: "#/components/schemas/ApprovalRecord" },
      plan_status: { $ref: "#/components/schemas/PlanStatus" },
    },
  };
}

function addReleaseSchemas({ schemas, uuid, digest }) {
  schemas.CreateReleaseRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "incident_id",
      "change_id",
      "release_ref",
      "target_version",
      "runbook_id",
      "runbook_version",
      "plan_id",
      "plan_hash",
    ],
    properties: {
      cluster_id: uuid,
      incident_id: uuid,
      change_id: boundedText(256),
      release_ref: boundedText(256),
      target_version: boundedText(128),
      runbook_id: uuid,
      runbook_version: boundedText(64),
      plan_id: uuid,
      plan_hash: digest,
      rollback_plan_id: nullableUuid,
      rollback_plan_hash: {
        oneOf: [digest, { type: "null" }],
      },
    },
  };
  schemas.PrepareReleaseRequest = {
    type: "object",
    additionalProperties: false,
    required: ["pdb_ready", "synthetic_probe_ready"],
    properties: {
      pdb_ready: { type: "boolean" },
      synthetic_probe_ready: { type: "boolean" },
      evidence_ids: {
        type: "array",
        minItems: 1,
        maxItems: 64,
        uniqueItems: true,
        items: uuid,
        default: [],
      },
      affected_resource_keys: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: boundedText(512),
        default: [],
      },
      configuration_changes: {
        type: "array",
        maxItems: 64,
        items: boundedText(512),
        default: [],
      },
    },
  };
  schemas.ReleaseExecutionRequest = {
    type: "object",
    additionalProperties: false,
    required: ["precondition_hash", "idempotency_key"],
    properties: {
      precondition_hash: digest,
      idempotency_key: {
        type: "string",
        minLength: 16,
        maxLength: 256,
        pattern: "^[A-Za-z0-9._:-]+$",
      },
    },
  };
  schemas.RecordReleaseObservationRequest = {
    type: "object",
    additionalProperties: false,
    required: [
      "phase",
      "slo_healthy",
      "synthetic_probe_healthy",
      "sanitized_summary",
    ],
    properties: {
      phase: { $ref: "#/components/schemas/ReleaseObservationPhase" },
      slo_healthy: { type: "boolean" },
      synthetic_probe_healthy: { type: "boolean" },
      evidence_ids: {
        type: "array",
        maxItems: 64,
        uniqueItems: true,
        items: uuid,
        default: [],
      },
      sanitized_summary: boundedText(2048),
    },
  };
  schemas.ReleaseTransitionRequest = {
    type: "object",
    additionalProperties: false,
    required: ["reason"],
    properties: {
      reason: boundedText(2048),
    },
  };
  schemas.CompleteRollbackRequest = {
    type: "object",
    additionalProperties: false,
    required: ["succeeded", "reason", "observation"],
    properties: {
      succeeded: { type: "boolean" },
      reason: boundedText(2048),
      observation: {
        $ref: "#/components/schemas/RecordReleaseObservationRequest",
      },
    },
  };
  schemas.ReleasePage = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "items", "partial"],
    properties: {
      schema_version: { const: "rocketmq-sre.release-page.v1" },
      items: {
        type: "array",
        maxItems: 200,
        items: { $ref: "#/components/schemas/ReleaseWorkflow" },
      },
      partial: { type: "boolean" },
    },
  };
  schemas.ReleaseDetail = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "workflow", "observations", "report"],
    properties: {
      schema_version: { const: "rocketmq-sre.release-detail.v1" },
      workflow: { $ref: "#/components/schemas/ReleaseWorkflow" },
      observations: {
        type: "array",
        maxItems: 512,
        items: { $ref: "#/components/schemas/ReleaseObservation" },
      },
      report: nullableRef("ReleaseReport"),
    },
  };
  schemas.ReleasePreparationView = {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "workflow",
      "upgrade_readiness",
      "simulation",
    ],
    properties: {
      schema_version: {
        const: "rocketmq-sre.release-preparation-view.v1",
      },
      workflow: { $ref: "#/components/schemas/ReleaseWorkflow" },
      upgrade_readiness: {
        $ref: "#/components/schemas/UpgradeReadinessReport",
      },
      simulation: { $ref: "#/components/schemas/WhatIfSimulation" },
    },
  };
  schemas.ReleaseExecutionView = {
    type: "object",
    additionalProperties: false,
    required: ["schema_version", "workflow", "execution_id"],
    properties: {
      schema_version: {
        const: "rocketmq-sre.release-execution-view.v1",
      },
      workflow: { $ref: "#/components/schemas/ReleaseWorkflow" },
      execution_id: uuid,
    },
  };
}

function addIntegrationPaths({
  document,
  operation,
  pathParameter,
  uuid,
}) {
  document.paths["/v1/integrations/descriptors"] = {
    get: operation({
      operationId: "listIntegrationDescriptorsV1",
      summary: "List versioned integration adapter capabilities",
      responseSchema: "IntegrationDescriptorList",
    }),
  };
  document.paths["/v1/integrations/targets"] = {
    get: operation({
      operationId: "listIntegrationTargetsV1",
      summary: "List scoped integration targets",
      responseSchema: "IntegrationTargetPage",
      parameters: [
        queryParameter("cluster_id", uuid, true),
        queryParameter("adapter_kind", {
          $ref: "#/components/schemas/IntegrationAdapterKind",
        }),
        queryParameter("enabled", { type: "boolean" }),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
        }),
      ],
    }),
    post: operation({
      operationId: "registerIntegrationTargetV1",
      summary: "Register a tenant- and cluster-scoped integration target",
      bodySchema: "RegisterIntegrationTargetRequest",
      responseSchema: "IntegrationTargetView",
    }),
  };
  document.paths["/v1/integrations/targets/{id}"] = {
    get: operation({
      operationId: "getIntegrationTargetV1",
      summary: "Read one authorized integration target",
      responseSchema: "IntegrationTargetView",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/integrations/targets/{id}/state"] = {
    post: operation({
      operationId: "setIntegrationTargetStateV1",
      summary: "Enable or disable one integration target",
      bodySchema: "SetIntegrationTargetStateRequest",
      responseSchema: "IntegrationTargetView",
      parameters: [pathParameter("id")],
    }),
  };
  document.paths["/v1/integrations/deliveries"] = {
    get: operation({
      operationId: "listIntegrationDeliveriesV1",
      summary: "List bounded idempotent integration deliveries",
      responseSchema: "IntegrationDeliveryPage",
      parameters: [
        queryParameter("cluster_id", uuid, true),
        queryParameter("target_id", uuid),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
        }),
      ],
    }),
  };
  document.paths["/v1/integrations/approvals/external"] = {
    post: operation({
      operationId: "applyExternalApprovalV1",
      summary: "Validate an external approval through the normal approval service",
      bodySchema: "ExternalApprovalRequest",
      responseSchema: "ExternalApprovalView",
    }),
  };
}

function addReleasePaths({ document, operation, pathParameter, uuid }) {
  document.paths["/v1/releases"] = {
    get: operation({
      operationId: "listReleaseWorkflowsV1",
      summary: "List bounded release escort workflows",
      responseSchema: "ReleasePage",
      parameters: [
        queryParameter("cluster_id", uuid, true),
        queryParameter("status", {
          $ref: "#/components/schemas/ReleaseStatus",
        }),
        queryParameter("limit", {
          type: "integer",
          minimum: 1,
          maximum: 200,
        }),
      ],
    }),
    post: operation({
      operationId: "createReleaseWorkflowV1",
      summary: "Create a release bound to approved typed plans and a runbook",
      bodySchema: "CreateReleaseRequest",
      responseSchema: "ReleaseDetail",
    }),
  };
  document.paths["/v1/releases/{id}"] = {
    get: operation({
      operationId: "getReleaseWorkflowV1",
      summary: "Read one release workflow, observations, and report",
      responseSchema: "ReleaseDetail",
      parameters: [pathParameter("id")],
    }),
  };

  const releasePost = (suffix, operationId, summary, responseSchema, bodySchema) => {
    document.paths[`/v1/releases/{id}/${suffix}`] = {
      post: operation({
        operationId,
        summary,
        responseSchema,
        bodySchema,
        parameters: [pathParameter("id")],
      }),
    };
  };

  releasePost(
    "prepare",
    "prepareReleaseWorkflowV1",
    "Run deterministic readiness and what-if gates",
    "ReleasePreparationView",
    "PrepareReleaseRequest",
  );
  releasePost(
    "start",
    "startReleaseWorkflowV1",
    "Start the approved canary or one-by-one release execution",
    "ReleaseExecutionView",
    "ReleaseExecutionRequest",
  );
  releasePost(
    "observations",
    "recordReleaseObservationV1",
    "Record a bounded SLO and synthetic-probe observation",
    "ReleaseDetail",
    "RecordReleaseObservationRequest",
  );
  releasePost(
    "pause",
    "pauseReleaseWorkflowV1",
    "Pause an active release escort workflow",
    "ReleaseDetail",
    "ReleaseTransitionRequest",
  );
  releasePost(
    "resume",
    "resumeReleaseWorkflowV1",
    "Resume a paused release after regression recovery",
    "ReleaseDetail",
    "ReleaseTransitionRequest",
  );
  releasePost(
    "verification/start",
    "beginReleaseVerificationV1",
    "Move an active release into post-release verification",
    "ReleaseDetail",
  );
  releasePost(
    "complete",
    "completeReleaseWorkflowV1",
    "Complete a verified release and generate its immutable report",
    "ReleaseDetail",
  );
  releasePost(
    "rollback/start",
    "startReleaseRollbackV1",
    "Start an already-approved typed rollback plan",
    "ReleaseExecutionView",
    "ReleaseExecutionRequest",
  );
  releasePost(
    "rollback/complete",
    "completeReleaseRollbackV1",
    "Reconcile rollback outcome and generate the release report",
    "ReleaseDetail",
    "CompleteRollbackRequest",
  );
  releasePost(
    "manual-takeover",
    "enterReleaseManualTakeoverV1",
    "Escalate a release into audited manual takeover",
    "ReleaseDetail",
    "ReleaseTransitionRequest",
  );
}

export function extendPhase3ReleaseManagement({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
  digest,
}) {
  addIntegrationSchemas({ schemas, uuid });
  addReleaseSchemas({ schemas, uuid, digest });
  addIntegrationPaths({ document, operation, pathParameter, uuid });
  addReleasePaths({ document, operation, pathParameter, uuid });

  document["x-rocketmq-release-management-schema"] =
    "rocketmq-sre.release-management.v1";
}
