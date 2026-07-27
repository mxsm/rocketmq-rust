// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const workspace = resolve(scriptDirectory, "..");
const sourcePath = join(workspace, "openapi", "rocketmq-sre-phase01.openapi.json");
const outputPath = join(workspace, "openapi", "rocketmq-sre-phase02.openapi.json");
const schemasDirectory = join(workspace, "schemas");

const schemaFiles = new Map([
  ["AlertEvent", "alert-event.schema.json"],
  ["TopologySnapshot", "topology-snapshot.schema.json"],
  ["ClusterHealthReport", "cluster-health-report.schema.json"],
  ["FleetHealthReport", "fleet-health-report.schema.json"],
  ["CapacityForecast", "capacity-forecast.schema.json"],
  ["ClusterForecastReport", "cluster-forecast-report.schema.json"],
  ["BacklogEta", "backlog-eta.schema.json"],
  ["WhatIfSimulationRequest", "what-if-simulation-request.schema.json"],
  ["WhatIfSimulation", "what-if-simulation.schema.json"],
  ["UpgradeReadinessReport", "upgrade-readiness-report.schema.json"],
  ["DrReadinessReport", "dr-readiness-report.schema.json"],
  ["NotificationDelivery", "notification-delivery.schema.json"],
  ["PostmortemDraft", "postmortem-draft.schema.json"],
  ["PostmortemRevision", "postmortem-revision.schema.json"],
  ["ActionItem", "action-item.schema.json"],
  ["Phase2ContractManifest", "phase2-contract-manifest.schema.json"],
]);

function rewriteReferences(value, componentName) {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteReferences(item, componentName));
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  const rewritten = {};
  for (const [key, item] of Object.entries(value)) {
    if (key === "$schema" || key === "$defs") {
      continue;
    }
    if (
      key === "$ref" &&
      typeof item === "string" &&
      item.startsWith("#/$defs/")
    ) {
      rewritten[key] =
        `#/components/schemas/${componentName}__${item.slice("#/$defs/".length)}`;
    } else {
      rewritten[key] = rewriteReferences(item, componentName);
    }
  }
  return rewritten;
}

async function embedSchema(document, componentName, fileName) {
  const schema = JSON.parse(
    await readFile(join(schemasDirectory, fileName), "utf8"),
  );
  document.components.schemas[componentName] = rewriteReferences(
    schema,
    componentName,
  );
  for (const [definitionName, definition] of Object.entries(
    schema.$defs ?? {},
  )) {
    document.components.schemas[`${componentName}__${definitionName}`] =
      rewriteReferences(definition, componentName);
  }
}

const document = JSON.parse(await readFile(sourcePath, "utf8"));
document.info.version = "2.0.0";
document.info.description =
  "Canonical checked-in Phase 02 Control Plane and UI contract. Phase 02 adds typed alert, topology, forecast, simulation, readiness, notification and postmortem schemas while preserving the read-only RocketMQ boundary.";
document["x-rocketmq-sre-phase"] = 2;
document["x-rocketmq-phase2-contracts"] = [...schemaFiles.keys()];

for (const [componentName, fileName] of schemaFiles) {
  await embedSchema(document, componentName, fileName);
}
await embedSchema(
  document,
  "EvidenceSnapshot",
  "evidence-snapshot.schema.json",
);

const boundedLabels = {
  type: "object",
  maxProperties: 64,
  additionalProperties: {
    type: "string",
    maxLength: 512,
  },
};
const uuid = {
  type: "string",
  format: "uuid",
};
const dateTime = {
  type: "string",
  format: "date-time",
};

Object.assign(document.components.schemas, {
  AlertmanagerAlertRequest: {
    type: "object",
    additionalProperties: false,
    required: ["status", "startsAt"],
    properties: {
      status: {
        type: "string",
        enum: ["firing", "resolved"],
      },
      labels: boundedLabels,
      annotations: boundedLabels,
      startsAt: dateTime,
      endsAt: {
        oneOf: [dateTime, { type: "null" }],
      },
      fingerprint: {
        type: "string",
        maxLength: 512,
      },
    },
  },
  AlertmanagerWebhookRequest: {
    type: "object",
    additionalProperties: false,
    required: ["version", "clusterId", "status", "alerts"],
    properties: {
      version: {
        type: "string",
        const: "4",
      },
      clusterId: uuid,
      status: {
        type: "string",
        enum: ["firing", "resolved"],
      },
      receiver: {
        type: "string",
        maxLength: 256,
      },
      groupKey: {
        type: "string",
        maxLength: 512,
      },
      commonLabels: boundedLabels,
      alerts: {
        type: "array",
        minItems: 1,
        maxItems: 128,
        items: {
          $ref: "#/components/schemas/AlertmanagerAlertRequest",
        },
      },
    },
  },
  IntegrationAlertSource: {
    type: "string",
    enum: [
      "kubernetes_event",
      "health_probe",
      "operator_query",
      "inspection",
      "deployment",
      "synthetic_probe",
    ],
  },
  IntegrationEventRequest: {
    type: "object",
    additionalProperties: false,
    required: [
      "cluster_id",
      "source",
      "source_event_id",
      "resource_kind",
      "resource_key",
      "symptom_family",
      "severity",
      "status",
      "summary",
      "sequence",
      "occurred_at",
    ],
    properties: {
      cluster_id: uuid,
      source: {
        $ref: "#/components/schemas/IntegrationAlertSource",
      },
      source_event_id: {
        type: "string",
        minLength: 1,
        maxLength: 512,
      },
      resource_kind: {
        $ref: "#/components/schemas/AlertEvent__ResourceKind",
      },
      resource_key: {
        type: "string",
        minLength: 1,
        maxLength: 512,
      },
      display_name: {
        type: "string",
        minLength: 1,
        maxLength: 512,
      },
      symptom_family: {
        type: "string",
        minLength: 1,
        maxLength: 128,
      },
      severity: {
        $ref: "#/components/schemas/AlertEvent__AlertSeverity",
      },
      status: {
        $ref: "#/components/schemas/AlertEvent__AlertStatus",
      },
      summary: {
        type: "string",
        minLength: 1,
        maxLength: 2048,
      },
      labels: boundedLabels,
      evidence_ids: {
        type: "array",
        maxItems: 64,
        items: uuid,
      },
      sequence: {
        type: "integer",
        format: "uint64",
        minimum: 0,
      },
      occurred_at: dateTime,
    },
  },
  AlertIngestionOutcome: {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "incident_id",
      "alert_ids",
      "created",
      "recurrence",
      "occurrence_count",
      "owner",
      "severity",
      "partial",
      "warnings",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.alert-ingestion.v1",
      },
      incident_id: uuid,
      alert_ids: {
        type: "array",
        maxItems: 128,
        items: uuid,
      },
      created: {
        type: "boolean",
      },
      recurrence: {
        type: "boolean",
      },
      occurrence_count: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
      owner: {
        type: "string",
        minLength: 1,
        maxLength: 128,
      },
      severity: {
        $ref: "#/components/schemas/AlertEvent__AlertSeverity",
      },
      partial: {
        type: "boolean",
      },
      warnings: {
        type: "array",
        items: {
          type: "string",
        },
      },
    },
  },
  IncidentTimelineActor: {
    type: "object",
    additionalProperties: false,
    required: ["subject"],
    properties: {
      subject: {
        type: "string",
      },
      display_name: {
        type: "string",
      },
    },
  },
  IncidentTimelineEvent: {
    type: "object",
    additionalProperties: false,
    required: [
      "id",
      "tenant_id",
      "cluster_id",
      "event_type",
      "summary",
      "details",
      "correlation_id",
      "actor",
      "occurred_at",
    ],
    properties: {
      id: uuid,
      tenant_id: uuid,
      cluster_id: uuid,
      investigation_id: uuid,
      incident_id: uuid,
      event_type: {
        type: "string",
      },
      summary: {
        type: "string",
        maxLength: 2048,
      },
      details: {
        type: "object",
        additionalProperties: true,
      },
      correlation_id: uuid,
      actor: {
        $ref: "#/components/schemas/IncidentTimelineActor",
      },
      occurred_at: dateTime,
    },
  },
  IncidentNoteRequest: {
    type: "object",
    additionalProperties: false,
    required: ["note"],
    properties: {
      note: {
        type: "string",
        minLength: 1,
        maxLength: 2048,
      },
    },
  },
  IncidentTopologyNode: {
    type: "object",
    additionalProperties: false,
    required: ["key", "kind", "display_name", "alert_count"],
    properties: {
      key: {
        type: "string",
      },
      kind: {
        type: "string",
      },
      display_name: {
        type: "string",
      },
      alert_count: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
    },
  },
  IncidentTopologyEdge: {
    type: "object",
    additionalProperties: false,
    required: ["from", "to", "relation"],
    properties: {
      from: {
        type: "string",
      },
      to: {
        type: "string",
      },
      relation: {
        type: "string",
      },
    },
  },
  IncidentTopologyView: {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "incident_id",
      "nodes",
      "edges",
      "partial",
      "warnings",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.incident-topology.v1",
      },
      incident_id: uuid,
      nodes: {
        type: "array",
        maxItems: 128,
        items: {
          $ref: "#/components/schemas/IncidentTopologyNode",
        },
      },
      edges: {
        type: "array",
        maxItems: 256,
        items: {
          $ref: "#/components/schemas/IncidentTopologyEdge",
        },
      },
      partial: {
        type: "boolean",
      },
      warnings: {
        type: "array",
        items: {
          type: "string",
        },
      },
    },
  },
  ClusterIncidentHealth: {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "cluster_id",
      "status",
      "active_incidents",
      "critical_incidents",
      "unassigned_incidents",
      "observed_at",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.cluster-incident-health.v1",
      },
      cluster_id: uuid,
      status: {
        type: "string",
        enum: ["healthy", "degraded", "critical"],
      },
      active_incidents: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
      critical_incidents: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
      unassigned_incidents: {
        type: "integer",
        format: "uint32",
        minimum: 0,
      },
      last_alert_at: {
        oneOf: [dateTime, { type: "null" }],
      },
      observed_at: dateTime,
    },
  },
  NotificationTestRequest: {
    type: "object",
    additionalProperties: false,
    required: ["cluster_id", "incident_id", "target_id"],
    properties: {
      cluster_id: uuid,
      incident_id: uuid,
      target_id: uuid,
    },
  },
  NotificationTestResponse: {
    type: "object",
    additionalProperties: false,
    required: [
      "schema_version",
      "delivery_id",
      "queued",
      "sanitized_summary",
      "deep_link",
    ],
    properties: {
      schema_version: {
        type: "string",
        const: "rocketmq-sre.notification-test.v1",
      },
      delivery_id: uuid,
      queued: {
        type: "boolean",
      },
      sanitized_summary: {
        type: "string",
        maxLength: 2048,
      },
      deep_link: {
        type: "string",
        format: "uri",
      },
    },
  },
});

document.paths["/v1/capabilities/phase2-contract"] = {
  get: {
    operationId: "getPhase2ContractManifest",
    responses: {
      200: {
        description: "Phase 2 read-only contract manifest",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/Phase2ContractManifest",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/incidents/{id}/timeline"] = {
  get: {
    operationId: "getIncidentTimeline",
    parameters: [
      {
        $ref: "#/components/parameters/Id",
      },
    ],
    responses: {
      200: {
        description: "Append-only incident timeline",
        content: {
          "application/json": {
            schema: {
              type: "array",
              items: {
                $ref: "#/components/schemas/IncidentTimelineEvent",
              },
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/incidents/{id}/notes"] = {
  post: {
    operationId: "addIncidentNote",
    parameters: [
      {
        $ref: "#/components/parameters/Id",
      },
    ],
    requestBody: {
      required: true,
      content: {
        "application/json": {
          schema: {
            $ref: "#/components/schemas/IncidentNoteRequest",
          },
        },
      },
    },
    responses: {
      200: {
        description: "Durable operator timeline note",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/IncidentTimelineEvent",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/incidents/{id}/topology"] = {
  get: {
    operationId: "getIncidentTopology",
    parameters: [
      {
        $ref: "#/components/parameters/Id",
      },
    ],
    responses: {
      200: {
        description: "Bounded topology projection for correlated resources",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/IncidentTopologyView",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/integrations/alertmanager/events"] = {
  post: {
    operationId: "ingestAlertmanagerEvents",
    "x-max-body-bytes": 262144,
    requestBody: {
      required: true,
      content: {
        "application/json": {
          schema: {
            $ref: "#/components/schemas/AlertmanagerWebhookRequest",
          },
        },
      },
    },
    responses: {
      200: {
        description: "Idempotent alert ingestion outcomes",
        content: {
          "application/json": {
            schema: {
              type: "array",
              maxItems: 128,
              items: {
                $ref: "#/components/schemas/AlertIngestionOutcome",
              },
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/integrations/events"] = {
  post: {
    operationId: "ingestIntegrationEvent",
    "x-max-body-bytes": 65536,
    requestBody: {
      required: true,
      content: {
        "application/json": {
          schema: {
            $ref: "#/components/schemas/IntegrationEventRequest",
          },
        },
      },
    },
    responses: {
      200: {
        description: "Idempotent canonical event ingestion outcome",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/AlertIngestionOutcome",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/integrations/webhook/test"] = {
  post: {
    operationId: "testNotificationWebhook",
    "x-max-body-bytes": 16384,
    requestBody: {
      required: true,
      content: {
        "application/json": {
          schema: {
            $ref: "#/components/schemas/NotificationTestRequest",
          },
        },
      },
    },
    responses: {
      200: {
        description: "Queued notification target test",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/NotificationTestResponse",
            },
          },
        },
      },
    },
  },
};

const healthOperation = (operationId, description) => ({
  get: {
    operationId,
    parameters: [
      {
        $ref: "#/components/parameters/Id",
      },
    ],
    responses: {
      200: {
        description,
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/ClusterHealthReport",
            },
          },
        },
      },
    },
  },
});

document.paths["/v1/clusters/{id}/slo"] = healthOperation(
  "getClusterSlo",
  "Explainable multi-window SLO and eight-dimension cluster score",
);

document.paths["/v1/clusters/{id}/health"] = healthOperation(
  "getClusterHealth",
  "Deterministic cluster health including SLO, evidence, changes and incidents",
);

document.paths["/v1/fleet/health"] = {
  get: {
    operationId: "getFleetHealth",
    parameters: [
      {
        name: "region",
        in: "query",
        required: false,
        schema: {
          type: "string",
          maxLength: 128,
        },
      },
    ],
    responses: {
      200: {
        description:
          "Tenant and optional region health aggregated by worst cluster without averaging",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/FleetHealthReport",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/clusters/{id}/forecasts"] = {
  get: {
    operationId: "getClusterForecasts",
    parameters: [{ $ref: "#/components/parameters/Id" }],
    responses: {
      200: {
        description:
          "Explainable 7d/30d capacity, backlog ETA, seasonal anomaly, change-point and accuracy report",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/ClusterForecastReport",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/simulations"] = {
  post: {
    operationId: "runWhatIfSimulation",
    "x-max-body-bytes": 32768,
    requestBody: {
      required: true,
      content: {
        "application/json": {
          schema: {
            $ref: "#/components/schemas/WhatIfSimulationRequest",
          },
        },
      },
    },
    responses: {
      200: {
        description:
          "Deterministic advisory-only what-if result with no execution eligibility",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/WhatIfSimulation",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/clusters/{id}/readiness/upgrade"] = {
  get: {
    operationId: "getUpgradeReadiness",
    parameters: [
      { $ref: "#/components/parameters/Id" },
      {
        name: "target_version",
        in: "query",
        required: true,
        schema: { type: "string", minLength: 1, maxLength: 128 },
      },
    ],
    responses: {
      200: {
        description: "Evidence-backed advisory-only upgrade readiness report",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/UpgradeReadinessReport",
            },
          },
        },
      },
    },
  },
};

document.paths["/v1/clusters/{id}/readiness/dr"] = {
  get: {
    operationId: "getDrReadiness",
    parameters: [
      { $ref: "#/components/parameters/Id" },
      {
        name: "target_region",
        in: "query",
        required: false,
        schema: { type: "string", minLength: 1, maxLength: 128 },
      },
      {
        name: "requested_rto_seconds",
        in: "query",
        required: false,
        schema: { type: "integer", format: "uint64", minimum: 0, maximum: 2592000, default: 3600 },
      },
      {
        name: "requested_rpo_seconds",
        in: "query",
        required: false,
        schema: { type: "integer", format: "uint64", minimum: 0, maximum: 2592000, default: 300 },
      },
    ],
    responses: {
      200: {
        description: "Evidence-backed advisory-only disaster-recovery readiness report",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/DrReadinessReport",
            },
          },
        },
      },
    },
  },
};

await writeFile(outputPath, `${JSON.stringify(document, null, 2)}\n`, "utf8");
console.log(
  `PHASE2_OPENAPI_EXPORTED paths=${Object.keys(document.paths).length} schemas=${Object.keys(document.components.schemas).length}`,
);
