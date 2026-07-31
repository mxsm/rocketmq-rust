// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendOperationsAnalytics } from "./openapi/operations_analytics.mjs";
import { extendPhase5FleetAndDr } from "./openapi/phase5_fleet_dr.mjs";
import { extendPhase5GovernanceAndFinOps } from "./openapi/phase5_governance_finops.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(
  root,
  "openapi",
  "rocketmq-sre-phase03.openapi.json",
);
const output = join(
  root,
  "openapi",
  "rocketmq-sre-phase05.openapi.json",
);
const document = JSON.parse(readFileSync(source, "utf8"));
const schemas = document.components.schemas;

const uuid = { type: "string", format: "uuid" };
const digest = {
  type: "string",
  pattern: "^sha256:[0-9A-Fa-f]{64}$",
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

extendPhase5FleetAndDr({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
});
extendPhase5GovernanceAndFinOps({
  document,
  schemas,
  operation,
  pathParameter,
  uuid,
  digest,
});
extendOperationsAnalytics({
  document,
  schemas,
  operation,
  uuid,
});

const phase5Prefixes = [
  ["/v1/fleet/", "Fleet"],
  ["/v1/dr/", "Disaster Recovery"],
  ["/v1/governance/", "Governance"],
  ["/v1/finops/", "FinOps"],
];
const mutationScopes = {
  Fleet: ["rocketmq:fleet:manage"],
  "Disaster Recovery": ["rocketmq:dr:manage"],
  Governance: ["rocketmq:governance:manage"],
  FinOps: ["rocketmq:finops:manage"],
};
for (const [path, pathItem] of Object.entries(document.paths)) {
  const tag = phase5Prefixes.find(([prefix]) =>
    path.startsWith(prefix),
  )?.[1];
  if (!tag) {
    continue;
  }
  for (const [method, value] of Object.entries(pathItem)) {
    if (!["get", "post", "patch"].includes(method)) {
      continue;
    }
    value.tags = [tag];
    value.security = [
      {
        oidc:
          method === "get"
            ? ["rocketmq:read"]
            : mutationScopes[tag],
      },
    ];
  }
}

document.info = {
  title: "RocketMQ Rust AI SRE Phase 5 API",
  version: "5.0.0",
  description:
    "Canonical enterprise Fleet operations API. Phase 5 adds tenant- and region-scoped Fleet, DR, governance, and FinOps contracts while preserving typed actions, bounded autonomy, explicit approval, and fail-closed safety boundaries.",
};
document.tags = [
  ...(document.tags ?? []),
  {
    name: "Fleet",
    description:
      "Tenant-, region-, cluster-, quota-, asset-, compliance-, and inspection-scoped Fleet operations.",
  },
  {
    name: "Disaster Recovery",
    description:
      "Readiness, tabletop, and supervised test-cluster recovery exercises. Production cutover is not representable.",
  },
  {
    name: "Governance",
    description:
      "Versioned, signed, separated-duty lifecycle and fail-closed admission for SRE artifacts.",
  },
  {
    name: "FinOps",
    description:
      "Append-only cost ledger, budgets, forecasts, coverage, and showback or explicitly confirmed chargeback.",
  },
];
document["x-rocketmq-sre-phase"] = 5;
document["x-rocketmq-effective-access"] =
  "bounded_autonomy_with_supervised_r2";
document["x-rocketmq-cluster-mutation-supported"] = true;
document["x-rocketmq-bounded-r1-autonomy-supported"] = true;
document["x-rocketmq-r2-supervision-required"] = true;
document["x-rocketmq-r3-agent-reachable"] = false;
document["x-rocketmq-unattended-arbitrary-mutation-supported"] = false;
document["x-rocketmq-production-dr-cutover-supported"] = false;
document["x-rocketmq-cli-boundary"] =
  "read_only_with_typed_plan_drafts";

function visit(value, callback) {
  if (Array.isArray(value)) {
    for (const child of value) {
      visit(child, callback);
    }
    return;
  }
  if (value === null || typeof value !== "object") {
    return;
  }
  callback(value);
  for (const child of Object.values(value)) {
    visit(child, callback);
  }
}

function validateDocument() {
  const operationIds = new Set();
  for (const [path, pathItem] of Object.entries(document.paths)) {
    for (const [method, value] of Object.entries(pathItem)) {
      if (!["get", "post", "patch"].includes(method)) {
        throw new Error(`unsupported method ${method} at ${path}`);
      }
      if (
        typeof value.operationId !== "string" ||
        value.operationId.length === 0
      ) {
        throw new Error(`${method} ${path} is missing operationId`);
      }
      if (operationIds.has(value.operationId)) {
        throw new Error(`duplicate operationId ${value.operationId}`);
      }
      operationIds.add(value.operationId);
      if (!value.responses || Object.keys(value.responses).length === 0) {
        throw new Error(`${method} ${path} has no response contract`);
      }
    }
  }

  visit(document, (value) => {
    if (
      typeof value.$ref === "string" &&
      value.$ref.startsWith("#/components/schemas/")
    ) {
      const name = value.$ref.slice("#/components/schemas/".length);
      if (!Object.hasOwn(schemas, name)) {
        throw new Error(`unresolved schema reference ${value.$ref}`);
      }
    }
  });

  const encoded = JSON.stringify(document);
  for (const forbidden of [
    '"delete":',
    "/apply",
    "/reset",
    "/truncate",
    "arbitrary_patch",
    "production_cutover",
    "raw_shell",
  ]) {
    if (encoded.includes(forbidden)) {
      throw new Error(`forbidden Phase 5 API surface: ${forbidden}`);
    }
  }
}

validateDocument();
writeFileSync(output, `${JSON.stringify(document, null, 2)}\n`);
