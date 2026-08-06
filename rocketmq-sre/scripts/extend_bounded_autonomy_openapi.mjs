// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendBoundedAutonomySettings } from "./openapi/bounded_autonomy_settings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const path = join(root, "openapi", "rocketmq-sre-phase05.openapi.json");
const document = JSON.parse(readFileSync(path, "utf8"));
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
const jsonResponse = (schema) => ({
  description: "Successful response",
  content: {
    "application/json": {
      schema: { $ref: "#/components/schemas/" + schema },
    },
  },
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
  ...(bodySchema
    ? {
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                $ref: "#/components/schemas/" + bodySchema,
              },
            },
          },
        },
      }
    : {}),
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

extendBoundedAutonomySettings({
  document,
  schemas,
  operation,
  uuid,
  digest,
});

if (
  !(document.tags ?? []).some(
    (tag) => tag.name === "Bounded Autonomy",
  )
) {
  document.tags = [
    ...(document.tags ?? []),
    {
      name: "Bounded Autonomy",
      description:
        "Operator-only action and cluster lifecycle controls, qualification, freezes, and kill switches. Autonomous promotion requires an owner-confirmed approval reference.",
    },
  ];
}

const visit = (value, callback) => {
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
};

visit(document, (value) => {
  if (
    typeof value.$ref === "string" &&
    value.$ref.startsWith("#/components/schemas/")
  ) {
    const name = value.$ref.slice("#/components/schemas/".length);
    if (!Object.hasOwn(schemas, name)) {
      throw new Error("unresolved schema reference " + value.$ref);
    }
  }
});

writeFileSync(path, JSON.stringify(document, null, 2) + "\n");
