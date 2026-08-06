// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendConversationMetrics } from "./openapi/conversation_metrics.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const path = join(root, "openapi", "rocketmq-sre-phase05.openapi.json");
const raw = readFileSync(path, "utf8");
const eol = raw.includes("\r\n") ? "\r\n" : "\n";

if (raw.includes('"/v1/conversations/{id}/turns"')) {
  process.exit(0);
}

const uuid = { type: "string", format: "uuid" };
const digest = { type: "string", pattern: "^sha256:[0-9A-Fa-f]{64}$" };
const errorResponse = {
  description: "Sanitized stable error envelope",
  content: {
    "application/json": {
      schema: { $ref: "#/components/schemas/ErrorEnvelope" },
    },
  },
};
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
              schema: { $ref: `#/components/schemas/${bodySchema}` },
            },
          },
        },
      }
    : {}),
  responses: {
    200: {
      description: "Successful response",
      content: {
        "application/json": {
          schema: { $ref: `#/components/schemas/${responseSchema}` },
        },
      },
    },
    400: errorResponse,
    401: errorResponse,
    403: errorResponse,
    404: errorResponse,
    409: errorResponse,
    503: errorResponse,
  },
});
const pathParameter = (name) => ({
  name,
  in: "path",
  required: true,
  schema: uuid,
});

const extension = { paths: {}, components: { schemas: {} }, tags: [] };
extendConversationMetrics({
  document: extension,
  schemas: extension.components.schemas,
  operation,
  pathParameter,
  uuid,
  digest,
});

function findContainer(source, property, depth) {
  let nesting = 0;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (character === '"') {
      const start = index;
      index += 1;
      let value = "";
      for (; index < source.length; index += 1) {
        if (source[index] === "\\") {
          value += source[index];
          index += 1;
          value += source[index] ?? "";
          continue;
        }
        if (source[index] === '"') {
          break;
        }
        value += source[index];
      }
      if (value !== property || nesting !== depth) {
        continue;
      }
      let cursor = index + 1;
      while (/\s/u.test(source[cursor] ?? "")) cursor += 1;
      if (source[cursor] !== ":") continue;
      cursor += 1;
      while (/\s/u.test(source[cursor] ?? "")) cursor += 1;
      const opening = source[cursor];
      if (opening !== "{" && opening !== "[") continue;
      const closing = opening === "{" ? "}" : "]";
      let level = 1;
      let inString = false;
      for (let end = cursor + 1; end < source.length; end += 1) {
        if (source[end] === "\\" && inString) {
          end += 1;
          continue;
        }
        if (source[end] === '"') {
          inString = !inString;
          continue;
        }
        if (inString) continue;
        if (source[end] === opening) level += 1;
        if (source[end] === closing) level -= 1;
        if (level === 0) return { propertyStart: start, open: cursor, close: end };
      }
      throw new Error(`unterminated ${property} container`);
    }
    if (character === "{" || character === "[") nesting += 1;
    if (character === "}" || character === "]") nesting -= 1;
  }
  throw new Error(`missing ${property} container`);
}

function objectEntries(value, indent) {
  return JSON.stringify(value, null, 2)
    .split("\n")
    .slice(1, -1)
    .map((line) => `${" ".repeat(indent)}${line}`)
    .join(eol);
}

function arrayEntry(value, indent) {
  return JSON.stringify(value, null, 2)
    .split("\n")
    .map((line) => `${" ".repeat(indent)}${line}`)
    .join(eol);
}

function insertion(container, fragment) {
  const lineStart = raw.lastIndexOf("\n", container.close) + 1;
  return {
    index: lineStart - eol.length,
    remove: eol.length,
    value: `,${eol}${fragment}${eol}`,
  };
}

const edits = [
  insertion(findContainer(raw, "paths", 1), objectEntries(extension.paths, 2)),
  insertion(
    findContainer(raw, "schemas", 2),
    objectEntries(extension.components.schemas, 4),
  ),
  insertion(
    findContainer(raw, "tags", 1),
    arrayEntry(extension.tags[0], 4),
  ),
].sort((left, right) => right.index - left.index);

let output = raw;
for (const edit of edits) {
  output = `${output.slice(0, edit.index)}${edit.value}${output.slice(edit.index + edit.remove)}`;
}

JSON.parse(output);
writeFileSync(path, output);
