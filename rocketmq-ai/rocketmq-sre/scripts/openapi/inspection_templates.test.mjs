// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { extendInspectionTemplates } from "./inspection_templates.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
const schema = JSON.parse(readFileSync(join(root, "schemas", "inspection-template.schema.json"), "utf8"));
const fixture = () => ({
  paths: { "/existing": { get: { operationId: "existingRead", security: [{ oidc: ["rocketmq:read"] }] } } },
  components: { schemas: {
    CreateInspectionRequest: { type: "object", properties: { template: { type: "string", enum: ["broker"] } } },
    InspectionRun: { type: "object", properties: { template: { type: "string", enum: ["broker"] } } },
    ScheduledInspectionEventEntryRequest: { type: "object", properties: { template: { type: "string", enum: ["broker"] } } },
    Existing: { type: "string", description: "Existing contract must remain unchanged" },
  } },
  "x-rocketmq-phase2-contracts": ["Existing"],
});

test("inspection templates use the Rust enum and preserve existing transport and non-template fields", () => {
  const document = fixture();
  const expected = structuredClone(document);
  extendInspectionTemplates({ document, schema });
  assert.deepEqual(document.components.schemas.InspectionTemplate.enum, schema.enum);
  assert.ok(schema.enum.includes("store_pressure_legacy_v1"));
  for (const name of ["CreateInspectionRequest", "InspectionRun", "ScheduledInspectionEventEntryRequest"]) {
    expected.components.schemas[name].properties.template = { $ref: "#/components/schemas/InspectionTemplate" };
  }
  expected.components.schemas.InspectionTemplate = { ...schema };
  delete expected.components.schemas.InspectionTemplate.$schema;
  expected["x-rocketmq-phase2-contracts"].push("InspectionTemplate");
  assert.deepEqual(document, expected);
  extendInspectionTemplates({ document, schema });
  assert.deepEqual(document, expected);
});

test("standalone template generation updates each maintained document once and keeps its line endings", () => {
  const directory = mkdtempSync(join(tmpdir(), "sre-inspection-template-"));
  try {
    for (const path of ["scripts/openapi", "schemas", "openapi"]) mkdirSync(join(directory, path), { recursive: true });
    for (const path of ["scripts/extend_inspection_templates_openapi.mjs", "scripts/openapi/inspection_templates.mjs", "schemas/inspection-template.schema.json"]) {
      copyFileSync(join(root, path), join(directory, path));
    }
    const paths = ["02", "03", "05"].map(phase => join(directory, "openapi", `rocketmq-sre-phase${phase}.openapi.json`));
    for (const path of paths) writeFileSync(path, JSON.stringify(fixture(), null, 2).replace(/\n/g, "\r\n") + "\r\n");
    const entry = join(directory, "scripts/extend_inspection_templates_openapi.mjs");
    execFileSync(process.execPath, [entry]);
    const first = paths.map(path => readFileSync(path, "utf8"));
    for (const raw of first) {
      assert.ok(raw.includes("\r\n"));
      const document = JSON.parse(raw);
      assert.deepEqual(document.paths, fixture().paths);
      assert.deepEqual(document.components.schemas.InspectionTemplate.enum, schema.enum);
    }
    execFileSync(process.execPath, [entry]);
    assert.deepEqual(paths.map(path => readFileSync(path, "utf8")), first);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
