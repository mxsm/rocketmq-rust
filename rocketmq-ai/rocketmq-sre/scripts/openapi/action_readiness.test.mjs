// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import { extendActionReadiness } from "./action_readiness.mjs";

const readSchema = (filename) => JSON.parse(fs.readFileSync(new URL(`../../schemas/${filename}`, import.meta.url), "utf8"));
const fresh = () => ({
  paths: { "/v1/plans": { post: { security: [{ bearerAuth: [] }], operationId: "createPlan" } } },
  components: { schemas: {
    ActionPlan: { type: "object", properties: { immutable: { const: true } } },
    ErrorEnvelope: { type: "object" },
    ExecutionPreconditionEvidenceView: { type: "object", required: ["evidence", "precondition_hash"],
      properties: { evidence: { type: "object" }, precondition_hash: { type: "string" } } },
  } },
  "x-rocketmq-phase3-contracts": ["ActionPlan"],
});

test("readiness adds one readonly route and an optional response projection while preserving immutable contracts", () => {
  const document = fresh();
  const before = structuredClone(document);
  extendActionReadiness({ document, readSchema });
  assert.deepEqual(document.paths["/v1/plans"], before.paths["/v1/plans"]);
  assert.deepEqual(document.components.schemas.ActionPlan, before.components.schemas.ActionPlan);
  const prepared = structuredClone(document.components.schemas.ExecutionPreconditionEvidenceView);
  delete prepared.properties.readiness;
  assert.deepEqual(prepared, before.components.schemas.ExecutionPreconditionEvidenceView);
  assert.equal(document.paths["/v1/incidents/{incident_id}/action-readiness"].post.operationId, "readActionReadiness");
  assert.deepEqual(Object.keys(document.paths["/v1/incidents/{incident_id}/action-readiness"].post.responses), ["200", "400", "401", "403", "404", "409"]);
});

test("Rust preparation scope and origin fields are exported and all local references resolve", () => {
  const document = fresh();
  extendActionReadiness({ document, readSchema });
  const schemas = document.components.schemas;
  assert.equal(schemas.ActionPreparationEvidence.properties.origin_signature.type, "string");
  assert.equal(schemas.ActionPreparationEvidence__ActionPreparationBinding.properties.target_scope_digest.type, "string");
  assert(schemas.ActionReadinessView__ActionReadinessReason.enum.includes("diagnosis_already_confirmed"));
  const visit = (value) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    if (value.$ref) {
      assert(value.$ref.startsWith("#/components/schemas/"));
      const key = value.$ref.slice("#/components/schemas/".length);
      assert(Object.hasOwn(schemas, key), `unresolved ${key}`);
    }
    Object.values(value).forEach(visit);
  };
  visit(document);
  assert(!JSON.stringify(document).includes("#/$defs/"));
});

test("repeated imports remove stale owned definitions and remain byte-stable", () => {
  const document = fresh(); document.components.schemas.ActionReadinessView__Obsolete = { const: "old" };
  extendActionReadiness({ document, readSchema });
  assert(!Object.hasOwn(document.components.schemas, "ActionReadinessView__Obsolete"));
  const once = JSON.stringify(document);
  extendActionReadiness({ document, readSchema });
  assert.equal(JSON.stringify(document), once);
  assert.equal(document["x-rocketmq-phase3-contracts"].filter((name) => name === "ActionReadinessView").length, 1);
});

test("unsupported external schema references fail without importing remote contracts", () => {
  assert.throws(() => extendActionReadiness({ document: fresh(), readSchema: () => ({ $ref: "https://untrusted.invalid/schema" }) }), /non-local reference/);
  assert.throws(() => extendActionReadiness({ document: { components: { schemas: {} } }, readSchema }), /supervised execution contract/);
});
