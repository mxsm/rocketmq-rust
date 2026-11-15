// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import { extendExecutionObservation } from "./execution_observation.mjs";

const readSchema = (filename) => JSON.parse(fs.readFileSync(new URL(`../../schemas/${filename}`, import.meta.url), "utf8"));
const fresh = () => ({ paths: { "/v1/executions": { post: { operationId: "submitExecution" } } },
  components: { schemas: { ExecutionSubmissionView: { const: "immutable legacy wire" }, ErrorEnvelope: { type: "object" } } },
  "x-rocketmq-phase3-contracts": ["ExecutionSubmissionView"],
});

test("safe observation adds three fixed reads without altering accepted receipt or write routes", () => {
  const document = fresh();
  const before = structuredClone(document);
  extendExecutionObservation({ document, readSchema });
  assert.deepEqual(document.paths["/v1/executions"], before.paths["/v1/executions"]);
  assert.deepEqual(document.components.schemas.ExecutionSubmissionView, before.components.schemas.ExecutionSubmissionView);
  assert.equal(document.paths["/v1/executions/lookup"].post.operationId, "lookupExecution");
  assert.equal(document.paths["/v1/plans/{plan_id}/executions"].get.operationId, "getPlanExecutions");
  assert.equal(document.paths["/v1/executions/{execution_id}/observation"].get.operationId, "getExecutionObservation");
  assert.deepEqual(Object.keys(document.paths).filter((path) => !Object.hasOwn(before.paths, path)).length, 3);
});

test("Rust-owned proof facts import with resolvable local references and no signed request", () => {
  const document = fresh();
  extendExecutionObservation({ document, readSchema });
  const schemas = document.components.schemas;
  assert(Object.hasOwn(schemas.ExecutionObservationView.properties, "business_recovery"));
  assert(!Object.hasOwn(schemas.ExecutionObservationView.properties, "execution"));
  const receipt = schemas.ExecutionObservationView__ExecutionReceiptReference;
  for (const secret of ["grant", "signature", "parameters", "idempotency_key"]) assert(!Object.hasOwn(receipt.properties, secret));
  const visit = (value) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    if (value.$ref) {
      assert(value.$ref.startsWith("#/components/schemas/"));
      assert(Object.hasOwn(schemas, value.$ref.slice("#/components/schemas/".length)), value.$ref);
    }
    Object.values(value).forEach(visit);
  };
  visit(document);
  assert(!JSON.stringify(document).includes("#/$defs/"));
});

test("normal regeneration replaces owned definitions only and rejects non-local references", () => {
  const document = fresh(); document.components.schemas.ExecutionObservationView__Obsolete = { const: true };
  extendExecutionObservation({ document, readSchema });
  assert(!Object.hasOwn(document.components.schemas, "ExecutionObservationView__Obsolete"));
  const once = JSON.stringify(document); extendExecutionObservation({ document, readSchema });
  assert.equal(JSON.stringify(document), once);
  assert.throws(() => extendExecutionObservation({ document: fresh(), readSchema: () => ({ $ref: "https://invalid.example/schema" }) }), /non-local reference/);
});
