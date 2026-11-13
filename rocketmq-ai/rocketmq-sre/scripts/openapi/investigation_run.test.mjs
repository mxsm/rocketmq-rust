// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  extendInvestigationRunContracts,
  extendInvestigationRunTransport,
  INVESTIGATION_RUN_SCHEMA_FILES,
} from "./investigation_run.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
const readSchema = (filename) =>
  JSON.parse(readFileSync(join(root, "schemas", filename), "utf8"));
const existing = () => ({
  paths: { "/existing": { get: { operationId: "existingRead" } } },
  components: { schemas: { Existing: { type: "string" }, ErrorEnvelope: { type: "object" } } },
});

function visit(value, callback) {
  if (Array.isArray(value)) value.forEach((item) => visit(item, callback));
  else if (value && typeof value === "object") {
    callback(value);
    Object.values(value).forEach((item) => visit(item, callback));
  }
}

function referenced(document, pointer) {
  assert.ok(pointer.startsWith("#/components/schemas/"), pointer);
  return pointer.slice(2)
    .split("/")
    .map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"))
    .reduce((value, key) => {
      assert.ok(value && Object.hasOwn(value, key), `unresolved ${pointer}`);
      return value[key];
    }, document);
}

test("Rust-exported run contracts have closed component references and preserve existing paths", () => {
  const document = existing();
  const paths = structuredClone(document.paths);
  extendInvestigationRunContracts({ schemas: document.components.schemas, readSchema });
  for (const name of Object.keys(INVESTIGATION_RUN_SCHEMA_FILES)) {
    assert.ok(Object.hasOwn(document.components.schemas, name), name);
  }
  visit(document.components.schemas, (value) => {
    assert.equal(value.$schema, undefined);
    assert.equal(value.$defs, undefined);
    if (value.$ref) assert.notEqual(referenced(document, value.$ref), undefined);
  });
  assert.deepEqual(document.paths, paths);
  assert.deepEqual(document.components.schemas.Existing, { type: "string" });
  const first = structuredClone(document);
  extendInvestigationRunContracts({ schemas: document.components.schemas, readSchema });
  assert.deepEqual(document, first);
});

test("committed investigation components match the Rust-exported schemas", () => {
  const published = JSON.parse(
    readFileSync(join(root, "openapi/rocketmq-sre-phase05.openapi.json"), "utf8"),
  );
  const regenerated = structuredClone(published);
  extendInvestigationRunContracts({ schemas: regenerated.components.schemas, readSchema });
  extendInvestigationRunTransport({ document: regenerated });
  assert.deepEqual(
    regenerated.components.schemas,
    published.components.schemas,
    "regenerate the investigation components from Rust schemas",
  );
  assert.deepEqual(regenerated.paths, published.paths);
});

test("run concurrency counters preserve decimal-string precision in components", () => {
  const document = existing();
  extendInvestigationRunContracts({ schemas: document.components.schemas, readSchema });
  for (const [rootName, field] of [
    ["InvestigationRun", "revision"],
    ["InvestigationRun", "control_epoch"],
    ["InvestigationCommandRequest", "expected_revision"],
  ]) {
    const property = document.components.schemas[rootName].properties[field];
    const schema = property.$ref ? referenced(document, property.$ref) : property;
    assert.equal(
      schema.type,
      "string",
      `${rootName}.${field} must not lose precision as a JS number`,
    );
  }
});

test("embedding handles root and escaped definition pointers and removes obsolete DTOs", () => {
  const document = existing();
  document.components.schemas.InvestigationRun__Obsolete = { type: "number" };
  const source = {
    $schema: "https://json-schema.org/draft/2020-12/schema",
    type: "object",
    properties: { value: { $ref: "#/$defs/A~1B" }, recursive: { $ref: "#" } },
    $defs: { "A/B": { type: "string" } },
  };
  extendInvestigationRunContracts({ schemas: document.components.schemas, readSchema: () => source });
  assert.equal(document.components.schemas.InvestigationRun__Obsolete, undefined);
  assert.equal(
    referenced(document, document.components.schemas.InvestigationRun.properties.value.$ref).type,
    "string",
  );
  assert.equal(
    referenced(document, document.components.schemas.InvestigationRun.properties.recursive.$ref),
    document.components.schemas.InvestigationRun,
  );
});

test("the standalone extender preserves existing conversation contracts, line endings and paths", () => {
  const temporary = mkdtempSync(join(tmpdir(), "sre-run-openapi-"));
  try {
    for (const directory of ["scripts/openapi", "openapi", "schemas"]) {
      mkdirSync(join(temporary, directory), { recursive: true });
    }
    copyFileSync(
      join(root, "scripts/extend_investigation_run_openapi.mjs"),
      join(temporary, "scripts/extend_investigation_run_openapi.mjs"),
    );
    copyFileSync(
      join(root, "scripts/openapi/investigation_run.mjs"),
      join(temporary, "scripts/openapi/investigation_run.mjs"),
    );
    for (const filename of Object.values(INVESTIGATION_RUN_SCHEMA_FILES)) {
      copyFileSync(join(root, "schemas", filename), join(temporary, "schemas", filename));
    }
    const document = existing();
    document.components.schemas.ConversationStreamEvent = {
      type: "object",
      description: "existing conversation contract",
    };
    const output = join(temporary, "openapi/rocketmq-sre-phase05.openapi.json");
    writeFileSync(output, `${JSON.stringify(document, null, 2)}\n`.replace(/\n/g, "\r\n"));
    const command = join(temporary, "scripts/extend_investigation_run_openapi.mjs");
    execFileSync(process.execPath, [command]);
    const encoded = readFileSync(output, "utf8");
    const extended = JSON.parse(encoded);
    assert.deepEqual(extended.paths["/existing"], document.paths["/existing"]);
    assert.ok(extended.paths["/v1/investigation-runs/{id}/events"].get);
    assert.deepEqual(
      extended.components.schemas.ConversationStreamEvent,
      document.components.schemas.ConversationStreamEvent,
    );
    assert.ok(extended.components.schemas.InvestigationCommandRequest);
    assert.equal(encoded.replace(/\r\n/g, "").includes("\n"), false);
    execFileSync(process.execPath, [command]);
    assert.equal(readFileSync(output, "utf8"), encoded);
    unlinkSync(join(temporary, "schemas/hypothesis-assessment.schema.json"));
    assert.throws(() => execFileSync(process.execPath, [command], { stdio: "pipe" }));
    assert.equal(
      readFileSync(output, "utf8"),
      encoded,
      "a failed export cannot leave a partial document",
    );
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
});

test("registered run transport references exported DTOs without replacing legacy operations", () => {
  const document = existing();
  const original = structuredClone(document.paths["/existing"]);
  extendInvestigationRunContracts({ schemas: document.components.schemas, readSchema });
  extendInvestigationRunTransport({ document });
  visit(document, (value) => {
    if (value.$ref) assert.notEqual(referenced(document, value.$ref), undefined);
  });
  assert.deepEqual(document.paths["/existing"], original);
  const first = structuredClone(document);
  extendInvestigationRunTransport({ document });
  assert.deepEqual(document, first);
  const operationIds = Object.values(document.paths)
    .flatMap((path) => Object.values(path).map((operation) => operation.operationId));
  assert.equal(new Set(operationIds).size, operationIds.length);
});

test("unexpected non-local schema references are rejected without fetching them", () => {
  assert.throws(
    () => extendInvestigationRunContracts({
      schemas: {},
      readSchema: () => ({ $ref: "https://invalid.example/schema" }),
    }),
    /non-local reference/,
  );
});
