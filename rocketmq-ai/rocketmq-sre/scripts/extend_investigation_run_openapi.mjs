// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendInvestigationRunContracts } from "./openapi/investigation_run.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const path = join(root, "openapi", "rocketmq-sre-phase05.openapi.json");
const raw = readFileSync(path, "utf8");
const document = JSON.parse(raw);

extendInvestigationRunContracts({
  schemas: document.components.schemas,
  readSchema: (filename) =>
    JSON.parse(readFileSync(join(root, "schemas", filename), "utf8")),
});

const eol = raw.includes("\r\n") ? "\r\n" : "\n";
const output = `${JSON.stringify(document, null, 2)}\n`.replace(/\n/g, eol);
if (output !== raw) writeFileSync(path, output);
