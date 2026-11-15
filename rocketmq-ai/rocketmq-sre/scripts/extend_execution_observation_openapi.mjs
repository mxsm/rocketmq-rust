// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { extendExecutionObservation } from "./openapi/execution_observation.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
for (const phase of ["03", "05"]) {
  const path = join(root, "openapi", `rocketmq-sre-phase${phase}.openapi.json`);
  const raw = readFileSync(path, "utf8");
  const document = JSON.parse(raw);
  extendExecutionObservation({ document, readSchema: (filename) =>
    JSON.parse(readFileSync(join(root, "schemas", filename), "utf8")) });
  const eol = raw.includes("\r\n") ? "\r\n" : "\n";
  const output = `${JSON.stringify(document, null, 2)}\n`.replace(/\n/g, eol);
  if (output !== raw) writeFileSync(path, output);
}
