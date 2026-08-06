// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { extendModelCapabilities } from "./openapi/model_capabilities.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const path = join(root, "openapi", "rocketmq-sre-phase05.openapi.json");
const document = JSON.parse(readFileSync(path, "utf8"));

extendModelCapabilities({
  document,
  schemas: document.components.schemas,
});

writeFileSync(path, `${JSON.stringify(document, null, 2)}\n`);
