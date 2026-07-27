// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const workspace = resolve(scriptDirectory, "..");
const sourcePath = join(workspace, "openapi", "rocketmq-sre-phase01.openapi.json");
const outputPath = join(workspace, "openapi", "rocketmq-sre-phase02.openapi.json");
const schemasDirectory = join(workspace, "schemas");

const schemaFiles = new Map([
  ["AlertEvent", "alert-event.schema.json"],
  ["TopologySnapshot", "topology-snapshot.schema.json"],
  ["CapacityForecast", "capacity-forecast.schema.json"],
  ["BacklogEta", "backlog-eta.schema.json"],
  ["WhatIfSimulation", "what-if-simulation.schema.json"],
  ["UpgradeReadinessReport", "upgrade-readiness-report.schema.json"],
  ["DrReadinessReport", "dr-readiness-report.schema.json"],
  ["NotificationDelivery", "notification-delivery.schema.json"],
  ["PostmortemDraft", "postmortem-draft.schema.json"],
  ["PostmortemRevision", "postmortem-revision.schema.json"],
  ["ActionItem", "action-item.schema.json"],
  ["Phase2ContractManifest", "phase2-contract-manifest.schema.json"],
]);

function rewriteReferences(value, componentName) {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteReferences(item, componentName));
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  const rewritten = {};
  for (const [key, item] of Object.entries(value)) {
    if (key === "$schema" || key === "$defs") {
      continue;
    }
    if (
      key === "$ref" &&
      typeof item === "string" &&
      item.startsWith("#/$defs/")
    ) {
      rewritten[key] =
        `#/components/schemas/${componentName}__${item.slice("#/$defs/".length)}`;
    } else {
      rewritten[key] = rewriteReferences(item, componentName);
    }
  }
  return rewritten;
}

async function embedSchema(document, componentName, fileName) {
  const schema = JSON.parse(
    await readFile(join(schemasDirectory, fileName), "utf8"),
  );
  document.components.schemas[componentName] = rewriteReferences(
    schema,
    componentName,
  );
  for (const [definitionName, definition] of Object.entries(
    schema.$defs ?? {},
  )) {
    document.components.schemas[`${componentName}__${definitionName}`] =
      rewriteReferences(definition, componentName);
  }
}

const document = JSON.parse(await readFile(sourcePath, "utf8"));
document.info.version = "2.0.0";
document.info.description =
  "Canonical checked-in Phase 02 Control Plane and UI contract. Phase 02 adds typed alert, topology, forecast, simulation, readiness, notification and postmortem schemas while preserving the read-only RocketMQ boundary.";
document["x-rocketmq-sre-phase"] = 2;
document["x-rocketmq-phase2-contracts"] = [...schemaFiles.keys()];

for (const [componentName, fileName] of schemaFiles) {
  await embedSchema(document, componentName, fileName);
}
await embedSchema(
  document,
  "EvidenceSnapshot",
  "evidence-snapshot.schema.json",
);

document.paths["/v1/capabilities/phase2-contract"] = {
  get: {
    operationId: "getPhase2ContractManifest",
    responses: {
      200: {
        description: "Phase 2 read-only contract manifest",
        content: {
          "application/json": {
            schema: {
              $ref: "#/components/schemas/Phase2ContractManifest",
            },
          },
        },
      },
    },
  },
};

await writeFile(outputPath, `${JSON.stringify(document, null, 2)}\n`, "utf8");
console.log(
  `PHASE2_OPENAPI_EXPORTED paths=${Object.keys(document.paths).length} schemas=${Object.keys(document.components.schemas).length}`,
);
