// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

/** Reuses the Rust-exported template enum without replacing unrelated APIs. */
export function extendInspectionTemplates({ document, schema }) {
  const rewrite = (value) => {
    if (Array.isArray(value)) return value.map(rewrite);
    if (value === null || typeof value !== "object") return value;
    return Object.fromEntries(Object.entries(value)
      .filter(([key]) => key !== "$schema" && key !== "$defs")
      .map(([key, child]) => [key,
        key === "$ref" && typeof child === "string" && child.startsWith("#/$defs/")
          ? `#/components/schemas/InspectionTemplate__${child.slice(8)}` : rewrite(child)]));
  };
  const schemas = document.components.schemas;
  schemas.InspectionTemplate = rewrite(schema);
  for (const [name, definition] of Object.entries(schema.$defs ?? {})) {
    schemas[`InspectionTemplate__${name}`] = rewrite(definition);
  }
  for (const name of ["CreateInspectionRequest", "InspectionRun", "ScheduledInspectionEventEntryRequest"]) {
    if (schemas[name]?.properties?.template) {
      schemas[name].properties.template = { $ref: "#/components/schemas/InspectionTemplate" };
    }
  }
  const contracts = document["x-rocketmq-phase2-contracts"];
  if (Array.isArray(contracts) && !contracts.includes("InspectionTemplate")) {
    contracts.push("InspectionTemplate");
  }
}
