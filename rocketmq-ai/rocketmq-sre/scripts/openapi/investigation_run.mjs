// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

/** Rust schema-export is the only source of investigation-run fields. */
export const INVESTIGATION_RUN_SCHEMA_FILES = Object.freeze({
  InvestigationRun: "investigation-run.schema.json",
  CreateInvestigationRunRequest: "create-investigation-run-request.schema.json",
  CreateInvestigationRunResponse: "create-investigation-run-response.schema.json",
  InvestigationCommandRequest: "investigation-command-request.schema.json",
  InvestigationCommandReceipt: "investigation-command-receipt.schema.json",
  InvestigationStep: "investigation-step.schema.json",
  InvestigationStepResult: "investigation-step-result.schema.json",
  HypothesisAssessment: "hypothesis-assessment.schema.json",
  InvestigationRunFailure: "investigation-run-failure.schema.json",
});

function rewriteSchema(value, prefix) {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteSchema(item, prefix));
  }
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .filter(([key]) => key !== "$schema" && key !== "$defs")
      .map(([key, child]) => {
        if (key === "$ref" && typeof child === "string") {
          if (child.startsWith("#/$defs/")) {
            return [key, `#/components/schemas/${prefix}__${child.slice(8)}`];
          }
          if (child === "#" || child.startsWith("#/")) {
            return [key, `#/components/schemas/${prefix}${child.slice(1)}`];
          }
          throw new Error(`Investigation-run schema has a non-local reference: ${prefix}`);
        }
        return [key, rewriteSchema(child, prefix)];
      }),
  );
}

/** Adds components only. D13 does not register HTTP paths or runtime behavior. */
export function extendInvestigationRunContracts({ schemas, readSchema }) {
  for (const [prefix, filename] of Object.entries(INVESTIGATION_RUN_SCHEMA_FILES)) {
    const source = readSchema(filename);
    if (!source || typeof source !== "object" || Array.isArray(source)) {
      throw new Error(`Investigation-run schema is not an object: ${prefix}`);
    }
    // Removed Rust definitions must not survive as stale generated DTOs.
    for (const key of Object.keys(schemas)) {
      if (key === prefix || key.startsWith(`${prefix}__`)) delete schemas[key];
    }
    schemas[prefix] = rewriteSchema(source, prefix);
    for (const [name, definition] of Object.entries(source.$defs ?? {})) {
      schemas[`${prefix}__${name}`] = rewriteSchema(definition, prefix);
    }
  }
}
