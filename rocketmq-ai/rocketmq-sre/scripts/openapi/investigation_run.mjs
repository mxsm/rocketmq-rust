// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

/** Rust schema-export is the only source of investigation-run fields. */
export const INVESTIGATION_RUN_SCHEMA_FILES = Object.freeze({
  InvestigationRunListPage: "investigation-run-list-page.schema.json",
  InvestigationRunControls: "investigation-run-controls.schema.json",
  SavedInvestigationRunRevision: "saved-investigation-run-revision.schema.json",
  InvestigationPackResult: "investigation-pack-result.schema.json",
  InvestigationRun: "investigation-run.schema.json",
  CreateInvestigationRunRequest: "create-investigation-run-request.schema.json",
  CreateInvestigationRunResponse: "create-investigation-run-response.schema.json",
  InvestigationCommandRequest: "investigation-command-request.schema.json",
  InvestigationCommandReceipt: "investigation-command-receipt.schema.json",
  InvestigationStep: "investigation-step.schema.json",
  InvestigationStepResult: "investigation-step-result.schema.json",
  HypothesisAssessment: "hypothesis-assessment.schema.json",
  InvestigationRunFailure: "investigation-run-failure.schema.json",
  InvestigationRunEvent: "investigation-run-event.schema.json",
  InvestigationRunEventPage: "investigation-run-event-page.schema.json",
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

/** Keeps Rust component import separate from transport registration. */
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

/** Bounded run control and read-only saved diagnostics; workers remain private. */
export function extendInvestigationRunTransport({ document }) {
  const response = (name, description) => ({
    description,
    content: { "application/json": { schema: { $ref: `#/components/schemas/${name}` } } },
  });
  const errors = Object.fromEntries([400, 401, 403, 404, 409].map((status) => [
    status, response("ErrorEnvelope", "Sanitized request refusal; no source content is returned"),
  ]));
  const id = { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } };
  const body = (name) => ({
    required: true,
    content: { "application/json": { schema: { $ref: `#/components/schemas/${name}` } } },
  });
  const register = (path, method, operation) => {
    document.paths[path] = { ...document.paths[path], [method]: operation };
  };
  register("/v1/investigations/{id}/runs", "post", {
    operationId: "createInvestigationRun",
    summary: "Persist a bounded read-only investigation run",
    description: "Requires current diagnose/operator authority and a matching saved investigation. Exact authorized replay returns its original creation snapshot. The managed dispatcher uses a bounded receipt-backed read-job delegation, exact selected compiled packs and shared budgets. This request performs no target mutation and exposes no arbitrary query operation.",
    parameters: [id],
    requestBody: body("CreateInvestigationRunRequest"),
    responses: {
      201: response("CreateInvestigationRunResponse", "New queued run and original creation receipt"),
      200: response("CreateInvestigationRunResponse", "Exact authorized creation replay; original snapshot"),
      ...errors,
    },
  });
  register("/v1/investigations/{id}/runs", "get", {
    operationId: "listInvestigationRuns",
    summary: "Discover bounded saved runs and current caller creation options",
    description: "Current tenant, cluster and saved parent authorization is rechecked. Optional start/end and resource_kind/resource_key must be supplied as pairs; they filter the exact effective scope before pagination. An opaque cursor is bound to this tenant, cluster, investigation and filter. Omitting filters lists this investigation's actual historical runs, with their original scopes. Creation options are advisory current-caller data and are never stored in Run.",
    parameters: [id,
      { name: "cursor", in: "query", schema: { type: "string", maxLength: 2048 } },
      { name: "limit", in: "query", schema: { type: "integer", minimum: 1, maximum: 50, default: 20 } },
      { name: "start", in: "query", schema: { type: "string", format: "date-time" } },
      { name: "end", in: "query", schema: { type: "string", format: "date-time" } },
      { name: "resource_kind", in: "query", schema: { $ref: "#/components/schemas/InvestigationRunListPage__ResourceKind" } },
      { name: "resource_key", in: "query", schema: { type: "string", minLength: 1, maxLength: 512 } },
    ],
    responses: { 200: response("InvestigationRunListPage", "Scoped summaries, filter-bound cursor and current creation policy"), ...errors },
  });
  register("/v1/investigation-runs/{id}/controls", "get", {
    operationId: "getInvestigationRunControls",
    summary: "Read a snapshot with fresh caller command availability and budget accounting",
    description: "Current authentication and saved-parent scope are rechecked. Commands and settled/reserved usage are derived for the returned Run revision. receipt_replay_allowed separately describes current control authority to retry an unchanged original request even when a new command of that kind is unavailable. Availability grants no authority: submissions independently authenticate and enforce actor/body/idempotency binding, provenance and new-command CAS. Receipts and events require a refresh and cannot overwrite a newer snapshot. No worker or target mutation operation is exposed.",
    parameters: [id],
    responses: { 200: response("InvestigationRunControls", "Same-revision snapshot and ephemeral caller affordances"), ...errors },
  });
  register("/v1/investigation-runs/{id}", "get", {
    operationId: "getInvestigationRun",
    summary: "Read the current authorized run snapshot",
    parameters: [id],
    responses: { 200: response("InvestigationRun", "Current persisted snapshot"), ...errors },
  });
  register("/v1/investigation-runs/{id}/revisions/{revision_id}", "get", {
    operationId: "getInvestigationRunRevision",
    summary: "Read one immutable run-owned diagnostic revision",
    description: "Current authentication, tenant/cluster and saved-parent scope are checked for this run and revision together. This record has no fabricated conversation, turn or answer IDs and grants no execution authority.",
    parameters: [id, { name: "revision_id", in: "path", required: true, schema: { type: "string", format: "uuid" } }],
    responses: { 200: response("SavedInvestigationRunRevision", "The actual persisted deterministic revision"), ...errors },
  });
  register("/v1/investigation-runs/{id}/commands", "post", {
    operationId: "commandInvestigationRun",
    summary: "Apply typed pause, resume, cancel or narrowing refinement",
    description: "Requires current diagnose/operator authority. Expected revisions and actor-bound idempotency are checked atomically. A replay reads the original receipt without reapplying its status or overwriting a newer snapshot. No tool, shell, model prompt or target mutation can be submitted.",
    parameters: [id],
    requestBody: body("InvestigationCommandRequest"),
    responses: {
      200: response("InvestigationCommandReceipt", "Immediate command or exact authorized receipt replay"),
      202: response("InvestigationCommandReceipt", "Newly accepted pause/cancel with admitted attempts still draining"),
      ...errors,
    },
  });
  register("/v1/investigation-runs/{id}/events", "get", {
    operationId: "listInvestigationRunEvents",
    summary: "Read bounded durable events or resynchronize a cursor",
    description: "Sequences are per-run lossless decimal strings, independent of revisions. Expired, ahead or gapped cursors return snapshot_required with the latest authorized snapshot; resume after next_sequence. Pages contain at most 100 events and no source bodies or worker lease tokens.",
    parameters: [id, {
      name: "after", in: "query", required: false,
      schema: { type: "string", pattern: "^(0|[1-9][0-9]{0,19})$", default: "0" },
    }, {
      name: "limit", in: "query", required: false,
      schema: { type: "integer", minimum: 1, maximum: 100, default: 50 },
    }],
    responses: { 200: response("InvestigationRunEventPage", "Contiguous events or an explicit snapshot reset"), ...errors },
  });
}
