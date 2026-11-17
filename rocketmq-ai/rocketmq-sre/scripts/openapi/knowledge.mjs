// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

export const KNOWLEDGE_SCHEMA_FILES = Object.freeze({
  KnowledgeItem: "knowledge-item.schema.json",
  KnowledgeReviewStatus: "knowledge-review-status.schema.json",
  KnowledgePage: "knowledge-page.schema.json",
  KnowledgeChunkView: "knowledge-chunk-view.schema.json",
  KnowledgeSearchPage: "knowledge-search-page.schema.json",
  KnowledgeHistoryPage: "knowledge-history-page.schema.json",
  ImportKnowledgeRequest: "import-knowledge-request.schema.json",
  KnowledgeImportResult: "knowledge-import-result.schema.json",
  KnowledgeReviewRequest: "knowledge-review-request.schema.json",
  KnowledgeFeedbackRequest: "knowledge-feedback-request.schema.json",
});

function rewrite(value, prefix) {
  if (Array.isArray(value)) return value.map((item) => rewrite(item, prefix));
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => key !== "$schema" && key !== "$defs")
    .map(([key, child]) => {
      if (key === "$ref" && typeof child === "string") {
        if (child.startsWith("#/$defs/")) return [key, `#/components/schemas/${prefix}__${child.slice(8)}`];
        if (child === "#" || child.startsWith("#/")) return [key, `#/components/schemas/${prefix}${child.slice(1)}`];
        throw new Error(`Knowledge schema has a non-local reference: ${prefix}`);
      }
      return [key, rewrite(child, prefix)];
    }));
}

/** Rust-owned Knowledge responses replace generic JSON; no execution authority is introduced. */
export function extendKnowledge({ document, readSchema }) {
  const schemas = document.components.schemas;
  for (const [name, filename] of Object.entries(KNOWLEDGE_SCHEMA_FILES)) {
    const source = readSchema(filename);
    for (const key of Object.keys(schemas)) if (key === name || key.startsWith(`${name}__`)) delete schemas[key];
    schemas[name] = rewrite(source, name);
    for (const [key, definition] of Object.entries(source.$defs ?? {})) schemas[`${name}__${key}`] = rewrite(definition, name);
  }
  const schema = (name) => ({ $ref: `#/components/schemas/${name}` });
  const response = (name) => ({ description: "Scoped server facts; no target operation or physical-version qualification",
    content: { "application/json": { schema: schema(name) } } });
  for (const [path, method, output, input] of [
    ["/v1/knowledge", "get", "KnowledgePage"],
    ["/v1/knowledge/search", "get", "KnowledgeSearchPage"],
    ["/v1/knowledge/{id}", "get", "KnowledgeItem"],
    ["/v1/knowledge/import", "post", "KnowledgeImportResult", "ImportKnowledgeRequest"],
    ["/v1/knowledge/{id}/review", "post", "KnowledgeItem", "KnowledgeReviewRequest"],
    ["/v1/knowledge/{id}/feedback", "post", "KnowledgeItem", "KnowledgeFeedbackRequest"],
  ]) {
    const operation = document.paths[path]?.[method];
    if (!operation) throw new Error(`Knowledge operation is missing: ${method} ${path}`);
    operation.responses = { ...operation.responses, 200: response(output) };
    if (input) operation.requestBody = { required: true, content: { "application/json": { schema: schema(input) } } };
  }
  document.paths["/v1/knowledge/search"].get.description = "Bounded lexical retrieval. partial retains its legacy included-ineligible meaning; bounds separately state candidate exhaustion and result truncation. Target version is supplied knowledge context.";
  document.paths["/v1/knowledge/{id}/history"] = { get: {
    operationId: "getKnowledgeHistory", summary: "Read bounded review, nonauthoritative feedback and stored publication lineage",
    parameters: [{ name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } }],
    responses: { 200: response("KnowledgeHistoryPage"), ...Object.fromEntries([400,401,403,404,503].map((code) => [code,response("ErrorEnvelope")])) },
  } };
}
