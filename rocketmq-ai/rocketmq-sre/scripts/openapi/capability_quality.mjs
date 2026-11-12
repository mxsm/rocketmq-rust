// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

/** Embed the generated read-only quality contract without changing legacy coverage fields. */
export function extendCapabilityQuality({ document, schemas, qualitySchema }) {
  const prefix = "CapabilityQualityProjection";
  const rewrite = (value) => {
    if (Array.isArray(value)) return value.map(rewrite);
    if (value === null || typeof value !== "object") return value;
    return Object.fromEntries(Object.entries(value)
      .filter(([key]) => key !== "$schema" && key !== "$defs")
      .map(([key, child]) => [key,
        key === "$ref" && typeof child === "string" && child.startsWith("#/$defs/")
          ? `#/components/schemas/${prefix}__${child.slice(8)}` : rewrite(child)]));
  };
  schemas[prefix] = rewrite(qualitySchema);
  for (const [name, schema] of Object.entries(qualitySchema.$defs ?? {})) {
    schemas[`${prefix}__${name}`] = rewrite(schema);
  }
  const get = document.paths["/v1/capabilities/coverage"].get;
  get.parameters = [{ name: "cluster_id", in: "query", required: false,
    description: "Authorized cluster. Omitting it selects the first authorized cluster; facts never combine clusters.",
    schema: { type: "string", format: "uuid" } },
    ...["investigation_id", "revision_id"].map((name) => ({ name, in: "query", required: false,
      description: "Supply both identifiers to evaluate only evidence saved in this authorized investigation revision.",
      schema: { type: "string", format: "uuid" } }))];
  get.responses["200"] = { description: "Legacy coverage plus independently sourced read-only capability facts",
    content: { "application/json": { schema: { type: "object", additionalProperties: true,
      properties: { quality: { $ref: `#/components/schemas/${prefix}` } } } } } };
}
