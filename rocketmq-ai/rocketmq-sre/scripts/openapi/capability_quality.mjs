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
      schema: { type: "string", format: "uuid" } })),
    { name: "resource", in: "query", required: false,
      description: "Exact canonical Evidence resource. Navigation resources are not interchangeable. A resource paired with a revision must be a visible saved reference; resource verification never certifies the cluster overview.",
      schema: { type: "string", maxLength: 512 } }];
  get.responses["200"] = { description: "Legacy coverage plus independently sourced read-only capability facts",
    content: { "application/json": { schema: { type: "object", additionalProperties: true,
      properties: { quality: { $ref: `#/components/schemas/${prefix}` } } } } } };
  document.paths["/internal/v1/capability-verifications"] = { post: {
    operationId: "recordCapabilityVerification",
    description: "Private workload listener only. Requires the dedicated capability_verifier_service OIDC role and authorized tenant/cluster. UI, Connector, model and execution identities cannot write these append-only records; development authentication cannot write production facts. The source report is never fetched. Execution authority is unchanged.",
    requestBody: { required: true, content: { "application/json": { schema: { $ref: `#/components/schemas/${prefix}__CapabilityVerificationRequest` } } } },
    responses: {
      "200": { description: "Immutable report, with server-owned identity and recording time; an identical report retry returns its original identity.",
        content: { "application/json": { schema: { $ref: `#/components/schemas/${prefix}__CapabilityVerificationRecord` } } } },
      "400": { description: "Invalid version, scope, time, provenance or unqualified production proof" },
      "403": { description: "Caller is not an authorized dedicated OIDC verifier" },
      "409": { description: "The report digest already identifies a different immutable assessment" }
    }
  } };
}
