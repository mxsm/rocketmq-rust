import type {
  CapabilityFact,
  CapabilityLayers,
  CapabilityQualityProjection,
  DiagnosticPackQuality,
} from "@/api/types";

export function scopedCapabilityFact(
  fact: CapabilityFact,
  projection: CapabilityQualityProjection,
  pack: DiagnosticPackQuality,
  layer: keyof CapabilityLayers,
): CapabilityFact {
  if (layer !== "test_verified" && layer !== "production_verified") return fact;
  if (!fact.verification) {
    return fact.status === "present"
      ? {
          status: "unknown",
          reason_code: "verification_record_missing",
          source_ref: "",
        }
      : fact;
  }
  const assessment = fact.verification.assessment;
  const expectedLevel = layer === "test_verified" ? "test" : "production";
  const required = pack.requirements.filter((item) => item.required);
  if (
    !assessment ||
    assessment.schema_version !== "rocketmq-sre.capability-verification.v1" ||
    assessment.tenant_id !== projection.tenant_id ||
    assessment.cluster_id !== projection.cluster_id ||
    assessment.pack_id !== pack.pack_id ||
    !pack.pack_version ||
    assessment.pack_version !== pack.pack_version ||
    assessment.level !== expectedLevel ||
    (assessment.investigation_id == null) !== (assessment.revision_id == null) ||
    (assessment.investigation_id != null &&
      (assessment.investigation_id !== projection.investigation_id ||
        assessment.revision_id !== projection.revision_id)) ||
    (projection.resource != null && assessment.resource !== projection.resource) ||
    (assessment.resource != null && !required.some(
      (item) => item.resource_prefix.length > 0 &&
        assessment.resource?.startsWith(item.resource_prefix),
    )) ||
    (!projection.resource &&
      !projection.investigation_id &&
      assessment.resource != null) ||
    (fact.status === "present" &&
      (assessment.outcome !== "passed" ||
        (expectedLevel === "production" &&
          (assessment.method !== "live_read_only" || !assessment.resource ||
            required.length === 0 || required.some((item) =>
              item.quality !== "available" || item.evidence_ids.length === 0 ||
              item.evidence_ids.some((id) => !assessment.evidence_ids.includes(id)),
            ))) ||
        !(Date.parse(assessment.valid_until) > Date.parse(projection.observed_at))))
  ) {
    // Discard the whole unbound fact, including provenance, before any surface uses it.
    return {
      status: "unknown",
      reason_code: "verification_scope_mismatch",
      source_ref: "",
    };
  }
  return fact;
}

/** Summary counts, matrix badges and record details share this scope decision. */
export function scopedCapabilityProjection(projection: CapabilityQualityProjection) {
  return {
    ...projection,
    packs: projection.packs.map((pack) => ({
      ...pack,
      layers: {
        ...pack.layers,
        test_verified: scopedCapabilityFact(
          pack.layers.test_verified,
          projection,
          pack,
          "test_verified",
        ),
        production_verified: scopedCapabilityFact(
          pack.layers.production_verified,
          projection,
          pack,
          "production_verified",
        ),
      },
    })),
  };
}
