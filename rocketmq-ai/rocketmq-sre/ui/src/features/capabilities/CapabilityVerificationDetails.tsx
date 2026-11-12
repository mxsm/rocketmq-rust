import type {
  CapabilityFact,
  CapabilityLayers,
  CapabilityQualityProjection,
  DiagnosticPackQuality,
} from "@/api/types";
import { DefinitionGrid } from "@/components/Phase1Primitives";
import { useCapabilityMessages } from "@/i18n/capabilityMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import { scopedCapabilityFact } from "./capabilityVerification";

/** The server owns verification decisions; this view reads their exact provenance. */
export function CapabilityVerificationDetails({
  fact,
  projection,
  pack,
  layer,
}: {
  fact: CapabilityFact;
  projection: CapabilityQualityProjection;
  pack: DiagnosticPackQuality;
  layer: keyof CapabilityLayers;
}) {
  const m = useCapabilityMessages();
  const { formatDateTime } = useOperatorPreferences();
  if (layer !== "test_verified" && layer !== "production_verified") return null;
  const safeFact = scopedCapabilityFact(fact, projection, pack, layer);
  const record = safeFact.verification;
  const mismatch = safeFact.reason_code === "verification_scope_mismatch";
  if (!record) {
    return (
      <p
        className="workspace-empty"
        role={mismatch ? "alert" : undefined}
      >
        {mismatch ? m.verificationMismatch : m.verificationMissing}
      </p>
    );
  }
  const assessment = record.assessment;
  return (
    <section aria-label={m.verificationRecord}>
      <h3>{m.verificationRecord}</h3>
      <p className="scenario-chain-note">{m.verificationHint}</p>
      <DefinitionGrid
        items={[
          { label: m.verificationLevel, value: m[assessment.level] },
          { label: m.verificationMethod, value: m[assessment.method] },
          { label: m.verificationOutcome, value: m[assessment.outcome] },
          {
            label: m.packVersion,
            value: `${assessment.pack_id}@${assessment.pack_version}`,
          },
          {
            label: m.verificationScope,
            value: assessment.resource
              ? `${m.resourceOnly}: ${assessment.resource}`
              : m.clusterScope,
          },
          {
            label: m.verificationWindow,
            value: `${formatDateTime(assessment.time_range.start)} → ${formatDateTime(assessment.time_range.end)}`,
          },
          {
            label: m.verificationObserved,
            value: formatDateTime(assessment.observed_at),
          },
          { label: m.validUntil, value: formatDateTime(assessment.valid_until) },
          { label: m.source, value: assessment.source_ref },
          { label: m.verifiedBy, value: record.verified_by },
          { label: m.recordedAt, value: formatDateTime(record.recorded_at) },
        ]}
      />
      <details className="investigation-audit-details">
        <summary>{m.verificationAudit}</summary>
        <DefinitionGrid
          items={[
            { label: "Record ID", value: record.record_id, mono: true },
            {
              label: "Report digest",
              value: assessment.report_digest,
              mono: true,
            },
            {
              label: "Investigation / Revision",
              value: assessment.investigation_id
                ? `${assessment.investigation_id} / ${assessment.revision_id}`
                : assessment.resource
                  ? m.resourceOnly
                  : m.clusterScope,
              mono: true,
            },
            {
              label: "Evidence IDs",
              value: assessment.evidence_ids.join(" · ") || m.unknown,
              mono: true,
            },
          ]}
        />
      </details>
    </section>
  );
}
