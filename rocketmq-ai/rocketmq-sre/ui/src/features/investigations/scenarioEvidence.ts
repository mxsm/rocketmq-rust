import type {
  CapabilityQualityProjection,
  DiagnosticRequirementQuality,
  DiagnosticEvidenceQuality,
  DiagnosticScenarioCoverage,
  InvestigationDiagnosisRevision,
} from "@/api/types";
import { withinWindow, type WorkspaceScope } from "@/context/workspaceScope";
import {
  evidenceFreshness,
  type EvidenceFreshnessPolicy,
  type EvidenceMetadata,
} from "@/features/evidence/evidencePresentation";
import { diagnosisHypotheses } from "./investigationPresentation";
import type { EvidenceReadFailure } from "@/features/evidence/useRevisionEvidenceMetadata";

type SlotReason =
  | "revision_reference_missing"
  | "metadata_unavailable"
  | "snapshot_scope_mismatch"
  | "source_mismatch"
  | "resource_mismatch"
  | "outside_selected_window"
  | "evidence_stale"
  | "metadata_partial"
  | "metadata_missing"
  | "metadata_not_verified"
  | "metadata_time_unknown"
  | "metadata_denied"
  | "metadata_not_found";
interface Evaluation {
  metadata: Record<string, EvidenceMetadata>;
  failures?: Record<string, EvidenceReadFailure>;
  scope: WorkspaceScope;
  tenantId: string;
}

export function scenarioEvidence(
  projection: CapabilityQualityProjection,
  scenario: DiagnosticScenarioCoverage,
  revision: InvestigationDiagnosisRevision,
  evaluation: Evaluation,
) {
  const allowed = new Set(revision.evidence_ids);
  const hypotheses = diagnosisHypotheses(revision);
  const support = new Set(hypotheses.flatMap((item) => item.support));
  const counter = new Set(hypotheses.flatMap((item) => item.counter));
  const slots = projection.packs
    .filter((pack) => scenario.pack_ids.includes(pack.pack_id))
    .flatMap((pack) =>
      pack.requirements.map((requirement) => {
        const ids = requirement.evidence_ids.filter((id) => allowed.has(id));
        const policy = requirementFreshnessPolicy(
          projection,
          pack.pack_id,
          requirement,
        );
        const assessment = assessSlot(ids, requirement, policy, evaluation);
        return {
          packId: pack.pack_id,
          requirement,
          ids,
          ...assessment,
          relation: (id: string) => ({
            support: support.has(id),
            counter: counter.has(id),
          }),
        };
      }),
    );
  const mapped = new Set(slots.flatMap((slot) => slot.ids));
  return {
    slots,
    contextIds: revision.evidence_ids.filter((id) => !mapped.has(id)),
    required: slots.filter((slot) => slot.requirement.required),
  };
}

function assessSlot(
  ids: string[],
  requirement: DiagnosticRequirementQuality,
  policy: EvidenceFreshnessPolicy,
  evaluation: Evaluation,
): { quality: DiagnosticEvidenceQuality | "outside"; reason?: SlotReason } {
  if (!ids.length)
    return { quality: "missing", reason: "revision_reference_missing" };
  const failures = ids.map((id) => evaluation.failures?.[id]);
  if (failures.includes("denied"))
    return { quality: "unknown", reason: "metadata_denied" };
  if (failures.includes("notFound"))
    return { quality: "unknown", reason: "metadata_not_found" };
  if (failures.includes("mismatch"))
    return { quality: "unknown", reason: "snapshot_scope_mismatch" };
  const samples = ids.map((id) => evaluation.metadata[id]);
  if (samples.some((sample) => !sample))
    return { quality: "unknown", reason: "metadata_unavailable" };
  if (
    samples.some(
      (sample, index) =>
        sample.evidence_id !== ids[index] ||
        sample.tenant_id !== evaluation.tenantId ||
        sample.cluster_id !== evaluation.scope.clusterId,
    )
  )
    return { quality: "unknown", reason: "snapshot_scope_mismatch" };
  if (
    samples.some(
      (sample) =>
        (sample.source === "mcp" ? "rocketmq-mcp" : sample.source) !==
        requirement.source,
    )
  )
    return { quality: "unknown", reason: "source_mismatch" };
  if (
    samples.some(
      (sample) => !sample.resource.startsWith(requirement.resource_prefix),
    )
  )
    return { quality: "unknown", reason: "resource_mismatch" };
  if (
    samples.some(
      (sample) => !withinWindow(sample.observed_at, evaluation.scope.window),
    )
  )
    return { quality: "outside", reason: "outside_selected_window" };
  const freshness = samples.map((sample) => evidenceFreshness(sample, policy));
  if (freshness.includes("stale"))
    return { quality: "stale", reason: "evidence_stale" };
  if (freshness.includes("unknown"))
    return { quality: "unknown", reason: "metadata_time_unknown" };
  if (samples.some((sample) => sample.coverage === "missing"))
    return { quality: "missing", reason: "metadata_missing" };
  if (samples.some((sample) => sample.coverage === "not_production_verified"))
    return { quality: "unsupported", reason: "metadata_not_verified" };
  if (samples.some((sample) => sample.partial || sample.coverage === "partial"))
    return { quality: "partial", reason: "metadata_partial" };
  return { quality: requirement.quality };
}

export function requirementFreshnessPolicy(
  projection: CapabilityQualityProjection,
  packId: string,
  requirement: DiagnosticRequirementQuality,
): EvidenceFreshnessPolicy {
  return {
    maxAgeSeconds: requirement.max_age_seconds,
    asOf: projection.observed_at,
    sourceRef: `compiled_diagnostic_registry:${packId}:${requirement.id}`,
  };
}

export function policyForEvidence(
  projection: CapabilityQualityProjection | undefined,
  id: string,
) {
  if (!projection) return undefined;
  const policies = projection.packs.flatMap((pack) =>
    pack.requirements
      .filter((item) => item.evidence_ids.includes(id))
      .map((item) =>
        requirementFreshnessPolicy(projection, pack.pack_id, item),
      ),
  );
  return policies.sort((a, b) => a.maxAgeSeconds - b.maxAgeSeconds)[0];
}
