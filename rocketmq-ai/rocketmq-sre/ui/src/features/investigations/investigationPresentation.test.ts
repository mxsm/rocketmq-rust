import type { InvestigationDiagnosisRevision } from "@/api/types";

import {
  diagnosisHypotheses,
  missingEvidence,
} from "./investigationPresentation";

function revision(
  hypotheses: Array<Record<string, unknown>>,
): InvestigationDiagnosisRevision {
  return {
    id: "revision",
    investigation_id: "investigation",
    conversation_id: "conversation",
    turn_id: "turn",
    answer_revision_id: "answer",
    revision: 1,
    pack_id: "consumer-lag.v2",
    pack_version: "2.0.0",
    status: "inconclusive",
    rule_result: {
      missing_required_evidence: ["consumer processing duration"],
    },
    hypotheses,
    evidence_ids: ["evidence-a", "evidence-b"],
    primary_model_invocation_id: null,
    execution_eligible: false,
    partial: true,
    correlation_id: "correlation",
    created_at: "2026-11-11T10:00:00Z",
  };
}

describe("investigation diagnostic projection", () => {
  it("reads the server hypothesis shape and keeps only explicit in-revision evidence links", () => {
    const view = revision([
      {
        root_cause: "Slow consumer",
        confidence_percent: 82,
        supporting_evidence: [
          { evidence_id: "evidence-a", relation: "supports" },
          { evidence_id: "foreign-evidence" },
        ],
        counter_evidence: [{ evidence_id: "evidence-b" }],
        missing_evidence: ["handler trace"],
      },
    ]);
    expect(diagnosisHypotheses(view)).toEqual([
      {
        title: "Slow consumer",
        confidence: 82,
        status: undefined,
        support: ["evidence-a"],
        counter: ["evidence-b"],
        missing: ["handler trace"],
      },
    ]);
    expect(missingEvidence(view)).toEqual([
      "consumer processing duration",
      "handler trace",
    ]);
  });

  it("does not display unknown shapes or invalid scores as conclusions", () => {
    const view = revision([
      { secret: "must not stringify" },
      { title: "Unknown", confidence: 9 },
      {
        root_cause: "Unscored",
        confidence_percent: Number.NaN,
        status: "invented",
        supporting_evidence: ["evidence-a"],
      },
    ]);
    const projected = diagnosisHypotheses(view);
    expect(projected).toHaveLength(2);
    expect(projected.every((item) => item.confidence === undefined)).toBe(true);
    expect(projected[1]?.status).toBeUndefined();
    expect(projected[1]?.support).toEqual([]);
    expect(JSON.stringify(projected)).not.toContain("must not stringify");
  });

  it("degrades null, non-array and mixed JSON fields without crashing", () => {
    const malformed = {
      ...revision([]),
      hypotheses: [null, 42, { root_cause: "Visible finding" }],
      rule_result: null,
    } as unknown as InvestigationDiagnosisRevision;
    expect(diagnosisHypotheses(malformed).map((item) => item.title)).toEqual([
      "Visible finding",
    ]);
    expect(missingEvidence(malformed)).toEqual([]);
    expect(
      diagnosisHypotheses({
        ...malformed,
        hypotheses: {},
      } as unknown as InvestigationDiagnosisRevision),
    ).toEqual([]);
  });
});
