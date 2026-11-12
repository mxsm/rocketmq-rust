import type {
  InvestigationDiagnosisRevision,
  InvestigationStatus,
  InvestigationView,
} from "@/api/types";
import {
  resourcePath,
  withinWindow,
  type WorkspaceScope,
} from "@/context/workspaceScope";

export const investigationStatusLabels: Record<
  InvestigationStatus,
  [string, string]
> = {
  open: ["已开启", "Open"],
  collecting: ["采集中", "Collecting"],
  diagnosing: ["诊断中", "Diagnosing"],
  needs_evidence: ["待补证", "Needs evidence"],
  monitoring: ["监测中", "Monitoring"],
  promoted: ["已升级事件", "Promoted"],
  closed: ["已关闭", "Closed"],
};

export interface HypothesisPresentation {
  title: string;
  status?: string;
  confidence?: number;
  support: string[];
  counter: string[];
  missing: string[];
}

export function diagnosisHypotheses(
  revision: InvestigationDiagnosisRevision,
): HypothesisPresentation[] {
  const allowed = new Set(
    Array.isArray(revision.evidence_ids)
      ? revision.evidence_ids.filter((id) => typeof id === "string")
      : [],
  );
  const hypotheses = Array.isArray(revision.hypotheses)
    ? revision.hypotheses
    : [];
  return hypotheses.slice(0, 32).flatMap((item) => {
    if (!isRecord(item)) return [];
    const title =
      boundedText(item.root_cause) ??
      boundedText(item.title) ??
      boundedText(item.statement);
    if (!title) return [];
    const confidenceValue =
      typeof item.confidence_percent === "number"
        ? item.confidence_percent
        : typeof item.confidence === "number"
          ? item.confidence * 100
          : isRecord(item.confidence)
            ? item.confidence.percent
            : undefined;
    const confidence =
      typeof confidenceValue === "number" &&
      Number.isFinite(confidenceValue) &&
      confidenceValue >= 0 &&
      confidenceValue <= 100
        ? confidenceValue
        : undefined;
    const status =
      typeof item.status === "string" &&
      ["supported", "contradicted", "unknown"].includes(item.status)
        ? item.status
        : undefined;
    return [
      {
        title,
        confidence,
        status,
        support: evidenceReferences(item.supporting_evidence, allowed),
        counter: evidenceReferences(item.counter_evidence, allowed),
        missing: textList(item.missing_evidence),
      },
    ];
  });
}

export function missingEvidence(revision: InvestigationDiagnosisRevision) {
  const result = isRecord(revision.rule_result) ? revision.rule_result : {};
  return [
    ...new Set([
      ...textList(result.missing_required_evidence),
      ...textList(result.missing_optional_evidence),
      ...diagnosisHypotheses(revision).flatMap((item) => item.missing),
    ]),
  ].slice(0, 64);
}

export function investigationInScope(
  view: InvestigationView,
  scope: WorkspaceScope,
  tenantId: string,
) {
  const object = view.investigation;
  const resource = resourcePath(scope);
  return (
    object.cluster_id === scope.clusterId &&
    object.tenant_id === tenantId &&
    (!resource ||
      scope.resourceKind === "cluster" ||
      object.resource === resource) &&
    withinWindow(object.updated_at, scope.window)
  );
}

function evidenceReferences(value: unknown, allowed: Set<string>) {
  if (!Array.isArray(value)) return [];
  return [
    ...new Set(
      value.slice(0, 128).flatMap((item) => {
        if (!isRecord(item)) return [];
        const id = boundedText(item.evidence_id);
        return id && allowed.has(id) ? [id] : [];
      }),
    ),
  ];
}

function textList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.slice(0, 64).flatMap((item) => {
        const text = boundedText(item);
        return text ? [text] : [];
      })
    : [];
}

function boundedText(value: unknown) {
  return typeof value === "string" && value.trim() && value.length <= 2048
    ? value.trim()
    : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
