import type {
  DiagnosisRevision,
  IncidentStatus,
  IncidentView,
} from "@/api/types";

export const incidentStatusLabels: Record<IncidentStatus, string> = {
  new: "新建",
  collecting: "采集中",
  diagnosing: "诊断中",
  needs_evidence: "需要证据",
  monitoring: "监测中",
  resolved: "已解决",
  escalated: "已升级人工",
};

export interface IncidentFilters {
  severity: string;
  status: string;
  owner: string;
  query: string;
}

export interface DiagnosisAttribution {
  pack: string;
  version: string;
  mode: string;
  provider: string;
  model: string;
  missingEvidence: string[];
}

const severityRank = {
  info: 0,
  warning: 1,
  error: 2,
  critical: 3,
} as const;

export function filterAndSortIncidents(
  incidents: IncidentView[],
  filters: IncidentFilters,
): IncidentView[] {
  const query = filters.query.trim().toLocaleLowerCase();
  return incidents
    .filter(({ incident }) => {
      const searchable = [
        incident.title,
        incident.cluster_id,
        incident.resource,
        incident.symptom_family,
        incident.owner,
      ]
        .filter(Boolean)
        .join(" ")
        .toLocaleLowerCase();
      return (
        (filters.severity === "all" ||
          incident.severity === filters.severity) &&
        (filters.status === "all" ||
          incident.status === filters.status) &&
        (filters.owner === "all" ||
          (filters.owner === "unassigned"
            ? !incident.owner
            : incident.owner === filters.owner)) &&
        (!query || searchable.includes(query))
      );
    })
    .sort(
      (left, right) =>
        incidentSeverityRank(right) - incidentSeverityRank(left) ||
        Date.parse(right.incident.updated_at) -
          Date.parse(left.incident.updated_at),
    );
}

export function incidentIsStale(
  incident: IncidentView,
  now: Date,
  thresholdMs = 30 * 60 * 1000,
) {
  const observed = Date.parse(
    incident.incident.last_alert_at ?? incident.incident.updated_at,
  );
  return Number.isFinite(observed) && now.getTime() - observed > thresholdMs;
}

export function latestDiagnosisIsPartial(incident: IncidentView) {
  return incident.diagnosis_revisions.at(-1)?.partial ?? false;
}

export function incidentOwnerOptions(incidents: IncidentView[]) {
  return [
    ...new Set(
      incidents
        .map(({ incident }) => incident.owner)
        .filter((owner): owner is string => Boolean(owner)),
    ),
  ].sort((left, right) => left.localeCompare(right));
}

export function diagnosisAttribution(
  revision: DiagnosisRevision,
): DiagnosisAttribution {
  const result = revision.rule_result;
  return {
    pack:
      stringValue(result.pack_id) ??
      stringValue(result.pack) ??
      "未记录",
    version:
      stringValue(result.pack_version) ??
      stringValue(result.version) ??
      inferVersion(stringValue(result.pack)) ??
      "未记录",
    mode:
      stringValue(result.diagnosis_mode) ??
      stringValue(result.mode) ??
      (revision.primary_model_invocation_id
        ? "model_assisted"
        : "rules_only"),
    provider:
      stringValue(result.provider_id) ??
      stringValue(result.provider) ??
      (revision.primary_model_invocation_id
        ? "见模型调用记录"
        : "规则引擎"),
    model: revision.primary_model_invocation_id ?? "无（rules-only）",
    missingEvidence: uniqueStrings([
      ...arrayStrings(result.missing_required_evidence),
      ...arrayStrings(result.missing_optional_evidence),
      ...arrayStrings(result.missing_evidence),
    ]),
  };
}

function incidentSeverityRank(view: IncidentView) {
  return severityRank[view.incident.severity ?? "warning"];
}

function stringValue(value: unknown) {
  return typeof value === "string" && value.trim()
    ? value.trim()
    : undefined;
}

function arrayStrings(value: unknown) {
  return Array.isArray(value)
    ? value.filter(
        (item): item is string =>
          typeof item === "string" && Boolean(item.trim()),
      )
    : [];
}

function uniqueStrings(values: string[]) {
  return [...new Set(values)];
}

function inferVersion(pack?: string) {
  const match = pack?.match(/\.v(\d+)$/);
  return match ? `v${match[1]}` : undefined;
}
