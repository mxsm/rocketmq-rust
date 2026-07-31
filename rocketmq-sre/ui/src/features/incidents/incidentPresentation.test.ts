import { describe, expect, it } from "vitest";

import type {
  DiagnosisRevision,
  IncidentView,
} from "@/api/types";

import {
  diagnosisAttribution,
  filterAndSortIncidents,
  incidentIsStale,
  incidentOwnerOptions,
  latestDiagnosisIsPartial,
} from "./incidentPresentation";

describe("incident presentation", () => {
  it("filters inbox fields and sorts the highest severity first", () => {
    const incidents = [
      incident("warning", "team-a", "2026-07-27T08:40:00Z"),
      incident("critical", "team-a", "2026-07-27T08:20:00Z"),
      incident("error", undefined, "2026-07-27T08:50:00Z"),
    ];

    expect(
      filterAndSortIncidents(incidents, {
        severity: "all",
        status: "all",
        owner: "team-a",
        query: "broker-a",
      }).map((view) => view.incident.severity),
    ).toEqual(["critical", "warning"]);
    expect(incidentOwnerOptions(incidents)).toEqual(["team-a"]);
  });

  it("marks stale and partial incidents explicitly", () => {
    const view = incident(
      "warning",
      "team-a",
      "2026-07-27T08:00:00Z",
      true,
    );

    expect(
      incidentIsStale(view, new Date("2026-07-27T09:00:01Z")),
    ).toBe(true);
    expect(latestDiagnosisIsPartial(view)).toBe(true);
  });

  it("extracts pack, model/provider and missing evidence safely", () => {
    const revision = incident(
      "critical",
      "team-a",
      "2026-07-27T08:00:00Z",
      true,
    ).diagnosis_revisions[0]!;
    revision.rule_result = {
      pack_id: "consumer-lag",
      pack_version: "2.1.0",
      mode: "rules_only",
      missing_required_evidence: ["consumer-runtime"],
      missing_optional_evidence: ["trace", "trace"],
    };

    expect(diagnosisAttribution(revision)).toEqual({
      pack: "consumer-lag",
      version: "2.1.0",
      mode: "rules_only",
      provider: "规则引擎",
      model: "无（rules-only）",
      missingEvidence: ["consumer-runtime", "trace"],
    });
  });
});

function incident(
  severity: "warning" | "error" | "critical",
  owner: string | undefined,
  updatedAt: string,
  partial = false,
): IncidentView {
  const revision: DiagnosisRevision = {
    id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    incident_id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    revision: 1,
    status: "diagnosing",
    rule_result: { pack: "consumer-lag.v1" },
    hypotheses: [],
    evidence_ids: [],
    execution_eligible: false,
    partial,
    created_at: updatedAt,
  };
  return {
    incident: {
      id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
      tenant_id: "ffffffff-ffff-4fff-8fff-ffffffffffff",
      cluster_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      title: "Broker-a consumer lag",
      resource: "broker:broker-a",
      symptom_family: "consumer_lag",
      status: "diagnosing",
      severity,
      owner,
      occurrence_count: 3,
      created_at: updatedAt,
      updated_at: updatedAt,
    },
    timeline: [],
    diagnosis_revisions: [revision],
  };
}
