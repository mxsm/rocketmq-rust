import type {
  CapabilityFact,
  ClusterSummary,
  CoverageMatrix,
  EvidenceRecord,
  InvestigationDiagnosisRevision,
  InvestigationView,
} from "../../src/api/types";

// API-shaped synthetic records exercise browser isolation, not target verification.
export const TENANT = "00000000-0000-4000-8000-000000000011";
export const CLUSTER_A = "10000000-0000-4000-8000-000000000011";
export const CLUSTER_B = "10000000-0000-4000-8000-000000000012";
export const INVESTIGATION = "20000000-0000-4000-8000-000000000011";
export const REVISION_ONE = "30000000-0000-4000-8000-000000000011";
export const REVISION_TWO = "30000000-0000-4000-8000-000000000012";
export const EVIDENCE_ONE = "40000000-0000-4000-8000-000000000011";
export const EVIDENCE_TWO = "40000000-0000-4000-8000-000000000012";
export const WINDOW_START = "2026-11-11T09:00:00.000Z";
export const WINDOW_END = "2026-11-11T11:00:00.000Z";
const PACK = "synthetic-consumer-lag";
const AS_OF = "2026-11-11T10:05:00.000Z";

export function oidcUser(
  subject = "synthetic-first",
  clusterIds = [CLUSTER_A, CLUSTER_B],
  token = `synthetic-access-${subject}`,
) {
  return {
    access_token: token,
    token_type: "Bearer",
    scope: "openid rocketmq:read rocketmq:diagnose",
    expires_at: Math.floor(Date.now() / 1000) + 3600,
    profile: {
      sub: subject,
      name: subject,
      rocketmq_tenant: TENANT,
      rocketmq_clusters: clusterIds,
      roles: ["rocketmq:read", "rocketmq:diagnose"],
    },
  };
}

export type SyntheticUser = ReturnType<typeof oidcUser>;

export function clusters(): ClusterSummary[] {
  return [CLUSTER_A, CLUSTER_B].map((id, index) => ({
    id,
    tenant_id: TENANT,
    external_cluster_key: `synthetic-cluster-${index + 1}`,
    environment: "synthetic-browser-fixture",
    region: "fixture",
    rocketmq_version: "fixture",
    deployment_mode: "fixture",
    owner: "synthetic-test",
    state: "ready_read_only",
    effective_access_profile: "read_only",
    updated_at: AS_OF,
  }));
}

function revision(number: number): InvestigationDiagnosisRevision {
  return {
    id: number === 1 ? REVISION_ONE : REVISION_TWO,
    investigation_id: INVESTIGATION,
    conversation_id: "50000000-0000-4000-8000-000000000011",
    turn_id: `60000000-0000-4000-8000-00000000001${number}`,
    answer_revision_id: `70000000-0000-4000-8000-00000000001${number}`,
    revision: number,
    pack_id: PACK,
    pack_version: "fixture-v1",
    status: "inconclusive",
    rule_result: { missing_required_evidence: [] },
    hypotheses: [],
    evidence_ids: [number === 1 ? EVIDENCE_ONE : EVIDENCE_TWO],
    primary_model_invocation_id: null,
    execution_eligible: false,
    partial: false,
    correlation_id: `synthetic-revision-${number}`,
    created_at:
      number === 1 ? "2026-11-11T09:30:00.000Z" : "2026-11-11T10:00:00.000Z",
  };
}

export function investigation(subject: string): InvestigationView {
  return {
    investigation: {
      id: INVESTIGATION,
      tenant_id: TENANT,
      cluster_id: CLUSTER_A,
      title: `Synthetic investigation for ${subject}`,
      symptom_family: "consumer_lag",
      fingerprint: "synthetic-browser-regression",
      status: "open",
      created_by: { subject: "synthetic-test" },
      created_at: WINDOW_START,
      updated_at: AS_OF,
    },
    timeline: [],
    diagnosis_revisions: [revision(1), revision(2)],
  };
}

export function evidence(id: string, subject: string): EvidenceRecord {
  const number = id === EVIDENCE_ONE ? 1 : 2;
  return {
    evidence_id: id,
    tenant_id: TENANT,
    cluster_id: CLUSTER_A,
    source: "rocketmq-mcp",
    resource: `consumer-lag/synthetic-revision-${number}`,
    observed_at: revision(number).created_at,
    time_range: { start: WINDOW_START, end: WINDOW_END },
    freshness_seconds: 0,
    schema: { family: "synthetic-evidence", major: 1, minor: 0 },
    coverage: "available",
    partial: false,
    content: { storage: "inline", value: { inline_must_not_render: true } },
    content_hash: `sha256:synthetic-${id}`,
    correlation_id: `80000000-0000-4000-8000-00000000001${number}`,
    query_id: `90000000-0000-4000-8000-00000000001${number}`,
    sensitivity: "internal",
    exposure: "synthetic",
    warnings: [`Synthetic sample for ${subject}; no production validation.`],
  };
}

export function coverage(revisionId: string | null): CoverageMatrix {
  const first = revisionId === REVISION_ONE;
  const fact: CapabilityFact = {
    status: "unknown",
    reason_code: "synthetic_fixture_is_not_verification",
    source_ref: "synthetic-browser-fixture",
  };
  return {
    generatedAt: AS_OF,
    semanticSignalCount: 1,
    semanticOwnerCount: 1,
    packs: [{ id: PACK, label: "Synthetic diagnostic pack" }],
    rows: [],
    selected: {
      component: "synthetic",
      pack: PACK,
      status: "not_production_verified",
      requirements: [],
    },
    quality: {
      schema_version: "rocketmq-sre.capability-quality.v1",
      tenant_id: TENANT,
      cluster_id: CLUSTER_A,
      investigation_id: revisionId ? INVESTIGATION : null,
      revision_id: revisionId,
      observed_at: AS_OF,
      execution_eligible: false,
      partial: false,
      packs: [
        {
          pack_id: PACK,
          pack_version: "fixture-v1",
          layers: {
            registered: fact,
            handler: fact,
            configured: fact,
            target_observed: fact,
            test_verified: fact,
            production_verified: fact,
          },
          requirements: [
            {
              id: first ? "synthetic.r1.signal" : "synthetic.r2.signal",
              source: "rocketmq-mcp",
              resource_prefix: "consumer-lag/",
              required: true,
              max_age_seconds: 3600,
              fields: [{ path: "synthetic_metric", kind: "number" }],
              quality: "available",
              reason_code: "synthetic_required_fields_observed",
              observed_at: revision(first ? 1 : 2).created_at,
              evidence_ids: [first ? EVIDENCE_ONE : EVIDENCE_TWO],
              missing_fields: [],
            },
          ],
        },
      ],
      scenarios: [
        {
          id: "backlog_store",
          pack_ids: [PACK],
          required_signal_ids: [first ? "synthetic.r1.signal" : "synthetic.r2.signal"],
        },
      ],
    },
  };
}

export function investigationUrl() {
  const search = new URLSearchParams({
    cluster_id: CLUSTER_A,
    window_start: WINDOW_START,
    window_end: WINDOW_END,
  });
  return `/investigations/${INVESTIGATION}?${search}`;
}
