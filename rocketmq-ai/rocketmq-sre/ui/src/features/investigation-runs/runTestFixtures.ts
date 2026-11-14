import type {
  InvestigationPackResult,
  InvestigationRun,
  InvestigationRunControls,
  InvestigationRunEventPage,
  InvestigationRunListPage,
  SavedInvestigationRunRevision,
} from "@/api/types";

export const RUN = "51000000-0000-4000-8000-000000000001";
export const OTHER_RUN = "51000000-0000-4000-8000-000000000002";
export const INVESTIGATION = "30000000-0000-4000-8000-000000000001";
export const TENANT = "00000000-0000-4000-8000-000000000001";
export const CLUSTER = "10000000-0000-4000-8000-000000000001";
export const REVISION = "52000000-0000-4000-8000-000000000001";
export const EVIDENCE = "40000000-0000-4000-8000-000000000001";
export const TIME = {
  start: "2026-11-14T09:00:00.000Z",
  end: "2026-11-14T10:00:00.000Z",
};
export const BUDGET = {
  limit: {
    read_queries: 12,
    model_calls: 1,
    evidence_items: 64,
    output_bytes: 4194304,
  },
  max_steps: 32,
  max_in_flight: 1,
  max_attempts_per_step: 3,
  max_query_retries: 2,
};
export function runFixture(
  overrides: Partial<InvestigationRun> = {},
): InvestigationRun {
  const scope = {
    tenant_id: TENANT,
    cluster_id: CLUSTER,
    investigation_id: INVESTIGATION,
    resource: null,
    time_range: TIME,
  };
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    id: RUN,
    created_by_subject: "operator",
    initial_scope: scope,
    effective_scope: scope,
    initial_packs: [{ pack_id: "broker-health.v1", pack_version: "1.0.0" }],
    effective_packs: [{ pack_id: "broker-health.v1", pack_version: "1.0.0" }],
    budget: BUDGET,
    revision: "2",
    control_epoch: "1",
    status: "running",
    evidence_ids: [],
    supplied_evidence_ids: [],
    steps: [],
    assessments: [],
    created_at: TIME.end,
    updated_at: TIME.end,
    deadline: "2026-11-14T11:00:00Z",
    ...overrides,
  };
}
export function controlsFixture(run = runFixture()): InvestigationRunControls {
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    run,
    observed_at: "2026-11-14T10:01:00Z",
    receipt_replay_allowed: true,
    commands: [
      { kind: "pause", allowed: true },
      { kind: "cancel", allowed: true },
      { kind: "steer", allowed: true },
      { kind: "resume", allowed: false, blocked_reason: "state_not_eligible" },
    ],
    budget_usage: {
      settled: {
        read_queries: 0,
        model_calls: 0,
        evidence_items: 0,
        output_bytes: 0,
      },
      reserved: {
        read_queries: 0,
        model_calls: 0,
        evidence_items: 0,
        output_bytes: 0,
      },
    },
  };
}
export function eventPage(run = runFixture()): InvestigationRunEventPage {
  return {
    schema_version: "rocketmq-sre.investigation-run-event-page.v1",
    tenant_id: run.initial_scope.tenant_id,
    cluster_id: run.initial_scope.cluster_id,
    investigation_id: INVESTIGATION,
    run_id: run.id,
    events: [],
    earliest_sequence: "1",
    latest_sequence: "0",
    next_sequence: "0",
    has_more: false,
    snapshot_required: false,
  };
}
export function listFixture(runs = [runFixture()]): InvestigationRunListPage {
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    tenant_id: TENANT,
    cluster_id: CLUSTER,
    investigation_id: INVESTIGATION,
    observed_at: TIME.end,
    items: runs,
    creation: {
      allowed: true,
      parent_resource: null,
      compiled_packs: runFixture().effective_packs,
      default_budget: structuredClone(BUDGET),
      maximum_budget: {
        ...BUDGET,
        limit: { ...BUDGET.limit, model_calls: 8, evidence_items: 256 },
        max_steps: 64,
        max_in_flight: 4,
      },
      default_duration_seconds: 900,
      max_duration_seconds: 3600,
      max_window_seconds: 604800,
    },
  };
}
export function revisionFixture(): SavedInvestigationRunRevision {
  return {
    schema_version: "rocketmq-sre.investigation-run-revision.v1",
    id: REVISION,
    run_id: RUN,
    scope: runFixture().effective_scope,
    control_epoch: "1",
    pack: runFixture().effective_packs[0],
    step_id: "53000000-0000-4000-8000-000000000001",
    attempt_id: "54000000-0000-4000-8000-000000000001",
    status: "fault",
    partial: false,
    evidence_ids: [EVIDENCE],
    requirements: [],
    findings: [
      {
        reason_code: "broker_not_ready",
        root_cause: "Saved deterministic fault",
        outcome: "fault",
        supporting_evidence_ids: [EVIDENCE],
        counter_evidence_ids: [],
      },
    ],
    input_fingerprint: `sha256:${"1".repeat(64)}`,
    created_at: TIME.end,
  };
}
export function resultFixture(
  revision = revisionFixture(),
): InvestigationPackResult {
  return {
    run_revision_id: revision.id,
    revision_digest: `sha256:${"2".repeat(64)}`,
    scope: revision.scope,
    control_epoch: revision.control_epoch,
    pack: revision.pack,
    step_id: revision.step_id,
    attempt_id: revision.attempt_id,
    status: revision.status,
    partial: revision.partial,
    input_fingerprint: revision.input_fingerprint,
    evidence_ids: revision.evidence_ids,
    required_evidence_quality: "available",
    fault_finding_count: 1,
    counter_evidence_ids: [],
    created_at: revision.created_at,
  };
}
export function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
