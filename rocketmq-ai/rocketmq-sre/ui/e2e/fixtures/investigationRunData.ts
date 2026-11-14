import type {
  InvestigationRun,
  InvestigationRunControls,
  InvestigationRunEventPage,
  InvestigationRunListPage,
} from "../../src/api/types";
import {
  CLUSTER_A,
  INVESTIGATION,
  TENANT,
  WINDOW_END,
  WINDOW_START,
} from "./evidenceData";

// API-shaped synthetic state exercises controls and isolation, never target verification.
export const RUN_ONE = "51000000-0000-4000-8000-000000000011";
export const RUN_TWO = "51000000-0000-4000-8000-000000000012";
export function run(
  id = RUN_ONE,
  revision = "9007199254740993",
  status: InvestigationRun["status"] = "running",
): InvestigationRun {
  const scope = {
    tenant_id: TENANT,
    cluster_id: CLUSTER_A,
    investigation_id: INVESTIGATION,
    resource: null,
    time_range: { start: WINDOW_START, end: WINDOW_END },
  };
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    id,
    created_by_subject: "synthetic-first",
    initial_scope: scope,
    effective_scope: scope,
    initial_packs: [{ pack_id: "synthetic-read-only", pack_version: "1.0.0" }],
    effective_packs: [
      { pack_id: "synthetic-read-only", pack_version: "1.0.0" },
    ],
    budget: {
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
    },
    revision,
    control_epoch: "1",
    status,
    evidence_ids: [],
    supplied_evidence_ids: [],
    steps: [],
    assessments: [],
    deadline: "2099-11-11T11:00:00Z",
    created_at: WINDOW_END,
    updated_at: WINDOW_END,
  };
}
export function runList(
  items: InvestigationRun[] = [],
  allowed = false,
): InvestigationRunListPage {
  const budget = run().budget;
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    tenant_id: TENANT,
    cluster_id: CLUSTER_A,
    investigation_id: INVESTIGATION,
    observed_at: WINDOW_END,
    items,
    creation: {
      allowed,
      ...(allowed
        ? {}
        : { blocked_reason: "control_permission_required" as const }),
      parent_resource: null,
      compiled_packs: run().effective_packs,
      default_budget: budget,
      maximum_budget: {
        ...budget,
        limit: { ...budget.limit, model_calls: 8, evidence_items: 256 },
        max_steps: 64,
        max_in_flight: 4,
      },
      default_duration_seconds: 900,
      max_duration_seconds: 3600,
      max_window_seconds: 604800,
    },
  };
}
export function runControls(
  value = run(),
  allowed = true,
): InvestigationRunControls {
  const terminal = [
    "completed",
    "inconclusive",
    "cancelled",
    "failed",
  ].includes(value.status);
  return {
    schema_version: "rocketmq-sre.investigation-run.v1",
    run: value,
    observed_at: WINDOW_END,
    receipt_replay_allowed: allowed,
    commands: (["pause", "resume", "cancel", "steer"] as const).map((kind) => {
      const eligible =
        !terminal &&
        (kind === "pause"
          ? value.status === "running"
          : kind === "resume"
            ? value.status === "paused"
            : kind === "steer"
              ? ["running", "paused", "needs_evidence"].includes(value.status)
              : value.status !== "cancelling");
      return {
        kind,
        allowed: allowed && eligible,
        ...(!allowed
          ? { blocked_reason: "control_permission_required" as const }
          : !eligible
            ? {
                blocked_reason: terminal
                  ? ("terminal" as const)
                  : ("state_not_eligible" as const),
              }
            : {}),
      };
    }),
    budget_usage: {
      settled: {
        read_queries: 1,
        model_calls: 0,
        evidence_items: 0,
        output_bytes: 0,
      },
      reserved: {
        read_queries: 1,
        model_calls: 0,
        evidence_items: 1,
        output_bytes: 655360,
      },
    },
  };
}
export function runEvents(value = run()): InvestigationRunEventPage {
  return {
    schema_version: "rocketmq-sre.investigation-run-event-page.v1",
    tenant_id: TENANT,
    cluster_id: CLUSTER_A,
    investigation_id: INVESTIGATION,
    run_id: value.id,
    events: [],
    earliest_sequence: "1",
    latest_sequence: "0",
    next_sequence: "0",
    has_more: false,
    snapshot_required: false,
  };
}
