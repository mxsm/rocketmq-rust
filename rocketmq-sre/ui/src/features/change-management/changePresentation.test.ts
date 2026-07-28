import { describe, expect, it } from "vitest";

import type { RunbookDefinition } from "@/api/types";

import {
  changeWindowKindLabel,
  conflictCodeLabel,
  diffRunbooks,
  scheduleStatusLabel,
  scheduleStatusTone,
} from "./changePresentation";

type ActionBody = Extract<
  RunbookDefinition["steps"][number]["body"],
  { kind: "action" }
>;

describe("change management presentation", () => {
  it("uses non-color labels for every operational state", () => {
    expect(scheduleStatusLabel("awaiting_manual_gate")).toBe("等待人工门");
    expect(scheduleStatusTone("rejected")).toBe("destructive");
    expect(changeWindowKindLabel("blackout")).toBe("禁止变更");
    expect(conflictCodeLabel("parallelism_exceeded")).toBe("并发上限冲突");
  });

  it("diffs typed runbook steps by stable sequence", () => {
    const before = runbook("1.0.0", [
      actionStep(1, "扩容 Proxy", "proxy.scale_out_one.v1"),
      gateStep(2),
    ]);
    const after = runbook("1.1.0", [
      actionStep(1, "扩容 Proxy", "proxy.scale_out_one.v1"),
      gateStep(2),
      actionStep(
        3,
        "轮换凭据",
        "security.credential_rotate_overlap.v1",
      ),
    ]);

    const rows = diffRunbooks(before, after);

    expect(rows.find((row) => row.key === "step-1")?.status).toBe(
      "unchanged",
    );
    expect(rows.find((row) => row.key === "step-3")).toMatchObject({
      status: "added",
      before: "—",
    });
  });
});

function runbook(
  version: string,
  steps: RunbookDefinition["steps"],
): RunbookDefinition {
  return {
    schema_version: "rocketmq-sre.runbook.v1",
    id: "00000000-0000-4000-8000-000000000001",
    name: "Proxy rollout",
    version,
    owner: "sre",
    description: "test",
    risk: "r2",
    max_parallelism: 1,
    steps,
    compensation_edges: [],
    created_at: "2026-07-28T00:00:00Z",
  };
}

function actionStep(
  sequence: number,
  name: string,
  action: ActionBody["action"],
): RunbookDefinition["steps"][number] {
  return {
    id: `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`,
    sequence,
    name,
    depends_on: [],
    body: {
      kind: "action",
      action,
      descriptor_version: "1.0.0",
      resource: "deployment/rocketmq/proxy",
      parameters: {},
    },
  };
}

function gateStep(sequence: number): RunbookDefinition["steps"][number] {
  return {
    id: `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`,
    sequence,
    name: "人工确认",
    depends_on: [],
    body: {
      kind: "manual_gate",
      gate: {
        gate_id: "verify",
        title: "确认",
        instructions: "确认验证通过",
        required_role: "approver",
        timeout_seconds: 900,
      },
    },
  };
}
