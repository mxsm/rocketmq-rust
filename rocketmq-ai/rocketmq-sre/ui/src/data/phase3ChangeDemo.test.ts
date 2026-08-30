import { describe, expect, it } from "vitest";

import type { ApiRequestContext } from "@/auth/AuthContext";

import { createMockChangeManagementApi } from "./phase3ChangeDemo";

const context: ApiRequestContext = {
  token: "test-only",
  tenantId: "00000000-0000-4000-8000-000000000001",
  clusterIds: ["10000000-0000-4000-8000-000000000001"],
  subject: "current-approver",
  roles: ["operator", "approver"],
};

describe("phase 3 change management demo", () => {
  it("supports the runbook, calendar, preview, and manual-gate journey", async () => {
    const api = createMockChangeManagementApi(context);
    const clusterId = context.clusterIds[0];
    const runbooks = await api.listRunbooks(clusterId);
    const windows = await api.listChangeWindows(
      clusterId,
      new Date(Date.now() - 60 * 60 * 1000).toISOString(),
      new Date(Date.now() + 4 * 24 * 60 * 60 * 1000).toISOString(),
    );
    const runbook = runbooks.items.find((item) => item.version === "1.1.0");

    expect(runbooks.items).toHaveLength(2);
    expect(windows.items.map((item) => item.kind)).toEqual([
      "maintenance",
      "freeze",
    ]);
    expect(runbook).toBeDefined();

    const actionSteps =
      runbook?.steps.filter((step) => step.body.kind === "action") ?? [];
    const preview = await api.previewSchedule({
      cluster_id: clusterId,
      runbook_id: runbook!.id,
      runbook_version: runbook!.version,
      scheduled_start: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
      scheduled_end: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(),
      plan_bindings: actionSteps.map((step, index) => ({
        step_id: step.id,
        plan_id: `34000000-0000-4000-8000-${String(index + 10).padStart(12, "0")}`,
        plan_hash: `sha256:${"a".repeat(64)}`,
        precondition_hash: `sha256:${"b".repeat(64)}`,
      })),
    });

    expect(preview.schedulable).toBe(true);
    expect(preview.conflicts).toEqual([]);

    const schedules = await api.listSchedules(clusterId);
    const waiting = schedules.items.find(
      (item) => item.status === "awaiting_manual_gate",
    );
    expect(waiting?.waiting_manual_gate).toBeDefined();

    const approved = await api.approveManualGate(
      waiting!.id,
      waiting!.waiting_manual_gate!,
      { reason: "验证指标稳定" },
    );
    expect(approved.status).toBe("running");
    expect(approved.waiting_manual_gate).toBeNull();
  });

  it("fails closed when a cluster is outside the identity scope", async () => {
    const api = createMockChangeManagementApi(context);

    await expect(
      api.listRunbooks("10000000-0000-4000-8000-000000000003"),
    ).rejects.toMatchObject({
      code: "cluster_not_allowed",
      status: 403,
    });
  });
});
