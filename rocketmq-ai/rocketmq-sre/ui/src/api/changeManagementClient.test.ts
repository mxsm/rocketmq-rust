import { afterEach, describe, expect, it, vi } from "vitest";

import { createChangeManagementApi } from "./changeManagementClient";
import type {
  CreateChangeScheduleRequest,
  CreateChangeWindowRequest,
  CreateRunbookRequest,
} from "./types";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createChangeManagementApi", () => {
  it("uses the complete scoped runbook, calendar, schedule, and manual gate surface", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () =>
        new Response(JSON.stringify({}), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        }),
      );
    const api = createChangeManagementApi({
      token: "phase3-token",
      tenantId: "tenant-1",
      clusterIds: ["cluster/one"],
      subject: "operator-a",
      roles: ["operator", "approver"],
    });
    const runbook = { cluster_id: "cluster/one" } as CreateRunbookRequest;
    const window = {
      cluster_id: "cluster/one",
    } as CreateChangeWindowRequest;
    const schedule = {
      cluster_id: "cluster/one",
    } as CreateChangeScheduleRequest;
    const reason = { reason: "reviewed against live evidence" };

    await api.listRunbooks("cluster/one", 50);
    await api.getRunbook("cluster/one", "runbook/one", "1.0.0");
    await api.createRunbook(runbook);
    await api.listChangeWindows(
      "cluster/one",
      "2026-07-28T00:00:00Z",
      "2026-08-04T00:00:00Z",
      100,
    );
    await api.createChangeWindow(window);
    await api.previewSchedule(schedule);
    await api.listSchedules("cluster/one", "running", 25);
    await api.createSchedule(schedule);
    await api.getSchedule("schedule/one");
    await api.pauseSchedule("schedule/one", reason);
    await api.resumeSchedule("schedule/one", reason);
    await api.cancelSchedule("schedule/one", reason);
    await api.reconcileSchedule("schedule/one", reason);
    await api.approveManualGate("schedule/one", "step/one", reason);
    await api.rejectManualGate("schedule/one", "step/one", reason);

    expect(fetchMock.mock.calls.map(([input]) => String(input))).toEqual([
      "/v1/runbooks?cluster_id=cluster%2Fone&limit=50",
      "/v1/runbooks/runbook%2Fone/versions/1.0.0?cluster_id=cluster%2Fone",
      "/v1/runbooks",
      "/v1/change-windows?cluster_id=cluster%2Fone&from=2026-07-28T00%3A00%3A00Z&to=2026-08-04T00%3A00%3A00Z&limit=100",
      "/v1/change-windows",
      "/v1/change-schedules/preview",
      "/v1/change-schedules?cluster_id=cluster%2Fone&status=running&limit=25",
      "/v1/change-schedules",
      "/v1/change-schedules/schedule%2Fone",
      "/v1/change-schedules/schedule%2Fone/pause",
      "/v1/change-schedules/schedule%2Fone/resume",
      "/v1/change-schedules/schedule%2Fone/cancel",
      "/v1/change-schedules/schedule%2Fone/reconcile",
      "/v1/change-schedules/schedule%2Fone/manual-gates/step%2Fone/approve",
      "/v1/change-schedules/schedule%2Fone/manual-gates/step%2Fone/reject",
    ]);

    const headers = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(headers.get("Authorization")).toBe("Bearer phase3-token");
    expect(headers.get("X-RocketMQ-Tenant")).toBe("tenant-1");
    expect(headers.get("X-RocketMQ-Clusters")).toBe("cluster/one");
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe("GET");
    expect(fetchMock.mock.calls[2]?.[1]?.method).toBe("POST");
    expect(JSON.parse(String(fetchMock.mock.calls[9]?.[1]?.body))).toEqual(
      reason,
    );
  });
});
