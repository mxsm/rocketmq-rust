import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "./client";
import { createSupervisedSreApi } from "./supervisedClient";
import type {
  ApprovalDecisionRequest,
  ClearQuarantineRequest,
  ConfirmDiagnosisExecutionRequest,
  CreatePlanRequest,
  CriticReviewRequest,
  SubmitExecutionRequest,
} from "./types";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createSupervisedSreApi", () => {
  it("uses the complete Phase 3 supervised workflow surface and scoped headers", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () =>
        new Response(JSON.stringify({}), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        }),
      );
    const api = createSupervisedSreApi({
      token: "phase3-token",
      tenantId: "tenant-1",
      clusterIds: ["cluster/one"],
      subject: "operator-a",
      roles: ["operator"],
    });
    const plan = {
      cluster_id: "cluster/one",
    } as CreatePlanRequest;
    const confirmation = {
      human_confirmed: true,
      reason: "Evidence and root cause reviewed",
    } as ConfirmDiagnosisExecutionRequest;
    const decision = {
      plan_hash: "sha256:plan",
      precondition_hash: "sha256:precondition",
      reason: "reviewed",
    } as ApprovalDecisionRequest;
    const critic = {
      plan_hash: "sha256:plan",
    } as CriticReviewRequest;
    const execution = {
      plan_id: "plan/one",
    } as SubmitExecutionRequest;
    const clear = {
      reason: "manually verified",
      evidence_ids: ["evidence-1"],
    } as ClearQuarantineRequest;

    await api.confirmDiagnosisExecution(
      "incident/one",
      "revision/one",
      confirmation,
    );
    await api.createPlan(plan);
    await api.getPlan("plan/one");
    await api.reviewPlanWithCritic("plan/one", critic);
    await api.approvePlan("plan/one", decision);
    await api.rejectPlan("plan/one", decision);
    await api.submitExecution(execution);
    await api.getExecution("execution/one");
    await api.getAudit("correlation/one");
    await api.listQuarantines("cluster/one", true);
    await api.clearQuarantine("quarantine/one", clear);

    expect(fetchMock.mock.calls.map(([input]) => String(input))).toEqual([
      "/v1/incidents/incident%2Fone/diagnosis-revisions/revision%2Fone/confirm-execution",
      "/v1/plans",
      "/v1/plans/plan%2Fone",
      "/v1/plans/plan%2Fone/critic",
      "/v1/plans/plan%2Fone/approve",
      "/v1/plans/plan%2Fone/reject",
      "/v1/executions",
      "/v1/executions/execution%2Fone",
      "/v1/audit/correlation%2Fone",
      "/v1/resource-quarantines?cluster_id=cluster%2Fone&include_cleared=true",
      "/v1/resource-quarantines/quarantine%2Fone/clear",
    ]);
    const headers = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(headers.get("Authorization")).toBe("Bearer phase3-token");
    expect(headers.get("X-RocketMQ-Tenant")).toBe("tenant-1");
    expect(headers.get("X-RocketMQ-Clusters")).toBe("cluster/one");
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe("POST");
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual(
      confirmation,
    );
    expect(fetchMock.mock.calls[2]?.[1]?.method).toBe("GET");
    expect(JSON.parse(String(fetchMock.mock.calls[3]?.[1]?.body))).toEqual(
      critic,
    );
    expect(JSON.parse(String(fetchMock.mock.calls[4]?.[1]?.body))).toEqual(
      decision,
    );
  });

  it("preserves the stable backend error code", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          code: "precondition_changed",
          message: "live state changed",
        }),
        {
          headers: { "Content-Type": "application/json" },
          status: 409,
        },
      ),
    );
    const api = createSupervisedSreApi({
      token: "phase3-token",
      tenantId: "tenant-1",
      clusterIds: ["cluster-1"],
      subject: "operator-a",
      roles: ["operator"],
    });

    const failure = await api
      .submitExecution({} as SubmitExecutionRequest)
      .catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ApiError);
    expect(failure).toMatchObject({
      status: 409,
      code: "precondition_changed",
      message: "live state changed",
    });
  });
});
