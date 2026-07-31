import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createHttpSreApi,
  parseWorkflowSseFrame,
  stateLabel,
} from "./client";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("stateLabel", () => {
  it("renders every onboarding state without exposing implementation names", () => {
    expect(stateLabel("pending")).toBe("待接入");
    expect(stateLabel("handshaking")).toBe("握手中");
    expect(stateLabel("ready_read_only")).toBe("只读就绪");
    expect(stateLabel("read_only_degraded")).toBe("只读降级");
    expect(stateLabel("rejected")).toBe("已拒绝");
    expect(stateLabel("offboarded")).toBe("已下线");
  });

  it("uses the versioned workflow, evidence and Phase 2 contract routes", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () =>
        new Response(JSON.stringify({}), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        }),
      );
    const api = createHttpSreApi({
      token: "bounded-test-token",
      tenantId: "tenant-1",
      clusterIds: ["cluster/id"],
      subject: "test-operator",
      roles: ["rocketmq:read"],
    });

    await api.promoteInvestigation("investigation/id", {
      reason: "bounded reason",
    });
    await api.runInspection("inspection/id");
    await api.getInspectionReport("inspection/id", "markdown");
    await api.dispositionRecommendation("recommendation/id", {
      status: "promoted",
      reason: "operator confirmed",
      promote_to: "incident",
    });
    await api.getEvidence("evidence/id");
    await api.getEvidenceContent("evidence/id");
    await api.listKnowledge("cluster/id");
    await api.getPhase2Contract();
    await api.getClusterSlo("cluster/id");
    await api.getClusterHealth("cluster/id");
    await api.getFleetHealth("cn/shanghai");
    await api.getIncidentTopology("incident/id");
    await api.createPostmortem("incident/id", {});
    await api.patchPostmortem("postmortem/id", {
      summary: "operator edit",
      human_confirmed: true,
    });
    await api.publishPostmortem("postmortem/id", {
      human_confirmed: true,
      owner: "test-operator",
      component: "broker",
      rocketmq_version_range: "*",
      review_due_at: "2027-01-01T00:00:00Z",
    });
    await api.listActionItems("cluster/id");
    await api.patchActionItem("action/id", {
      status: "in_progress",
      owner: "test-operator",
      evidence_ids: [],
    });
    await api.listModelProfileLifecycles();
    await api.transitionModelProfileLifecycle("profile/id", {
      target_state: "certified",
      expected_revision: 2,
      reason_code: "operator.certified",
      operator_confirmed: true,
    });
    await api.rollbackModelProfile("profile/id", {
      expected_revision: 3,
      reason_code: "operator.rollback",
      operator_confirmed: true,
    });
    await api.runModelProfileSmoke("profile/id");
    await api.listAutonomyOutcomes({
      clusterId: "cluster/id",
      action: "proxy.restart_one.v1",
      class: "success",
      from: "2026-07-01T00:00:00Z",
      until: "2026-08-01T00:00:00Z",
      limit: 25,
    });
    await api.getAutonomyOperationalReport({
      period: "monthly",
      anchor: "2026-07-15T08:30:00Z",
      clusterId: "cluster/id",
    });
    await api.getOperationsAnalytics({
      period: "monthly",
      anchor: "2026-07-15T08:30:00Z",
      clusterId: "cluster/id",
      scenario: "consumer_lag",
      providerFamily: "deepseek",
      modelFamily: "deepseek-chat",
      actionId: "proxy.restart_one.v1",
    });

    expect(
      fetchMock.mock.calls.map(([input]) => String(input)),
    ).toEqual([
      "/v1/investigations/investigation%2Fid/promote",
      "/v1/inspections/inspection%2Fid/run",
      "/v1/inspections/inspection%2Fid/report?format=markdown",
      "/v1/recommendations/recommendation%2Fid/disposition",
      "/v1/evidence/evidence%2Fid",
      "/v1/evidence/evidence%2Fid/content",
      "/v1/knowledge?cluster_id=cluster%2Fid",
      "/v1/capabilities/phase2-contract",
      "/v1/clusters/cluster%2Fid/slo",
      "/v1/clusters/cluster%2Fid/health",
      "/v1/fleet/health?region=cn%2Fshanghai",
      "/v1/incidents/incident%2Fid/topology",
      "/v1/incidents/incident%2Fid/postmortems",
      "/v1/postmortems/postmortem%2Fid",
      "/v1/postmortems/postmortem%2Fid/publish",
      "/v1/action-items?cluster_id=cluster%2Fid",
      "/v1/action-items/action%2Fid",
      "/v1/models/profiles/lifecycle",
      "/v1/models/profiles/profile%2Fid/lifecycle",
      "/v1/models/profiles/profile%2Fid/rollback",
      "/v1/models/profiles/profile%2Fid/smoke",
      "/v1/autonomy/outcomes?cluster_id=cluster%2Fid&action=proxy.restart_one.v1&class=success&from=2026-07-01T00%3A00%3A00Z&until=2026-08-01T00%3A00%3A00Z&limit=25",
      "/v1/autonomy/reports?period=monthly&anchor=2026-07-15T08%3A30%3A00Z&cluster_id=cluster%2Fid",
      "/v1/operations/analytics?period=monthly&anchor=2026-07-15T08%3A30%3A00Z&cluster_id=cluster%2Fid&scenario=consumer_lag&provider_family=deepseek&model_family=deepseek-chat&action_id=proxy.restart_one.v1",
    ]);
    const healthHeaders = new Headers(
      fetchMock.mock.calls[9]?.[1]?.headers,
    );
    expect(healthHeaders.get("Authorization")).toBe(
      "Bearer bounded-test-token",
    );
    expect(healthHeaders.get("X-RocketMQ-Tenant")).toBe("tenant-1");
    expect(healthHeaders.get("X-RocketMQ-Clusters")).toBe(
      "cluster/id",
    );
    expect(
      JSON.parse(
        String(
          (fetchMock.mock.calls[3]?.[1] as RequestInit | undefined)?.body,
        ),
      ),
    ).toMatchObject({
      status: "promoted",
      promote_to: "incident",
    });
    expect(fetchMock.mock.calls[13]?.[1]?.method).toBe("PATCH");
    expect(fetchMock.mock.calls[16]?.[1]?.method).toBe("PATCH");
    expect(fetchMock.mock.calls[18]?.[1]?.method).toBe("POST");
    expect(fetchMock.mock.calls[19]?.[1]?.method).toBe("POST");
    expect(fetchMock.mock.calls[20]?.[1]?.method).toBe("POST");
    expect(
      JSON.parse(
        String(
          (fetchMock.mock.calls[18]?.[1] as RequestInit | undefined)
            ?.body,
        ),
      ),
    ).toMatchObject({
      target_state: "certified",
      expected_revision: 2,
      operator_confirmed: true,
    });
    expect(fetchMock.mock.calls[20]?.[1]?.body).toBeUndefined();
  });

  it("parses backend SSE payloads with an optional transport event id", () => {
    const data = {
      tenant_id: "tenant",
      cluster_id: "cluster",
      aggregate_type: "inspection",
      aggregate_id: "inspection",
      event_type: "inspection_created",
      payload: { status: "scheduled" },
      correlation_id: "correlation",
      occurred_at: "2026-07-27T08:42:10Z",
    };

    expect(
      parseWorkflowSseFrame(`event: inspection_created\ndata: ${JSON.stringify(data)}`),
    ).toEqual(data);
    expect(
      parseWorkflowSseFrame(
        `id: stream-42\nevent: inspection_created\ndata: ${JSON.stringify(data)}`,
      ),
    ).toMatchObject({ event_id: "stream-42", payload: data.payload });
  });
});
