import type { ApiRequestContext } from "@/auth/AuthContext";
import { createMockSreApi } from "@/data/mockApi";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "@/data/phase1Demo";

const auth: ApiRequestContext = {
  token: "test-token",
  tenantId: DEMO_TENANT_ID,
  clusterIds: [DEMO_CLUSTER_ID],
  subject: "test-sre",
  roles: ["rocketmq:read", "rocketmq:diagnose"],
};

describe("mock SRE API", () => {
  it("filters clusters and fails closed outside the authenticated scope", async () => {
    const api = createMockSreApi(auth);

    const clusters = await api.listClusters();
    expect(clusters).toHaveLength(1);
    expect(clusters[0]?.id).toBe(DEMO_CLUSTER_ID);

    await expect(
      api.listAssets("10000000-0000-4000-8000-000000000099"),
    ).rejects.toMatchObject({
      code: "cluster_not_allowed",
      status: 403,
    });
  });

  it("exposes message journey metadata without a message body", async () => {
    const journey = await createMockSreApi(auth).getMessageJourney(
      DEMO_CLUSTER_ID,
      "message-id",
    );

    expect(journey.message_body_available).toBe(false);
    expect(Object.hasOwn(journey, "message_body")).toBe(false);
    expect(JSON.stringify(journey)).not.toContain("test-token");
  });

  it("supports the operator workflow without exposing cluster mutation", async () => {
    const api = createMockSreApi(auth);
    const incident = await api.promoteInvestigation(
      "30000000-0000-4000-8000-000000000002",
      { reason: "operator confirmed impact" },
    );
    expect(incident.incident.status).toBe("new");

    const recommendation = await api.dispositionRecommendation(
      "51000000-0000-4000-8000-000000000001",
      {
        status: "promoted",
        reason: "needs incident tracking",
        promote_to: "incident",
      },
    );
    expect(recommendation.status).toBe("promoted");
    expect(recommendation.incident_id).toBeTruthy();

    const report = await api.getInspectionReport(
      "50000000-0000-4000-8000-000000000001",
      "markdown",
    );
    expect(report.media_type).toBe("text/markdown; charset=utf-8");
  });

  it("loads canonical evidence content by its stable evidence id", async () => {
    const api = createMockSreApi(auth);
    const evidenceId = "40000000-0000-4000-8000-000000000001";
    const snapshot = await api.getEvidence(evidenceId);
    const content = await api.getEvidenceContent(evidenceId);

    expect(snapshot.evidence_id).toBe(evidenceId);
    expect(snapshot.content_hash).toMatch(/^sha256:/);
    expect(content).toMatchObject({ lag: 1284 });
  });
});
