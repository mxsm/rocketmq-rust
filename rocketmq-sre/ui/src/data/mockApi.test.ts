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

  it("keeps deterministic cluster and fleet health read-only and explainable", async () => {
    const api = createMockSreApi(auth);

    const cluster = await api.getClusterHealth(DEMO_CLUSTER_ID);
    const slo = await api.getClusterSlo(DEMO_CLUSTER_ID);
    const fleet = await api.getFleetHealth();

    expect(cluster.algorithm_version).toBe(
      "rocketmq-sre.health-score.v1",
    );
    expect(cluster.dimensions).toHaveLength(8);
    expect(cluster.model_adjustment_supported).toBe(false);
    expect(cluster.execution_eligible).toBe(false);
    expect(slo.id).toBe(cluster.id);
    expect(fleet.aggregation).toBe(
      "worst_cluster_no_average_masking",
    );
    expect(fleet.clusters).toHaveLength(1);
  });

  it("returns bounded incident topology without a mutation surface", async () => {
    const api = createMockSreApi(auth);
    const incident = (await api.listIncidents(DEMO_CLUSTER_ID)).items[0];
    expect(incident).toBeDefined();

    const topology = await api.getIncidentTopology(
      incident!.incident.id,
    );
    expect(topology.schema_version).toBe(
      "rocketmq-sre.incident-topology.v1",
    );
    expect(topology.nodes.length).toBeGreaterThan(0);
    expect(topology.edges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ relation: "member_of" }),
      ]),
    );
    expect(JSON.stringify(topology)).not.toContain("execution");
  });

  it("keeps postmortem publication human-controlled and action journals empty", async () => {
    const api = createMockSreApi(auth);
    const incident = (await api.listIncidents(DEMO_CLUSTER_ID)).items[0]!;
    const draft = await api.createPostmortem(incident.incident.id, {});

    expect(draft.postmortem.status).toBe("draft");
    expect(draft.execution_journal_empty).toBe(true);
    await expect(
      api.publishPostmortem(draft.postmortem.id, {
        human_confirmed: true,
        owner: "test-sre",
        component: "consumer",
        rocketmq_version_range: "*",
        review_due_at: "2027-01-01T00:00:00Z",
      }),
    ).rejects.toMatchObject({ code: "human_validation_required" });

    const confirmed = await api.patchPostmortem(
      draft.postmortem.id,
      {
        summary: "人工确认的证据化摘要",
        human_confirmed: true,
      },
    );
    expect(confirmed.revisions).toHaveLength(2);
    expect(confirmed.postmortem.status).toBe("confirmed");

    const published = await api.publishPostmortem(
      draft.postmortem.id,
      {
        human_confirmed: true,
        owner: "test-sre",
        component: "consumer",
        rocketmq_version_range: "*",
        review_due_at: "2027-01-01T00:00:00Z",
      },
    );
    expect(published.knowledge_item?.review_status).toBe("validated");
    expect(published.execution_journal_empty).toBe(true);
  });

  it("supports bounded shift handoff, reports and audited incident metadata operations", async () => {
    const api = createMockSreApi(auth);
    const handoff = await api.getShiftHandoff(DEMO_CLUSTER_ID);
    const report = await api.getOperationsReport(
      "weekly",
      DEMO_CLUSTER_ID,
    );
    const incidentId = handoff.unresolved_incidents[0]?.incident_id;
    expect(incidentId).toBeTruthy();

    const operation = await api.applyIncidentOperation(incidentId!, {
      action: "assign",
      owner: "next-shift",
      reason: "shift handoff",
    });
    const state = await api.getIncidentOperations(incidentId!);

    expect(handoff.schema_version).toBe(
      "rocketmq-sre.shift-handoff.v1",
    );
    expect(report.window).toBe("weekly");
    expect(report.cluster_mutation_count).toBe(0);
    expect(operation.cluster_mutation_performed).toBe(false);
    expect(state.owner).toBe("next-shift");
  });

  it("serves scoped autonomy outcomes and human-reviewed cost operations", async () => {
    const api = createMockSreApi(auth);
    const outcomes = await api.listAutonomyOutcomes({
      clusterId: DEMO_CLUSTER_ID,
      class: "success",
      limit: 1,
    });
    const report = await api.getAutonomyOperationalReport({
      period: "monthly",
      clusterId: DEMO_CLUSTER_ID,
    });
    const analytics = await api.getOperationsAnalytics({
      period: "weekly",
      clusterId: DEMO_CLUSTER_ID,
      scenario: "consumer_lag",
      providerFamily: "deepseek",
      modelFamily: "deepseek",
      actionId: "observability.logger_level_ttl.v1",
    });

    expect(outcomes.items).toHaveLength(1);
    expect(outcomes.items[0]?.class).toBe("success");
    expect(outcomes.truncated).toBe(true);
    expect(report.outcomes.candidates).toBe(186);
    expect(report.model_usage.cost_micros).toBeGreaterThan(0);
    expect(report.model_usage.calls_missing_cost).toBeGreaterThan(0);
    expect(analytics.filters.scenario).toBe("consumer_lag");
    expect(analytics.model_usage.cost_micros).toBeGreaterThan(0);
    expect(analytics.recommendation_feedback.adoption_basis_points).toBe(
      7_391,
    );
    expect(analytics.executions.success_basis_points).toBe(9_000);
    expect(analytics.savings.successful_autonomous_actions).toBe(18);
    expect(analytics.savings.estimated_minutes_saved).toBe(270);
    expect(report.budget_alerts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          automatic_provider_mutation: false,
        }),
      ]),
    );
    expect(
      report.optimization_candidates.every(
        (candidate) =>
          candidate.requires_human_review &&
          !candidate.publication_allowed,
      ),
    ).toBe(true);
    expect(JSON.stringify(report)).not.toContain("test-token");

    await expect(
      api.getAutonomyOperationalReport({
        period: "weekly",
        clusterId: "10000000-0000-4000-8000-000000000099",
      }),
    ).rejects.toMatchObject({
      code: "cluster_not_allowed",
      status: 403,
    });
  });
});
