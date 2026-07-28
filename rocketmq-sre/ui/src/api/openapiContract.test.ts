import specification from "../../../openapi/rocketmq-sre-phase03.openapi.json";

describe("checked-in Phase 3 OpenAPI", () => {
  it("keeps every mutation human-approved, typed, and bounded", () => {
    expect(
      specification["x-rocketmq-cluster-mutation-supported"],
    ).toBe(true);
    expect(specification["x-rocketmq-effective-access"]).toBe(
      "human_approved_supervised",
    );
    expect(specification["x-rocketmq-unattended-mutation-supported"]).toBe(
      false,
    );
    expect(specification["x-rocketmq-arbitrary-mutation-supported"]).toBe(
      false,
    );

    const paths = Object.entries(specification.paths);
    expect(
      paths.some(([path]) =>
        /\/(delete|reset|truncate)(\/|$)/i.test(path),
      ),
    ).toBe(false);
    expect(
      paths.some(([, operations]) => "delete" in operations),
    ).toBe(false);
  });

  it("publishes typed Phase 2 and supervised Phase 3 contracts", () => {
    expect(specification["x-rocketmq-sre-phase"]).toBe(3);
    expect(specification["x-rocketmq-phase2-contracts"]).toContain(
      "PostmortemRevision",
    );
    expect(specification.components.schemas.AlertEvent).toBeDefined();
    expect(
      specification.components.schemas.ClusterHealthReport,
    ).toBeDefined();
    expect(
      specification.components.schemas.FleetHealthReport,
    ).toBeDefined();
    expect(specification.components.schemas.CapacityForecast).toBeDefined();
    expect(
      specification.components.schemas.ClusterForecastReport,
    ).toBeDefined();
    expect(
      specification.components.schemas.WhatIfSimulationRequest,
    ).toBeDefined();
    expect(specification.components.schemas.WhatIfSimulation).toBeDefined();
    expect(specification.paths["/v1/clusters/{id}/slo"]).toBeDefined();
    expect(specification.paths["/v1/clusters/{id}/health"]).toBeDefined();
    expect(specification.paths["/v1/fleet/health"]).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/forecasts"],
    ).toBeDefined();
    expect(specification.paths["/v1/simulations"]).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/readiness/upgrade"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/readiness/dr"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/incidents/{id}/postmortems"],
    ).toBeDefined();
    expect(specification.paths["/v1/postmortems/{id}"]?.patch).toBeDefined();
    expect(
      specification.paths["/v1/postmortems/{id}/publish"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/incidents/{id}/operations"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/operations/shift-handoff"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/operations/reports"],
    ).toBeDefined();
    expect(specification.paths["/v1/action-items/{id}"]?.patch).toBeDefined();
    expect(specification.components.schemas.ActionPlan).toBeDefined();
    expect(specification.components.schemas.ApprovalGrant).toBeDefined();
    expect(specification.components.schemas.PolicyDecision).toBeDefined();
    expect(specification.components.schemas.ExecutionRequest).toBeDefined();
    expect(specification.components.schemas.ResourceQuarantine).toBeDefined();
    expect(specification.paths["/v1/plans"]?.post).toBeDefined();
    expect(specification.paths["/v1/plans/{id}/approve"]?.post).toBeDefined();
    expect(specification.paths["/v1/executions"]?.post).toBeDefined();
    expect(
      specification.paths["/v1/resource-quarantines/{id}/clear"]?.post,
    ).toBeDefined();
  });

  it("publishes bounded model lifecycle and smoke operations", () => {
    expect(
      specification.paths["/v1/models/profiles/lifecycle"]?.get,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/lifecycle"]?.get,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/lifecycle"]?.post,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/rollback"]?.post,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/smoke"]?.post,
    ).toBeDefined();
    expect(
      "delete" in
        specification.paths["/v1/models/profiles/{id}/lifecycle"],
    ).toBe(false);
  });

  it("publishes read-only bounded autonomy outcome and report queries", () => {
    const outcomes =
      specification.paths["/v1/autonomy/outcomes"];
    const reports = specification.paths["/v1/autonomy/reports"];

    expect(outcomes?.get).toBeDefined();
    expect(reports?.get).toBeDefined();
    expect("post" in outcomes).toBe(false);
    expect("delete" in outcomes).toBe(false);
    expect("post" in reports).toBe(false);
    expect("delete" in reports).toBe(false);
    expect(outcomes.get.parameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "cluster_id",
          in: "query",
        }),
        expect.objectContaining({
          name: "limit",
          schema: expect.objectContaining({
            minimum: 1,
            maximum: 200,
            default: 100,
          }),
        }),
      ]),
    );
    expect(reports.get.parameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "period",
          schema: expect.objectContaining({
            enum: ["weekly", "monthly"],
            default: "weekly",
          }),
        }),
      ]),
    );
  });
});
