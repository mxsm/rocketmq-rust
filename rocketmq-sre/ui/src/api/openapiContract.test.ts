import specification from "../../../openapi/rocketmq-sre-phase02.openapi.json";

describe("checked-in Phase 2 OpenAPI", () => {
  it("keeps the generated UI contract read-only at the RocketMQ boundary", () => {
    expect(
      specification["x-rocketmq-cluster-mutation-supported"],
    ).toBe(false);
    expect(specification["x-rocketmq-effective-access"]).toBe(
      "read_only",
    );

    const paths = Object.entries(specification.paths);
    expect(
      paths.some(([path]) =>
        /\/(apply|delete|reset|restart|scale|truncate|update)(\/|$)/i.test(
          path,
        ),
      ),
    ).toBe(false);
    expect(
      paths.some(([, operations]) => "delete" in operations),
    ).toBe(false);
  });

  it("publishes typed Phase 2 contracts without adding a cluster mutation", () => {
    expect(specification["x-rocketmq-sre-phase"]).toBe(2);
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
    expect(specification.paths["/v1/action-items/{id}"]?.patch).toBeDefined();
  });
});
