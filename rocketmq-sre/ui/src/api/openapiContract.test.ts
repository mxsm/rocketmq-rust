import specification from "../../../openapi/rocketmq-sre-phase01.openapi.json";

describe("checked-in Phase 1 OpenAPI", () => {
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
});
