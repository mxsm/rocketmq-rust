import {
  parseReadOnlyUrlContext,
  withoutReadOnlyUrlContext,
} from "./useClusterScope";

const CLUSTER_ID = "10000000-0000-4000-8000-000000000001";

describe("read-only URL context", () => {
  it("accepts a bounded resource only inside the cluster scope", () => {
    expect(
      parseReadOnlyUrlContext(
        `?cluster_id=${CLUSTER_ID}&resource_kind=topic&resource_key=orders`,
        [CLUSTER_ID],
      ),
    ).toEqual({
      status: "valid",
      context: {
        clusterId: CLUSTER_ID,
        resourceKind: "topic",
        resourceKey: "orders",
      },
    });
  });

  it("fails closed for an out-of-scope cluster or malformed resource", () => {
    expect(
      parseReadOnlyUrlContext(
        `?cluster_id=${CLUSTER_ID}&resource_kind=topic&resource_key=orders`,
        [],
      ),
    ).toEqual({ status: "invalid" });
    expect(
      parseReadOnlyUrlContext(
        `?cluster_id=${CLUSTER_ID}&resource_kind=unknown&resource_key=secret`,
        [CLUSTER_ID],
      ),
    ).toEqual({ status: "invalid" });
  });

  it("removes rejected context without echoing its values", () => {
    expect(
      withoutReadOnlyUrlContext(
        "?cluster_id=forbidden&resource_kind=topic&resource_key=secret&tab=summary",
      ),
    ).toBe("?tab=summary");
  });
});
