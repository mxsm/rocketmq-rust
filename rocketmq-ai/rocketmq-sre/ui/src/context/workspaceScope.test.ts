import {
  inheritWorkspaceSearch,
  parseAbsoluteWindow,
  parseReadOnlyUrlContext,
  projectWorkspaceSearch,
} from "./workspaceScope";

const A = "10000000-0000-4000-8000-000000000001";
const B = "10000000-0000-4000-8000-000000000002";
const window = {
  start: "2026-11-11T10:00:00.000Z",
  end: "2026-11-11T11:00:00.000Z",
};
const scope = {
  clusterId: A,
  resourceKind: "topic" as const,
  resourceKey: "orders",
  window,
};

describe("workspace URL scope", () => {
  it("projects a complete scope while preserving business parameters", () => {
    const search = projectWorkspaceSearch(
      "?tab=timeline&cluster_id=old",
      scope,
    );
    expect(new URLSearchParams(search).get("tab")).toBe("timeline");
    expect(parseReadOnlyUrlContext(search, [A, B])).toEqual({
      status: "valid",
      context: { clusterId: A, resourceKind: "topic", resourceKey: "orders" },
    });
    expect(parseAbsoluteWindow(search)).toEqual(window);
  });

  it("carries scope across pages while letting explicit targets own their resource", () => {
    const current = projectWorkspaceSearch("", scope);
    const inherited = inheritWorkspaceSearch(
      current,
      "?tab=coverage",
      "/coverage",
    );
    expect(parseReadOnlyUrlContext(inherited, [A])).toMatchObject({
      status: "valid",
      context: { clusterId: A, resourceKey: "orders" },
    });
    const explicit = inheritWorkspaceSearch(
      current,
      `?cluster_id=${B}&tab=timeline`,
      "/investigations/object",
    );
    expect(parseReadOnlyUrlContext(explicit, [A, B])).toEqual({
      status: "valid",
      context: {
        clusterId: B,
        resourceKind: undefined,
        resourceKey: undefined,
      },
    });
    expect(parseAbsoluteWindow(explicit)).toEqual(window);
  });

  it("resolves explicit cluster path links and preserves scope for ordinary object links", () => {
    const current = projectWorkspaceSearch("", scope);
    expect(
      parseReadOnlyUrlContext(
        inheritWorkspaceSearch(current, "", `/clusters/${B}`),
        [A, B],
      ),
    ).toMatchObject({
      status: "valid",
      context: { clusterId: B, resourceKind: undefined },
    });
    const objectLink = inheritWorkspaceSearch(
      current,
      "?tab=timeline",
      "/incidents/object-b",
    );
    expect(parseReadOnlyUrlContext(objectLink, [A, B])).toEqual({
      status: "valid",
      context: { clusterId: A, resourceKind: "topic", resourceKey: "orders" },
    });
    expect(parseAbsoluteWindow(objectLink)).toEqual(window);
    expect(new URLSearchParams(objectLink).get("tab")).toBe("timeline");
  });

  it("keeps duplicate parameters invalid after navigation", () => {
    const current = `?cluster_id=${A}&cluster_id=${B}&window_start=${window.start}&window_start=${window.end}&window_end=${window.end}`;
    const inherited = inheritWorkspaceSearch(
      current,
      "?tab=summary",
      "/evidence",
    );
    expect(parseReadOnlyUrlContext(inherited, [A, B])).toEqual({
      status: "invalid",
    });
    expect(parseAbsoluteWindow(inherited)).toBe("invalid");
  });

  it.each([
    ["2026-02-30T10:00:00Z", "2026-03-02T11:00:00Z"],
    ["2026-11-11T24:00:00Z", "2026-11-12T01:00:00Z"],
    ["2026-11-11T10:00:00", "2026-11-11T11:00:00Z"],
    ["2026-11-11T11:00:00Z", "2026-11-11T10:00:00Z"],
    ["2026-11-11T10:00:00Z", ""],
  ])("rejects malformed absolute windows: %s", (start, end) => {
    expect(
      parseAbsoluteWindow(
        new URLSearchParams({
          window_start: start,
          window_end: end,
        }).toString(),
      ),
    ).toBe("invalid");
  });

  it("normalizes valid explicit UTC offsets without changing instants", () => {
    expect(
      parseAbsoluteWindow(
        "?window_start=2024-02-29T10%3A00%3A00%2B08%3A00&window_end=2024-02-29T11%3A00%3A00%2B08%3A00",
      ),
    ).toEqual({
      start: "2024-02-29T02:00:00.000Z",
      end: "2024-02-29T03:00:00.000Z",
    });
  });
});
