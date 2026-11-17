import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { createHttpSreApi } from "@/api/client";
import type { components } from "@/api/generated";

import { KnowledgePage } from "./InsightsPages";

const clusterId = "10000000-0000-4000-8000-000000000001";
const httpApi = createHttpSreApi();

vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ api: httpApi }),
}));

vi.mock("@/hooks/useClusterScope", () => ({
  useClusterScope: () => ({
    clusterId,
    clusters: [],
    setClusterId: vi.fn(),
  }),
}));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => ({ session: {
  tenantId: "30000000-0000-4000-8000-000000000001", subject: "wire-reader", clusterIds: [clusterId], roles: ["viewer"],
} }) }));
vi.mock("@/preferences/OperatorPreferences", () => ({ useOperatorPreferences: () => ({ locale: "zh-CN", formatDateTime: (value: string) => value }) }));

const wireItem = {
  id: "20000000-0000-4000-8000-000000000001",
  tenant_id: "30000000-0000-4000-8000-000000000001",
  cluster_id: clusterId,
  title: "Synthetic no-summary wire item",
  component: "consumer",
  rocketmq_version_range: ">=5.0.0, <6.0.0",
  source_uri: "https://d22-fixture.invalid/consumer-guide",
  source_version: "synthetic-corpus-v1",
  owner: "declared-owner",
  review_status: "validated",
  review_due_at: "2026-12-01T00:00:00Z",
  sensitivity: "internal",
  content_hash: `sha256:${"a".repeat(64)}`,
  conflict: false,
  valid_from: null,
  valid_until: null,
  created_at: "2026-11-16T00:00:00Z",
  updated_at: "2026-11-16T00:00:00Z",
} satisfies components["schemas"]["KnowledgeItem"];

beforeEach(() => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    new Response(JSON.stringify({
      items: [wireItem],
      partial: false,
      warnings: [],
      observed_at: "2026-11-16T01:00:00Z",
      next_cursor: null,
    }), { headers: { "Content-Type": "application/json" } }),
  );
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Knowledge metadata from the versioned HTTP API", () => {
  it("preserves actual generated metadata without inventing a summary and forwards the read scope and signal", async () => {
    const controller = new AbortController();
    const page = await httpApi.listKnowledge(clusterId, controller.signal);

    expect(page.items).toEqual([wireItem]);
    expect(Object.hasOwn(page.items[0], "summary")).toBe(false);
    const [input, options] = vi.mocked(fetch).mock.calls[0];
    const request = new URL(String(input), "http://localhost");
    expect(request.pathname).toBe("/v1/knowledge");
    expect(request.searchParams.get("cluster_id")).toBe(clusterId);
    expect(options?.signal).toBe(controller.signal);
    expect(options?.method ?? "GET").toBe("GET");
  });

  it("keeps a real metadata list usable when a filter matches neither title nor component", async () => {
    const user = userEvent.setup();
    window.history.replaceState(null, "", "/knowledge");
    render(<RouterProvider router={createBrowserRouter([{ path: "/knowledge", element: <KnowledgePage /> }])} />);
    expect(await screen.findByRole("heading", { name: wireItem.title })).toBeInTheDocument();

    await user.type(screen.getByRole("searchbox", { name: "搜索知识" }), "unrelatedneedle");

    expect(screen.queryByRole("heading", { name: wireItem.title })).not.toBeInTheDocument();
    expect(screen.getByText("没有匹配的知识条目")).toBeInTheDocument();
  });
});
