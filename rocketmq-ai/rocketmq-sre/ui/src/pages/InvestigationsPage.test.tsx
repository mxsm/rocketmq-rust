import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import type { SreApi } from "@/api/client";
import type { CollectionEnvelope, InvestigationView, WorkflowListOptions } from "@/api/types";
import { WorkspaceScopeProvider, useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { demoClusters } from "@/data/demo";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID, phase1Investigations } from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { InvestigationsPage } from "./InvestigationsPage";

const state = vi.hoisted(() => ({ api: undefined as unknown as SreApi }));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => ({ api: state.api, clusters: demoClusters }) }));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: { tenantId: DEMO_TENANT_ID, subject: "reader", roles: ["reader"], clusterIds: demoClusters.map((cluster) => cluster.id) },
    hasClusterScope: (id: string) => demoClusters.some((cluster) => cluster.id === id),
  }),
}));

function view(number: number, clusterId = DEMO_CLUSTER_ID): InvestigationView {
  return {
    investigation: {
      ...phase1Investigations.items[0].investigation,
      id: `33000000-0000-4000-8000-${String(number).padStart(12, "0")}`,
      tenant_id: DEMO_TENANT_ID,
      cluster_id: clusterId,
      title: `Investigation ${String(number).padStart(4, "0")}`,
      updated_at: "2026-11-12T10:00:00Z",
      resource: undefined,
    },
    timeline: [],
    diagnosis_revisions: [],
  };
}
function page(clusterId: string, options?: WorkflowListOptions): CollectionEnvelope<InvestigationView> {
  const start = options?.cursor ? Number(options.cursor.slice(-12)) : 0;
  const items = Array.from({ length: Math.min(50, 1000 - start) }, (_, index) => view(start + index + 1, clusterId));
  return {
    items,
    next_cursor: start + items.length < 1000 ? items.at(-1)!.investigation.id : null,
    partial: start + items.length < 1000,
    warnings: [],
    observed_at: "2026-11-12T10:00:00Z",
  };
}
function ScopeControls() {
  const scope = useWorkspaceScope();
  return <>
    <button onClick={() => scope.setClusterId(demoClusters[1].id)}>Change cluster</button>
    <button onClick={() => scope.setWindow({ start: "2026-11-12T09:00:00Z", end: "2026-11-12T11:00:00Z" })}>Change window</button>
  </>;
}
function open() {
  return render(
    <OperatorPreferencesProvider>
      <MemoryRouter initialEntries={[`/investigations?cluster_id=${DEMO_CLUSTER_ID}&window_start=2026-11-12T08:00:00Z&window_end=2026-11-12T12:00:00Z`]}>
        <WorkspaceScopeProvider>
          <ScopeControls />
          <InvestigationsPage />
        </WorkspaceScopeProvider>
      </MemoryRouter>
    </OperatorPreferencesProvider>,
  );
}

describe("saved investigation pagination", () => {
  beforeEach(() => {
    localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({ locale: "en-US", timeZone: "UTC", theme: "dark" }));
    state.api = {
      listInvestigations: vi.fn((cluster: string, _signal?: AbortSignal, options?: WorkflowListOptions) => Promise.resolve(page(cluster, options))),
      getInvestigation: vi.fn(),
      subscribeWorkflowEvents: vi.fn(() => new Promise<void>(() => undefined)),
    } as unknown as SreApi;
  });
  afterEach(() => localStorage.clear());

  it("reaches record 1000 using bounded server pages and can move back and forward again", async () => {
    open();
    await screen.findByRole("link", { name: "Investigation 0001" });
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(51);
    expect(screen.getAllByTitle(/does not provide a revision count/)).toHaveLength(50);
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByRole("link", { name: "Investigation 0051" });
    fireEvent.click(screen.getByRole("button", { name: "Previous page" }));
    await screen.findByRole("link", { name: "Investigation 0001" });
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByRole("link", { name: "Investigation 0051" });
    for (let index = 2; index < 20; index += 1) {
      fireEvent.click(screen.getByRole("button", { name: "Next page" }));
      await screen.findByRole("link", { name: `Investigation ${String(index * 50 + 1).padStart(4, "0")}` });
    }
    expect(screen.getByRole("link", { name: "Investigation 1000" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Previous page" }));
    await screen.findByRole("link", { name: "Investigation 0901" });
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByRole("link", { name: "Investigation 1000" });
    expect(state.api.listInvestigations).toHaveBeenCalledTimes(24);
    expect(vi.mocked(state.api.listInvestigations).mock.calls.every((call) => call[2]?.limit === 50)).toBe(true);
    expect(state.api.getInvestigation).not.toHaveBeenCalled();
    expect(state.api.subscribeWorkflowEvents).toHaveBeenCalledTimes(1);
  });

  it("clears cursor history for a new scope and discards a late previous page", async () => {
    let finish!: (value: CollectionEnvelope<InvestigationView>) => void;
    vi.mocked(state.api.listInvestigations).mockImplementation((cluster, _signal, options) => {
      if (cluster === DEMO_CLUSTER_ID && options?.cursor) {
        return new Promise((resolve) => { finish = resolve; });
      }
      return Promise.resolve(page(cluster, options));
    });
    open();
    await screen.findByRole("link", { name: "Investigation 0001" });
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await waitFor(() => expect(state.api.listInvestigations).toHaveBeenCalledTimes(2));
    const oldSignal = vi.mocked(state.api.listInvestigations).mock.calls[1][1];
    fireEvent.click(screen.getByRole("button", { name: "Change cluster" }));
    await screen.findByRole("link", { name: "Investigation 0001" });
    expect(oldSignal?.aborted).toBe(true);
    expect(state.api.listInvestigations).toHaveBeenLastCalledWith(demoClusters[1].id, expect.any(AbortSignal), { cursor: undefined, limit: 50 });
    const oldPage = page(DEMO_CLUSTER_ID, { cursor: view(50).investigation.id });
    oldPage.items[0].investigation.title = "Late old page";
    await act(async () => finish(oldPage));
    expect(screen.queryByRole("link", { name: "Late old page" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByRole("link", { name: "Investigation 0051" });
    fireEvent.click(screen.getByRole("button", { name: "Change window" }));
    await screen.findByRole("link", { name: "Investigation 0001" });
    expect(state.api.listInvestigations).toHaveBeenLastCalledWith(demoClusters[1].id, expect.any(AbortSignal), { cursor: undefined, limit: 50 });
    expect(screen.getByRole("button", { name: "Previous page" })).toBeDisabled();
  });

  it("stops repeated cursors while keeping next available when filters hide the current page", async () => {
    vi.mocked(state.api.listInvestigations).mockImplementation((cluster, _signal, options) => Promise.resolve({
      ...page(cluster), items: [view(options?.cursor ? 2 : 1)], next_cursor: "repeated-cursor",
    }));
    open();
    await screen.findByRole("link", { name: "Investigation 0001" });
    await userEvent.setup().type(screen.getByRole("searchbox"), "not on this page");
    expect(screen.getByRole("button", { name: "Next page" })).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await waitFor(() => expect(state.api.listInvestigations).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled());
    expect(screen.getByText(/filter the current loaded page/)).toBeVisible();
  });

  it("keeps legacy full-list responses paginated locally without extra HTTP reads", async () => {
    vi.mocked(state.api.listInvestigations).mockResolvedValue({ ...page(DEMO_CLUSTER_ID), items: Array.from({ length: 1000 }, (_, index) => view(index + 1)), next_cursor: null, partial: false });
    open();
    await screen.findByRole("link", { name: "Investigation 0001" });
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByRole("link", { name: "Investigation 0051" });
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(51);
    expect(state.api.listInvestigations).toHaveBeenCalledTimes(1);
  });
});
