import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useSyncExternalStore } from "react";
import { createBrowserRouter, Link, RouterProvider } from "react-router-dom";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { knowledgeHistory, knowledgeItem, cluster, otherCluster, tenant } from "@/features/knowledge/test-fixtures";
import { AppLayout } from "@/layouts/AppLayout";
import { KnowledgePage } from "./KnowledgePage";

const state = vi.hoisted(() => ({
  subject: "reader", loading: false, roster: true, revision: 0, listeners: new Set<() => void>(),
  listAssets: vi.fn(), listKnowledgePage: vi.fn(), getKnowledge: vi.fn(), getKnowledgeHistory: vi.fn(),
  searchKnowledge: vi.fn(), reviewKnowledge: vi.fn(), feedbackKnowledge: vi.fn(),
}));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => {
  useTestState();
  return {
    mode: "oidc", session: { tenantId: tenant, subject: state.subject, displayName: "Reader", roles: ["viewer"], clusterIds: [cluster, otherCluster] },
    hasClusterScope: (id: string) => [cluster, otherCluster].includes(id), signOut: vi.fn(),
  };
} }));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => {
  useTestState();
  return {
    api: state, loading: state.loading, error: null, demoMode: false,
    clusters: state.roster ? [cluster, otherCluster].map((id) => ({ id, tenant_id: tenant, external_cluster_key: id, environment: "synthetic" })) : [],
  };
} }));
vi.mock("@/preferences/OperatorPreferences", () => ({ useOperatorPreferences: () => ({
  locale: "en-US", theme: "dark", timeZone: "UTC", formatDateTime: (value: string) => value,
  setLocale: vi.fn(), setTimeZone: vi.fn(), setTheme: vi.fn(),
}) }));

beforeEach(() => {
  for (const fn of [state.listAssets, state.listKnowledgePage, state.getKnowledge, state.getKnowledgeHistory, state.searchKnowledge, state.reviewKnowledge, state.feedbackKnowledge]) fn.mockReset();
  state.subject = "reader"; state.loading = false; state.roster = true; state.revision = 0;
  firstScope = undefined;
  const item = { ...knowledgeItem(), cluster_id: otherCluster, title: "Authorized B focus" };
  const history = knowledgeHistory(item);
  history.feedback = [{ id: "feedback-b", cluster_id: otherCluster, actor_subject: "b-reader", kind: "useful",
    comment: "B_HISTORY_CANARY", created_at: history.observed_at, details_truncated: false }];
  state.listAssets.mockResolvedValue({ items: [], partial: false, warnings: [], observed_at: history.observed_at });
  state.listKnowledgePage.mockImplementation(async (selected: string) => ({
    items: selected === otherCluster ? [item] : [], next_cursor: null, partial: false, warnings: [], observed_at: history.observed_at,
  }));
  state.getKnowledge.mockResolvedValue(item);
  state.getKnowledgeHistory.mockResolvedValue(history);
});

function useTestState() {
  useSyncExternalStore((listener) => { state.listeners.add(listener); return () => { state.listeners.delete(listener); }; }, () => state.revision);
}
function changeSession(next: Partial<Pick<typeof state, "subject" | "loading" | "roster">>) {
  act(() => { Object.assign(state, next); state.revision++; for (const listener of [...state.listeners]) listener(); });
}
let firstScope: ReturnType<typeof useWorkspaceScope> | undefined;
function ScopeIntentProbe() {
  const scope = useWorkspaceScope();
  firstScope ??= scope;
  return null;
}
function mount(selectedCluster = cluster, pathname = "/knowledge") {
  const item = knowledgeItem();
  window.history.replaceState(null, "", `${pathname}?cluster_id=${selectedCluster}&focus=${item.id}&tab=existing`);
  const router = createBrowserRouter([{ element: <AppLayout />, children: [{ path: "/knowledge", element: <>
    <ScopeIntentProbe /><KnowledgePage />
    <Link to={`/knowledge?cluster_id=${otherCluster}&focus=${item.id}&tab=existing`}>Open old item link</Link>
  </> }] }]);
  return render(<RouterProvider router={router} />);
}

describe("knowledge focus through the actual workspace layout", () => {
  it("opens an authorized old B link explicitly and never revives it on ordinary A/B toolbar changes", async () => {
    const item = { ...knowledgeItem(), cluster_id: otherCluster, title: "Authorized B focus" };
    mount(cluster, "/knowledge/");
    const user = userEvent.setup();
    const toolbar = () => screen.getByRole("combobox", { name: "Authorized cluster" });
    const expectHiddenDetail = () => {
      for (const detail of screen.queryAllByRole("region", { name: "Item details" })) {
        expect(within(detail).queryByRole("heading", { name: item.title })).not.toBeInTheDocument();
        expect(detail.querySelector(".knowledge-history")).toBeNull();
        expect(detail.querySelector(".knowledge-actions")).toBeNull();
      }
      expect(document.body.innerHTML).not.toContain("B_HISTORY_CANARY");
      expect(document.body.innerHTML).not.toContain("URI_SECRET");
    };

    const explicitSwitch = await screen.findByRole("button", { name: "Switch to this cluster and open item" });
    expect(screen.queryByRole("heading", { name: item.title })).not.toBeInTheDocument();
    expectHiddenDetail();
    await user.click(explicitSwitch);
    const opened = await screen.findByRole("region", { name: "Item details" });
    expect(await within(opened).findByRole("heading", { name: item.title })).toBeVisible();
    expect(await within(opened).findByText("B_HISTORY_CANARY")).toBeVisible();
    expect(toolbar()).toHaveValue(otherCluster);

    await user.selectOptions(toolbar(), cluster);
    await waitFor(() => expect(toolbar()).toHaveValue(cluster));
    expect(screen.queryByRole("heading", { name: item.title })).not.toBeInTheDocument();
    expectHiddenDetail();
    await user.selectOptions(toolbar(), otherCluster);
    await waitFor(() => expect(state.listKnowledgePage).toHaveBeenLastCalledWith(otherCluster, expect.any(AbortSignal)));
    expect(await within(screen.getByRole("region", { name: "Knowledge items" })).findByRole("heading", { name: item.title })).toBeVisible();
    expectHiddenDetail();

    expect(new URLSearchParams(window.location.search).has("focus")).toBe(false);
    expect(new URLSearchParams(window.location.search).get("tab")).toBe("existing");
    await user.click(screen.getByRole("link", { name: "Open old item link" }));
    const reopened = await screen.findByRole("region", { name: "Item details" });
    expect(await within(reopened).findByRole("heading", { name: item.title })).toBeVisible();
    expect(await within(reopened).findByText("B_HISTORY_CANARY")).toBeVisible();
    expect(state.searchKnowledge).not.toHaveBeenCalled();
    expect(state.reviewKnowledge).not.toHaveBeenCalled();
    expect(state.feedbackKnowledge).not.toHaveBeenCalled();
  });

  it("preserves an initial old link through the first authorized-roster load", async () => {
    state.loading = true; state.roster = false;
    mount();
    expect(screen.getByText("Loading authorized workspace…")).toBeVisible();
    expect(state.getKnowledge).not.toHaveBeenCalled();
    changeSession({ loading: false, roster: true });
    const explicitSwitch = await screen.findByRole("button", { name: "Switch to this cluster and open item" });
    expect(new URLSearchParams(window.location.search).get("focus")).toBe(knowledgeItem().id);
    expect(screen.queryByRole("heading", { name: "Authorized B focus" })).not.toBeInTheDocument();
    await userEvent.click(explicitSwitch);
    expect(await within(screen.getByRole("region", { name: "Item details" })).findByText("B_HISTORY_CANARY")).toBeVisible();
  });

  it("consumes an old focus across a principal change and roster-driven outlet unmount, rejecting the old switch closure", async () => {
    mount(otherCluster);
    expect(await screen.findByText("B_HISTORY_CANARY")).toBeVisible();
    const signal = state.getKnowledge.mock.calls[0][1] as AbortSignal;
    const previousScope = firstScope!;
    changeSession({ subject: "another-reader", loading: true, roster: false });
    expect(screen.getByText("Loading authorized workspace…")).toBeVisible();
    expect(signal.aborted).toBe(true);
    expect(document.body.innerHTML).not.toContain("B_HISTORY_CANARY");
    await waitFor(() => expect(new URLSearchParams(window.location.search).has("focus")).toBe(false));
    changeSession({ loading: false, roster: true });
    expect(await within(screen.getByRole("region", { name: "Knowledge items" })).findByRole("heading", { name: "Authorized B focus" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "Item details" })).not.toBeInTheDocument();
    expect(state.getKnowledge).toHaveBeenCalledTimes(1);
    act(() => previousScope.setScope({ clusterId: otherCluster, window: previousScope.window }, { knowledgeFocus: knowledgeItem().id }));
    expect(new URLSearchParams(window.location.search).has("focus")).toBe(false);
    expect(state.getKnowledge).toHaveBeenCalledTimes(1);
    await userEvent.click(screen.getByRole("link", { name: "Open old item link" }));
    expect(await screen.findByText("B_HISTORY_CANARY")).toBeVisible();
  });

  it("rejects a malformed item intent or a target outside the authenticated cluster set", async () => {
    mount();
    await screen.findByRole("button", { name: "Switch to this cluster and open item" });
    const before = window.location.href;
    const scope = firstScope!;
    act(() => scope.setScope({ clusterId: otherCluster, window: scope.window }, { knowledgeFocus: "not-an-item-id" }));
    expect(window.location.href).toBe(before);
    act(() => scope.setScope({ clusterId: "10000000-0000-4000-8000-000000000099", window: scope.window }, { knowledgeFocus: knowledgeItem().id }));
    expect(window.location.href).toBe(before);
    expect(state.getKnowledge).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("heading", { name: "Authorized B focus" })).not.toBeInTheDocument();
    expect(state.searchKnowledge).not.toHaveBeenCalled();
    expect(state.reviewKnowledge).not.toHaveBeenCalled();
    expect(state.feedbackKnowledge).not.toHaveBeenCalled();
  });
});
