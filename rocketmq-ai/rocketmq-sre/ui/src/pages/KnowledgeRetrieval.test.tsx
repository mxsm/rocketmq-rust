import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import type { KnowledgeSearchInput } from "@/api/knowledgeClient";
import { knowledgeChunk, knowledgeHistory, knowledgeItem, searchPage, cluster, tenant, otherCluster } from "@/features/knowledge/test-fixtures";
import { KnowledgePage } from "./KnowledgePage";

const state = vi.hoisted(() => ({
  cluster: "", tenant: "", subject: "reader", roles: ["viewer"], locale: "en-US",
  listKnowledge: vi.fn(), listKnowledgePage: vi.fn(), setCluster: vi.fn(), searchKnowledge: vi.fn(), getKnowledge: vi.fn(), getKnowledgeHistory: vi.fn(), reviewKnowledge: vi.fn(), feedbackKnowledge: vi.fn(),
}));
const api = {
  listKnowledge: state.listKnowledge, listKnowledgePage: state.listKnowledgePage, searchKnowledge: state.searchKnowledge, getKnowledge: state.getKnowledge,
  getKnowledgeHistory: state.getKnowledgeHistory, reviewKnowledge: state.reviewKnowledge, feedbackKnowledge: state.feedbackKnowledge,
};
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => ({ api, demoMode: false }) }));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => ({ session: { tenantId: state.tenant, subject: state.subject, clusterIds: [cluster, otherCluster], roles: state.roles } }) }));
vi.mock("@/hooks/useClusterScope", () => ({ useClusterScope: () => ({ clusterId: state.cluster, clusters: [], setClusterId: (id: string) => { state.cluster = id; state.setCluster(id); } }) }));
vi.mock("@/preferences/OperatorPreferences", () => ({ useOperatorPreferences: () => ({ locale: state.locale, formatDateTime: (value: string) => value }) }));

beforeEach(() => {
  state.cluster = cluster; state.tenant = tenant; state.subject = "reader"; state.roles = ["viewer"]; state.locale = "en-US";
  for (const fn of Object.values(api)) fn.mockReset();
  state.setCluster.mockReset();
  state.listKnowledgePage.mockResolvedValue({ items: [knowledgeItem()], next_cursor: null, partial: false, warnings: [], observed_at: "2026-11-17T01:00:00Z" });
  state.searchKnowledge.mockImplementation(async (input: KnowledgeSearchInput) => searchPage(input));
  state.getKnowledge.mockResolvedValue(knowledgeItem());
  state.getKnowledgeHistory.mockResolvedValue(knowledgeHistory());
  state.feedbackKnowledge.mockResolvedValue(knowledgeItem()); state.reviewKnowledge.mockResolvedValue(knowledgeItem());
});

function mount(query = "") {
  window.history.replaceState(null, "", `/knowledge${query}`);
  const router = createBrowserRouter([{ path: "/knowledge", element: <KnowledgePage /> }]);
  const root = render(<RouterProvider router={router} />);
  return { ...root, refresh: () => act(async () => root.rerender(<RouterProvider router={createBrowserRouter([{ path: "/knowledge", element: <KnowledgePage /> }])} />)) };
}
async function search() {
  const user = userEvent.setup();
  await user.type(screen.getByRole("searchbox", { name: "Search text" }), "消费积压");
  await user.type(screen.getByRole("textbox", { name: "RocketMQ semantic version" }), "5.3.0");
  await user.click(screen.getByRole("button", { name: "Search" }));
  return user;
}
function deferred<T>() { let resolve!: (value: T) => void; let reject!: (reason: unknown) => void; const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; }); return { promise, resolve, reject }; }

describe("versioned reviewed retrieval", () => {
  it("requires an explicit RocketMQ version and forwards UTF-8 text and exact selectors only after submission", async () => {
    mount();
    expect(await screen.findByRole("heading", { name: knowledgeItem().title })).toBeVisible();
    expect(screen.getByRole("textbox", { name: "RocketMQ semantic version" })).toHaveValue("");
    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(state.searchKnowledge).not.toHaveBeenCalled();
    await search();
    expect(await screen.findByRole("heading", { name: knowledgeChunk().title })).toBeVisible();
    expect(state.searchKnowledge).toHaveBeenCalledWith({ cluster_id: cluster, q: "消费积压", component: null, rocketmq_version: "5.3.0", include_unvalidated: false, limit: 20 }, expect.any(AbortSignal));
    expect(document.body.innerHTML).not.toContain("URI_SECRET");
  });

  it("enforces the 500 UTF-8 byte query bound rather than 500 JavaScript characters", async () => {
    mount();
    fireEvent.change(screen.getByRole("searchbox", { name: "Search text" }), { target: { value: "积".repeat(167) } });
    fireEvent.change(screen.getByRole("textbox", { name: "RocketMQ semantic version" }), { target: { value: "5.3.0" } });
    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(state.searchKnowledge).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent("UTF-8");
  });

  it("labels a locally filtered first page rather than claiming the corpus has no matches", async () => {
    state.listKnowledgePage.mockResolvedValue({ items: [knowledgeItem()], next_cursor: "next-page", partial: false, warnings: [], observed_at: "2026-11-17T01:00:00Z" });
    mount();
    expect(await screen.findByText(/Only one page is loaded/)).toBeVisible();
    await userEvent.type(screen.getByRole("searchbox", { name: "Filter knowledge metadata" }), "not-on-this-page");
    expect(screen.getByText(/filter covers loaded items only/)).toBeVisible();
    expect(screen.getByText("No matching knowledge items")).toBeVisible();
    expect(state.searchKnowledge).not.toHaveBeenCalled();
  });

  it("shows bounded empty and ineligible contexts independently from search completeness", async () => {
    const page = searchPage(); page.items = []; page.bounds.candidate_limit_reached = true; page.bounds.scanned_candidates = 1024;
    state.searchKnowledge.mockResolvedValueOnce(page);
    mount(); await search();
    expect(await screen.findByText("This bounded search returned no results.")).toBeVisible();
    expect(screen.getByText(/candidate limit was reached/)).toBeVisible();
    const next = searchPage(); next.partial = true; next.bounds.results_truncated = true;
    Object.assign(next.items[0], { eligible_for_diagnosis: false, review_status: "draft", expired: true, validity: ["not_yet_valid"], exclusion_reasons: ["not_validated", "knowledge_expired"] });
    state.searchKnowledge.mockResolvedValueOnce(next);
    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText("Not yet valid")).toBeVisible();
    expect(screen.getByText("Ineligible as diagnostic fact")).toBeVisible();
    expect(screen.getByText(/Additional matches/)).toBeVisible();
  });

  it.each(["legacy", "tenant", "cluster", "query", "version", "bounds"])("hides %s wrong-wire results without crashing or inventing empty completeness", async (kind) => {
    state.searchKnowledge.mockImplementation(async (input: KnowledgeSearchInput) => {
      const page = searchPage(input);
      if (kind === "legacy") return { items: page.items, partial: false };
      if (kind === "tenant") page.binding.tenant_id = "foreign-tenant";
      if (kind === "cluster") page.binding.cluster_id = otherCluster;
      if (kind === "query") page.binding.q = "different";
      if (kind === "version") page.binding.rocketmq_version = "4.9.0";
      if (kind === "bounds") return { ...page, bounds: null };
      return page;
    });
    mount(); await search();
    expect(await screen.findByRole("alert")).toHaveTextContent("does not match");
    expect(screen.queryByText(knowledgeChunk().content)).not.toBeInTheDocument();
    expect(screen.queryByText("This bounded search returned no results.")).not.toBeInTheDocument();
  });

  it("immediately hides results on selector changes and rejects an ignored-abort late response", async () => {
    const late = deferred<ReturnType<typeof searchPage>>();
    state.searchKnowledge.mockReturnValueOnce(late.promise);
    mount(); const user = await search();
    const signal = state.searchKnowledge.mock.calls[0][1] as AbortSignal;
    await user.type(screen.getByRole("textbox", { name: "Component" }), "store");
    expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(searchPage()));
    expect(screen.queryByText(knowledgeChunk().content)).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Search results" })).not.toBeInTheDocument();
  });

  it("does not let a late failure replace the state of an edited search", async () => {
    const late = deferred<ReturnType<typeof searchPage>>(); state.searchKnowledge.mockReturnValueOnce(late.promise);
    mount(); const user = await search();
    await user.clear(screen.getByRole("textbox", { name: "RocketMQ semantic version" }));
    await act(async () => late.reject(new Error("old-request-failure")));
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Search results" })).not.toBeInTheDocument();
  });

  it("distinguishes server errors from an empty result and offers a read retry", async () => {
    state.searchKnowledge.mockRejectedValueOnce(new Error("DO_NOT_REFLECT_PRIVATE_SERVER_ERROR"));
    mount(); await search();
    expect(await screen.findByRole("alert")).toHaveTextContent("not an empty result");
    expect(document.body.innerHTML).not.toContain("DO_NOT_REFLECT");
    await userEvent.click(within(screen.getByRole("alert")).getByRole("button", { name: "Read again" }));
    expect(await screen.findByText(knowledgeChunk().content)).toBeVisible();
  });
});

describe("authorized knowledge focus and provenance", () => {
  it("opens a valid legacy focus without searching and retains metadata when history is unavailable", async () => {
    state.getKnowledgeHistory.mockRejectedValue(new Error("404"));
    mount(`?focus=${knowledgeItem().id}`);
    const detail = await screen.findByRole("region", { name: "Item details" });
    expect(await within(detail).findByRole("heading", { name: knowledgeItem().title })).toBeVisible();
    expect(within(detail).getByText(/read failed/)).toBeVisible();
    expect(state.searchKnowledge).not.toHaveBeenCalled();
    expect(document.body.innerHTML).not.toContain("URI_SECRET");
  });

  it("rejects invalid focus without requesting an item", async () => {
    mount("?focus=not-a-uuid");
    await screen.findByRole("heading", { name: knowledgeItem().title });
    expect(screen.getByRole("alert")).toHaveTextContent("link is invalid");
    expect(state.getKnowledge).not.toHaveBeenCalled();
  });

  it("does not display a cross-scope item or its history", async () => {
    state.getKnowledge.mockResolvedValue({ ...knowledgeItem(), cluster_id: "10000000-0000-4000-8000-000000000099", title: "FOREIGN_ITEM_CANARY" });
    mount(`?focus=${knowledgeItem().id}`);
    expect(await screen.findByRole("alert")).toHaveTextContent("does not match");
    expect(document.body.innerHTML).not.toContain("FOREIGN_ITEM_CANARY");
    expect(screen.queryByRole("button", { name: "Submit feedback" })).not.toBeInTheDocument();
  });

  it("offers an explicit authorized B-scope switch for an old B focus opened in A, without auto-opening it on later ordinary switches", async () => {
    const item = { ...knowledgeItem(), cluster_id: otherCluster, title: "Authorized B item" };
    state.getKnowledge.mockResolvedValue(item); state.getKnowledgeHistory.mockResolvedValue(knowledgeHistory(item));
    state.listKnowledgePage.mockResolvedValue({ items: [], next_cursor: null, partial: false, warnings: [], observed_at: "2026-11-17T01:00:00Z" });
    const root = mount(`?focus=${item.id}`);
    const button = await screen.findByRole("button", { name: "Switch to this cluster and open item" });
    expect(screen.queryByRole("heading", { name: item.title })).not.toBeInTheDocument();
    await userEvent.click(button); await root.refresh();
    expect(state.setCluster).toHaveBeenCalledWith(otherCluster);
    expect(await screen.findByRole("heading", { name: item.title })).toBeVisible();
    state.cluster = cluster; await root.refresh();
    expect(screen.queryByRole("region", { name: "Item details" })).not.toBeInTheDocument();
    state.cluster = otherCluster; await root.refresh();
    expect(screen.queryByRole("region", { name: "Item details" })).not.toBeInTheDocument();
    expect(state.searchKnowledge).not.toHaveBeenCalled();
  });

  it.each(["cluster", "principal"])("aborts focus and hides retained details immediately on %s changes", async (change) => {
    const late = deferred<ReturnType<typeof knowledgeItem>>();
    state.getKnowledge.mockReturnValue(late.promise);
    const root = mount(`?focus=${knowledgeItem().id}`);
    await waitFor(() => expect(state.getKnowledge).toHaveBeenCalledOnce());
    const signal = state.getKnowledge.mock.calls[0][1] as AbortSignal;
    if (change === "cluster") state.cluster = otherCluster; else state.subject = "another-reader";
    await root.refresh();
    expect(screen.queryByRole("region", { name: "Item details" })).not.toBeInTheDocument();
    expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(knowledgeItem()));
    expect(screen.queryByRole("region", { name: "Item details" })).not.toBeInTheDocument();
    expect(state.getKnowledge).toHaveBeenCalledOnce();
  });

  it("uses actual typed reviewers, scope-labeled feedback and stored postmortem relation without inferring from owner or URI", async () => {
    const item = { ...knowledgeItem(), cluster_id: null };
    const history = knowledgeHistory(item);
    history.reviews = [{ id: "r1", event_kind: null, actor_subject: null, from_status: null, to_status: null, item_content_hash: null,
      human_confirmed: null, ai_generated: null, reason: "Legacy free text names NOT_A_TYPED_REVIEWER", task_status: "completed",
      created_at: history.observed_at, completed_at: null, details_truncated: false }];
    history.feedback = [{ id: "f1", cluster_id: otherCluster, kind: "incorrect", comment: "Counterevidence from B", actor_subject: "actual-feedback-actor", created_at: history.observed_at, details_truncated: false }];
    state.getKnowledge.mockResolvedValue(item); state.getKnowledgeHistory.mockResolvedValue(history);
    mount(`?focus=${item.id}`);
    const detail = await screen.findByRole("region", { name: "Item details" });
    expect(await within(detail).findByText(/Typed provenance is unknown/)).toBeVisible();
    expect(within(detail).getByText(`Feedback scope: ${otherCluster}`)).toBeVisible();
    expect(within(detail).getByText(/actual-feedback-actor/)).toBeVisible();
    expect(within(detail).queryByText("Stored postmortem publication relation")).not.toBeInTheDocument();
    expect(within(detail).queryByRole("heading", { name: "Human review" })).not.toBeInTheDocument();
  });

  it("displays a genuine typed publication relation and bounded histories while keeping missing actors unknown", async () => {
    const history = knowledgeHistory(); history.reviews_truncated = true; history.feedback_truncated = true;
    history.postmortem = { postmortem_id: "50000000-0000-4000-8000-000000000001", incident_id: "50000000-0000-4000-8000-000000000002", revision: 3, confirmed_by: "actual-confirming-actor", confirmed_at: history.observed_at };
    history.reviews = [{ id: "typed-review", event_kind: "review", actor_subject: null, from_status: "in_review", to_status: "validated", item_content_hash: knowledgeItem().content_hash,
      human_confirmed: true, ai_generated: null, reason: "Actual counterexample considered", task_status: "completed", created_at: history.observed_at, completed_at: history.observed_at, details_truncated: false }];
    state.getKnowledgeHistory.mockResolvedValue(history);
    mount(`?focus=${knowledgeItem().id}`);
    expect(await screen.findByText("Stored postmortem publication relation")).toBeVisible();
    expect(screen.getByText(/actual-confirming-actor/)).toBeVisible();
    expect(screen.getByText(/older records are not fully displayed/)).toBeVisible();
    expect(screen.getByText("Recorded actor: Unknown / not supplied")).toBeVisible();
  });

  it("keeps valid metadata but hides history content bound to another item hash", async () => {
    const history = knowledgeHistory(); history.item_content_hash = "another-hash";
    history.feedback = [{ id: "bad", kind: "useful", cluster_id: cluster, comment: "WRONG_HISTORY_CANARY", created_at: history.observed_at, details_truncated: false }];
    state.getKnowledgeHistory.mockResolvedValue(history);
    mount(`?focus=${knowledgeItem().id}`);
    const detail = await screen.findByRole("region", { name: "Item details" });
    expect(await within(detail).findByRole("alert")).toHaveTextContent("does not match");
    expect(within(detail).getByRole("heading", { name: knowledgeItem().title })).toBeVisible();
    expect(document.body.innerHTML).not.toContain("WRONG_HISTORY_CANARY");
  });
});

describe("explicit scoped curation", () => {
  it.each([["operator"], ["sre-admin"]])("does not offer tenant-wide curation to %s alone", async (...roles) => {
    state.roles = roles;
    const item = { ...knowledgeItem(), cluster_id: null }; state.getKnowledge.mockResolvedValue(item); state.getKnowledgeHistory.mockResolvedValue(knowledgeHistory(item));
    mount(`?focus=${item.id}`);
    expect(await screen.findByRole("heading", { name: "Submit non-authoritative feedback" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Submit review" })).not.toBeInTheDocument();
  });

  it("requires a reason and explicit human confirmation to validate, without automatic retry after a lost response", async () => {
    state.roles = ["operator", "sre-admin"];
    const item = { ...knowledgeItem(), review_status: "in_review" as const, cluster_id: null };
    state.getKnowledge.mockResolvedValue(item); state.getKnowledgeHistory.mockResolvedValue(knowledgeHistory(item));
    state.reviewKnowledge.mockRejectedValue(new Error("LOST_PRIVATE_RESPONSE"));
    mount(`?focus=${item.id}`);
    const button = await screen.findByRole("button", { name: "Submit review" });
    expect(button).toBeDisabled();
    const user = userEvent.setup();
    await user.type(screen.getByRole("textbox", { name: "Review reason / counterevidence" }), "Reviewed actual applicability");
    expect(button).toBeDisabled();
    await user.click(screen.getByRole("checkbox", { name: /explicitly confirm/ })); await user.click(button);
    expect(await screen.findByText(/outcome is unconfirmed/)).toBeVisible();
    expect(state.reviewKnowledge).toHaveBeenCalledOnce();
    expect(state.reviewKnowledge).toHaveBeenCalledWith(item.id, { status: "validated", human_confirmed: true, reason: "Reviewed actual applicability" }, expect.any(AbortSignal));
    expect(button).toBeDisabled(); expect(document.body.innerHTML).not.toContain("LOST_PRIVATE");
  });

  it("lets a reader append feedback without promoting the item and ignores its late acknowledgement after an identity change", async () => {
    const late = deferred<ReturnType<typeof knowledgeItem>>(); state.feedbackKnowledge.mockReturnValueOnce(late.promise);
    const root = mount(`?focus=${knowledgeItem().id}`);
    const button = await screen.findByRole("button", { name: "Submit feedback" });
    const user = userEvent.setup();
    await user.selectOptions(screen.getByRole("combobox", { name: "Submit non-authoritative feedback" }), "incorrect");
    await user.type(screen.getByRole("textbox", { name: /Comment \(optional/ }), "Actual counterevidence");
    await user.click(button);
    expect(state.feedbackKnowledge).toHaveBeenCalledWith(knowledgeItem().id, { kind: "incorrect", comment: "Actual counterevidence" }, expect.any(AbortSignal));
    const signal = state.feedbackKnowledge.mock.calls[0][2] as AbortSignal;
    state.subject = "new-identity"; await root.refresh();
    expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(knowledgeItem()));
    expect(screen.queryByText(/Submission acknowledged/)).not.toBeInTheDocument();
    expect(state.feedbackKnowledge).toHaveBeenCalledOnce(); expect(state.reviewKnowledge).not.toHaveBeenCalled();
  });
});
