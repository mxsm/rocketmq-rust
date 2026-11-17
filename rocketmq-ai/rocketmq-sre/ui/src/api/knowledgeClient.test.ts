import { createHttpSreApi } from "./client";
import { knowledgeHistory, knowledgeItem, searchInput, searchPage } from "@/features/knowledge/test-fixtures";

afterEach(() => vi.restoreAllMocks());

it("uses fixed versioned knowledge reads and explicit POSTs with exact scope, body, auth and cancellation", async () => {
  const item = knowledgeItem();
  const spy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify(item), { headers: { "Content-Type": "application/json" } }));
  const auth = { token: "SYNTHETIC_TEST_TOKEN", tenantId: item.tenant_id, subject: "reader", roles: ["operator"], clusterIds: [item.cluster_id!] };
  const api = createHttpSreApi(auth);
  const controller = new AbortController();
  const reply = (value: unknown) => spy.mockResolvedValueOnce(new Response(JSON.stringify(value), { headers: { "Content-Type": "application/json" } }));
  reply({ items: [item], next_cursor: "next", partial: false, warnings: [], observed_at: item.updated_at });
  expect((await api.listKnowledgePage(item.cluster_id!, controller.signal)).next_cursor).toBe("next");
  reply(searchPage()); await api.searchKnowledge(searchInput(), controller.signal);
  reply(item); await api.getKnowledge(item.id, controller.signal);
  reply(knowledgeHistory()); await api.getKnowledgeHistory(item.id, controller.signal);
  reply(item); await api.reviewKnowledge(item.id, { status: "deprecated", reason: "Counterexample", human_confirmed: false }, controller.signal);
  reply(item); await api.feedbackKnowledge(item.id, { kind: "incorrect", comment: "Observed counterexample" }, controller.signal);
  const urls = spy.mock.calls.map(([path]) => new URL(String(path), "http://localhost"));
  expect(urls.map((url) => url.pathname)).toEqual(["/v1/knowledge", "/v1/knowledge/search", `/v1/knowledge/${item.id}`, `/v1/knowledge/${item.id}/history`, `/v1/knowledge/${item.id}/review`, `/v1/knowledge/${item.id}/feedback`]);
  expect(urls[1].searchParams.get("q")).toBe("消费积压");
  expect(urls[1].searchParams.get("rocketmq_version")).toBe("5.3.0");
  expect(urls[1].searchParams.get("cluster_id")).toBe(item.cluster_id);
  expect(urls[1].searchParams.has("component")).toBe(false);
  for (const [, options] of spy.mock.calls) {
    expect(options?.signal).toBe(controller.signal);
    expect(new Headers(options?.headers).get("Authorization")).toBe(`Bearer ${auth.token}`);
  }
  expect(spy.mock.calls.map(([, options]) => options?.method ?? "GET")).toEqual(["GET", "GET", "GET", "GET", "POST", "POST"]);
  expect(JSON.parse(String(spy.mock.calls[4][1]?.body))).toEqual({ status: "deprecated", reason: "Counterexample", human_confirmed: false });
});
