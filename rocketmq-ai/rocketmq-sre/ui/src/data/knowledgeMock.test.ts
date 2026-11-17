import { knowledgeItem } from "@/features/knowledge/test-fixtures";
import { isKnowledgeHistory } from "@/features/knowledge/responseGuards";
import { createKnowledgeMock } from "./knowledgeMock";

it("keeps repeated reader feedback within the public history shape without promoting metadata or fabricating reviews", async () => {
  const item = { ...knowledgeItem(), review_status: "draft" as const };
  const api = createKnowledgeMock([item], { token: "synthetic", tenantId: item.tenant_id, clusterIds: [item.cluster_id!], subject: "synthetic-reader", roles: ["viewer"] }, async () => {});
  for (let n = 0; n < 33; n++) await api.feedbackKnowledge(item.id, { kind: "incorrect", comment: `Counterexample ${n}` });
  const history = await api.getKnowledgeHistory(item.id);
  expect(isKnowledgeHistory(history)).toBe(true);
  expect(history.feedback_truncated).toBe(true);
  expect(history.feedback).toHaveLength(32);
  expect(history.feedback[0].comment).toBe("Counterexample 32");
  expect(history.feedback.at(-1)?.comment).toBe("Counterexample 1");
  expect(history.reviews).toEqual([]);
  expect(await api.getKnowledge(item.id)).toEqual(item);
});
