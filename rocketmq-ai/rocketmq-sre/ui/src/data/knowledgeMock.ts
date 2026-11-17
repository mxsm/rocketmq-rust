import { ApiError } from "@/api/client";
import type { KnowledgeSreApi } from "@/api/knowledgeClient";
import type { KnowledgeHistoryPage, KnowledgeItem } from "@/api/types";
import type { ApiRequestContext } from "@/auth/AuthContext";

/** Metadata-only legacy demo corpus has no server retrieval or reviewer proof. */
export function createKnowledgeMock(items: KnowledgeItem[], auth: ApiRequestContext | undefined, wait: (signal?: AbortSignal) => Promise<void>): KnowledgeSreApi {
  const histories = new Map<string, KnowledgeHistoryPage>();
  const item = (id: string) => {
    const value = items.find((entry) => entry.id === id && (!auth || entry.tenant_id === auth.tenantId) &&
      (!entry.cluster_id || !auth || auth.clusterIds.includes(entry.cluster_id)));
    if (!value) throw new ApiError(404, "source_unavailable", "demo knowledge is unavailable");
    return value;
  };
  const history = (value: KnowledgeItem) => {
    let found = histories.get(value.id);
    if (!found) {
      found = {
        knowledge_item_id: value.id, tenant_id: value.tenant_id, cluster_id: value.cluster_id,
        item_content_hash: value.content_hash, source_uri_display: null,
        observed_at: new Date().toISOString(), reviews: [], feedback: [],
        reviews_truncated: false, feedback_truncated: false, postmortem: null,
      };
      histories.set(value.id, found);
    }
    return found;
  };
  return {
    listKnowledgePage: async (clusterId, signal) => {
      await wait(signal);
      if (auth && !auth.clusterIds.includes(clusterId)) throw new ApiError(403, "cluster_not_allowed", "demo cluster is unavailable");
      return { items: structuredClone(items.filter((value) => (!auth || value.tenant_id === auth.tenantId) && (!value.cluster_id || value.cluster_id === clusterId))),
        next_cursor: null, partial: false, warnings: [], observed_at: new Date().toISOString() };
    },
    searchKnowledge: async (_input, signal) => {
      await wait(signal);
      // Do not fabricate a version-applicability judgment from metadata-only fixtures.
      throw new ApiError(503, "source_unavailable", "server retrieval is unavailable in the legacy demo corpus");
    },
    getKnowledge: async (id, signal) => { await wait(signal); return structuredClone(item(id)); },
    getKnowledgeHistory: async (id, signal) => {
      await wait(signal);
      return structuredClone({ ...history(item(id)), observed_at: new Date().toISOString() });
    },
    feedbackKnowledge: async (id, input, signal) => {
      await wait(signal);
      const value = item(id);
      const records = history(value);
      records.feedback.unshift({ id: crypto.randomUUID(), cluster_id: value.cluster_id, kind: input.kind, comment: input.comment,
        actor_subject: auth?.subject ?? "synthetic-demo-reader", created_at: new Date().toISOString(), details_truncated: false });
      if (records.feedback.length > 32) {
        records.feedback.length = 32;
        records.feedback_truncated = true;
      }
      return structuredClone(value);
    },
    reviewKnowledge: async (_id, _input, signal) => {
      await wait(signal);
      throw new ApiError(503, "source_unavailable", "curation requires the versioned control-plane service");
    },
  };
}
