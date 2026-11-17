import { apiQuery, apiRequest } from "./client";
import type {
  KnowledgeFeedbackRequest,
  KnowledgeHistoryPage,
  KnowledgeItem,
  KnowledgePage,
  KnowledgeReviewRequest,
  KnowledgeSearchPage,
} from "./types";
import type { ApiRequestContext } from "@/auth/AuthContext";

export type KnowledgeSearchInput = Omit<KnowledgeSearchPage["binding"], "tenant_id">;

export interface KnowledgeSreApi {
  listKnowledgePage: (clusterId: string, signal?: AbortSignal) => Promise<KnowledgePage>;
  searchKnowledge: (input: KnowledgeSearchInput, signal?: AbortSignal) => Promise<KnowledgeSearchPage>;
  getKnowledge: (id: string, signal?: AbortSignal) => Promise<KnowledgeItem>;
  getKnowledgeHistory: (id: string, signal?: AbortSignal) => Promise<KnowledgeHistoryPage>;
  reviewKnowledge: (id: string, input: KnowledgeReviewRequest, signal?: AbortSignal) => Promise<KnowledgeItem>;
  feedbackKnowledge: (id: string, input: KnowledgeFeedbackRequest, signal?: AbortSignal) => Promise<KnowledgeItem>;
}

export function createKnowledgeClient(auth?: ApiRequestContext): KnowledgeSreApi {
  const path = (id: string) => `/v1/knowledge/${encodeURIComponent(id)}`;
  return {
    listKnowledgePage: (clusterId, signal) => apiRequest(apiQuery("/v1/knowledge", { cluster_id: clusterId }), { auth, signal }),
    searchKnowledge: (input, signal) => apiRequest(apiQuery("/v1/knowledge/search", {
      cluster_id: input.cluster_id, q: input.q, component: input.component ?? undefined,
      rocketmq_version: input.rocketmq_version, limit: String(input.limit),
      include_unvalidated: String(input.include_unvalidated),
    }), { auth, signal }),
    getKnowledge: (id, signal) => apiRequest(path(id), { auth, signal }),
    getKnowledgeHistory: (id, signal) => apiRequest(`${path(id)}/history`, { auth, signal }),
    reviewKnowledge: (id, body, signal) => apiRequest(`${path(id)}/review`, { auth, signal, body, method: "POST" }),
    feedbackKnowledge: (id, body, signal) => apiRequest(`${path(id)}/feedback`, { auth, signal, body, method: "POST" }),
  };
}
