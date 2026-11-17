import type { KnowledgeChunkView, KnowledgeHistoryPage, KnowledgeItem, KnowledgeSearchPage } from "@/api/types";
import type { KnowledgeSearchInput } from "@/api/knowledgeClient";

export const tenant = "30000000-0000-4000-8000-000000000001";
export const cluster = "10000000-0000-4000-8000-000000000001";
export const otherCluster = "10000000-0000-4000-8000-000000000002";
export const now = "2026-11-17T01:00:00Z";

export function knowledgeItem(): KnowledgeItem {
  return {
    id: "20000000-0000-4000-8000-000000000001", tenant_id: tenant, cluster_id: cluster,
    title: "Synthetic no-summary wire item", component: "consumer", rocketmq_version_range: ">=5.0.0, <6.0.0",
    source_uri: "https://user:URI_SECRET@example.invalid/source?token=URI_SECRET#URI_SECRET",
    source_version: "synthetic-corpus-v1", owner: "declared-owner-not-reviewer", review_status: "validated",
    review_due_at: "2026-12-01T00:00:00Z", sensitivity: "internal", content_hash: `sha256:${"a".repeat(64)}`,
    conflict: false, valid_from: null, valid_until: null, created_at: now, updated_at: now,
  };
}

export function knowledgeChunk(): KnowledgeChunkView {
  const item = knowledgeItem();
  return {
    id: "40000000-0000-4000-8000-000000000001", knowledge_item_id: item.id,
    title: "Synthetic Chinese retrieval", heading: "反例与适用性", component: item.component,
    content: "消费积压需要检查真实消费进度。此文本仅是 Synthetic 测试材料。", source_uri: item.source_uri,
    source_uri_display: "https://example.invalid/source", source_version: item.source_version,
    rocketmq_version_range: item.rocketmq_version_range, valid_from: null, valid_until: null,
    review_due_at: item.review_due_at, validity: ["current"], sensitivity: "internal", item_hash: item.content_hash,
    chunk_hash: `sha256:${"b".repeat(64)}`, review_status: item.review_status, conflict: false, expired: false,
    content_truncated: false, metadata_truncated: false, eligible_for_diagnosis: true, exclusion_reasons: [],
  };
}

export function searchInput(): KnowledgeSearchInput {
  return { cluster_id: cluster, q: "消费积压", component: null, rocketmq_version: "5.3.0", limit: 20, include_unvalidated: false };
}

export function searchPage(input = searchInput()): KnowledgeSearchPage {
  return { items: [knowledgeChunk()], partial: false, observed_at: now, binding: { tenant_id: tenant, ...input },
    bounds: { candidate_limit: 1024, scanned_candidates: 1, candidate_limit_reached: false, results_truncated: false } };
}

export function knowledgeHistory(item = knowledgeItem()): KnowledgeHistoryPage {
  return { knowledge_item_id: item.id, tenant_id: item.tenant_id, cluster_id: item.cluster_id,
    item_content_hash: item.content_hash, source_uri_display: "https://example.invalid/source", observed_at: now,
    reviews: [], feedback: [], reviews_truncated: false, feedback_truncated: false, postmortem: null };
}
