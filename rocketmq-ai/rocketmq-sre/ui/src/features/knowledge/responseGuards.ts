import type { KnowledgeChunkView, KnowledgeHistoryPage, KnowledgeItem, KnowledgeSearchPage } from "@/api/types";

const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown, max = 2000): v is string => typeof v === "string" && v.length <= max;
const nullableText = (v: unknown, max = 2000) => v == null || text(v, max);
const date = (v: unknown) => text(v, 100) && Number.isFinite(Date.parse(v));
const nullableDate = (v: unknown) => v == null || date(v);
const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const booleanOrNull = (v: unknown) => v == null || typeof v === "boolean";
const status = (v: unknown) => typeof v === "string" && ["draft", "in_review", "validated", "deprecated", "expired"].includes(v);
const nullableStatus = (v: unknown) => v == null || status(v);

export function isKnowledgeItem(v: unknown): v is KnowledgeItem {
  return object(v) && ["id", "tenant_id", "title", "component", "rocketmq_version_range", "source_version", "owner", "content_hash", "sensitivity"].every((k) => text(v[k])) &&
    (v.cluster_id == null || text(v.cluster_id)) && status(v.review_status) && typeof v.conflict === "boolean" &&
    date(v.review_due_at) && date(v.updated_at) && date(v.created_at) && nullableDate(v.valid_from) && nullableDate(v.valid_until);
}

function isChunk(v: unknown): v is KnowledgeChunkView {
  return object(v) && ["id", "knowledge_item_id", "title", "component", "source_version", "rocketmq_version_range", "item_hash", "chunk_hash", "sensitivity"].every((k) => text(v[k])) &&
    text(v.content, 8192) && nullableText(v.heading, 1024) && nullableText(v.source_uri_display) && status(v.review_status) &&
    ["conflict", "expired", "content_truncated", "metadata_truncated", "eligible_for_diagnosis"].every((k) => typeof v[k] === "boolean") &&
    date(v.review_due_at) && nullableDate(v.valid_from) && nullableDate(v.valid_until) &&
    Array.isArray(v.validity) && v.validity.length <= 5 && v.validity.every((value) => typeof value === "string" && ["current", "not_yet_valid", "review_overdue", "validity_ended", "status_expired"].includes(value)) &&
    Array.isArray(v.exclusion_reasons) && v.exclusion_reasons.length <= 16 && v.exclusion_reasons.every((r) => text(r, 100));
}

export function isKnowledgeSearchPage(v: unknown): v is KnowledgeSearchPage {
  if (!object(v) || !object(v.binding) || !object(v.bounds)) return false;
  const b = v.binding, n = v.bounds;
  return date(v.observed_at) && typeof v.partial === "boolean" && Array.isArray(v.items) && v.items.length <= 100 && v.items.every(isChunk) &&
    ["tenant_id", "cluster_id", "q", "rocketmq_version"].every((k) => text(b[k])) && nullableText(b.component, 100) &&
    integer(b.limit) && b.limit > 0 && b.limit <= 100 && v.items.length <= b.limit && typeof b.include_unvalidated === "boolean" &&
    integer(n.candidate_limit) && integer(n.scanned_candidates) && n.scanned_candidates <= n.candidate_limit &&
    typeof n.candidate_limit_reached === "boolean" && typeof n.results_truncated === "boolean";
}

export function isKnowledgeHistory(v: unknown): v is KnowledgeHistoryPage {
  if (!object(v) || !Array.isArray(v.reviews) || !Array.isArray(v.feedback) || v.reviews.length > 32 || v.feedback.length > 32) return false;
  return ["knowledge_item_id", "tenant_id", "item_content_hash"].every((k) => text(v[k])) &&
    (v.cluster_id == null || text(v.cluster_id)) && nullableText(v.source_uri_display) && date(v.observed_at) &&
    typeof v.reviews_truncated === "boolean" && typeof v.feedback_truncated === "boolean" &&
    v.reviews.every((r: unknown) => object(r) && text(r.id) && nullableText(r.actor_subject) &&
      (r.event_kind == null || ["import", "review", "feedback"].includes(String(r.event_kind))) &&
      nullableStatus(r.from_status) && nullableStatus(r.to_status) && nullableText(r.item_content_hash) &&
      booleanOrNull(r.human_confirmed) && booleanOrNull(r.ai_generated) && text(r.reason) && text(r.task_status) && date(r.created_at) && nullableDate(r.completed_at) && typeof r.details_truncated === "boolean") &&
    v.feedback.every((r: unknown) => object(r) && text(r.id) && ["useful", "incorrect", "outdated"].includes(String(r.kind)) &&
      (r.cluster_id == null || text(r.cluster_id)) && nullableText(r.comment) && nullableText(r.actor_subject) && date(r.created_at) && typeof r.details_truncated === "boolean") &&
    (v.postmortem == null || (object(v.postmortem) && text(v.postmortem.postmortem_id) && text(v.postmortem.incident_id) &&
      integer(v.postmortem.revision) && nullableText(v.postmortem.confirmed_by) && nullableDate(v.postmortem.confirmed_at)));
}
