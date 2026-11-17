import type { KnowledgeSearchInput } from "@/api/knowledgeClient";
import type { KnowledgeItem, KnowledgeSearchPage } from "@/api/types";
import { isKnowledgeItem, isKnowledgeSearchPage } from "./responseGuards";

export const bytes = (value: string) => new TextEncoder().encode(value).length;
export const itemId = (value: string) => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

export function safeSource(value?: string | null): string | undefined {
  if (!value || bytes(value) > 2000 || [...value].some((c) => c.charCodeAt(0) < 32 || c.charCodeAt(0) === 127)) return undefined;
  try {
    const url = new URL(value);
    if (!(["http:", "https:"].includes(url.protocol) || (url.protocol === "rocketmq-sre:" && url.hostname === "postmortems"))) return undefined;
    if (url.username || url.password || url.search || url.hash) return undefined;
    return url.href;
  } catch { return undefined; }
}

export function validSearch(q: string, version: string, component: string) {
  return bytes(q.trim()) > 0 && bytes(q) <= 500 && bytes(component) <= 100 && bytes(version) <= 128 &&
    /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/.test(version.trim());
}

export function searchMatches(page: KnowledgeSearchPage, input: KnowledgeSearchInput, tenant: string) {
  if (!isKnowledgeSearchPage(page)) return false;
  const b = page.binding;
  return b.tenant_id === tenant && b.cluster_id === input.cluster_id && b.q === input.q &&
    (b.component ?? null) === (input.component ?? null) && b.rocketmq_version === input.rocketmq_version &&
    b.include_unvalidated === input.include_unvalidated && b.limit === input.limit;
}

export function itemMatches(item: KnowledgeItem, tenant: string, cluster: string) {
  return isKnowledgeItem(item) && item.tenant_id === tenant && (item.cluster_id == null || item.cluster_id === cluster);
}

export function reviewTransitions(status: KnowledgeItem["review_status"]): KnowledgeItem["review_status"][] {
  switch (status) {
    case "draft": return ["in_review", "deprecated", "expired"];
    case "in_review": return ["validated", "deprecated", "expired"];
    case "validated": return ["deprecated", "expired"];
    default: return [];
  }
}
