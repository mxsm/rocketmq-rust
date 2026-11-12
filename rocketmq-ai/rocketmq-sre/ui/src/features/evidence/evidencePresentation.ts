import type { EvidenceRecord } from "@/api/types";
import {
  resourcePath,
  withinWindow,
  type WorkspaceScope,
} from "@/context/workspaceScope";

export type EvidenceMetadata = Omit<EvidenceRecord, "content"> & {
  storage: EvidenceRecord["content"]["storage"];
};
export type EvidenceAccess =
  | { kind: "selected_resource" }
  | {
      kind: "revision_evidence";
      revisionId: string;
      evidenceIds: readonly string[];
    };
export type EvidenceAvailability =
  | "available"
  | "partial"
  | "missing"
  | "not_verified";
export interface EvidenceFreshnessPolicy {
  maxAgeSeconds: number;
  asOf: string;
  sourceRef: string;
}

export function evidenceFreshness(
  evidence: EvidenceMetadata | EvidenceRecord,
  policy?: EvidenceFreshnessPolicy,
) {
  if (
    !policy ||
    !Number.isFinite(policy.maxAgeSeconds) ||
    policy.maxAgeSeconds <= 0
  )
    return "unknown";
  const elapsed =
    (Date.parse(policy.asOf) - Date.parse(evidence.observed_at)) / 1000;
  if (
    !Number.isFinite(elapsed) ||
    elapsed < 0 ||
    !Number.isFinite(evidence.freshness_seconds) ||
    evidence.freshness_seconds < 0
  )
    return "unknown";
  return Math.max(elapsed, evidence.freshness_seconds) > policy.maxAgeSeconds
    ? "stale"
    : "current";
}

export function metadataInScope(
  evidence: EvidenceRecord | EvidenceMetadata,
  requestedId: string,
  scope: WorkspaceScope,
  tenantId: string,
  access: EvidenceAccess,
) {
  if (
    evidence.evidence_id !== requestedId ||
    evidence.tenant_id !== tenantId ||
    evidence.cluster_id !== scope.clusterId
  )
    return false;
  if (
    access.kind === "revision_evidence" &&
    !access.evidenceIds.includes(requestedId)
  )
    return false;
  const selected = resourcePath(scope);
  if (
    !selected ||
    scope.resourceKind === "cluster" ||
    evidence.resource === selected
  )
    return true;
  return (
    access.kind === "revision_evidence" &&
    access.evidenceIds.includes(requestedId)
  );
}

export function toEvidenceMetadata(evidence: EvidenceRecord): EvidenceMetadata {
  const { content, ...metadata } = evidence;
  return { ...metadata, storage: content.storage };
}

export function evidenceAvailability(
  evidence: EvidenceMetadata | EvidenceRecord,
): EvidenceAvailability {
  if (evidence.coverage === "missing") return "missing";
  if (evidence.coverage === "not_production_verified") return "not_verified";
  return evidence.partial || evidence.coverage === "partial"
    ? "partial"
    : "available";
}

export function evidenceOutsideWindow(
  evidence: EvidenceMetadata | EvidenceRecord,
  scope: WorkspaceScope,
) {
  return !withinWindow(evidence.observed_at, scope.window);
}

/** Content is a bounded display value, never executable HTML or a full configuration. */
export function safeEvidenceJson(value: unknown) {
  let remaining = 500;
  const visit = (item: unknown, depth: number): unknown => {
    if (remaining-- <= 0 || depth > 6) return "[truncated]";
    if (typeof item === "string") {
      if (
        /\bBearer\s+\S+|-----BEGIN [^-\r\n]+-----|\b[a-z][a-z0-9+.-]*:\/\/[^/\s:@]+:[^/\s@]+@/i.test(
          item,
        )
      )
        return "[redacted]";
      return item.length > 2048 ? `${item.slice(0, 2048)}…` : item;
    }
    if (Array.isArray(item))
      return item.slice(0, 100).map((entry) => visit(entry, depth + 1));
    if (item && typeof item === "object") {
      return Object.fromEntries(
        Object.entries(item)
          .slice(0, 64)
          .map(([key, entry]) => {
            const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
            const sensitive =
              /body|payload|token|secret|password|credential|privatekey|tls|acl|apikey|accesskey|authorization|cookie|config|certificate|pem|keystore|truststore|clientcert|servercert|cabundle|certchain|pfx|pkcs|ssl/.test(
                normalized,
              );
            return [key, sensitive ? "[redacted]" : visit(entry, depth + 1)];
          }),
      );
    }
    return item;
  };
  const serialized =
    JSON.stringify(visit(value ?? { status: "missing" }, 0), null, 2) ??
    "missing";
  return serialized.length > 8192
    ? `${serialized.slice(0, 8192)}\n… truncated`
    : serialized;
}
