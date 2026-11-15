import type { SubmitExecutionRequest } from "@/api/types";

/** Browser-owned uncertainty only. No credential, grant or raw action parameter. */
export interface SubmissionIntentOwner {
  tenantId: string;
  subject: string;
  clusterId: string;
  principalKey: string;
  scopeKey: string;
}
export interface StoredSubmissionIntent {
  schema_version: "rocketmq-sre.ui-execution-intent.v1";
  owner: SubmissionIntentOwner;
  request: SubmitExecutionRequest;
}
export type SubmissionIntentRead =
  | { status: "empty" }
  | { status: "uncertain"; intent: StoredSubmissionIntent }
  | { status: "unavailable" };
type StorageAccess = Pick<Storage, "getItem" | "setItem" | "removeItem">;
const SCHEMA = "rocketmq-sre.ui-execution-intent.v1";
const MAX_BYTES = 16 * 1024;
const digest = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/u.test(value);
const object = (value: unknown): value is Record<string, unknown> => Boolean(value && typeof value === "object" && !Array.isArray(value));
const keysMatch = (value: Record<string, unknown>, keys: readonly string[]) => Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));

function storageKey(owner: SubmissionIntentOwner, planId: string) {
  // Role/window changes cannot hide an outstanding intent and allow another
  // write. Their exact record must match before any selector is reused.
  return "rocketmq-sre.execution-intent.v1:" + JSON.stringify([owner.tenantId, owner.subject, planId]);
}

export function readSubmissionIntent(
  storage: StorageAccess,
  owner: SubmissionIntentOwner,
  current: Pick<SubmitExecutionRequest, "plan_id" | "plan_hash" | "precondition_hash">,
): SubmissionIntentRead {
  try {
    const raw = storage.getItem(storageKey(owner, current.plan_id));
    if (raw === null) return { status: "empty" };
    if (raw.length > MAX_BYTES || new TextEncoder().encode(raw).byteLength > MAX_BYTES) return { status: "unavailable" };
    const value: unknown = JSON.parse(raw);
    if (!object(value) || !keysMatch(value, ["schema_version", "owner", "request"]) || value.schema_version !== SCHEMA ||
      !object(value.owner) || !keysMatch(value.owner, ["tenantId", "subject", "clusterId", "principalKey", "scopeKey"]) ||
      !Object.entries(owner).every(([key, field]) => value.owner && (value.owner as Record<string, unknown>)[key] === field) ||
      !object(value.request) || !keysMatch(value.request, ["plan_id", "plan_hash", "precondition_hash", "idempotency_key"]) ||
      value.request.plan_id !== current.plan_id || value.request.plan_hash !== current.plan_hash ||
      value.request.precondition_hash !== current.precondition_hash || !digest(value.request.plan_hash) || !digest(value.request.precondition_hash) ||
      typeof value.request.idempotency_key !== "string" || !/^[A-Za-z0-9_-]{1,128}$/u.test(value.request.idempotency_key)) return { status: "unavailable" };
    return { status: "uncertain", intent: value as unknown as StoredSubmissionIntent };
  } catch {
    // Corruption/quota/blocked storage never becomes permission to submit again.
    return { status: "unavailable" };
  }
}

/** Persist before POST. Refuse to replace any unresolved intent, including a gap. */
export function rememberSubmissionIntent(storage: StorageAccess, owner: SubmissionIntentOwner, request: SubmitExecutionRequest) {
  if (!object(request) || !keysMatch(request, ["plan_id", "plan_hash", "precondition_hash", "idempotency_key"]) ||
    typeof request.plan_id !== "string" || !request.plan_id || request.plan_id.length > 128 ||
    !digest(request.plan_hash) || !digest(request.precondition_hash) ||
    typeof request.idempotency_key !== "string" || !/^[A-Za-z0-9_-]{1,128}$/u.test(request.idempotency_key)) return false;
  if (readSubmissionIntent(storage, owner, request).status !== "empty") return false;
  const value: StoredSubmissionIntent = { schema_version: SCHEMA, owner, request };
  const raw = JSON.stringify(value);
  if (raw.length > MAX_BYTES || new TextEncoder().encode(raw).byteLength > MAX_BYTES) return false;
  try {
    storage.setItem(storageKey(owner, request.plan_id), raw);
    const read = readSubmissionIntent(storage, owner, request);
    return read.status === "uncertain" && read.intent.request.idempotency_key === request.idempotency_key;
  } catch { return false; }
}

/** Call only after an exact accepted receipt/lookup; NotObserved never calls this. */
export function resolveSubmissionIntent(storage: StorageAccess, owner: SubmissionIntentOwner, request: SubmitExecutionRequest) {
  const read = readSubmissionIntent(storage, owner, request);
  if (read.status !== "uncertain" || read.intent.request.idempotency_key !== request.idempotency_key) return;
  try { storage.removeItem(storageKey(owner, request.plan_id)); } catch { /* A later readonly lookup can resolve it again. */ }
}
