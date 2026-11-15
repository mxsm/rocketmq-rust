import { describe, expect, it } from "vitest";
import { readSubmissionIntent, rememberSubmissionIntent, resolveSubmissionIntent } from "./submissionIntentStorage";
import { executionFixture } from "./test-fixtures/execution";

function fixture() {
  const { owner, selector } = executionFixture();
  const identity = { tenantId: owner.tenantId, subject: owner.subject, clusterId: owner.clusterIds[0], principalKey: "synthetic-exact-role-and-scope-key", scopeKey: "synthetic-window-a" };
  const rows = new Map<string, string>();
  const storage = { getItem: (key: string) => rows.get(key) ?? null, setItem: (key: string, value: string) => { rows.set(key, value); }, removeItem: (key: string) => { rows.delete(key); } };
  return { rows, storage, identity, selector };
}

describe("original submission uncertainty across remount", () => {
  it("restores only the exact original request and cannot generate a replacement key while unresolved", () => {
    const { storage, identity, selector, rows } = fixture();
    expect(rememberSubmissionIntent(storage, identity, selector)).toBe(true);
    expect(readSubmissionIntent(storage, identity, selector)).toEqual({ status: "uncertain", intent: { schema_version: "rocketmq-sre.ui-execution-intent.v1", owner: identity, request: selector } });
    expect(rememberSubmissionIntent(storage, identity, { ...selector, idempotency_key: "another-key" })).toBe(false);
    expect([...rows.values()].join()).not.toMatch(/signature|nonce|token|credential|parameters/);
    resolveSubmissionIntent(storage, identity, { ...selector, idempotency_key: "another-key" });
    expect(rows.size).toBe(1);
    resolveSubmissionIntent(storage, identity, selector);
    expect(rows.size).toBe(0);
  });

  it.each(["clusterId", "principalKey", "scopeKey"])("does not borrow an intent across %s or erase uncertainty to allow a new write", (key) => {
    const { storage, identity, selector } = fixture();
    rememberSubmissionIntent(storage, identity, selector);
    const changed = { ...identity, [key]: "different" };
    expect(readSubmissionIntent(storage, changed, selector)).toEqual({ status: "unavailable" });
    expect(rememberSubmissionIntent(storage, changed, { ...selector, idempotency_key: "another-key" })).toBe(false);
    expect(readSubmissionIntent(storage, identity, selector).status).toBe("uncertain");
  });

  it("never supplies another tenant/actor's private pending selector", () => {
    const { storage, identity, selector } = fixture();
    rememberSubmissionIntent(storage, identity, selector);
    expect(readSubmissionIntent(storage, { ...identity, subject: "other-actor" }, selector)).toEqual({ status: "empty" });
    expect(readSubmissionIntent(storage, { ...identity, tenantId: "other-tenant" }, selector)).toEqual({ status: "empty" });
  });

  it("keeps malformed storage and an unavailable store closed instead of treating them as no request", () => {
    const { storage, identity, selector, rows } = fixture();
    rememberSubmissionIntent(storage, identity, selector);
    const key = [...rows.keys()][0];
    rows.set(key, "{broken");
    expect(readSubmissionIntent(storage, identity, selector)).toEqual({ status: "unavailable" });
    expect(rememberSubmissionIntent(storage, identity, selector)).toBe(false);
    expect(rows.get(key)).toBe("{broken");
    const blocked = { ...storage, getItem: () => { throw new Error("private-storage-canary"); } };
    expect(readSubmissionIntent(blocked, identity, selector)).toEqual({ status: "unavailable" });
  });

  it("rejects credential/raw-parameter additions before anything is stored or submitted", () => {
    const { storage, identity, selector, rows } = fixture();
    const unexpected = { ...selector, signature: "private-canary", parameters: { token: "private-parameter-canary" } };
    expect(rememberSubmissionIntent(storage, identity, unexpected)).toBe(false);
    expect(rows.size).toBe(0);
    expect(rememberSubmissionIntent(storage, identity, { ...selector, precondition_hash: "invalid" })).toBe(false);
    expect(rows.size).toBe(0);
  });
});
