import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { executionReceiptMatches, originalSubmissionMatches } from "./executionPresentation";
import { executionFixture } from "./test-fixtures/execution";

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime("2026-11-17T11:00:00Z");
});
afterEach(() => vi.useRealTimers());

describe("accepted execution read boundaries", () => {
  it("preserves a saved historical receipt binding without re-authorizing its expired grant", () => {
    const { receipt, owner, selector } = executionFixture();
    receipt.execution.expires_at = "2026-11-17T10:00:01Z";
    receipt.execution.approvals[0].expires_at = "2026-11-17T10:00:01Z";
    expect(originalSubmissionMatches(receipt, owner, selector)).toBe(true);
  });

  it.each(["execution", "tenant", "cluster", "nested_tenant", "nested_cluster"])("rejects an otherwise correct receipt across the %s boundary", (kind) => {
    const { receipt, owner } = executionFixture();
    if (kind === "execution") receipt.execution.id = "different-execution";
    if (kind === "tenant") receipt.execution.tenant_id = "different-tenant";
    if (kind === "cluster") receipt.execution.cluster_id = "different-cluster";
    if (kind === "nested_tenant") receipt.execution.plan.tenant_id = "different-tenant";
    if (kind === "nested_cluster") receipt.execution.plan.cluster_id = "different-cluster";
    expect(executionReceiptMatches(receipt, owner)).toBe(false);
  });

  it.each(["plan_id", "plan_hash", "precondition_hash", "idempotency_key", "actor"])("does not treat a changed %s as the original accepted request", (kind) => {
    const { receipt, owner, selector } = executionFixture();
    if (kind === "actor") owner.subject = "different-operator";
    else selector[kind as keyof typeof selector] = kind.endsWith("hash") ? "sha256:" + "0".repeat(64) : "different";
    expect(originalSubmissionMatches(receipt, owner, selector)).toBe(false);
  });

  it("allows another authorized reader to inspect history without treating it as that reader's submission", () => {
    const { receipt, owner, selector } = executionFixture();
    owner.subject = "independent-viewer";
    expect(executionReceiptMatches(receipt, owner)).toBe(true);
    expect(originalSubmissionMatches(receipt, owner, selector)).toBe(false);
  });

  it("rejects a malformed nested request without reading or displaying private grant fields", () => {
    const { receipt, owner, selector } = executionFixture();
    expect(originalSubmissionMatches({ ...receipt, execution: { ...receipt.execution, approvals: "private-canary" } }, owner, selector)).toBe(false);
    expect(executionReceiptMatches({ ...receipt, execution: { ...receipt.execution, autonomy_grant: "private-canary" } }, owner)).toBe(false);
  });
});
