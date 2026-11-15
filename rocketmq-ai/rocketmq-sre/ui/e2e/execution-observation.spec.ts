import { expect, test } from "@playwright/test";
import type { BrowserContext, Page } from "@playwright/test";
import type { SubmitExecutionRequest } from "../src/api/types";
import { actionPlanFixture, planReadinessFixture } from "../src/features/incidents/test-fixtures/actionReadiness";
import { observationFixture } from "../src/features/executions/test-fixtures/observation";
import { CLUSTER_A, TENANT, oidcUser } from "./fixtures/evidenceData";
import { EvidenceHttpFixture } from "./fixtures/evidenceHttp";

async function restoreNetwork(page: Page, context: BrowserContext) {
  if (page.isClosed()) return;
  try { await context.setOffline(false); }
  catch (error) { if (!page.isClosed()) throw error; }
}

test("SyntheticFixture response loss restores the original intent through readonly HTTP lookup and never repeats a write", async ({ page, baseURL, context }) => {
  // This controlled server fixture exercises client recovery, not physical
  // target qualification, Agent success or independent IdP authorization.
  const fixture = observationFixture();
  const { planView } = actionPlanFixture(fixture.view.steps[0].target);
  const subject = "synthetic-first";
  const now = new Date().toISOString();
  const deadline = new Date(Date.now() + 600_000).toISOString();
  planView.plan = { ...planView.plan, tenant_id: TENANT, cluster_id: CLUSTER_A, status: "approved", created_by: subject, created_at: now, expires_at: deadline };
  planView.latest_approval = {
    id: "38000000-0000-4000-8000-000000000001", tenant_id: TENANT, cluster_id: CLUSTER_A,
    plan_id: planView.plan.id, plan_hash: planView.plan.plan_hash, requester_subject: subject,
    approver_subject: "synthetic-independent-approver", approver_role: "approver", decision: "approved",
    reason: "Synthetic browser fixture only", decided_at: now, expires_at: deadline,
  };
  fixture.view.binding = { ...fixture.view.binding, tenant_id: TENANT, cluster_id: CLUSTER_A, requested_by: subject,
    accepted_at: now, state: "succeeded" };
  fixture.view.steps[0].verifications = [{ ...fixture.verification, proof_state: "legacy_missing", provenance: "recorded_only",
    observed_stable_seconds: null, sample_count: null, max_source_age_seconds: null, max_sample_gap_seconds: null,
    source_first_at: null, source_last_at: null, resource_observed_at: null, technical_sli_observed_at: null,
    collected_at: null, policy_digest: null, declared_target_version: null, satisfied_conditions: [],
    reason_codes: ["legacy_window_proof_missing"] }];
  let original: SubmitExecutionRequest | undefined;
  let submissions = 0;
  let lookups = 0;
  const times = () => ({ observed_at: new Date().toISOString(), valid_until: new Date(Date.now() + 5_000).toISOString() });
  const http = new EvidenceHttpFixture(page, baseURL!, (call, body) => {
    if (call.url.pathname === `/v1/plans/${planView.plan.id}`) return { body: planView };
    if (call.url.pathname.endsWith("/action-readiness")) {
      const readiness = planReadinessFixture(planView, 0, subject);
      readiness.valid_until = deadline;
      readiness.observed_at = now;
      readiness.execution_readiness = "satisfied";
      for (const fact of Object.values(readiness.stages)) {
        fact.state = "satisfied";
        fact.reason_codes = ["observation_available"];
        fact.observed_at = now;
        fact.valid_until = deadline;
      }
      return { body: readiness };
    }
    if (call.url.pathname === `/v1/plans/${planView.plan.id}/executions`) return { body: {
      ...fixture.page, tenant_id: TENANT, cluster_id: CLUSTER_A,
      items: lookups >= 2 ? [fixture.view.binding] : [], ...times(),
    } };
    if (call.url.pathname === "/v1/executions" && call.method === "POST") {
      original = JSON.parse(body!) as SubmitExecutionRequest;
      submissions += 1;
      return { networkError: "connectionreset", body: undefined };
    }
    if (call.url.pathname === "/v1/executions/lookup") {
      expect(JSON.parse(body!)).toEqual(original);
      lookups += 1;
      return { body: { ...fixture.lookup, tenant_id: TENANT, actor_subject: subject, request: original,
        status: lookups === 1 ? "not_observed" : "accepted", receipt: lookups === 1 ? null : fixture.view.binding, ...times() } };
    }
    if (call.url.pathname === `/v1/executions/${fixture.view.binding.execution_id}/observation`) return { body: { ...fixture.view, ...times() } };
  });
  await http.install();
  let offline = false;
  await page.route(`**/v1/executions/${fixture.view.binding.execution_id}/observation`, async (route) => {
    // Route fulfillment bypasses offline transport. Let Chromium fail this fetch
    // through its actual offline network stack instead of returning fixture data.
    if (offline) await route.continue();
    else await route.fallback();
  });
  const operator = oidcUser();
  operator.profile.roles.push("operator");
  const submit = page.getByRole("button", { name: "Submit to Executor", exact: true });
  try {
    await test.step("Open the declared synthetic Plan through the authenticated HTTP client", async () => {
      await page.goto(`/changes/plans/${planView.plan.id}?cluster_id=${CLUSTER_A}`);
      await expect(page.getByRole("heading", { name: "Accepted requests for this Plan" })).toBeVisible();
      await http.publish(operator);
      await expect(submit).toBeEnabled();
    });
    await test.step("Visible submit loses its response and pins the original absolute window", async () => {
      const lost = page.waitForEvent("requestfailed", (request) => new URL(request.url()).pathname === "/v1/executions" && request.method() === "POST");
      await submit.click();
      expect((await lost).failure()?.errorText).toContain("CONNECTION_RESET");
      await expect(page.getByRole("heading", { name: "Submission outcome uncertain" })).toBeVisible();
      const selected = new URL(page.url());
      expect(selected.searchParams.has("window_start")).toBe(true);
      expect(selected.searchParams.has("window_end")).toBe(true);
    });
    await test.step("Reload recovers the original owner and NotObserved keeps new writes closed", async () => {
      await page.reload();
      await expect(page.getByRole("heading", { name: "Accepted requests for this Plan" })).toBeVisible();
      await http.publish(operator);
      await expect(page.getByRole("button", { name: "Read original-request receipt", exact: true })).toBeVisible();
      await expect(submit).toBeDisabled();
      await page.getByRole("button", { name: "Read original-request receipt", exact: true }).click();
      await expect(page.getByText("The original receipt is not observed. Continue read-only inspection; do not resubmit.")).toBeVisible();
      await expect(submit).toBeDisabled();
      expect(submissions).toBe(1);
    });
    await test.step("A second readonly lookup locates the same accepted execution", async () => {
      await page.getByRole("button", { name: "Read original-request receipt", exact: true }).click();
      await expect(page).toHaveURL(new RegExp(`/changes/executions/${fixture.view.binding.execution_id}`));
      await expect(page.getByRole("heading", { name: "Business recovery", exact: true })).toBeVisible();
      await expect(page.getByText("Independent business-recovery observation is missing.")).toBeVisible();
      await expect(page.getByText("Legacy proof missing")).toBeVisible();
      await expect(page.getByText("Successful execution state recorded")).toBeVisible();
    });
    await test.step("Actual Chromium offline reads fail and online readonly refresh restores the snapshot", async () => {
      const failedRead = page.waitForEvent("requestfailed", (request) => new URL(request.url()).pathname.endsWith("/observation"));
      offline = true;
      await context.setOffline(true);
      await page.getByRole("button", { name: "Read-only refresh", exact: true }).click();
      expect((await failedRead).failure()?.errorText).toContain("INTERNET_DISCONNECTED");
      await expect(page.getByRole("alert")).toContainText("Execution observation could not be read.");
      offline = false;
      await context.setOffline(false);
      await page.getByRole("button", { name: "Read-only refresh", exact: true }).click();
      await expect(page.getByText("Independent business-recovery observation is missing.")).toBeVisible();
    });
    expect(submissions).toBe(1);
    expect(lookups).toBe(2);
    expect(http.unexpected).toEqual([]);
    expect(await page.locator("body").innerText()).not.toContain("synthetic-private-grant-signature");
  } finally {
    await restoreNetwork(page, context);
    await http.dispose();
  }
});
