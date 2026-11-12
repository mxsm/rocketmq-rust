import { expect, test as base, type Page } from "@playwright/test";

import {
  CLUSTER_A,
  CLUSTER_B,
  EVIDENCE_ONE,
  EVIDENCE_TWO,
  REVISION_ONE,
  REVISION_TWO,
  TENANT,
  investigationUrl,
  oidcUser,
} from "./fixtures/evidenceData";
import { EvidenceHttpFixture } from "./fixtures/evidenceHttp";

const test = base.extend<{ http: EvidenceHttpFixture }>({
  http: async ({ page, baseURL }, provide) => {
    const fixture = new EvidenceHttpFixture(page, baseURL!);
    await fixture.install();
    try {
      await provide(fixture);
      expect(fixture.unexpected).toEqual([]);
      expect(fixture.calls.filter((call) => call.method !== "GET")).toEqual([]);
    } finally {
      await fixture.dispose();
    }
  },
});

async function ready(page: Page) {
  await page.goto(investigationUrl());
  await expect(page.getByRole("heading", { name: "Synthetic investigation for synthetic-first" })).toBeVisible();
  await expect(page.getByText("Required signals matched to this revision: 1 / 1", { exact: true })).toBeVisible();
}

async function selectRevision(page: Page, number: number) {
  const button = page.getByRole("button", { name: new RegExp(`^Revision ${number}\\b`) });
  await button.click();
  await expect(button).toHaveAttribute("aria-pressed", "true");
}

async function readContent(page: Page) {
  await page.getByRole("button", { name: "View evidence", exact: true }).click();
  const drawer = page.getByRole("dialog", { name: "Evidence details" });
  await drawer.getByRole("button", { name: "Read redacted content", exact: true }).click();
  return drawer;
}

test("a late quality response from the other revision is aborted and cannot replace the selected chain", async ({ page, http }) => {
  await ready(page);
  const old = http.holdNext((call) =>
    call.url.pathname === "/v1/capabilities/coverage" &&
    call.url.searchParams.get("revision_id") === REVISION_ONE,
  );
  await selectRevision(page, 1);
  const request = await old.entered;
  expect(request.headers["x-rocketmq-tenant"]).toBe(TENANT);
  expect(request.headers["x-rocketmq-clusters"]).toBe(`${CLUSTER_A},${CLUSTER_B}`);
  expect(request.headers.authorization).toBe("Bearer synthetic-access-synthetic-first");
  await expect(page.getByText("synthetic.r2.signal", { exact: true })).toHaveCount(0);
  await selectRevision(page, 2);
  await expect(page.getByText("synthetic.r2.signal", { exact: true })).toBeVisible();
  await expect.poll(() => http.aborted(`revision_id=${REVISION_ONE}`)).toBe(true);
  await old.release();
  await expect(page.getByText("synthetic.r1.signal", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Required signals matched to this revision: 1 / 1", { exact: true })).toBeVisible();
});

test("metadata from a superseded revision stays hidden after its delayed HTTP response", async ({ page, http }) => {
  await ready(page);
  const old = http.holdNext((call) => call.url.pathname === `/v1/evidence/${EVIDENCE_ONE}`);
  await selectRevision(page, 1);
  await old.entered;
  await expect(page.getByText("Required signals matched to this revision: 1 / 1", { exact: true })).toHaveCount(0);
  await selectRevision(page, 2);
  await expect(page.getByText("synthetic.r2.signal", { exact: true })).toBeVisible();
  await expect(page.getByText("Required signals matched to this revision: 1 / 1", { exact: true })).toBeVisible();
  await expect.poll(() => http.aborted(`/v1/evidence/${EVIDENCE_ONE}`)).toBe(true);
  await old.release();
  await expect(page.getByText("synthetic.r1.signal", { exact: true })).toHaveCount(0);
  const drawer = await readContent(page);
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
  await expect(drawer).not.toContainText("inline_must_not_render");
});

test("closing the drawer and changing revision cancels a pending content read without restoring old content", async ({ page, http }) => {
  await ready(page);
  await selectRevision(page, 1);
  const drawer = await readContent(page);
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-one");
  const old = http.holdNext((call) => call.url.pathname === `/v1/evidence/${EVIDENCE_ONE}/content`);
  await drawer.getByRole("button", { name: "Read redacted content", exact: true }).click();
  await old.entered;
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "View evidence", exact: true })).toBeFocused();
  await selectRevision(page, 2);
  const next = await readContent(page);
  await expect(next.locator("pre")).toContainText("synthetic-first:revision-two");
  await expect.poll(() => http.aborted(`/v1/evidence/${EVIDENCE_ONE}/content`)).toBe(true);
  await old.release();
  await expect(next).not.toContainText("synthetic-first:revision-one");
});

test("same-principal token renewal preserves the drawer and uses the renewed HTTP identity", async ({ page, http }) => {
  await ready(page);
  const drawer = await readContent(page);
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
  const original = await drawer.elementHandle();
  await http.publish(oidcUser("synthetic-first", [CLUSTER_A, CLUSTER_B], "synthetic-renewed-access"));
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
  expect(await original?.evaluate((element) => element.isConnected)).toBe(true);
  const next = http.holdNext((call) => call.url.pathname === `/v1/evidence/${EVIDENCE_TWO}/content`);
  await drawer.getByRole("button", { name: "Read redacted content", exact: true }).click();
  const request = await next.entered;
  expect(request.headers.authorization).toBe("Bearer synthetic-renewed-access");
  await next.release();
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
});

test("a subject change hides old content before the new roster arrives and discards the old read", async ({ page, http }) => {
  await ready(page);
  const drawer = await readContent(page);
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
  const old = http.holdNext((call) => call.url.pathname === `/v1/evidence/${EVIDENCE_TWO}/content`);
  await drawer.getByRole("button", { name: "Read redacted content", exact: true }).click();
  await old.entered;
  const roster = http.holdNext((call) =>
    call.url.pathname === "/v1/clusters" &&
    call.headers["x-rocketmq-subject"] === "synthetic-second",
  );
  await http.publish(oidcUser("synthetic-second"));
  await roster.entered;
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByText("synthetic-first:revision-two", { exact: false })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Synthetic investigation for synthetic-first" })).toHaveCount(0);
  await expect.poll(() => http.aborted(`/v1/evidence/${EVIDENCE_TWO}/content`)).toBe(true);
  await old.release();
  await roster.release();
  await expect(page.getByRole("heading", { name: "Synthetic investigation for synthetic-second" })).toBeVisible();
  const next = await readContent(page);
  await expect(next.locator("pre")).toContainText("synthetic-second:revision-two");
  await expect(next).not.toContainText("synthetic-first:revision-two");
});

test("revoked cluster claims and an unloaded session clear the drawer and never reuse an old HTTP result", async ({ page, http }) => {
  await ready(page);
  const drawer = await readContent(page);
  await expect(drawer.locator("pre")).toContainText("synthetic-first:revision-two");
  const old = http.holdNext((call) => call.url.pathname === `/v1/evidence/${EVIDENCE_TWO}/content`);
  await drawer.getByRole("button", { name: "Read redacted content", exact: true }).click();
  await old.entered;
  const roster = http.holdNext((call) =>
    call.url.pathname === "/v1/clusters" &&
    call.headers["x-rocketmq-clusters"] === CLUSTER_B,
  );
  await http.publish(oidcUser("synthetic-first", [CLUSTER_B]));
  await roster.entered;
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByText("synthetic-first:revision-two", { exact: false })).toHaveCount(0);
  await expect.poll(() => http.aborted(`/v1/evidence/${EVIDENCE_TWO}/content`)).toBe(true);
  await old.release();
  await roster.release();
  await expect(page.getByText(
    "Invalid or unauthorized scope. No scoped queries were started.",
    { exact: true },
  )).toBeVisible();
  await http.unload();
  await expect(page.getByRole("heading", { name: "需要 OIDC 登录" })).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByText("synthetic-first:revision-two", { exact: false })).toHaveCount(0);
});

test("opening bound capability facts preserves the revision and a new cluster selection clears it", async ({ page, http }) => {
  await ready(page);
  const old = http.holdNext((call) =>
    call.url.pathname === "/v1/capabilities/coverage" &&
    call.url.searchParams.get("revision_id") === REVISION_TWO,
  );
  await page.getByRole("link", { name: "View capability layers" }).click();
  await expect(page.getByText(/Bound saved revision: Revision 2/)).toBeVisible();
  await old.entered;
  await page.getByRole("combobox", { name: "Authorized cluster", exact: true }).selectOption(CLUSTER_B);
  await expect.poll(() => http.aborted(`revision_id=${REVISION_TWO}`)).toBe(true);
  const target = new URL(page.url());
  expect(target.searchParams.get("cluster_id")).toBe(CLUSTER_B);
  expect(target.searchParams.has("investigation_id")).toBe(false);
  expect(target.searchParams.has("revision_id")).toBe(false);
  expect(target.searchParams.has("resource")).toBe(false);
  await expect(page.getByText(/Bound saved revision:/)).toHaveCount(0);
  await old.release();
  await expect(page.getByText("synthetic.r2.signal", { exact: true })).toHaveCount(0);
});
