import { expect, test } from "@playwright/test";
import type {
  CreateInvestigationRunRequest,
  InvestigationCommandRequest,
  InvestigationRun,
} from "../src/api/types";
import {
  INVESTIGATION,
  TENANT,
  investigation,
  investigationUrl,
  oidcUser,
} from "./fixtures/evidenceData";
import { EvidenceHttpFixture } from "./fixtures/evidenceHttp";
import {
  RUN_ONE,
  RUN_TWO,
  run,
  runControls,
  runEvents,
  runList,
} from "./fixtures/investigationRunData";

test("server discovery, bounded creation, immutable receipts and browser history use the real HTTP client", async ({
  page,
  baseURL,
}) => {
  let current = run();
  const requests: Array<
    CreateInvestigationRunRequest | InvestigationCommandRequest
  > = [];
  const http = new EvidenceHttpFixture(page, baseURL!, (call, body) => {
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}/runs`) {
      if (call.method === "POST") {
        const request = JSON.parse(body!) as CreateInvestigationRunRequest;
        requests.push(request);
        current = {
          ...run(request.run_id, "9007199254740994", "running"),
          initial_scope: request.scope,
          effective_scope: request.scope,
          budget: request.budget,
        };
        return {
          status: 200,
          body: {
            run: { ...current, revision: "0", status: "queued" },
            actor_subject: "synthetic-first",
            idempotency_key: request.idempotency_key,
            replayed: true,
            request_digest: "synthetic-replay",
          },
        };
      }
      return { body: runList([current], true) };
    }
    if (call.url.pathname.endsWith("/controls"))
      return { body: runControls(current) };
    if (call.url.pathname.endsWith("/events"))
      return { body: runEvents(current) };
    if (call.url.pathname.endsWith("/commands")) {
      const request = JSON.parse(body!) as InvestigationCommandRequest;
      requests.push(request);
      current = { ...current, revision: "9007199254740995", status: "paused" };
      return {
        status: 200,
        body: {
          schema_version: current.schema_version,
          command_id: request.command_id,
          run_id: current.id,
          tenant_id: TENANT,
          actor_subject: "synthetic-first",
          idempotency_key: request.idempotency_key,
          applied_revision: "2",
          control_epoch: "1",
          status: "pausing",
          recorded_at: current.updated_at,
          request_digest: "synthetic-command",
        },
      };
    }
  });
  await http.install();
  try {
    await page.goto(investigationUrl());
    await page
      .getByRole("button", {
        name: "Create bounded investigation",
        exact: true,
      })
      .click();
    const dialog = page.getByRole("dialog", {
      name: "Create bounded investigation",
    });
    await expect(dialog).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(
      page.getByRole("button", {
        name: "Create bounded investigation",
        exact: true,
      }),
    ).toBeFocused();
    await page
      .getByRole("button", {
        name: "Create bounded investigation",
        exact: true,
      })
      .click();
    await dialog
      .getByRole("button", { name: "Submit read-only investigation" })
      .click();
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toBeVisible();
    const id = new URL(page.url()).searchParams.get("run_id");
    expect(id).toBeTruthy();
    expect(requests[0]).toMatchObject({
      scope: { investigation_id: INVESTIGATION },
      budget: { max_query_retries: 2 },
    });
    await page
      .getByLabel("Control reason", { exact: true })
      .fill("Synthetic controlled pause");
    await page
      .getByRole("button", { name: "Pause investigation", exact: true })
      .click();
    await expect(
      page.getByText("Paused", { exact: true }).first(),
    ).toBeVisible();
    await expect(
      page.getByText("9007199254740995", { exact: true }),
    ).toBeAttached();
    await page.reload();
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toBeVisible();
    await page
      .getByRole("combobox", { name: "Select investigation run", exact: true })
      .selectOption("");
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toHaveCount(0);
    await page.goBack();
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toBeVisible();
    expect(http.unexpected).toEqual([]);
  } finally {
    await http.dispose();
  }
});

test("a delayed run cannot refill controls after selecting another run or replacing identity", async ({
  page,
  baseURL,
}) => {
  const values: Record<string, InvestigationRun> = {
    [RUN_ONE]: run(),
    [RUN_TWO]: run(RUN_TWO, "9007199254740994", "paused"),
  };
  const http = new EvidenceHttpFixture(page, baseURL!, (call) => {
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}/runs`)
      return { body: runList(Object.values(values)) };
    const id = call.url.pathname.split("/")[3];
    if (call.url.pathname.endsWith("/controls") && values[id]) {
      if (call.headers["x-rocketmq-subject"] !== "synthetic-first")
        return {
          status: 403,
          body: { code: "forbidden", message: "foreign-marker" },
        };
      return { body: runControls(values[id]) };
    }
    if (call.url.pathname.endsWith("/events") && values[id])
      return { body: runEvents(values[id]) };
  });
  await http.install();
  try {
    const old = http.holdNext(
      (call) =>
        call.url.pathname === `/v1/investigation-runs/${RUN_ONE}/controls`,
    );
    await page.goto(`${investigationUrl()}&run_id=${RUN_ONE}`);
    await old.entered;
    await page
      .getByRole("combobox", { name: "Select investigation run", exact: true })
      .selectOption(RUN_TWO);
    await expect(
      page.getByText("Paused", { exact: true }).first(),
    ).toBeVisible();
    await expect
      .poll(() => http.aborted(`/v1/investigation-runs/${RUN_ONE}/controls`))
      .toBe(true);
    await old.release();
    await expect(
      page.getByText("9007199254740993", { exact: true }),
    ).toHaveCount(0);
    await http.publish(oidcUser("synthetic-second"));
    await expect(
      page.getByText("Your current identity cannot read or control this run."),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toHaveCount(0);
    await expect(page.getByText("foreign-marker")).toHaveCount(0);
    expect(http.calls.filter((call) => call.method !== "GET")).toEqual([]);
    expect(http.unexpected).toEqual([]);
  } finally {
    await http.dispose();
  }
});

test("refinement aligns the typed run subject without weakening the old manual boundary, then restores resume and cancel", async ({
  page,
  baseURL,
}) => {
  let current = run(RUN_ONE, "9007199254740993", "paused");
  current = {
    ...current,
    initial_scope: {
      ...current.initial_scope,
      resource: { kind: "topic", key: "orders" },
    },
    effective_scope: {
      ...current.effective_scope,
      resource: { kind: "topic", key: "orders" },
    },
  };
  const commands: InvestigationCommandRequest[] = [];
  const http = new EvidenceHttpFixture(page, baseURL!, (call, body) => {
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}`) {
      const view = investigation("synthetic-first");
      return {
        body: {
          ...view,
          investigation: { ...view.investigation, resource: "topics/orders" },
        },
      };
    }
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}/runs`)
      return { body: runList([current]) };
    if (call.url.pathname.endsWith("/controls"))
      return { body: runControls(current) };
    if (call.url.pathname.endsWith("/events"))
      return { body: runEvents(current) };
    if (call.url.pathname.endsWith("/commands")) {
      const request = JSON.parse(body!) as InvestigationCommandRequest;
      commands.push(request);
      current = {
        ...current,
        revision: (BigInt(current.revision) + 1n).toString(),
        control_epoch: ["steer", "cancel"].includes(request.command.kind)
          ? (BigInt(current.control_epoch) + 1n).toString()
          : current.control_epoch,
        status:
          request.command.kind === "resume"
            ? "running"
            : request.command.kind === "cancel"
              ? "cancelled"
              : "paused",
        ...(request.command.kind === "steer"
          ? {
              effective_scope: request.command.refinement.scope,
              effective_packs: request.command.refinement.packs,
            }
          : {}),
      };
      return {
        body: {
          schema_version: current.schema_version,
          command_id: request.command_id,
          run_id: current.id,
          tenant_id: TENANT,
          actor_subject: "synthetic-first",
          idempotency_key: request.idempotency_key,
          applied_revision: current.revision,
          control_epoch: current.control_epoch,
          status: current.status,
          recorded_at: current.updated_at,
          request_digest: "synthetic-control",
        },
      };
    }
  });
  await http.install();
  try {
    await page.goto(`${investigationUrl()}&run_id=${RUN_ONE}`);
    await page
      .getByLabel("Control reason", { exact: true })
      .fill("Narrow the saved time window");
    await page
      .getByRole("button", { name: "Narrow investigation", exact: true })
      .click();
    const dialog = page.getByRole("dialog", { name: "Narrow investigation" });
    await dialog
      .getByLabel("Start time (UTC)", { exact: true })
      .fill("2026-11-11T09:30:00.123456789Z");
    await dialog
      .getByRole("button", { name: "Submit refinement", exact: true })
      .click();
    await page
      .getByRole("button", { name: "Align to run scope", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Resume investigation", exact: true }),
    ).toBeVisible();
    expect(new URL(page.url()).searchParams.get("window_start")).toBe(
      "2026-11-11T09:30:00.123456789Z",
    );
    await page.reload();
    await page
      .getByLabel("Control reason", { exact: true })
      .fill("Resume the bounded saved work");
    await page
      .getByRole("button", { name: "Resume investigation", exact: true })
      .click();
    await expect(
      page.getByText("Running", { exact: true }).first(),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Cancel investigation", exact: true })
      .click();
    await expect(
      page.getByText("Cancelled", { exact: true }).first(),
    ).toBeVisible();
    expect(commands.map((request) => request.command.kind)).toEqual([
      "steer",
      "resume",
      "cancel",
    ]);
    expect(commands.map((request) => request.expected_revision)).toEqual([
      "9007199254740993",
      "9007199254740994",
      "9007199254740995",
    ]);
    expect(http.unexpected).toEqual([]);
  } finally {
    await http.dispose();
  }
});

test("a browser connection loss hides usable controls and online retry restores the authoritative snapshot", async ({
  page,
  baseURL,
  context,
}) => {
  let current = run();
  const http = new EvidenceHttpFixture(page, baseURL!, (call) => {
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}/runs`)
      return { body: runList([current]) };
    if (call.url.pathname.endsWith("/controls"))
      return { body: runControls(current) };
    if (call.url.pathname.endsWith("/events"))
      return { body: runEvents(current) };
  });
  await http.install();
  let offline = false;
  await page.route(
    `**/v1/investigation-runs/${RUN_ONE}/controls`,
    async (route) => {
      // Let Chromium's offline network stack fail this real fetch; do not fulfill it from a route.
      if (offline) await route.continue();
      else await route.fallback();
    },
  );
  try {
    await page.goto(`${investigationUrl()}&run_id=${RUN_ONE}`);
    await expect(
      page.getByRole("heading", { name: "Shared budget", exact: true }),
    ).toBeVisible();
    await page
      .getByLabel("Control reason", { exact: true })
      .fill("Verify loss and recovery");
    await expect(
      page.getByRole("button", { name: "Pause investigation", exact: true }),
    ).toBeEnabled();
    offline = true;
    await context.setOffline(true);
    const failed = page.waitForEvent(
      "requestfailed",
      (request) =>
        new URL(request.url()).pathname ===
        `/v1/investigation-runs/${RUN_ONE}/controls`,
    );
    await page
      .getByRole("button", { name: "Refresh run", exact: true })
      .click();
    expect((await failed).failure()?.errorText).toMatch(
      /INTERNET_DISCONNECTED/,
    );
    const failure = page
      .getByRole("alert")
      .filter({ hasText: "Could not read run state. Retry the request." });
    await expect(failure).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Pause investigation", exact: true }),
    ).toHaveCount(0);
    current = run(RUN_ONE, "9007199254740995", "paused");
    offline = false;
    await context.setOffline(false);
    await page.getByRole("button", { name: "Retry read", exact: true }).click();
    await expect(
      page.getByText("Paused", { exact: true }).first(),
    ).toBeVisible();
    await expect(
      page.getByText("9007199254740995", { exact: true }),
    ).toBeAttached();
    await expect(failure).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "Resume investigation", exact: true }),
    ).toBeEnabled();
    expect(http.calls.filter((call) => call.method !== "GET")).toEqual([]);
    expect(http.unexpected).toEqual([]);
  } finally {
    await context.setOffline(false);
    await http.dispose();
  }
});

test("an accepted command response loss replays the same request once without replacing a newer snapshot", async ({
  page,
  baseURL,
}) => {
  let current = run();
  let accepted = 0;
  const commands: InvestigationCommandRequest[] = [];
  const http = new EvidenceHttpFixture(page, baseURL!, (call, body) => {
    if (call.url.pathname === `/v1/investigations/${INVESTIGATION}/runs`)
      return { body: runList([current]) };
    if (call.url.pathname.endsWith("/controls"))
      return { body: runControls(current) };
    if (call.url.pathname.endsWith("/events"))
      return { body: runEvents(current) };
    if (call.url.pathname.endsWith("/commands")) {
      const request = JSON.parse(body!) as InvestigationCommandRequest;
      commands.push(request);
      if (commands.length === 1) {
        accepted += 1;
        current = run(RUN_ONE, "9007199254740995", "paused");
        return { networkError: "connectionreset", body: undefined };
      }
      expect(request).toEqual(commands[0]);
      return {
        body: {
          schema_version: current.schema_version,
          command_id: request.command_id,
          run_id: current.id,
          tenant_id: TENANT,
          actor_subject: "synthetic-first",
          idempotency_key: request.idempotency_key,
          applied_revision: "9007199254740994",
          control_epoch: "1",
          status: "pausing",
          recorded_at: current.updated_at,
          request_digest: "synthetic-original-receipt",
        },
      };
    }
  });
  await http.install();
  try {
    await page.goto(`${investigationUrl()}&run_id=${RUN_ONE}`);
    await page
      .getByLabel("Control reason", { exact: true })
      .fill("Pause with an unconfirmed response");
    const failed = page.waitForEvent(
      "requestfailed",
      (request) =>
        request.method() === "POST" &&
        new URL(request.url()).pathname.endsWith("/commands"),
    );
    await page
      .getByRole("button", { name: "Pause investigation", exact: true })
      .click();
    expect((await failed).failure()?.errorText).toMatch(/CONNECTION_RESET/);
    await expect(
      page.getByText(
        "The request outcome is unconfirmed. Check current state or retry the same request.",
        { exact: true },
      ),
    ).toBeVisible();
    await expect(
      page.getByText("Paused", { exact: true }).first(),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Pause investigation", exact: true }),
    ).toHaveCount(0);
    await page
      .getByRole("button", { name: "Retry the same request", exact: true })
      .click();
    await expect(
      page.getByText("Request received. Server state has been read again.", {
        exact: true,
      }),
    ).toBeVisible();
    expect(commands).toHaveLength(2);
    expect(commands[1]).toEqual(commands[0]);
    expect(commands[0].expected_revision).toBe("9007199254740993");
    expect(accepted).toBe(1);
    await expect(
      page.getByText("9007199254740995", { exact: true }),
    ).toBeAttached();
    await expect(
      page.getByText("Paused", { exact: true }).first(),
    ).toBeVisible();
    await expect(page.getByText("Pausing", { exact: true })).toHaveCount(0);
    expect(http.unexpected).toEqual([]);
  } finally {
    await http.dispose();
  }
});
