import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import type {
  InvestigationCommandRequest,
  InvestigationRunControls,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { RunCommands } from "./RunCommands";
import { RunProgress } from "./RunProgress";
import {
  controlsFixture,
  deferred,
  EVIDENCE,
  runFixture,
  TIME,
} from "./runTestFixtures";

it("submits a narrowing Steer using only current saved IDs and the exact server revision", async () => {
  const user = userEvent.setup();
  const run = runFixture({ evidence_ids: [EVIDENCE] });
  const submit = vi.fn().mockResolvedValue(undefined);
  render(
    <OperatorPreferencesProvider>
      <RunCommands
        run={run}
        controls={controlsFixture(run)}
        submit={submit}
        refresh={async () => {}}
      />
    </OperatorPreferencesProvider>,
  );
  await user.type(screen.getByLabelText("控制原因"), "Focus this saved window");
  await user.click(screen.getByRole("button", { name: "收窄调查" }));
  const dialog = await screen.findByRole("dialog", { name: "收窄调查" });
  const start = within(dialog).getByLabelText("开始时间（UTC）");
  await user.clear(start);
  await user.type(start, "2026-11-13T09:00:00Z");
  await user.click(within(dialog).getByRole("button", { name: "提交收窄" }));
  expect(submit).not.toHaveBeenCalled();
  expect(within(dialog).getByRole("alert")).toHaveTextContent("必须保持或收窄");
  await user.clear(start);
  await user.type(start, "2026-11-14T09:30:00Z");
  await user.click(within(dialog).getByLabelText(EVIDENCE));
  await user.click(within(dialog).getByRole("button", { name: "提交收窄" }));
  await waitFor(() => expect(submit).toHaveBeenCalledTimes(1));
  expect(submit.mock.calls[0][0]).toMatchObject({
    run_id: run.id,
    expected_revision: run.revision,
    reason: "Focus this saved window",
    command: {
      kind: "steer",
      refinement: {
        scope: {
          ...run.effective_scope,
          time_range: { start: "2026-11-14T09:30:00.000Z", end: TIME.end },
        },
        evidence_ids: [],
        packs: run.effective_packs,
      },
    },
  });
});

it("prevents duplicate command admission while one request is pending", async () => {
  const result = deferred<void>();
  const submit = vi.fn<(request: InvestigationCommandRequest) => Promise<void>>(
    () => result.promise,
  );
  const run = runFixture();
  const view = render(
    <OperatorPreferencesProvider>
      <RunCommands
        run={run}
        controls={controlsFixture(run)}
        submit={submit}
        refresh={async () => {}}
      />
    </OperatorPreferencesProvider>,
  );
  fireEvent.change(screen.getByLabelText("控制原因"), {
    target: { value: "Check bounded requests" },
  });
  const button = screen.getByRole("button", { name: "暂停调查" });
  act(() => {
    button.click();
    button.click();
  });
  expect(submit).toHaveBeenCalledTimes(1);
  view.unmount();
  await act(async () => result.resolve());
});

it("refreshes a revision conflict and asks for review instead of replaying it as a new command", async () => {
  const user = userEvent.setup();
  const run = runFixture();
  const refresh = vi.fn().mockResolvedValue(undefined);
  const submit = vi
    .fn()
    .mockRejectedValue(
      new ApiError(409, "revision_conflict", "sensitive-server-detail"),
    );
  render(
    <OperatorPreferencesProvider>
      <RunCommands
        run={run}
        controls={controlsFixture(run)}
        submit={submit}
        refresh={refresh}
      />
    </OperatorPreferencesProvider>,
  );
  await user.type(screen.getByLabelText("控制原因"), "Review current state");
  await user.click(screen.getByRole("button", { name: "暂停调查" }));
  expect(
    await screen.findByText("服务端状态已变化；已重新读取，请复核后再提交。"),
  ).toBeVisible();
  expect(refresh).toHaveBeenCalledTimes(1);
  expect(submit).toHaveBeenCalledTimes(1);
  expect(
    screen.queryByRole("button", { name: "重试相同请求" }),
  ).not.toBeInTheDocument();
  expect(screen.queryByText("sensitive-server-detail")).not.toBeInTheDocument();
});

it.each(["paused", "cancelled"] as const)(
  "keeps an unconfirmed original request retryable at %s only with fresh server replay permission",
  async (status) => {
    const user = userEvent.setup();
    const initial = runFixture();
    const submit = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("connection reset after acceptance"))
      .mockResolvedValueOnce(undefined);
    const refresh = vi.fn().mockResolvedValue(undefined);
    const view = render(
      <OperatorPreferencesProvider>
        <RunCommands
          run={initial}
          controls={controlsFixture(initial)}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    await user.type(
      screen.getByLabelText("控制原因"),
      "Recover original pause receipt",
    );
    await user.click(screen.getByRole("button", { name: "暂停调查" }));
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));
    const current = runFixture({
      revision: "4",
      status,
      control_epoch: status === "cancelled" ? "2" : "1",
    });
    const controls = controlsFixture(current);
    controls.commands = controls.commands.map((command) => ({
      ...command,
      allowed: status === "paused" && command.kind !== "pause",
      blocked_reason:
        status === "cancelled"
          ? "terminal"
          : command.kind === "pause"
            ? "state_not_eligible"
            : undefined,
    }));
    view.rerender(
      <OperatorPreferencesProvider>
        <RunCommands
          run={current}
          controls={controls}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    expect(
      screen.queryByRole("button", { name: "暂停调查" }),
    ).not.toBeInTheDocument();
    await user.clear(screen.getByLabelText("控制原因"));
    await user.click(screen.getByRole("button", { name: "重试相同请求" }));
    await waitFor(() => expect(submit).toHaveBeenCalledTimes(2));
    expect(submit.mock.calls[1][0]).toEqual(submit.mock.calls[0][0]);
    expect(submit.mock.calls[1][0].expected_revision).toBe("2");
  },
);

it.each([false, undefined])(
  "does not offer original-request replay when the server replay permission is %s",
  async (permission) => {
    const user = userEvent.setup();
    const run = runFixture();
    const submit = vi.fn().mockRejectedValue(new TypeError("response lost"));
    const refresh = vi.fn().mockResolvedValue(undefined);
    const view = render(
      <OperatorPreferencesProvider>
        <RunCommands
          run={run}
          controls={controlsFixture(run)}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    await user.type(
      screen.getByLabelText("控制原因"),
      "Check the original request",
    );
    await user.click(screen.getByRole("button", { name: "暂停调查" }));
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));
    // Simulate an older HTTP response that omitted the newly defaulted field.
    const controls = {
      ...controlsFixture(run),
      receipt_replay_allowed: permission,
    } as InvestigationRunControls;
    view.rerender(
      <OperatorPreferencesProvider>
        <RunCommands
          run={run}
          controls={controls}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    expect(
      screen.queryByRole("button", { name: "重试相同请求" }),
    ).not.toBeInTheDocument();
    expect(submit).toHaveBeenCalledTimes(1);
    view.rerender(
      <OperatorPreferencesProvider>
        <RunCommands
          run={run}
          controls={undefined}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    expect(
      screen.queryByRole("button", { name: "重试相同请求" }),
    ).not.toBeInTheDocument();
    view.rerender(
      <OperatorPreferencesProvider>
        <RunCommands
          run={run}
          controls={controlsFixture(run)}
          submit={submit}
          refresh={refresh}
        />
      </OperatorPreferencesProvider>,
    );
    if (permission === false) {
      expect(
        screen.queryByRole("button", { name: "重试相同请求" }),
      ).not.toBeInTheDocument();
      expect(screen.getByLabelText("控制原因")).toHaveValue("");
    } else {
      expect(
        screen.getByRole("button", { name: "重试相同请求" }),
      ).toBeVisible();
      await user.click(screen.getByRole("button", { name: "重试相同请求" }));
      expect(submit.mock.calls[1][0]).toEqual(submit.mock.calls[0][0]);
    }
  },
);

it("clears an uncertain request after a definite authorization refusal on replay", async () => {
  const user = userEvent.setup();
  const run = runFixture();
  const submit = vi
    .fn()
    .mockRejectedValueOnce(new TypeError("response lost"))
    .mockRejectedValueOnce(
      new ApiError(403, "forbidden", "private-refusal-marker"),
    );
  render(
    <OperatorPreferencesProvider>
      <RunCommands
        run={run}
        controls={controlsFixture(run)}
        submit={submit}
        refresh={async () => {}}
      />
    </OperatorPreferencesProvider>,
  );
  await user.type(
    screen.getByLabelText("控制原因"),
    "Check the original request",
  );
  await user.click(screen.getByRole("button", { name: "暂停调查" }));
  await user.click(await screen.findByRole("button", { name: "重试相同请求" }));
  expect(
    await screen.findByText("当前身份无权读取或控制此运行。"),
  ).toBeVisible();
  expect(
    screen.queryByRole("button", { name: "重试相同请求" }),
  ).not.toBeInTheDocument();
  expect(screen.queryByText("private-refusal-marker")).not.toBeInTheDocument();
});

it("shows an interrupted read as unknown actual consumption while retaining server reservations", () => {
  const run = runFixture();
  run.steps = [
    {
      id: "step",
      spec: {
        scope: run.effective_scope,
        pack: run.effective_packs[0],
        work: { kind: "collect_evidence", requirement_id: "broker-runtime" },
      },
      attempts: [
        {
          id: "attempt",
          control_epoch: "1",
          admitted_revision: "1",
          admitted_at: TIME.end,
          reservation: {
            read_queries: 1,
            model_calls: 0,
            evidence_items: 1,
            output_bytes: 655360,
          },
          result: null,
          disposition: null,
          interruption: {
            reason: "source_outcome_unknown",
            recovered_at: TIME.end,
          },
        },
      ],
    },
  ];
  const controls = controlsFixture(run);
  controls.budget_usage.reserved = {
    read_queries: 1,
    model_calls: 0,
    evidence_items: 1,
    output_bytes: 655360,
  };
  render(
    <OperatorPreferencesProvider>
      <RunProgress run={run} controls={controls} />
    </OperatorPreferencesProvider>,
  );
  expect(screen.getByText("最终消耗未知，预约未释放")).toBeVisible();
  expect(screen.getByText("实际报告: —")).toBeVisible();
  const row = screen.getByRole("row", { name: "只读查询 12 0 1 11" });
  expect(row).toBeVisible();
  expect(screen.getByText("1 / 32")).toBeVisible();
});
