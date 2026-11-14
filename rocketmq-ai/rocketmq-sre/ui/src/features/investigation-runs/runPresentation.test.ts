import { parseAbsoluteWindow } from "@/context/workspaceScope";
import {
  commandAvailable,
  compareRevision,
  resourceForWorkspace,
  runInWorkspace,
  runSelection,
} from "./runPresentation";
import {
  CLUSTER,
  controlsFixture,
  RUN,
  runFixture,
  TIME,
} from "./runTestFixtures";

it("compares persisted u64 counters without losing precision", () => {
  expect(compareRevision("9007199254740993", "9007199254740992")).toBe(1);
  expect(compareRevision("18446744073709551615", "18446744073709551614")).toBe(
    1,
  );
  expect(() => compareRevision("18446744073709551616", "1")).toThrow();
});
it("rejects ambiguous run selections instead of requesting an arbitrary ID", () => {
  expect(runSelection("")).toEqual({ kind: "manual" });
  expect(runSelection(`?run_id=${RUN}&run_id=${RUN}`)).toEqual({
    kind: "invalid",
  });
  expect(runSelection(`?run_id=${RUN}&run_revision_id=`)).toEqual({
    kind: "invalid",
  });
  expect(runSelection(`?run_revision_id=${RUN}`)).toEqual({ kind: "invalid" });
});
it("preserves exact nanosecond windows through URL alignment and detects a real window conflict", () => {
  const precise = {
    start: "2026-11-14T09:00:00.123456789Z",
    end: "2026-11-14T10:00:00.123456789Z",
  };
  const parsed = parseAbsoluteWindow(
    `?window_start=${precise.start}&window_end=${precise.end}`,
  );
  expect(parsed).toEqual(precise);
  const run = runFixture();
  run.effective_scope = { ...run.effective_scope, time_range: precise };
  expect(runInWorkspace(run, { clusterId: CLUSTER, window: precise })).toBe(
    true,
  );
  expect(
    runInWorkspace(run, {
      clusterId: CLUSTER,
      window: { ...precise, end: "2026-11-14T10:00:00.123456788Z" },
    }),
  ).toBe(false);
  expect(
    parseAbsoluteWindow(
      "?window_start=2026-11-14T10:00:00.1234Z&window_end=2026-11-14T10:00:00.1233Z",
    ),
  ).toBe("invalid");
});
it("aligns server consumer/producer subjects to their existing workspace selectors", () => {
  expect(
    resourceForWorkspace({ kind: "consumer_group", key: "workers" }),
  ).toEqual({ resourceKind: "consumer", resourceKey: "workers" });
  expect(
    resourceForWorkspace({ kind: "producer_group", key: "senders" }),
  ).toEqual({ resourceKind: "producer", resourceKey: "senders" });
  const run = runFixture();
  run.effective_scope = {
    ...run.effective_scope,
    resource: { kind: "consumer_group", key: "workers" },
  };
  expect(
    runInWorkspace(run, {
      clusterId: CLUSTER,
      resourceKind: "consumer",
      resourceKey: "workers",
      window: TIME,
    }),
  ).toBe(true);
  expect(
    runInWorkspace(run, {
      clusterId: CLUSTER,
      resourceKind: "consumer",
      resourceKey: "other",
      window: TIME,
    }),
  ).toBe(false);
});
it("never infers commands from a status, truthy transport values or contradictory server policy", () => {
  const run = runFixture();
  expect(commandAvailable(undefined, run, "pause")).toBe(false);
  const controls = controlsFixture(run);
  expect(commandAvailable(controls, run, "pause")).toBe(true);
  controls.commands[0] = {
    kind: "pause",
    allowed: true,
    blocked_reason: "control_permission_required",
  };
  expect(commandAvailable(controls, run, "pause")).toBe(false);
  controls.commands[0] = {
    kind: "pause",
    allowed: "false" as unknown as boolean,
  };
  expect(commandAvailable(controls, run, "pause")).toBe(false);
});
