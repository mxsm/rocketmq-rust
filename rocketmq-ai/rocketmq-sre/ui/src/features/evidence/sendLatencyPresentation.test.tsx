import { render, screen, within } from "@testing-library/react";

import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { SendLatencySummary } from "./SendLatencySummary";
import { sendLatencyDisplay } from "./sendLatencyPresentation";
import { sendLatencyFixture } from "./test-fixtures/sendLatency";

function show(value: unknown, evidence = sendLatencyFixture().evidence) {
  const display = sendLatencyDisplay(value, evidence);
  if (!display) throw new Error("Expected a recognized display fixture");
  render(<OperatorPreferencesProvider><SendLatencySummary display={display} /></OperatorPreferencesProvider>);
  return display;
}

describe("saved latency display", () => {
  beforeEach(() => window.localStorage.clear());

  it("shows five distinct populations and the saved estimate without inventing whole-path health", () => {
    const { payload, evidence } = sendLatencyFixture();
    show(payload, evidence);
    const rows = within(screen.getByRole("list", { name: "发送延迟分段" })).getAllByRole("listitem");
    expect(rows).toHaveLength(5);
    expect(screen.getByText("保存的 P99 估计: 248.5 ms")).toBeVisible();
    expect(screen.getAllByText("缺少精确 Topic 范围")).toHaveLength(4);
    expect(screen.getByText(/不是端到端发送延迟/)).toBeVisible();
    expect(screen.getByText("保存的局部估计超过阈值", { exact: false })).toBeVisible();
    expect(screen.queryByText(/全链健康|dominant|Healthy/)).not.toBeInTheDocument();
  });

  it.each(["tenant_id", "cluster_id", "canonical_resource"] as const)(
    "hides a summary whose saved content has another %s",
    (key) => {
      const { payload, evidence } = sendLatencyFixture();
      payload.scope[key] = "foreign-scope-marker";
      show(payload, evidence);
      expect(screen.getByRole("alert")).toHaveTextContent("范围不一致");
      expect(screen.queryByText(/248.5|foreign-scope-marker/)).not.toBeInTheDocument();
    },
  );

  it.each(["orders%retry", "ns:orders"])("keeps the authorized literal topic %s", (topic) => {
    const { payload, evidence } = sendLatencyFixture();
    payload.scope.topic = topic;
    payload.scope.canonical_resource = `send-latency/${topic}`;
    evidence.resource = payload.scope.canonical_resource;
    show(payload, evidence);
    expect(screen.getByText(topic)).toBeVisible();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("rejects a nonliteral topic and fabricated measurements for unsupported populations", () => {
    const { payload, evidence } = sendLatencyFixture();
    payload.scope.topic = "orders/other";
    payload.scope.canonical_resource = "send-latency/orders/other";
    evidence.resource = payload.scope.canonical_resource;
    expect(sendLatencyDisplay(payload, evidence)).toEqual({ kind: "invalid", reason: "scope" });
    const valid = sendLatencyFixture();
    valid.payload.segments[0].availability = "available";
    valid.payload.segments[0].observation = { ...valid.payload.segments[3].observation! };
    expect(sendLatencyDisplay(valid.payload, valid.evidence)).toEqual({ kind: "invalid", reason: "structure" });
  });

  it("keeps a tail estimate and absent threshold separate from a usable finite predicate", () => {
    const { payload, evidence } = sendLatencyFixture();
    const broker = payload.segments.find((item) => item.id === "broker")!;
    broker.availability = "insufficient_data";
    broker.reason_code = "tail_resolution_limited";
    Object.assign(broker.observation!, {
      estimate_millis: 10000, lower_bound_millis: 10000,
      upper_bound_millis: null, resolution: "tail_limited",
    });
    payload.policy.status = "not_qualified";
    payload.policy.reason_code = "tail_resolution_limited";
    show(payload, evidence);
    expect(screen.getByText(/不代表尾延迟上界/)).toBeVisible();
    expect(screen.getByText(/此观测不可作阈值比较/)).toBeVisible();
    expect(screen.queryByText(/严格大于阈值/)).not.toBeInTheDocument();
  });

  it("does not turn no traffic or an unknown reason into zero latency or backend text", () => {
    const { payload, evidence } = sendLatencyFixture();
    const broker = payload.segments.find((item) => item.id === "broker")!;
    broker.availability = "no_traffic";
    broker.observation = null;
    broker.reason_code = "Bearer private-marker";
    payload.policy.status = "not_configured";
    payload.policy.reason_code = "policy_not_configured";
    show(payload, evidence);
    expect(screen.getByText("没有记录到窗口内流量")).toBeVisible();
    expect(screen.getByText("未提供可识别的观测原因。")).toBeVisible();
    expect(screen.queryByText(/private-marker|P99 估计: 0/)).not.toBeInTheDocument();
    expect(screen.queryByText(/严格大于阈值/)).not.toBeInTheDocument();
  });

  it("rejects duplicate segments and nonfinite observations without displaying a false estimate", () => {
    const { payload, evidence } = sendLatencyFixture();
    payload.segments[0] = { ...payload.segments[3] };
    expect(sendLatencyDisplay(payload, evidence)).toEqual({ kind: "invalid", reason: "structure" });
    const valid = sendLatencyFixture().payload;
    valid.segments[3].observation!.estimate_millis = Number.NaN;
    expect(sendLatencyDisplay(valid, evidence)).toEqual({ kind: "invalid", reason: "structure" });
  });

  it("suppresses a foreign policy value while preserving the actual local observation", () => {
    const { payload, evidence } = sendLatencyFixture();
    payload.policy.scope.cluster_id = "foreign";
    payload.policy.p99_millis = 987654;
    show(payload, evidence);
    expect(screen.getByText(/248.5 ms/)).toBeVisible();
    expect(screen.queryByText(/987,654|987654|严格大于阈值/)).not.toBeInTheDocument();
    expect(screen.getByText(/策略与此证据的范围不一致/)).toBeVisible();
  });

  it("keeps legacy histogram content on the existing redacted audit path", () => {
    const { evidence } = sendLatencyFixture();
    expect(sendLatencyDisplay({ profile: "send_latency", histogram_buckets: [] }, evidence)).toBeUndefined();
  });

  it("accepts the documented outer microsecond normalization while preserving original nanosecond content", () => {
    const { payload, evidence } = sendLatencyFixture();
    payload.window.requested_start = "2026-07-27T08:55:00.123456789Z";
    payload.window.requested_end = "2026-07-27T09:00:00.987654321Z";
    payload.collected_at = "2026-07-27T09:00:01.111222999Z";
    evidence.time_range.start = "2026-07-27T08:55:00.123456Z";
    evidence.time_range.end = "2026-07-27T09:00:00.987654Z";
    evidence.observed_at = "2026-07-27T09:00:01.111222Z";
    const display = sendLatencyDisplay(payload, evidence);
    expect(display?.kind).toBe("summary");
    if (display?.kind !== "summary") throw new Error("Expected normalized saved content");
    expect(display.requested.start).toBe(payload.window.requested_start);
    expect(display.collectedAt).toBe(payload.collected_at);
  });

  it("rejects a genuinely different range or collection time even within the same millisecond", () => {
    const { payload, evidence } = sendLatencyFixture();
    payload.window.requested_start = "2026-07-27T08:55:00.123456789Z";
    evidence.time_range.start = "2026-07-27T08:55:00.123455Z";
    expect(sendLatencyDisplay(payload, evidence)).toEqual({ kind: "invalid", reason: "scope" });
    const other = sendLatencyFixture();
    other.payload.collected_at = "2026-07-27T09:00:01.123456789Z";
    other.evidence.observed_at = "2026-07-27T09:00:01.123455Z";
    expect(sendLatencyDisplay(other.payload, other.evidence)).toEqual({ kind: "invalid", reason: "scope" });
  });

  it("rejects content attributed to another source and displays the English boundary when selected", () => {
    const { payload, evidence } = sendLatencyFixture();
    expect(sendLatencyDisplay(payload, { ...evidence, source: "admin-query" }))
      .toEqual({ kind: "invalid", reason: "scope" });
    window.localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({
      locale: "en-US", timeZone: "UTC", theme: "system",
    }));
    show(payload, evidence);
    expect(screen.getByRole("heading", { name: "Send latency segments" })).toBeVisible();
    expect(screen.getByText(/not end-to-end send latency/)).toBeVisible();
    expect(screen.getAllByText("Exact topic scope unavailable")).toHaveLength(4);
  });
});
