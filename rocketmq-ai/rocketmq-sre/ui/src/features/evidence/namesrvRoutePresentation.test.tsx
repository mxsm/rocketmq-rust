import { render, screen, within } from "@testing-library/react";

import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { NamesrvRouteSummary } from "./NamesrvRouteSummary";
import { namesrvRouteDisplay } from "./namesrvRoutePresentation";
import { namesrvRouteFixture } from "./test-fixtures/namesrvRoute";

function show(payload: unknown, evidence = namesrvRouteFixture().evidence) {
  const display = namesrvRouteDisplay(payload, evidence);
  if (!display) throw new Error("Expected recognized synthetic route data");
  render(<OperatorPreferencesProvider><NamesrvRouteSummary display={display} /></OperatorPreferencesProvider>);
  return display;
}

describe("saved NameServer route display", () => {
  beforeEach(() => window.localStorage.clear());

  it("keeps two rounds per peer, current advisory and unknown historical registration facts distinct", () => {
    const { payload, evidence } = namesrvRouteFixture();
    show(payload, evidence);
    expect(screen.getByRole("heading", { name: "NameServer 路由对比" })).toBeVisible();
    const peers = within(screen.getByRole("list", { name: "独立节点声明下的采样" })).getAllByRole("listitem");
    expect(peers).toHaveLength(2);
    for (const peer of peers) {
      expect(within(peer).getByRole("region", { name: "第 1 轮" })).toBeVisible();
      expect(within(peer).getByRole("region", { name: "第 2 轮" })).toBeVisible();
    }
    expect(screen.getByText("持续观察到局部路由差异")).toBeVisible();
    expect(screen.getByText("部分观测 · 结论未定")).toBeVisible();
    expect(screen.getByText(/不能证明历史状态或根因/)).toBeVisible();
    expect(screen.getByText("首次注册时刻：未知")).toBeInTheDocument();
    expect(screen.getByText("源路由版本：未知")).toBeInTheDocument();
    expect(screen.queryByText(/RootCause|Healthy|根因已确定|Broker 不可达$/)).not.toBeInTheDocument();
  });

  it("shows source-reported absence separately from skipped or failed reads and actual RPC counts", () => {
    const { payload } = namesrvRouteFixture();
    for (const round of payload.rounds) {
      const peer = round.peers[1];
      peer.configuration.status = "unavailable";
      peer.configuration.reason_code = "view_configuration_unavailable";
      peer.configuration.flags = { cluster_test: null, order_message_enable: null, support_acting_master: null };
      Object.assign(peer.route, { status: "not_attempted", reason_code: "route_not_attempted_view_ineligible", started_at: null, finished_at: null });
    }
    payload.comparison.state = "incomparable";
    payload.comparison.reason_code = "current_routes_incomparable";
    payload.read_statistics.started_route_rpcs = 2;
    show(payload);
    expect(screen.getByText("无法进行完整比较")).toBeVisible();
    expect(screen.getAllByText("未发起")).toHaveLength(2);
    expect(screen.queryByText("源明确报告 Topic 不存在")).not.toBeInTheDocument();
    expect(screen.getByText("4 / 2")).toBeInTheDocument();
    expect(screen.getAllByText(/该轮没有发起路由请求/)).toHaveLength(2);
  });

  it.each(["tenant_id", "cluster_id", "topic"] as const)("blocks foreign %s before displaying content", (key) => {
    const { payload, evidence } = namesrvRouteFixture();
    payload[key] = "foreign-marker";
    show(payload, evidence);
    expect(screen.getByRole("alert")).toHaveTextContent("范围不一致");
    expect(screen.queryByText(/foreign-marker|ns-a|Topic 存在状态/)).not.toBeInTheDocument();
  });

  it("uses the exact source/exposure and preserves literal topic characters", () => {
    const { payload, evidence } = namesrvRouteFixture();
    expect(namesrvRouteDisplay(payload, { ...evidence, source: "rocketmq-mcp" })).toEqual({ kind: "invalid", reason: "scope" });
    expect(namesrvRouteDisplay(payload, { ...evidence, exposure: "synthetic" })).toEqual({ kind: "invalid", reason: "scope" });
    payload.topic = "ns:orders%retry";
    evidence.resource = `namesrv-route/${payload.topic}`;
    show(payload, evidence);
    expect(screen.getByText(payload.topic)).toBeVisible();
  });

  it("rejects duplicate peers/rounds, fabricated historical coverage and malformed dates", () => {
    const duplicate = namesrvRouteFixture();
    duplicate.payload.rounds[1].peers[1].peer_id = "ns-a";
    expect(namesrvRouteDisplay(duplicate.payload, duplicate.evidence)?.kind).toBe("invalid");
    const round = namesrvRouteFixture();
    round.payload.rounds[1].round = 1;
    expect(namesrvRouteDisplay(round.payload, round.evidence)?.kind).toBe("invalid");
    const coverage = namesrvRouteFixture();
    coverage.payload.historical_coverage = "available";
    expect(namesrvRouteDisplay(coverage.payload, coverage.evidence)?.kind).toBe("invalid");
    const date = namesrvRouteFixture();
    date.payload.collection_started_at = "2026-02-30T09:00:00Z";
    expect(namesrvRouteDisplay(date.payload, date.evidence)?.kind).toBe("invalid");
  });

  it("rejects unbounded lists, missing source facts and unsafe RPC numbers", () => {
    const { payload, evidence } = namesrvRouteFixture();
    payload.configuration.peers.push(...Array.from({ length: 3 }, (_, i) => ({
      peer_id: `ns-${i}`, peer_endpoint_id: `sha256:${String(i).repeat(64)}`,
    })));
    expect(namesrvRouteDisplay(payload, evidence)?.kind).toBe("invalid");
    const missing = namesrvRouteFixture();
    missing.payload.missing_facts = [];
    expect(namesrvRouteDisplay(missing.payload, missing.evidence)?.kind).toBe("invalid");
    const count = namesrvRouteFixture();
    count.payload.read_statistics.started_route_rpcs = Number.MAX_SAFE_INTEGER + 1;
    expect(namesrvRouteDisplay(count.payload, count.evidence)?.kind).toBe("invalid");
  });

  it("rejects misleading planned/started counts and route results without an available view", () => {
    const planned = namesrvRouteFixture();
    planned.payload.read_statistics.planned_route_rpcs = 1;
    expect(namesrvRouteDisplay(planned.payload, planned.evidence)?.kind).toBe("invalid");
    const started = namesrvRouteFixture();
    started.payload.read_statistics.started_config_rpcs = 0;
    expect(namesrvRouteDisplay(started.payload, started.evidence)?.kind).toBe("invalid");
    const skipped = namesrvRouteFixture();
    skipped.payload.rounds[0].peers[0].route.status = "not_attempted";
    expect(namesrvRouteDisplay(skipped.payload, skipped.evidence)?.kind).toBe("invalid");
    const view = namesrvRouteFixture();
    view.payload.rounds[0].peers[0].configuration.status = "invalid";
    expect(namesrvRouteDisplay(view.payload, view.evidence)?.kind).toBe("invalid");
  });

  it("keeps malformed-source row work unknown while preserving safe partial read states", () => {
    const { payload } = namesrvRouteFixture();
    for (const round of payload.rounds) {
      const peer = round.peers[1];
      Object.assign(peer.configuration, { status: "invalid", reason_code: "view_configuration_invalid", flags: null });
      Object.assign(peer.route, { status: "not_attempted", reason_code: "source_response_bounded", started_at: null, finished_at: null });
    }
    payload.read_statistics.started_route_rpcs = 2;
    payload.read_statistics.row_work = null;
    payload.comparison.state = "incomparable";
    const display = show(payload);
    expect(display.kind).toBe("summary");
    expect(screen.getByText("1,420 / 未知")).toBeInTheDocument();
    expect(screen.getAllByText("观测无效")).toHaveLength(2);
    expect(screen.getAllByText(/未读取的内容不代表 Topic 不存在或路由相同/)).toHaveLength(2);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("requires the configured safe view for every available read and skips routes for ineligible views", () => {
    for (const expected of [null, { cluster_test: false }, {
      cluster_test: true, order_message_enable: false, support_acting_master: false,
    }]) {
      const fixture = namesrvRouteFixture();
      Object.assign(fixture.payload.configuration, { expected_view: expected });
      expect(namesrvRouteDisplay(fixture.payload, fixture.evidence)?.kind).toBe("invalid");
    }
    const mismatched = namesrvRouteFixture();
    mismatched.payload.rounds[0].peers[0].configuration.flags.order_message_enable = true;
    expect(namesrvRouteDisplay(mismatched.payload, mismatched.evidence)?.kind).toBe("invalid");
    const ineligible = namesrvRouteFixture();
    ineligible.payload.rounds[0].peers[0].configuration.status = "view_mismatch";
    ineligible.payload.rounds[0].peers[0].route.status = "unavailable";
    expect(namesrvRouteDisplay(ineligible.payload, ineligible.evidence)?.kind).toBe("invalid");
  });

  it("binds each round to distinct configured endpoint digests without returning those identifiers", () => {
    const fixture = namesrvRouteFixture();
    const display = namesrvRouteDisplay(fixture.payload, fixture.evidence);
    expect(display?.kind).toBe("summary");
    expect(JSON.stringify(display)).not.toContain(fixture.payload.configuration.peers[0].peer_endpoint_id);
    const duplicate = namesrvRouteFixture();
    duplicate.payload.configuration.peers[1].peer_endpoint_id = duplicate.payload.configuration.peers[0].peer_endpoint_id;
    expect(namesrvRouteDisplay(duplicate.payload, duplicate.evidence)?.kind).toBe("invalid");
    const swapped = namesrvRouteFixture();
    swapped.payload.rounds[1].peers[0].peer_endpoint_id = swapped.payload.configuration.peers[1].peer_endpoint_id;
    expect(namesrvRouteDisplay(swapped.payload, swapped.evidence)?.kind).toBe("invalid");
    const malformed = namesrvRouteFixture();
    malformed.payload.configuration.peers[0].peer_endpoint_id = "private-endpoint-marker";
    expect(namesrvRouteDisplay(malformed.payload, malformed.evidence)?.kind).toBe("invalid");
  });

  it("preserves a legitimate requested point window as current context", () => {
    const { payload, evidence } = namesrvRouteFixture();
    payload.requested_window.start = payload.requested_window.end;
    evidence.time_range.start = evidence.time_range.end;
    const display = show(payload, evidence);
    expect(display.kind).toBe("summary");
    expect(screen.getByText("部分观测 · 结论未定")).toBeVisible();
    expect(screen.getByText(/不能证明历史状态或根因/)).toBeVisible();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("whitelists audit fields and never reflects unknown reasons, config, endpoint or normalized content", () => {
    const { payload, evidence } = namesrvRouteFixture();
    payload.rounds[0].peers[0].route.reason_code = "Bearer secret-error-marker";
    payload.rounds[0].peers[0].route.normalized = { address: "private-endpoint-marker", config: "private-config-marker" };
    payload.comparison.unknown_dimensions = ["private-dimension-marker"];
    Object.assign(payload, { raw_config: "private-config-marker", endpoint: "private-endpoint-marker" });
    const display = show(payload, evidence);
    expect(screen.getByText("未提供可识别的安全原因。")).toBeVisible();
    expect(screen.getByText("未识别的比较维度")).toBeVisible();
    expect(JSON.stringify(display)).not.toMatch(/secret-error-marker|private-config-marker|private-endpoint-marker|private-dimension-marker/);
  });

  it("accepts only exact or documented nanosecond-to-microsecond persistence binding", () => {
    const { payload, evidence } = namesrvRouteFixture();
    payload.requested_window.start = "2026-07-27T08:55:00.123456789Z";
    payload.requested_window.end = "2026-07-27T09:00:00.987654321Z";
    payload.collected_at = "2026-07-27T09:00:01.100000999Z";
    evidence.time_range = { start: "2026-07-27T08:55:00.123456Z", end: "2026-07-27T09:00:00.987654Z" };
    expect(namesrvRouteDisplay(payload, evidence)?.kind).toBe("summary");
    evidence.time_range.start = "2026-07-27T08:55:00.123455Z";
    expect(namesrvRouteDisplay(payload, evidence)).toEqual({ kind: "invalid", reason: "scope" });
  });

  it("retains legacy MCP data on its existing path and localizes English read-only boundaries", () => {
    const fixture = namesrvRouteFixture();
    expect(namesrvRouteDisplay({ topic: "orders", route_available: true }, { ...fixture.evidence, source: "rocketmq-mcp" })).toBeUndefined();
    window.localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({ locale: "en-US", timeZone: "UTC", theme: "system" }));
    show(fixture.payload, fixture.evidence);
    expect(screen.getByRole("heading", { name: "NameServer route comparison" })).toBeVisible();
    expect(screen.getByText(/does not attest instance identity/)).toBeVisible();
    expect(screen.getByText(/no execution authority/)).toBeVisible();
  });
});
