import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { useState } from "react";

import { ApiError, type SreApi } from "@/api/client";
import { WorkspaceScopeProvider } from "@/context/WorkspaceScopeContext";
import { demoClusters } from "@/data/demo";
import {
  DEMO_CLUSTER_ID,
  DEMO_TENANT_ID,
  phase1Evidence,
} from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { EvidenceDrawer } from "./EvidenceDrawer";
import type { EvidenceAccess, EvidenceFreshnessPolicy } from "./evidencePresentation";
import { sendLatencyFixture } from "./test-fixtures/sendLatency";
import { namesrvRouteFixture } from "./test-fixtures/namesrvRoute";

const state = vi.hoisted(() => ({ api: undefined as unknown as SreApi }));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ api: state.api, clusters: demoClusters }),
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: {
      tenantId: DEMO_TENANT_ID,
      subject: "reader",
      clusterIds: demoClusters.map((cluster) => cluster.id),
      roles: ["reader"],
    },
    hasClusterScope: (id: string) =>
      demoClusters.some((cluster) => cluster.id === id),
  }),
}));

const ID = phase1Evidence.items[0].evidence_id;
function OpenDrawer({
  id = ID,
  access,
  freshnessPolicy,
}: {
  id?: string;
  access?: EvidenceAccess;
  freshnessPolicy?: EvidenceFreshnessPolicy;
}) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)} type="button">
        open evidence
      </button>
      {open && (
        <EvidenceDrawer
          evidenceId={id}
          onClose={() => setOpen(false)}
          access={access}
          freshnessPolicy={freshnessPolicy}
        />
      )}
    </>
  );
}
function Fixture({
  id,
  access,
  resource = "",
  freshnessPolicy,
}: {
  id?: string;
  access?: EvidenceAccess;
  resource?: string;
  freshnessPolicy?: EvidenceFreshnessPolicy;
}) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter
        initialEntries={[
          `/evidence?cluster_id=${DEMO_CLUSTER_ID}&window_start=2026-07-27T08%3A00%3A00Z&window_end=2026-07-27T09%3A00%3A00Z${resource}`,
        ]}
      >
        <WorkspaceScopeProvider>
          <OpenDrawer id={id} access={access} freshnessPolicy={freshnessPolicy} />
        </WorkspaceScopeProvider>
      </MemoryRouter>
    </OperatorPreferencesProvider>
  );
}

describe("shared evidence drawer", () => {
  beforeEach(() => {
    window.localStorage.clear();
    state.api = {
      getEvidence: vi.fn().mockResolvedValue(phase1Evidence.items[0]),
      getEvidenceContent: vi.fn().mockResolvedValue({
        lag: 0,
        accessToken: "secret-token",
        messageBody: "secret-body",
      }),
    } as unknown as SreApi;
  });

  it("reads metadata first, traps keyboard focus, reads content on demand and returns focus on Escape", async () => {
    const user = userEvent.setup();
    render(<Fixture />);
    const trigger = screen.getByRole("button", { name: "open evidence" });
    await user.click(trigger);
    const close = screen.getByRole("button", { name: "关闭证据详情" });
    expect(close).toHaveFocus();
    const read = await screen.findByRole("button", { name: "读取脱敏内容" });
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    await user.keyboard("{Shift>}{Tab}{/Shift}");
    expect(read).toHaveFocus();
    await user.tab();
    expect(close).toHaveFocus();
    await user.click(read);
    expect(await screen.findByText(/"lag": 0/)).toHaveTextContent("[redacted]");
    expect(
      screen.queryByText(/secret-token|secret-body/),
    ).not.toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it.each([
    ["zh-CN", "此证据仅提供当前采样上下文；历史覆盖仍未知。"],
    ["en-US", "This evidence provides current sampling context only; historical coverage remains unknown."],
  ])("explains the current-context metadata warning in %s without reading content", async (locale, message) => {
    window.localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({
      locale, timeZone: "UTC", theme: "system",
    }));
    const { evidence } = namesrvRouteFixture();
    vi.mocked(state.api.getEvidence).mockResolvedValue({
      ...evidence,
      warnings: ["namesrv_current_context_only", "existing_warning_marker"],
    });
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    expect(await screen.findByText(message)).toBeVisible();
    expect(screen.queryByText("namesrv_current_context_only")).not.toBeInTheDocument();
    expect(screen.getByText("existing_warning_marker")).toBeVisible();
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
  });

  it("keeps new preparation MAC and private Agent content out of the explicit audit viewer", async () => {
    vi.mocked(state.api.getEvidence).mockResolvedValue({
      ...phase1Evidence.items[0], source: "execution-agent", resource: "broker/broker-a",
    });
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue({
      schema_version: "rocketmq-sre.action-preparation.v1",
      evidence_id: ID,
      candidate_binding: { action_id: "observability.logger_level_ttl.v1", private_extra: "private-binding-marker" },
      agent_result: { target: "private-agent-marker", ready: true },
      origin_signature: "private-origin-marker",
      debug: "private-debug-marker",
    });
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    const read = await screen.findByRole("button", { name: "读取脱敏内容" });
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    await user.click(read);
    expect(await screen.findByText(/observability\.logger_level_ttl\.v1/)).toHaveTextContent("[redacted]");
    expect(screen.queryByText(/private-(agent|binding|origin|debug)-marker/)).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).toHaveBeenCalledOnce();
  });

  it.each([
    [403, "当前身份无权读取此证据"],
    [404, "未找到此证据记录"],
    [503, "证据数据源暂不可用"],
  ])(
    "keeps HTTP %s separate and does not fetch content",
    async (status, message) => {
      const user = userEvent.setup();
      vi.mocked(state.api.getEvidence).mockRejectedValue(
        new ApiError(status, "read_failed", "sensitive backend details"),
      );
      render(<Fixture />);
      await user.click(screen.getByRole("button", { name: "open evidence" }));
      expect(await screen.findByText(message)).toBeVisible();
      expect(
        screen.queryByText("sensitive backend details"),
      ).not.toBeInTheDocument();
      expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    },
  );

  it("blocks a forged related-resource ID before any content or foreign warning is displayed", async () => {
    const user = userEvent.setup();
    vi.mocked(state.api.getEvidence).mockResolvedValue({
      ...phase1Evidence.items[0],
      warnings: ["foreign-warning-marker"],
    });
    render(
      <Fixture
        access={{
          kind: "revision_evidence",
          revisionId: "revision",
          evidenceIds: [],
        }}
        resource="&resource_kind=topic&resource_key=orders"
      />,
    );
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    expect(
      await screen.findByText("证据与当前租户、集群、资源或引用不一致"),
    ).toBeVisible();
    expect(
      screen.queryByText("foreign-warning-marker"),
    ).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
  });

  it("accepts a saved same-cluster related resource with its explicit provenance", async () => {
    const user = userEvent.setup();
    render(
      <Fixture
        access={{
          kind: "revision_evidence",
          revisionId: "revision",
          evidenceIds: [ID],
        }}
        resource="&resource_kind=topic&resource_key=orders"
      />,
    );
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    expect(await screen.findByText("当前修订关联资源的证据")).toBeVisible();
    expect(screen.getByRole("button", { name: "读取脱敏内容" })).toBeEnabled();
  });

  it("cancels pending content when the referenced revision changes and ignores its late result", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = sendLatencyFixture();
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    let resolve!: (value: unknown) => void;
    let signal: AbortSignal | undefined;
    vi.mocked(state.api.getEvidenceContent).mockImplementation(
      (_id, nextSignal) => {
        signal = nextSignal;
        return new Promise((done) => {
          resolve = done;
        });
      },
    );
    const access = {
      kind: "revision_evidence" as const,
      revisionId: "first",
      evidenceIds: [ID],
    };
    const { rerender } = render(<Fixture access={access} />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    await user.click(
      await screen.findByRole("button", { name: "读取脱敏内容" }),
    );
    await waitFor(() =>
      expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1),
    );
    rerender(<Fixture access={{ ...access, revisionId: "second" }} />);
    expect(signal?.aborted).toBe(true);
    await act(async () =>
      resolve({ ...payload, stale_revision_marker: "must stay hidden" }),
    );
    expect(screen.queryByText(/stale_revision_marker/)).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "发送延迟分段" })).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1);
  });

  it("keeps missing evidence separate from unavailable content", async () => {
    const user = userEvent.setup();
    vi.mocked(state.api.getEvidence).mockResolvedValue({
      ...phase1Evidence.items[0],
      coverage: "missing",
    });
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    expect(await screen.findByText("证据缺失")).toBeVisible();
    expect(screen.getByText("缺失证据没有可读取的内容。")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "读取脱敏内容" }),
    ).not.toBeInTheDocument();
  });

  it("renders latency only after the authorized content click, independently of bounded audit JSON", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = sendLatencyFixture();
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue({
      raw_histograms: Array.from({ length: 100 }, () => Array.from({ length: 100 }, () => 1)),
      ...payload,
      accessToken: "private-marker",
    });
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    const read = await screen.findByRole("button", { name: "读取脱敏内容" });
    expect(screen.queryByRole("heading", { name: "发送延迟分段" })).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    await user.click(read);
    expect(await screen.findByText("保存的 P99 估计: 248.5 ms")).toBeVisible();
    expect(screen.getByText(/采集完成时间在当前时间窗外/)).toBeVisible();
    expect(screen.getAllByText("缺少精确 Topic 范围")).toHaveLength(4);
    expect(screen.queryByText(/private-marker/)).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1);
  });

  it("does not display foreign latency content under authorized metadata", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = sendLatencyFixture();
    payload.scope.cluster_id = "foreign-scope-marker";
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue(payload);
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    await user.click(await screen.findByRole("button", { name: "读取脱敏内容" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("范围不一致");
    expect(screen.queryByText(/foreign-scope-marker|248.5/)).not.toBeInTheDocument();
  });

  it("loads the route summary only on the authorized content click and whitelists its expanded audit", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = namesrvRouteFixture();
    payload.rounds[0].peers[0].route.normalized = { addresses: ["private-route-address"], config: "private-route-config" };
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue({
      ...payload, remote_error: "private-remote-error", raw_config: "private-route-config",
    });
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    const read = await screen.findByRole("button", { name: "读取脱敏内容" });
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    expect(screen.queryByRole("heading", { name: "NameServer 路由对比" })).not.toBeInTheDocument();
    await user.click(read);
    expect(await screen.findByRole("heading", { name: "NameServer 路由对比" })).toBeVisible();
    expect(screen.getByText(/不能证明所选历史窗的路由状态/)).toBeVisible();
    await user.click(screen.getByText("脱敏 JSON 审计内容"));
    expect(screen.getByText(/"plannedRoute": 4/)).toBeVisible();
    expect(screen.queryByText(/private-route-address|private-route-config|private-remote-error/)).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1);
  });

  it.each(["scope", "structure"] as const)("hides raw route content after a recognized %s failure", async (failure) => {
    const user = userEvent.setup();
    const { payload, evidence } = namesrvRouteFixture();
    if (failure === "scope") payload.cluster_id = "foreign-route-marker";
    else payload.rounds[1].round = 1;
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue({ ...payload, raw_config: "private-route-config" });
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    await user.click(await screen.findByRole("button", { name: "读取脱敏内容" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(failure === "scope" ? "范围不一致" : "安全展示校验");
    expect(screen.queryByRole("heading", { name: "NameServer 路由对比" })).not.toBeInTheDocument();
    expect(screen.queryByText(/foreign-route-marker|private-route-config|"ns-a"/)).not.toBeInTheDocument();
  });

  it("does not restore late route content after the saved revision changes", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = namesrvRouteFixture();
    let finish!: (value: unknown) => void;
    let oldSignal: AbortSignal | undefined;
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockImplementation((_id, signal) => {
      oldSignal = signal;
      return new Promise((resolve) => { finish = resolve; });
    });
    const access = { kind: "revision_evidence" as const, revisionId: "route-revision-a", evidenceIds: [evidence.evidence_id] };
    const view = render(<Fixture access={access} />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    await user.click(await screen.findByRole("button", { name: "读取脱敏内容" }));
    await waitFor(() => expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1));
    view.rerender(<Fixture access={{ ...access, revisionId: "route-revision-b" }} />);
    await waitFor(() => expect(oldSignal?.aborted).toBe(true));
    await act(async () => finish(payload));
    expect(screen.queryByRole("heading", { name: "NameServer 路由对比" })).not.toBeInTheDocument();
    expect(screen.queryByText("持续观察到局部路由差异")).not.toBeInTheDocument();
  });

  it("keeps expired route evidence expired when its saved comparison is opened", async () => {
    const user = userEvent.setup();
    const { payload, evidence } = namesrvRouteFixture();
    vi.mocked(state.api.getEvidence).mockResolvedValue(evidence);
    vi.mocked(state.api.getEvidenceContent).mockResolvedValue(payload);
    render(<Fixture freshnessPolicy={{ maxAgeSeconds: 300, asOf: "2026-07-27T09:05:03Z", sourceRef: "compiled-route-fixture" }} />);
    await user.click(screen.getByRole("button", { name: "open evidence" }));
    expect(await screen.findByText(/超过要求时效/)).toBeVisible();
    await user.click(await screen.findByRole("button", { name: "读取脱敏内容" }));
    expect(await screen.findByRole("heading", { name: "NameServer 路由对比" })).toBeVisible();
    expect(screen.getByText("保存时的比较结果")).toBeVisible();
    expect(screen.getByText(/超过要求时效/)).toBeVisible();
    expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1);
  });
});
