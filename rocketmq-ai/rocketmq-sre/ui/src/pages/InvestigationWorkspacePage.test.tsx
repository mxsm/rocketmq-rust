import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { App } from "@/App";
import type { SreApi } from "@/api/client";
import type {
  EvidenceRecord,
  InvestigationDiagnosisRevision,
  InvestigationView,
} from "@/api/types";
import { demoClusters } from "@/data/demo";
import {
  DEMO_CLUSTER_ID,
  DEMO_TENANT_ID,
  phase1Evidence,
  phase1Investigations,
} from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";

const state = vi.hoisted(() => ({
  api: undefined as unknown as SreApi,
  auth: {
    mode: "development",
    session: {
      tenantId: "00000000-0000-4000-8000-000000000001",
      subject: "operator",
      clusterIds: [
        "10000000-0000-4000-8000-000000000001",
        "10000000-0000-4000-8000-000000000002",
      ],
      roles: ["reader"],
      displayName: "SRE",
    },
    hasClusterScope: (id: string) =>
      [
        "10000000-0000-4000-8000-000000000001",
        "10000000-0000-4000-8000-000000000002",
      ].includes(id),
  },
}));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => state.auth }));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({
    api: state.api,
    clusters: demoClusters.slice(0, 2),
    loading: false,
    demoMode: false,
  }),
}));

const INVESTIGATION = "30000000-0000-4000-8000-000000000001";
const CONVERSATION = "20000000-0000-4000-8000-000000000001";
const EVIDENCE = "40000000-0000-4000-8000-000000000001";
const WINDOW =
  "window_start=2026-11-11T09%3A00%3A00Z&window_end=2026-11-11T11%3A00%3A00Z";

function fixture(): InvestigationView {
  const revision: InvestigationDiagnosisRevision = {
    id: "revision",
    investigation_id: INVESTIGATION,
    conversation_id: CONVERSATION,
    turn_id: "turn",
    answer_revision_id: "answer",
    revision: 1,
    pack_id: "consumer-lag.v2",
    pack_version: "2.0.0",
    status: "inconclusive",
    rule_result: { missing_required_evidence: ["Consumer handler duration"] },
    hypotheses: [
      {
        root_cause: "Slow consumer handler",
        confidence_percent: 82,
        supporting_evidence: [{ evidence_id: EVIDENCE }],
        counter_evidence: [],
      },
    ],
    evidence_ids: [EVIDENCE],
    primary_model_invocation_id: null,
    execution_eligible: false,
    partial: true,
    correlation_id: "correlation",
    created_at: "2026-11-11T10:00:00Z",
  };
  return {
    investigation: {
      ...phase1Investigations.items[0].investigation,
      status: "needs_evidence",
      incident_id: undefined,
      conversation_id: CONVERSATION,
      resource: "consumer/order-worker",
      updated_at: "2026-11-11T10:00:00Z",
    },
    diagnosis_revisions: [revision],
    timeline: [],
  };
}
function evidence(): EvidenceRecord {
  return {
    ...phase1Evidence.items[0],
    evidence_id: EVIDENCE,
    cluster_id: DEMO_CLUSTER_ID,
    tenant_id: DEMO_TENANT_ID,
    source: "rocketmq-mcp",
    resource: "consumer/order-worker",
    observed_at: "2026-11-11T10:00:00Z",
    time_range: { start: "2026-11-11T09:45:00Z", end: "2026-11-11T10:00:00Z" },
  };
}
function open(path = `/investigations/${INVESTIGATION}`, extra = "") {
  window.history.replaceState(
    null,
    "",
    `${path}?cluster_id=${DEMO_CLUSTER_ID}&${WINDOW}${extra}`,
  );
  return render(
    <OperatorPreferencesProvider>
      <App />
    </OperatorPreferencesProvider>,
  );
}

describe("investigation workspace and evidence navigation", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    state.api = {
      getInvestigation: vi.fn().mockResolvedValue(fixture()),
      getEvidence: vi.fn().mockResolvedValue(evidence()),
      getEvidenceContent: vi.fn().mockResolvedValue({
        visible_connections: 3,
        password: "must-not-display",
      }),
      listEvidence: vi.fn().mockResolvedValue({
        items: [],
        partial: false,
        warnings: [],
        observed_at: "",
      }),
      listAssets: vi.fn().mockResolvedValue({
        items: [],
        partial: false,
        warnings: [],
        observed_at: "",
      }),
      subscribeWorkflowEvents: vi.fn(() => new Promise<void>(() => undefined)),
      promoteInvestigation: vi.fn(),
    } as unknown as SreApi;
  });

  it("renders server hypotheses, missing evidence, a manual next step and scoped related links", async () => {
    const user = userEvent.setup();
    open();
    expect(
      await screen.findByRole("heading", { name: "Slow consumer handler" }),
    ).toBeVisible();
    expect(screen.getByText("支持评分 82/100")).toBeVisible();
    expect(screen.getByText(/不是校准概率/)).toBeVisible();
    expect(
      screen.getAllByText("Consumer handler duration").length,
    ).toBeGreaterThan(0);
    expect(screen.getByText("手动只读调查")).toBeVisible();
    const conversation = screen.getAllByRole("link", {
      name: "打开关联会话",
    })[0];
    expect(
      new URL(
        conversation.getAttribute("href") ?? "",
        window.location.origin,
      ).searchParams.get("cluster_id"),
    ).toBe(DEMO_CLUSTER_ID);
    const evidenceLink = await screen.findByRole("link", {
      name: "在证据浏览器查看脱敏内容",
    });
    expect(
      new URL(
        evidenceLink.getAttribute("href") ?? "",
        window.location.origin,
      ).searchParams.get("evidence_id"),
    ).toBe(EVIDENCE);
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    const auditDetails = screen.getAllByText("审计详情");
    expect(screen.getByText("turn")).not.toBeVisible();
    expect(screen.getByText(evidence().content_hash)).not.toBeVisible();
    await user.click(auditDetails[0]);
    expect(screen.getByText("turn")).toBeVisible();
    await user.click(auditDetails[1]);
    expect(screen.getByText(evidence().content_hash)).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /执行|暂停|预算/ }),
    ).not.toBeInTheDocument();
  });

  it("keeps promotion input during a same-scope background refresh", async () => {
    const user = userEvent.setup();
    open();
    const reason = await screen.findByLabelText("升级原因");
    await user.type(reason, "Need sustained incident tracking");
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() =>
      expect(state.api.getInvestigation).toHaveBeenCalledTimes(2),
    );
    expect(screen.getByLabelText("升级原因")).toHaveValue(
      "Need sustained incident tracking",
    );
  });

  it("blocks mismatched resource content until the object scope is explicitly aligned", async () => {
    const user = userEvent.setup();
    open(undefined, "&resource_kind=topic&resource_key=other-topic");
    expect(
      await screen.findByText("此对象与当前选择的范围不一致"),
    ).toBeVisible();
    expect(screen.queryByText("Slow consumer handler")).not.toBeInTheDocument();
    expect(state.api.getEvidence).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "对齐到对象范围" }));
    expect(
      await screen.findByRole("heading", { name: "Slow consumer handler" }),
    ).toBeVisible();
    expect(
      new URLSearchParams(window.location.search).get("resource_key"),
    ).toBe("order-worker");
  });

  it("does not borrow an out-of-window diagnosis to imply a current conclusion", async () => {
    const user = userEvent.setup();
    const view = fixture();
    view.diagnosis_revisions[0].created_at = "2026-11-10T10:00:00Z";
    vi.mocked(state.api.getInvestigation).mockResolvedValue(view);
    open();
    expect(await screen.findByText("尚无诊断结论")).toBeVisible();
    expect(screen.queryByText("Slow consumer handler")).not.toBeInTheDocument();
    expect(state.api.getEvidence).not.toHaveBeenCalled();
    expect(screen.getByText("1 份修订不在当前时间窗内。")).toBeVisible();
    await user.click(
      screen.getByRole("button", { name: "扩展时间窗查看修订" }),
    );
    expect(
      await screen.findByRole("heading", { name: "Slow consumer handler" }),
    ).toBeVisible();
  });

  it("opens an evidence reference directly even when it is absent from the list page", async () => {
    const user = userEvent.setup();
    open("/evidence", `&evidence_id=${EVIDENCE}`);
    expect(
      await screen.findByText("rocketmq-mcp", { selector: "code" }),
    ).toBeVisible();
    expect(state.api.getEvidence).toHaveBeenCalledWith(
      EVIDENCE,
      expect.any(AbortSignal),
    );
    expect(state.api.listEvidence).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "展开详情" }));
    expect(await screen.findByText(/visible_connections/)).toHaveTextContent(
      "[redacted]",
    );
    expect(screen.queryByText(/must-not-display/)).not.toBeInTheDocument();
  });

  it("rejects a referenced evidence object from a different cluster before loading content", async () => {
    vi.mocked(state.api.getEvidence).mockResolvedValue({
      ...evidence(),
      cluster_id: demoClusters[1].id,
      partial: true,
      warnings: ["foreign-scope-marker"],
    });
    open("/evidence", `&evidence_id=${EVIDENCE}`);
    expect(
      await screen.findByText("指定证据不在当前范围内或当前身份无权读取"),
    ).toBeVisible();
    expect(
      screen.queryByText("rocketmq-mcp", { selector: "code" }),
    ).not.toBeInTheDocument();
    expect(state.api.getEvidenceContent).not.toHaveBeenCalled();
    expect(screen.queryByText("foreign-scope-marker")).not.toBeInTheDocument();
  });

  it("cancels an expanded evidence read when scope changes and ignores its late content", async () => {
    const user = userEvent.setup();
    let finish!: (value: unknown) => void;
    let signal: AbortSignal | undefined;
    vi.mocked(state.api.getEvidenceContent).mockImplementation(
      (_id, nextSignal) => {
        signal = nextSignal;
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    );
    open("/evidence", `&evidence_id=${EVIDENCE}`);
    await user.click(await screen.findByRole("button", { name: "展开详情" }));
    await waitFor(() =>
      expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1),
    );
    await user.selectOptions(
      screen.getByRole("combobox", { name: "授权集群" }),
      demoClusters[1].id,
    );
    expect(signal?.aborted).toBe(true);
    await act(async () => finish({ old_cluster_marker: "late content" }));
    expect(screen.queryByText(/old_cluster_marker/)).not.toBeInTheDocument();
  });
});
