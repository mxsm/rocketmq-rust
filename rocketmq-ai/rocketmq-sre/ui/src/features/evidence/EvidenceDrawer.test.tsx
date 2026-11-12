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
import type { EvidenceAccess } from "./evidencePresentation";

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
}: {
  id?: string;
  access?: EvidenceAccess;
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
        />
      )}
    </>
  );
}
function Fixture({
  id,
  access,
  resource = "",
}: {
  id?: string;
  access?: EvidenceAccess;
  resource?: string;
}) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter
        initialEntries={[
          `/evidence?cluster_id=${DEMO_CLUSTER_ID}&window_start=2026-07-27T08%3A00%3A00Z&window_end=2026-07-27T09%3A00%3A00Z${resource}`,
        ]}
      >
        <WorkspaceScopeProvider>
          <OpenDrawer id={id} access={access} />
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
      resolve({ stale_revision_marker: "must stay hidden" }),
    );
    expect(screen.queryByText(/stale_revision_marker/)).not.toBeInTheDocument();
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
});
