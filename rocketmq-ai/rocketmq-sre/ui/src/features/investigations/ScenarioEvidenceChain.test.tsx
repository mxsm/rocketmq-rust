import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { ApiError, type SreApi } from "@/api/client";
import { WorkspaceScopeProvider } from "@/context/WorkspaceScopeContext";
import { demoClusters } from "@/data/demo";
import {
  createEvidenceQualityDemo,
  demoCapabilityQuality,
} from "@/data/evidenceQualityDemo";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { ScenarioEvidenceChain } from "./ScenarioEvidenceChain";

const state = vi.hoisted(() => ({ api: undefined as unknown as SreApi }));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ api: state.api, clusters: demoClusters }),
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: {
      tenantId: DEMO_TENANT_ID,
      subject: "reader",
      roles: ["reader"],
      clusterIds: [DEMO_CLUSTER_ID],
    },
    hasClusterScope: (id: string) => id === DEMO_CLUSTER_ID,
  }),
}));

function fixture() {
  const demo = createEvidenceQualityDemo(new Date("2026-11-11T10:00:00Z"));
  const projection = demoCapabilityQuality(
    DEMO_CLUSTER_ID,
    DEMO_TENANT_ID,
    demo.evidence,
    "2026-11-11T10:00:00Z",
  );
  const revision = {
    ...demo.investigations[0].diagnosis_revisions[0],
    evidence_ids: [
      ...demo.investigations[0].diagnosis_revisions[0].evidence_ids,
      ...Array.from({ length: 126 }, (_, index) => `context-${index}`),
    ],
  };
  state.api = {
    getEvidence: vi.fn(async (id: string) => {
      const evidence = demo.evidence.find((item) => item.evidence_id === id);
      if (!evidence) throw new Error("unexpected context prefetch");
      return evidence;
    }),
  } as unknown as SreApi;
  return { demo, revision, projection };
}
function Wrapper({ children }: { children: React.ReactNode }) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter
        initialEntries={[
          `/investigations/object?cluster_id=${DEMO_CLUSTER_ID}&window_start=2026-11-11T09%3A00%3A00Z&window_end=2026-11-11T11%3A00%3A00Z`,
        ]}
      >
        <WorkspaceScopeProvider>{children}</WorkspaceScopeProvider>
      </MemoryRouter>
    </OperatorPreferencesProvider>
  );
}

describe("scenario chain metadata proof", () => {
  beforeEach(() => window.localStorage.clear());
  it("prefetches only current-scenario proof references while leaving 126 context records on demand", async () => {
    const { revision, projection } = fixture();
    render(
      <Wrapper>
        <ScenarioEvidenceChain
          projection={projection}
          revision={revision}
          timeline={[]}
          onOpenEvidence={vi.fn()}
        />
      </Wrapper>,
    );
    expect(
      await screen.findByText("已匹配当前修订的必需信号: 2 / 2"),
    ).toBeVisible();
    expect(state.api.getEvidence).toHaveBeenCalledTimes(2);
    expect(
      vi.mocked(state.api.getEvidence).mock.calls.map(([id]) => id),
    ).toEqual(revision.evidence_ids.slice(0, 2));
  });

  it("shows a metadata failure reason and can retry without presenting the backend available fact as proof", async () => {
    const user = userEvent.setup();
    const { revision, projection } = fixture();
    const read = vi.mocked(state.api.getEvidence).getMockImplementation()!;
    vi.mocked(state.api.getEvidence)
      .mockImplementationOnce(async () => {
        throw new ApiError(503, "unavailable", "private-error-details");
      })
      .mockImplementation(read);
    render(
      <Wrapper>
        <ScenarioEvidenceChain
          projection={projection}
          revision={revision}
          timeline={[]}
          onOpenEvidence={vi.fn()}
        />
      </Wrapper>,
    );
    expect(await screen.findByRole("button", { name: "重试" })).toBeVisible();
    expect(screen.getByText("已匹配当前修订的必需信号: 1 / 2")).toBeVisible();
    expect(screen.queryByText("private-error-details")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重试" }));
    await waitFor(() =>
      expect(screen.getByText("已匹配当前修订的必需信号: 2 / 2")).toBeVisible(),
    );
    expect(
      screen.queryByRole("button", { name: "重试" }),
    ).not.toBeInTheDocument();
  });
});
