import { render, screen, waitFor, within } from "@testing-library/react";
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
    expect(screen.getByText(/store-pressure\.v1/)).toBeVisible();
    expect(
      vi.mocked(state.api.getEvidence).mock.calls.map(([id]) => id),
    ).toEqual(revision.evidence_ids.slice(0, 2));
  });

  it("uses the server's current Store v2 chain without borrowing the retained legacy pack's complete fact", async () => {
    const user = userEvent.setup();
    const { demo, revision, projection } = fixture();
    const onOpenEvidence = vi.fn();
    const legacy = projection.packs.find(
      (pack) => pack.pack_id === "store-pressure.v1",
    )!;
    const current = structuredClone(legacy);
    current.pack_id = "store-pressure.v2";
    current.pack_version = "2.0.0";
    current.layers.target_observed.status = "absent";
    current.layers.target_observed.reason_code = "target_evidence_not_qualified";
    current.requirements[0] = {
      ...current.requirements[0],
      quality: "partial",
      reason_code: "compiled_evidence_facts_incomplete",
      missing_fields: [
        "store-pressure.flush_dispatch_monotonic_progress_and_policy",
      ],
      fields: [
        { path: "schema_version", kind: "text" },
        { path: "profile", kind: "text" },
        { path: "observed_brokers", kind: "number" },
      ],
    };
    const storeId = current.requirements[0].evidence_ids[0];
    const sample = demo.evidence.find((item) => item.evidence_id === storeId)!;
    sample.partial = true;
    sample.coverage = "partial";
    sample.content = {
      storage: "inline",
      value: {
        schema_version: "rocketmq.sre-diagnostic-source.v1",
        profile: "store_pressure",
        observed_brokers: 1,
        pressure_observed_brokers: 0,
        unavailable_brokers: 0,
        missing_dimensions: ["flush_dispatch_monotonic_progress"],
        brokers: [
          {
            broker_name: "demo",
            broker_id: 0,
            store_pressure: null,
            observation_status: "store_pressure_observation_missing",
          },
        ],
      },
    };
    projection.packs.push(current);
    projection.scenarios[0] = {
      id: "backlog_store",
      pack_ids: ["consumer-lag.v2", "store-pressure.v2"],
      required_signal_ids: [
        "consumer-lag.v2:consumer-lag",
        "store-pressure.v2:store-pressure",
      ],
    };
    revision.hypotheses = [];
    render(
      <Wrapper>
        <ScenarioEvidenceChain
          projection={projection}
          revision={revision}
          timeline={[]}
          onOpenEvidence={onOpenEvidence}
        />
      </Wrapper>,
    );
    expect(
      await screen.findByText("已匹配当前修订的必需信号: 1 / 2"),
    ).toBeVisible();
    const currentSlot = screen.getByText(/store-pressure\.v2/).closest("li")!;
    expect(
      within(currentSlot).getByText(
        /store-pressure\.flush_dispatch_monotonic_progress_and_policy/,
      ),
    ).toBeVisible();
    expect(screen.queryByText(/store-pressure\.v1/)).not.toBeInTheDocument();
    expect(state.api.getEvidence).toHaveBeenCalledTimes(2);
    expect(vi.mocked(state.api.getEvidence).mock.calls.map(([id]) => id)).toEqual(
      revision.evidence_ids.slice(0, 2),
    );
    await user.click(
      within(currentSlot).getByRole("button", { name: /查看证据/ }),
    );
    expect(onOpenEvidence).toHaveBeenCalledWith(storeId, {
      maxAgeSeconds: 300,
      asOf: projection.observed_at,
      sourceRef:
        "compiled_diagnostic_registry:store-pressure.v2:store-pressure",
    });
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
