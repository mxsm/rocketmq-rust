import {
  act,
  render,
  renderHook,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";

import type { SreApi } from "@/api/client";
import type { CapabilityQualityProjection, CoverageMatrix } from "@/api/types";
import { WorkspaceScopeProvider } from "@/context/WorkspaceScopeContext";
import { demoClusters, demoCoverage } from "@/data/demo";
import {
  createEvidenceQualityDemo,
  demoCapabilityQuality,
} from "@/data/evidenceQualityDemo";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "@/data/phase1Demo";
import { CoverageMatrixPage } from "@/pages/CoverageMatrixPage";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import {
  CapabilityScopeError,
  useCapabilityQuality,
} from "./useCapabilityQuality";

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
      clusterIds: demoClusters.map((cluster) => cluster.id),
    },
    hasClusterScope: (id: string) =>
      demoClusters.some((cluster) => cluster.id === id),
  }),
}));

function quality() {
  const demo = createEvidenceQualityDemo(new Date("2026-11-11T10:00:00Z"));
  return demoCapabilityQuality(
    DEMO_CLUSTER_ID,
    DEMO_TENANT_ID,
    demo.evidence,
    "2026-11-11T10:00:00Z",
  );
}
function Wrapper({ children }: PropsWithChildren) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter
        initialEntries={[`/coverage?cluster_id=${DEMO_CLUSTER_ID}`]}
      >
        <WorkspaceScopeProvider>{children}</WorkspaceScopeProvider>
      </MemoryRouter>
    </OperatorPreferencesProvider>
  );
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("cluster capability quality", () => {
  beforeEach(() => {
    window.localStorage.clear();
    state.api = {
      getCoverage: vi
        .fn()
        .mockResolvedValue({ ...demoCoverage, quality: quality() }),
    } as unknown as SreApi;
  });

  it("uses the selected cluster and shows independent fact provenance without execution controls", async () => {
    const user = userEvent.setup();
    const projection = quality();
    projection.packs[0].layers.registered.source_ref = "compiled-proof-marker";
    vi.mocked(state.api.getCoverage).mockResolvedValue({
      ...demoCoverage,
      quality: projection,
    });
    render(
      <Wrapper>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    const fact = await screen.findByRole("button", {
      name: "consumer-lag.v2 · 源码注册 · 已确认",
    });
    expect(state.api.getCoverage).toHaveBeenCalledWith(
      expect.any(AbortSignal),
      DEMO_CLUSTER_ID,
      undefined,
    );
    await user.click(fact);
    expect(screen.getByText("compiled-proof-marker")).toBeVisible();
    expect(
      screen.getByRole("button", { name: "consumer-lag.v2 · 生产验证 · 未知" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /执行|审批/ }),
    ).not.toBeInTheDocument();
  });

  it("hides a projection belonging to another tenant before rendering source facts", async () => {
    const projection = quality();
    projection.tenant_id = "foreign-tenant";
    projection.packs[0].layers.registered.source_ref = "foreign-source-marker";
    vi.mocked(state.api.getCoverage).mockResolvedValue({
      ...demoCoverage,
      quality: projection,
    });
    render(
      <Wrapper>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    expect(
      await screen.findByText("质量投影与当前租户或集群不一致，已隐藏。"),
    ).toBeVisible();
    expect(screen.queryByText("foreign-source-marker")).not.toBeInTheDocument();
  });

  it("binds both revision query and response and discards late previous-revision results", async () => {
    const first = deferred<CoverageMatrix>();
    const second = deferred<CoverageMatrix>();
    vi.mocked(state.api.getCoverage)
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const { result, rerender } = renderHook(
      ({ revisionId }) =>
        useCapabilityQuality({ investigationId: "investigation", revisionId }),
      { initialProps: { revisionId: "first" }, wrapper: Wrapper },
    );
    rerender({ revisionId: "second" });
    const bound = (revisionId: string): CapabilityQualityProjection => ({
      ...quality(),
      investigation_id: "investigation",
      revision_id: revisionId,
    });
    await act(async () =>
      second.resolve({ ...demoCoverage, quality: bound("second") }),
    );
    expect(result.current.projection?.revision_id).toBe("second");
    await act(async () =>
      first.resolve({ ...demoCoverage, quality: bound("first") }),
    );
    expect(result.current.projection?.revision_id).toBe("second");
    expect(state.api.getCoverage).toHaveBeenLastCalledWith(
      expect.any(AbortSignal),
      DEMO_CLUSTER_ID,
      { investigationId: "investigation", revisionId: "second" },
    );
  });

  it("rejects an unbound or wrong-revision response for a saved revision", async () => {
    const { result } = renderHook(
      () =>
        useCapabilityQuality({
          investigationId: "investigation",
          revisionId: "requested",
        }),
      { wrapper: Wrapper },
    );
    await waitFor(() =>
      expect(result.current.error).toBeInstanceOf(CapabilityScopeError),
    );
    expect(result.current.projection).toBeUndefined();
  });
});
