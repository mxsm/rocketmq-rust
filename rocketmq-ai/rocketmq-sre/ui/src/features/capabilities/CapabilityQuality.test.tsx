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
import { CapabilityQualitySummary } from "./CapabilityQualitySummary";

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
function Wrapper({ children, initial }: PropsWithChildren<{ initial?: string }>) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter
        initialEntries={[initial ?? `/coverage?cluster_id=${DEMO_CLUSTER_ID}`]}
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
      getInvestigation: vi.fn(),
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
      await screen.findByText("质量投影与当前租户、集群、目标或修订范围不一致，已隐藏。"),
    ).toBeVisible();
    expect(screen.queryByText("foreign-source-marker")).not.toBeInTheDocument();
  });

  it("rejects an unrequested canonical target projection instead of treating it as cluster coverage", async () => {
    const projection = quality();
    projection.resource = "store-pressure/unrequested-target";
    projection.packs[0].layers.registered.source_ref = "unrequested-target-marker";
    vi.mocked(state.api.getCoverage).mockResolvedValue({ ...demoCoverage, quality: projection });
    render(
      <Wrapper>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    expect(await screen.findByText("质量投影与当前租户、集群、目标或修订范围不一致，已隐藏。")).toBeVisible();
    expect(screen.queryByText("unrequested-target-marker")).not.toBeInTheDocument();
  });

  it("does not count a foreign nested verification record in the cluster summary", async () => {
    const projection = quality();
    projection.packs = [projection.packs[0]];
    const pack = projection.packs[0];
    pack.pack_version = "synthetic-version";
    pack.layers.test_verified = {
      status: "present",
      reason_code: "synthetic-passed",
      source_ref: "foreign-fact-source-marker",
      observed_at: "1980-01-01T12:34:56Z",
      verification: {
        record_id: "synthetic-record",
        verified_by: "foreign-verifier-marker",
        recorded_at: "2026-11-11T09:55:00Z",
        assessment: {
          schema_version: "rocketmq-sre.capability-verification.v1",
          tenant_id: "foreign-tenant",
          cluster_id: DEMO_CLUSTER_ID,
          pack_id: pack.pack_id,
          pack_version: "synthetic-version",
          level: "test",
          method: "deterministic_fixture",
          outcome: "passed",
          resource: null,
          investigation_id: null,
          revision_id: null,
          time_range: { start: "2026-11-11T09:00:00Z", end: "2026-11-11T09:50:00Z" },
          observed_at: "2026-11-11T09:50:00Z",
          valid_until: "2026-11-11T10:30:00Z",
          source_ref: "synthetic-controlled-report",
          report_digest: "sha256:synthetic-digest",
          evidence_ids: [],
        },
      },
    };
    vi.mocked(state.api.getCoverage).mockResolvedValue({ ...demoCoverage, quality: projection });
    render(
      <Wrapper>
        <CapabilityQualitySummary />
      </Wrapper>,
    );
    expect(await screen.findByText(/^测试验证:\s*未知$/)).toBeVisible();
    expect(screen.queryByText(/测试验证:.*已确认/)).not.toBeInTheDocument();
    expect(screen.queryByText("foreign-fact-source-marker")).not.toBeInTheDocument();
    expect(screen.queryByText(/1980/)).not.toBeInTheDocument();
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

  it("opens the saved revision binding after validating its investigation scope", async () => {
    const view = createEvidenceQualityDemo(new Date("2026-11-11T10:00:00Z")).investigations[0];
    const revision = view.diagnosis_revisions[0];
    vi.mocked(state.api.getInvestigation).mockResolvedValue(view);
    vi.mocked(state.api.getCoverage).mockResolvedValue({
      ...demoCoverage,
      quality: {
        ...quality(),
        investigation_id: view.investigation.id,
        revision_id: revision.id,
      },
    });
    render(
      <Wrapper initial={`/coverage?cluster_id=${DEMO_CLUSTER_ID}&investigation_id=${view.investigation.id}&revision_id=${revision.id}`}>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    expect(await screen.findByText(/已绑定保存修订: Revision/)).toBeVisible();
    expect(await screen.findByRole("button", {
      name: "consumer-lag.v2 · 源码注册 · 已确认",
    })).toBeVisible();
    expect(state.api.getCoverage).toHaveBeenCalledWith(
      expect.any(AbortSignal),
      DEMO_CLUSTER_ID,
      { investigationId: view.investigation.id, revisionId: revision.id },
    );
  });

  it("blocks a bound matrix when its investigation conflicts with the navigation resource", async () => {
    const view = createEvidenceQualityDemo(new Date("2026-11-11T10:00:00Z")).investigations[0];
    vi.mocked(state.api.getInvestigation).mockResolvedValue(view);
    render(
      <Wrapper initial={`/coverage?cluster_id=${DEMO_CLUSTER_ID}&resource_kind=topic&resource_key=other&investigation_id=${view.investigation.id}&revision_id=${view.diagnosis_revisions[0].id}`}>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    expect(await screen.findByText("此对象与当前选择的范围不一致")).toBeVisible();
    expect(state.api.getCoverage).not.toHaveBeenCalled();
  });

  it.each([
    "investigation_id=one",
    "revision_id=one",
    "investigation_id=one&revision_id=one&revision_id=two",
    "resource=store-pressure/broker-a",
  ])("rejects an incomplete or unsupported coverage query: %s", async (query) => {
    render(
      <Wrapper initial={`/coverage?cluster_id=${DEMO_CLUSTER_ID}&${query}`}>
        <CoverageMatrixPage />
      </Wrapper>,
    );
    expect(await screen.findByRole("alert")).toHaveTextContent("质量范围不完整或不受支持");
    expect(state.api.getInvestigation).not.toHaveBeenCalled();
    expect(state.api.getCoverage).not.toHaveBeenCalled();
  });
});
