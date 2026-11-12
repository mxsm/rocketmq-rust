import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import type { SreApi } from "@/api/client";
import type { FleetHealthReport } from "@/api/types";
import { demoClusters } from "@/data/demo";
import { demoFleetHealth } from "@/data/phase2HealthDemo";
import { OverviewPage } from "./OverviewPage";

const state = vi.hoisted(() => ({ api: undefined as unknown as SreApi }));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({
    api: state.api,
    clusters: demoClusters.slice(0, 2),
    loading: false,
    refresh: vi.fn(),
  }),
}));
vi.mock("@/features/investigations/DutyQueue", () => ({ DutyQueue: () => null }));
vi.mock("@/features/capabilities/CapabilityQualitySummary", () => ({ CapabilityQualitySummary: () => null }));
vi.mock("@/components/HealthOverview", () => ({
  FleetHealthOverview: ({ report }: { report: FleetHealthReport }) => <output data-testid="fleet">{report.observed_at}</output>,
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

describe("Overview Fleet reads", () => {
  const added: string[] = [];
  beforeAll(() => {
    // jsdom lacks the pointer capture and scrolling used by Radix Select.
    for (const [name, value] of Object.entries({
      hasPointerCapture: (): boolean => false,
      setPointerCapture: (): undefined => undefined,
      releasePointerCapture: (): undefined => undefined,
      scrollIntoView: (): undefined => undefined,
    })) {
      if (!(name in HTMLElement.prototype)) {
        Object.defineProperty(HTMLElement.prototype, name, { value, configurable: true });
        added.push(name);
      }
    }
  });
  afterAll(() => added.forEach((name) => Reflect.deleteProperty(HTMLElement.prototype, name)));
  beforeEach(() => {
    state.api = {
      getClusterCapabilities: vi.fn((id: string) => Promise.resolve({
        cluster_id: id,
        digest: "current",
        protocol_version: "fixture-protocol",
        schema_version: "fixture-schema",
        mutation_supported: false,
        observed_at: "2026-11-12T10:00:00Z",
        data_sources: [],
      })),
      getFleetHealth: vi.fn(),
    } as unknown as SreApi;
  });

  it("does not refetch cluster capabilities when the Fleet response reorders rows", async () => {
    const health = deferred<FleetHealthReport>();
    vi.mocked(state.api.getFleetHealth).mockReturnValue(health.promise);
    render(<OverviewPage />);
    await waitFor(() => expect(screen.getAllByText("fixture-protocol")).toHaveLength(3));
    expect(state.api.getClusterCapabilities).toHaveBeenCalledTimes(2);
    await act(async () => health.resolve(demoFleetHealth()));
    expect(screen.getByTestId("fleet")).toBeVisible();
    expect(state.api.getClusterCapabilities).toHaveBeenCalledTimes(2);
  });

  it("hides the old region during loading and ignores a late response even when the source ignores abort", async () => {
    const first = deferred<FleetHealthReport>();
    const second = deferred<FleetHealthReport>();
    vi.mocked(state.api.getFleetHealth)
      .mockResolvedValueOnce(demoFleetHealth())
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    render(<OverviewPage />);
    await screen.findByTestId("fleet");
    const user = userEvent.setup();
    await user.click(screen.getByRole("combobox", { name: "区域筛选" }));
    await user.click(screen.getByRole("option", { name: demoClusters[0].region }));
    expect(screen.queryByTestId("fleet")).not.toBeInTheDocument();
    await user.click(screen.getByRole("combobox", { name: "区域筛选" }));
    await user.click(screen.getByRole("option", { name: demoClusters[1].region }));
    expect(vi.mocked(state.api.getFleetHealth).mock.calls[1][1]?.aborted).toBe(true);
    await act(async () => second.resolve({ ...demoFleetHealth(), observed_at: "2026-11-12T10:00:00Z" }));
    expect(screen.getByTestId("fleet")).toHaveTextContent("2026-11-12T10:00:00Z");
    await act(async () => first.resolve({ ...demoFleetHealth(), observed_at: "1980-01-01T00:00:00Z" }));
    expect(screen.getByTestId("fleet")).toHaveTextContent("2026-11-12T10:00:00Z");
    expect(screen.queryByText("1980-01-01T00:00:00Z")).not.toBeInTheDocument();
  });
});
