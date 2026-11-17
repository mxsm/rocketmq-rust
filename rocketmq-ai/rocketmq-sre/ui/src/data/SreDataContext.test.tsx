import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode } from "react";

import type { SreApi } from "@/api/client";
import type { ClusterSummary } from "@/api/types";
import { useClusterCapabilitySnapshots } from "@/features/fleet/useClusterCapabilitySnapshots";
import { SreDataProvider, useSreData } from "./SreDataContext";

const A = "10000000-0000-4000-8000-000000000001";
const B = "10000000-0000-4000-8000-000000000002";
const TENANT = "00000000-0000-4000-8000-000000000001";
const state = vi.hoisted(() => ({
  auth: {
    session: {
      tenantId: "",
      subject: "operator",
      clusterIds: [] as string[],
      roles: ["reader"],
    },
    requestContext: {
      token: "",
      tenantId: "",
      subject: "operator",
      clusterIds: [] as string[],
      roles: ["reader"],
    },
    hasClusterScope: (() => false) as (id: string) => boolean,
  },
  apis: new Map<string, SreApi>(),
  exposed: undefined as SreApi | undefined,
}));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => state.auth }));
vi.mock("@/api/client", () => ({
  createHttpSreApi: (auth: { token: string }) => state.apis.get(auth.token),
}));

function authorize(
  token: string,
  clusterIds = [A, B],
  tenantId = TENANT,
  subject = "operator",
) {
  const session = { tenantId, subject, clusterIds, roles: ["reader"] };
  state.auth = {
    session,
    requestContext: { ...session, token },
    hasClusterScope: (id) => clusterIds.includes(id),
  };
}
function cluster(id: string, tenantId = TENANT) {
  return {
    id,
    tenant_id: tenantId,
    external_cluster_key: id,
  } as ClusterSummary;
}
function apiFor(items: ClusterSummary[] | Promise<ClusterSummary[]>, digest = "first") {
  return {
    listClusters: vi.fn(() => Promise.resolve(items)),
    getHealth: vi.fn().mockResolvedValue({ status: "healthy" }),
    getReadiness: vi.fn().mockResolvedValue({ status: "ready" }),
    getClusterCapabilities: vi.fn((id: string) => Promise.resolve({
      cluster_id: id,
      digest,
      protocol_version: "fixture",
      schema_version: "fixture",
      mutation_supported: false,
      observed_at: "2026-11-16T10:00:00Z",
      data_sources: [],
    })),
  } as unknown as SreApi;
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function Probe() {
  const data = useSreData();
  state.exposed = data.api;
  return (
    <>
      <span data-testid="clusters">
        {data.clusters.map((item) => item.id).join(",")}
      </span>
      <span data-testid="health">{data.health}</span>
      <span data-testid="loading">{String(data.loading)}</span>
      <button onClick={() => void data.refresh()} type="button">
        refresh
      </button>
      <input aria-label="draft" defaultValue="saved draft" />
    </>
  );
}
function Fixture() {
  return (
    <SreDataProvider>
      <Probe />
    </SreDataProvider>
  );
}

function CapabilityProbe() {
  const { clusters } = useSreData();
  const resource = useClusterCapabilitySnapshots(clusters.map((item) => item.id));
  return <output data-testid="capabilities">{JSON.stringify(resource.data?.snapshots ?? {})}</output>;
}

function CachedFixture({ visible = true }: { visible?: boolean }) {
  return (
    <SreDataProvider>
      <Probe />
      {visible && <CapabilityProbe />}
    </SreDataProvider>
  );
}

describe("SRE data principal ownership", () => {
  beforeEach(() => {
    window.sessionStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.stubEnv("VITE_SRE_API_MODE", "http");
    vi.stubEnv("VITE_SRE_DEMO_MODE", "false");
    state.apis.clear();
    authorize("first");
  });
  afterEach(() => vi.unstubAllEnvs());

  it("preserves the roster and page drafts during a same-principal refresh", async () => {
    const user = userEvent.setup();
    const pending = deferred<ClusterSummary[]>();
    const api = apiFor([cluster(A)]);
    vi.mocked(api.listClusters)
      .mockResolvedValueOnce([cluster(A)])
      .mockReturnValueOnce(pending.promise);
    state.apis.set("first", api);
    render(<Fixture />);
    await waitFor(() =>
      expect(screen.getByTestId("clusters")).toHaveTextContent(A),
    );
    await user.clear(screen.getByLabelText("draft"));
    await user.type(screen.getByLabelText("draft"), "review in progress");
    await user.click(screen.getByRole("button", { name: "refresh" }));
    expect(screen.getByTestId("clusters")).toHaveTextContent(A);
    expect(screen.getByLabelText("draft")).toHaveValue("review in progress");
    expect(screen.getByTestId("loading")).toHaveTextContent("true");
    await act(async () => pending.resolve([cluster(A)]));
  });

  it("keeps the API facade and roster stable on token renewal and delegates to the renewed client", async () => {
    const initial = apiFor([cluster(A)]);
    const pending = deferred<ClusterSummary[]>();
    const renewed = apiFor(pending.promise);
    state.apis.set("first", initial);
    state.apis.set("renewed", renewed);
    const { rerender } = render(<Fixture />);
    await waitFor(() =>
      expect(screen.getByTestId("clusters")).toHaveTextContent(A),
    );
    const facade = state.exposed;
    authorize("renewed");
    rerender(<Fixture />);
    expect(screen.getByTestId("clusters")).toHaveTextContent(A);
    expect(state.exposed).toBe(facade);
    await state.exposed?.getHealth();
    expect(renewed.getHealth).toHaveBeenCalledTimes(2);
    await act(async () => pending.resolve([cluster(A)]));
  });

  it("hides an old principal synchronously and ignores its late roster response", async () => {
    const old = deferred<ClusterSummary[]>();
    const next = deferred<ClusterSummary[]>();
    state.apis.set("first", apiFor(old.promise));
    state.apis.set("other", apiFor(next.promise));
    const { rerender } = render(<Fixture />);
    authorize("other", [B], TENANT, "other-operator");
    rerender(<Fixture />);
    expect(screen.getByTestId("clusters")).toBeEmptyDOMElement();
    expect(screen.getByTestId("health")).toHaveTextContent("unavailable");
    await act(async () =>
      next.resolve([cluster(B), cluster(A), cluster(B, "another-tenant")]),
    );
    expect(screen.getByTestId("clusters")).toHaveTextContent(B);
    expect(screen.getByTestId("clusters")).not.toHaveTextContent(A);
    await act(async () => old.resolve([cluster(A)]));
    expect(screen.getByTestId("clusters")).not.toHaveTextContent(A);
  });

  it("retains capability reads across navigation and token renewal, then refreshes explicitly", async () => {
    const initial = apiFor([cluster(A)]);
    const renewed = apiFor([cluster(A)], "renewed");
    state.apis.set("first", initial);
    state.apis.set("renewed", renewed);
    const view = render(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    view.rerender(<CachedFixture visible={false} />);
    authorize("renewed");
    view.rerender(<CachedFixture visible={false} />);
    await waitFor(() => expect(screen.getByTestId("loading")).toHaveTextContent("false"));
    view.rerender(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    expect(initial.getClusterCapabilities).toHaveBeenCalledTimes(1);
    expect(renewed.getClusterCapabilities).not.toHaveBeenCalled();
    await userEvent.setup().click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("renewed"));
    expect(renewed.getClusterCapabilities).toHaveBeenCalledTimes(1);
  });

  it.each(["principal", "tenant", "claims", "clusters"])("discards cached snapshots when %s changes", async (change) => {
    state.apis.set("first", apiFor([cluster(A), cluster(B)]));
    const view = render(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    const tenantId = change === "tenant" ? "other-tenant" : TENANT;
    const items = change === "clusters" ? [cluster(B)] : [cluster(A, tenantId)];
    const pending = deferred<ClusterSummary[]>();
    const changed = apiFor(pending.promise, "changed");
    state.apis.set("changed", changed);
    authorize("changed", change === "clusters" ? [B] : [A, B], tenantId,
      change === "principal" ? "other-operator" : "operator");
    if (change === "claims") {
      state.auth.session.roles = ["reader", "diagnose"];
      state.auth.requestContext.roles = state.auth.session.roles;
    }
    view.rerender(<CachedFixture />);
    expect(screen.getByTestId("capabilities")).not.toHaveTextContent("first");
    await act(async () => pending.resolve(items));
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("changed"));
    expect(changed.getClusterCapabilities).toHaveBeenCalledTimes(1);
    if (change === "clusters") expect(screen.getByTestId("capabilities")).not.toHaveTextContent(A);
  });

  it.each(["configuration", "revocation"])("invalidates server-side %s changes on renewal", async (change) => {
    state.apis.set("first", apiFor([cluster(A), cluster(B)]));
    const items = change === "configuration"
      ? [{ ...cluster(A), updated_at: "2026-11-16T12:00:00Z" }, cluster(B)]
      : [cluster(A)];
    const changed = apiFor(items, "reconfigured");
    state.apis.set("renewed", changed);
    const view = render(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent(B));
    authorize("renewed");
    view.rerender(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("reconfigured"));
    if (change === "revocation") expect(screen.getByTestId("capabilities")).not.toHaveTextContent(B);
    expect(changed.getClusterCapabilities).toHaveBeenCalledTimes(items.length);
  });

  it("retains the current owner after StrictMode replays provider cleanup", async () => {
    const api = apiFor([cluster(A)]);
    state.apis.set("first", api);
    render(<StrictMode><CachedFixture /></StrictMode>);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    expect(api.getClusterCapabilities).toHaveBeenCalledTimes(1);
  });

  it("drops the cache with the authenticated provider on logout", async () => {
    const api = apiFor([cluster(A)]);
    state.apis.set("first", api);
    const view = render(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    // AuthGate removes this entire provider when the session is revoked/expired.
    view.rerender(<div>signed out</div>);
    view.rerender(<CachedFixture />);
    await waitFor(() => expect(screen.getByTestId("capabilities")).toHaveTextContent("first"));
    expect(api.getClusterCapabilities).toHaveBeenCalledTimes(2);
  });
});
