import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createBrowserRouter,
  MemoryRouter,
  Outlet,
  RouterProvider,
  useLocation,
} from "react-router-dom";

import {
  WorkspaceScopeProvider,
  useWorkspaceScope,
} from "@/context/WorkspaceScopeContext";
import { ObjectScopeBoundary } from "./ObjectScopeBoundary";

const A = "10000000-0000-4000-8000-000000000001";
const B = "10000000-0000-4000-8000-000000000002";
const TENANT = "00000000-0000-4000-8000-000000000001";
const state = vi.hoisted(() => ({
  session: {
    tenantId: "",
    subject: "operator",
    clusterIds: [] as string[],
    roles: ["operator"],
  },
  clusters: [] as Array<{ id: string; tenant_id: string }>,
  renderedScopes: [] as string[],
}));

vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: state.session,
    hasClusterScope: (clusterId: string) =>
      state.session.clusterIds.includes(clusterId),
  }),
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ clusters: state.clusters }),
}));
vi.mock("@/preferences/OperatorPreferences", () => ({
  useOperatorPreferences: () => ({ locale: "en-US" }),
}));

const object = {
  tenant_id: TENANT,
  cluster_id: B,
  resource: "broker/broker-b",
};

describe("object scope boundary", () => {
  beforeEach(() => {
    state.session = {
      tenantId: TENANT,
      subject: "operator",
      clusterIds: [A, B],
      roles: ["operator"],
    };
    state.clusters = [A, B].map((id) => ({ id, tenant_id: TENANT }));
    state.renderedScopes = [];
    window.history.replaceState(null, "", "/");
  });

  it("aligns an authorized legacy link before rendering object content", async () => {
    render(<Fixture initialSearch="?tab=timeline#citation" />);

    await screen.findByText("private object content");
    const location = screen.getByTestId("location").textContent ?? "";
    const url = new URL(location, "http://router.local");
    expect(url.searchParams.get("cluster_id")).toBe(B);
    expect(url.searchParams.get("resource_kind")).toBe("broker");
    expect(url.searchParams.get("resource_key")).toBe("broker-b");
    expect(url.searchParams.get("tab")).toBe("timeline");
    expect(url.hash).toBe("#citation");
    expect(url.searchParams.get("window_start")).toBeTruthy();
    expect(state.renderedScopes.length).toBeGreaterThan(0);
    expect(
      state.renderedScopes.every(
        (search) => new URLSearchParams(search).get("cluster_id") === B,
      ),
    ).toBe(true);
  });

  it("replaces automatic alignment so browser Back does not return to an unscoped entry", async () => {
    window.history.replaceState(null, "", "/incidents/object-b?tab=timeline");
    const entries = window.history.length;
    const router = createBrowserRouter([
      {
        path: "/",
        element: (
          <WorkspaceScopeProvider>
            <Outlet />
          </WorkspaceScopeProvider>
        ),
        children: [
          { path: "/incidents/:id", element: <Content object={object} /> },
        ],
      },
    ]);
    render(<RouterProvider router={router} />);

    await screen.findByText("private object content");
    expect(new URLSearchParams(window.location.search).get("cluster_id")).toBe(
      B,
    );
    expect(window.history.length).toBe(entries);
  });

  it("blocks an explicit cluster conflict until the user aligns it", async () => {
    const user = userEvent.setup();
    render(<Fixture initialSearch={`?cluster_id=${A}`} />);

    expect(
      screen.queryByText("private object content"),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Align to object" }));
    expect(await screen.findByText("private object content")).toBeVisible();
  });

  it("blocks an explicit resource conflict and preserves the chosen absolute window on alignment", async () => {
    const user = userEvent.setup();
    const window =
      "&window_start=2026-11-11T10%3A00%3A00.000Z&window_end=2026-11-11T11%3A00%3A00.000Z";
    render(
      <Fixture
        initialSearch={`?cluster_id=${B}&resource_kind=topic&resource_key=orders${window}`}
      />,
    );

    expect(
      screen.queryByText("private object content"),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Align to object" }));
    expect(await screen.findByText("private object content")).toBeVisible();
    const params = new URLSearchParams(
      new URL(
        screen.getByTestId("location").textContent ?? "",
        "http://router.local",
      ).search,
    );
    expect(params.get("resource_key")).toBe("broker-b");
    expect(params.get("window_start")).toBe("2026-11-11T10:00:00.000Z");
    expect(params.get("window_end")).toBe("2026-11-11T11:00:00.000Z");
  });

  it("hides a previously visible object when the global cluster changes", async () => {
    const user = userEvent.setup();
    render(<Fixture initialSearch={`?cluster_id=${B}`} />);
    expect(screen.getByText("private object content")).toBeVisible();

    await user.click(
      screen.getByRole("button", { name: "Switch to cluster A" }),
    );

    expect(
      screen.queryByText("private object content"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Align to object" }),
    ).toBeVisible();
  });

  it("does not treat a registered cluster as permission to view an object", () => {
    state.session.clusterIds = [A];
    render(<Fixture initialSearch="" />);

    expect(
      screen.queryByText("private object content"),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent(
      "outside your authorized scope",
    );
    expect(
      screen.queryByRole("button", { name: "Align to object" }),
    ).not.toBeInTheDocument();
    expect(state.renderedScopes).toEqual([]);
  });

  it("hides old tenant data synchronously when the identity changes", async () => {
    const { rerender } = render(<Fixture initialSearch={`?cluster_id=${B}`} />);
    expect(screen.getByText("private object content")).toBeVisible();
    state.session = {
      ...state.session,
      tenantId: "another-tenant",
      subject: "another-operator",
    };

    rerender(<Fixture initialSearch={`?cluster_id=${B}`} />);

    expect(
      screen.queryByText("private object content"),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent(
      "outside your authorized scope",
    );
    await waitFor(() => expect(state.renderedScopes).toHaveLength(1));
  });

  it("allows the matching cluster-wide scope without inventing a resource match", () => {
    render(
      <Fixture
        initialSearch={`?cluster_id=${B}&resource_kind=cluster&resource_key=cluster-b`}
      />,
    );
    expect(screen.getByText("private object content")).toBeVisible();
  });
});

function Fixture({ initialSearch }: { initialSearch: string }) {
  return (
    <MemoryRouter initialEntries={[`/incidents/object-b${initialSearch}`]}>
      <WorkspaceScopeProvider>
        <Content object={object} />
      </WorkspaceScopeProvider>
    </MemoryRouter>
  );
}

function Content({ object: value }: { object: typeof object }) {
  const scope = useWorkspaceScope();
  const location = useLocation();
  return (
    <>
      <button onClick={() => scope.setClusterId(A)} type="button">
        Switch to cluster A
      </button>
      <output data-testid="location">
        {location.pathname}
        {location.search}
        {location.hash}
      </output>
      <ObjectScopeBoundary object={value}>
        <ConfidentialContent />
      </ObjectScopeBoundary>
    </>
  );
}

function ConfidentialContent() {
  state.renderedScopes.push(useLocation().search);
  return <p>private object content</p>;
}
