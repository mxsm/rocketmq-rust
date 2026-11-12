import { act, render, screen, waitFor } from "@testing-library/react";
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
import {
  AuditPageView,
  ChangeCenterPage,
  QuarantinePageView,
} from "./SupervisedExecutionPages";

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
  context: {
    tenantId: "",
    subject: "operator",
    clusterIds: [] as string[],
    roles: ["operator"],
    token: "test-token",
  },
  clusters: [] as Array<{
    id: string;
    tenant_id: string;
    external_cluster_key: string;
  }>,
  api: {
    listQuarantines: vi.fn(),
    clearQuarantine: vi.fn(),
    getAudit: vi.fn(),
  },
}));

vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: state.session,
    requestContext: state.context,
    hasClusterScope: (clusterId: string) =>
      state.session.clusterIds.includes(clusterId),
  }),
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ clusters: state.clusters }),
}));
vi.mock("@/api/supervisedClient", () => ({
  createSupervisedSreApi: () => state.api,
}));
vi.mock("@/preferences/OperatorPreferences", () => ({
  useOperatorPreferences: () => ({ locale: "en-US" }),
}));

describe("supervised change scope", () => {
  beforeEach(() => {
    state.session = {
      tenantId: TENANT,
      subject: "operator",
      clusterIds: [A, B],
      roles: ["operator"],
    };
    state.context = { ...state.session, token: "test-token" };
    state.clusters = [A, B].map((id, index) => ({
      id,
      tenant_id: TENANT,
      external_cluster_key: `cluster-${index}`,
    }));
    state.api.listQuarantines
      .mockReset()
      .mockImplementation(async (clusterId: string) =>
        quarantinePage(clusterId),
      );
    state.api.clearQuarantine.mockReset();
    state.api.getAudit.mockReset().mockResolvedValue(auditPage());
  });

  it("opens quarantines in the shared cluster rather than the first claim", async () => {
    const user = userEvent.setup();
    render(<Fixture initialSearch={`?cluster_id=${B}`} center />);

    await user.click(screen.getByRole("button", { name: "打开隔离清单" }));

    expect(locationUrl().pathname).toBe("/changes/quarantines");
    expect(locationUrl().searchParams.get("cluster_id")).toBe(B);
    expect(locationUrl().searchParams.has("cluster")).toBe(false);
  });

  it("migrates an authorized old cluster alias while preserving unrelated query and hash", async () => {
    render(<Fixture initialSearch={`?cluster=${B}&tab=history#entry`} />);

    await waitFor(() => expect(state.api.listQuarantines).toHaveBeenCalled());
    expect(
      state.api.listQuarantines.mock.calls.every(([cluster]) => cluster === B),
    ).toBe(true);
    expect(locationUrl().searchParams.get("cluster_id")).toBe(B);
    expect(locationUrl().searchParams.has("cluster")).toBe(false);
    expect(locationUrl().searchParams.get("tab")).toBe("history");
    expect(locationUrl().hash).toBe("#entry");
    expect(await screen.findByText("broker/broker-b")).toBeVisible();
  });

  it("gives explicit shared scope priority over the old alias and follows later global changes", async () => {
    const user = userEvent.setup();
    render(<Fixture initialSearch={`?cluster=${A}&cluster_id=${B}`} />);

    expect(await screen.findByText("broker/broker-b")).toBeVisible();
    expect(
      state.api.listQuarantines.mock.calls.every(([cluster]) => cluster === B),
    ).toBe(true);
    expect(locationUrl().searchParams.has("cluster")).toBe(false);
    await user.click(
      screen.getByRole("button", { name: "Switch to cluster A" }),
    );

    expect(await screen.findByText("broker/broker-a")).toBeVisible();
    expect(screen.queryByText("broker/broker-b")).not.toBeInTheDocument();
    expect(locationUrl().searchParams.get("cluster_id")).toBe(A);
    expect(locationUrl().searchParams.has("cluster")).toBe(false);
  });

  it("ignores an old cluster response after the global scope has changed", async () => {
    const user = userEvent.setup();
    let finishOld!: (page: ReturnType<typeof quarantinePage>) => void;
    state.api.listQuarantines.mockImplementation((clusterId: string) =>
      clusterId === B
        ? new Promise((resolve) => {
            finishOld = resolve;
          })
        : Promise.resolve(quarantinePage(clusterId)),
    );
    render(<Fixture initialSearch={`?cluster_id=${B}`} />);
    await waitFor(() =>
      expect(state.api.listQuarantines).toHaveBeenCalledWith(
        B,
        true,
        expect.any(AbortSignal),
      ),
    );

    await user.click(
      screen.getByRole("button", { name: "Switch to cluster A" }),
    );
    expect(await screen.findByText("broker/broker-a")).toBeVisible();
    await act(async () => finishOld(quarantinePage(B)));

    expect(screen.queryByText("broker/broker-b")).not.toBeInTheDocument();
    expect(screen.getByText("broker/broker-a")).toBeVisible();
    expect(
      (state.api.listQuarantines.mock.calls[0][2] as AbortSignal).aborted,
    ).toBe(true);
  });

  it("does not query a registered cluster absent from the user's claims through a legacy alias", async () => {
    state.session.clusterIds = [A];
    render(<Fixture initialSearch={`?cluster=${B}`} />);

    expect(screen.getByRole("alert")).toHaveTextContent(
      "旧链接中的集群范围无效或未授权",
    );
    await act(async () => undefined);
    expect(state.api.listQuarantines).not.toHaveBeenCalled();
    expect(
      screen.queryByRole("option", { name: "cluster-1" }),
    ).not.toBeInTheDocument();
  });

  it("rejects ambiguous repeated legacy aliases instead of choosing the first", async () => {
    render(<Fixture initialSearch={`?cluster=${A}&cluster=${B}`} />);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "旧链接中的集群范围无效或未授权",
    );
    await act(async () => undefined);
    expect(state.api.listQuarantines).not.toHaveBeenCalled();
  });

  it("keeps the legacy alias usable in an isolated page without the shared provider", async () => {
    render(
      <MemoryRouter initialEntries={[`/changes/quarantines?cluster=${B}`]}>
        <QuarantinePageView />
        <LocationProbe />
      </MemoryRouter>,
    );

    expect(await screen.findByText("broker/broker-b")).toBeVisible();
    expect(locationUrl().searchParams.get("cluster_id")).toBe(B);
    expect(locationUrl().searchParams.has("cluster")).toBe(false);
    expect(
      state.api.listQuarantines.mock.calls.every(([cluster]) => cluster === B),
    ).toBe(true);
  });

  it("keeps a mixed-cluster audit link readable within the explicit authorized scope", async () => {
    renderAudit(`?cluster_id=${A}`);

    expect(await screen.findByText("audit-event-a")).toBeVisible();
    expect(screen.queryByText("audit-event-b")).not.toBeInTheDocument();
    expect(screen.getByText(/其他范围的记录已过滤/)).toHaveAttribute(
      "role",
      "status",
    );
    expect(screen.getByText("部分结果")).toBeVisible();
  });

  it("aligns a bare mixed-cluster audit link from the first authorized item and follows global selection", async () => {
    const user = userEvent.setup();
    renderAudit("");

    expect(await screen.findByText("audit-event-b")).toBeVisible();
    expect(locationUrl().searchParams.get("cluster_id")).toBe(B);
    expect(screen.queryByText("audit-event-a")).not.toBeInTheDocument();
    await user.click(
      screen.getByRole("button", { name: "Switch to cluster A" }),
    );
    expect(await screen.findByText("audit-event-a")).toBeVisible();
    expect(screen.queryByText("audit-event-b")).not.toBeInTheDocument();
  });
});

function renderAudit(search: string) {
  window.history.replaceState(null, "", `/changes/audit/correlation${search}`);
  const router = createBrowserRouter([
    {
      path: "/",
      element: (
        <WorkspaceScopeProvider>
          <SwitchScope />
          <Outlet />
          <LocationProbe />
        </WorkspaceScopeProvider>
      ),
      children: [
        { path: "/changes/audit/:correlationId", element: <AuditPageView /> },
      ],
    },
  ]);
  return render(<RouterProvider router={router} />);
}

function auditPage() {
  return {
    schema_version: "rocketmq-sre.audit-page.v1",
    correlation_id: "correlation",
    partial: false,
    items: [B, A].map((clusterId) => ({
      id: `audit-${clusterId}`,
      tenant_id: TENANT,
      cluster_id: clusterId,
      correlation_id: "correlation",
      actor_role: "operator",
      actor_subject: "operator",
      event_kind: "plan_created",
      details: {},
      occurred_at: "2026-11-11T10:00:00Z",
      reason_code: clusterId === A ? "audit-event-a" : "audit-event-b",
      resource_id: `plan-${clusterId}`,
      resource_kind: "plan",
    })),
  };
}

function Fixture({
  initialSearch,
  center = false,
}: {
  initialSearch: string;
  center?: boolean;
}) {
  return (
    <MemoryRouter
      initialEntries={[
        `/changes${center ? "" : "/quarantines"}${initialSearch}`,
      ]}
    >
      <WorkspaceScopeProvider>
        <SwitchScope />
        {center ? <ChangeCenterPage /> : <QuarantinePageView />}
        <LocationProbe />
      </WorkspaceScopeProvider>
    </MemoryRouter>
  );
}

function SwitchScope() {
  const scope = useWorkspaceScope();
  return (
    <button onClick={() => scope.setClusterId(A)} type="button">
      Switch to cluster A
    </button>
  );
}

function LocationProbe() {
  const location = useLocation();
  return (
    <output data-testid="location">
      {location.pathname}
      {location.search}
      {location.hash}
    </output>
  );
}

function locationUrl() {
  return new URL(
    screen.getByTestId("location").textContent ?? "",
    "http://router.local",
  );
}

function quarantinePage(clusterId: string) {
  return {
    schema_version: "rocketmq-sre.resource-quarantine-page.v1",
    partial: false,
    items: [
      {
        id: `quarantine-${clusterId}`,
        tenant_id: TENANT,
        cluster_id: clusterId,
        resource_key: clusterId === A ? "broker/broker-a" : "broker/broker-b",
        action_id: "test-action",
        reason_code: "rollback_failed",
        created_at: "2026-11-11T10:00:00Z",
        cleared_at: null,
      },
    ],
  };
}
