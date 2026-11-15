import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ActionPlanView } from "@/api/types";
import { WorkspaceScopeProvider } from "@/context/WorkspaceScopeContext";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { actionPlanFixture } from "@/features/incidents/test-fixtures/actionReadiness";
import { ObjectScopeBoundary } from "@/features/scope/ObjectScopeBoundary";
import { planNavigationSearch, planScopeObject } from "./planReadinessPresentation";

const cluster = "10000000-0000-4000-8000-000000000001";
const otherCluster = "10000000-0000-4000-8000-000000000002";
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: { tenantId: "00000000-0000-4000-8000-000000000001", subject: "operator", roles: ["operator"],
      clusterIds: ["10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002"] },
    hasClusterScope: (id: string) => ["10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002"].includes(id),
  }),
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ clusters: ["10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002"].map(
    (id) => ({ id, tenant_id: "00000000-0000-4000-8000-000000000001", external_cluster_key: id }),
  ) }),
}));

beforeEach(() => window.localStorage.clear());

function Fixture({ plan, search = "" }: { plan: ActionPlanView["plan"]; search?: string }) {
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter initialEntries={[`/changes/plans/${plan.id}${search}`]}>
        <WorkspaceScopeProvider>
          <ObjectScopeBoundary object={planScopeObject(plan)}><p>authorized plan content</p></ObjectScopeBoundary>
          <Location />
        </WorkspaceScopeProvider>
      </MemoryRouter>
    </OperatorPreferencesProvider>
  );
}

function Location() {
  const value = useLocation();
  return <output aria-label="location">{value.pathname}{value.search}</output>;
}

describe("authorized Plan workspace scope", () => {
  it("opens the exact target from the immutable response and preserves the selected absolute window", () => {
    const { planView } = actionPlanFixture("broker/127.0.0.1:10911");
    const window = { start: "2026-11-17T09:00:00.123456789Z", end: "2026-11-17T10:00:00.987654321Z" };
    const search = planNavigationSearch(planView.plan, window);
    const params = new URLSearchParams(search);
    expect(params.get("resource_key")).toBe("127.0.0.1:10911");
    expect(params.get("window_start")).toBe(window.start);
    expect(params.get("window_end")).toBe(window.end);
    render(<Fixture plan={planView.plan} search={search} />);
    expect(screen.getByText("authorized plan content")).toBeVisible();
  });

  it("preserves bare historical Plan links with authorized object alignment", async () => {
    const { planView } = actionPlanFixture("broker/127.0.0.1:10911");
    render(<Fixture plan={planView.plan} />);
    expect(await screen.findByText("authorized plan content")).toBeVisible();
    const url = new URL(screen.getByLabelText("location").textContent!, "http://localhost");
    expect(url.searchParams.get("cluster_id")).toBe(cluster);
    expect(url.searchParams.get("resource_key")).toBe("127.0.0.1:10911");
  });

  it("requires explicit alignment when an old Incident alias remains selected", async () => {
    const { planView } = actionPlanFixture("broker/127.0.0.1:10911");
    const user = userEvent.setup();
    render(<Fixture plan={planView.plan} search={`?cluster_id=${cluster}&resource_kind=broker&resource_key=broker-a`} />);
    expect(screen.queryByText("authorized plan content")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "对齐到对象范围" }));
    expect(await screen.findByText("authorized plan content")).toBeVisible();
    const url = new URL(screen.getByLabelText("location").textContent!, "http://localhost");
    expect(url.searchParams.get("resource_key")).toBe("127.0.0.1:10911");
  });

  it("keeps a multi-target Plan blocked in a narrow resource until explicit cluster alignment", async () => {
    const { planView } = actionPlanFixture();
    planView.plan.steps.push({ ...planView.plan.steps[0], id: "36000000-0000-4000-8000-000000000002", resource: "broker/broker-b" });
    const user = userEvent.setup();
    render(<Fixture plan={planView.plan} search={`?cluster_id=${cluster}&resource_kind=broker&resource_key=broker-a`} />);
    expect(screen.queryByText("authorized plan content")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "对齐到对象范围" }));
    await waitFor(() => expect(screen.getByText("authorized plan content")).toBeVisible());
    const url = new URL(screen.getByLabelText("location").textContent!, "http://localhost");
    expect(url.searchParams.get("resource_kind")).toBeNull();
    expect(url.searchParams.get("cluster_id")).toBe(cluster);
  });

  it("does not use a Plan UUID to override an explicit other-cluster scope", () => {
    const { planView } = actionPlanFixture();
    render(<Fixture plan={planView.plan} search={`?cluster_id=${otherCluster}`} />);
    expect(screen.getByRole("alert")).toHaveTextContent("此对象与当前选择的范围不一致");
    expect(screen.queryByText("authorized plan content")).not.toBeInTheDocument();
  });

  it("does not expose a Plan belonging to another tenant", () => {
    const { planView } = actionPlanFixture();
    planView.plan.tenant_id = "00000000-0000-4000-8000-000000000099";
    render(<Fixture plan={planView.plan} search={`?cluster_id=${cluster}`} />);
    expect(screen.getByRole("alert")).toHaveTextContent("对象不在当前授权范围内");
    expect(screen.queryByText("authorized plan content")).not.toBeInTheDocument();
  });
});
