import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createBrowserRouter, Outlet, RouterProvider } from "react-router-dom";
import { ApiError } from "@/api/client";
import { WorkspaceScopeProvider } from "@/context/WorkspaceScopeContext";
import { observationFixture } from "@/features/executions/test-fixtures/observation";
import { ExecutionPage } from "./ExecutionObservationPage";

const state = vi.hoisted(() => ({
  tenant: "00000000-0000-4000-8000-000000000001", cluster: "10000000-0000-4000-8000-000000000001",
  subject: "operator", getObservation: vi.fn(),
}));
vi.mock("@/hooks/useSupervisedApi", () => ({ useSupervisedApi: () => ({
  identityKey: `${state.tenant}:${state.subject}`, auth: { session: { tenantId: state.tenant, subject: state.subject, clusterIds: [state.cluster], roles: ["operator"] } },
  api: { getExecutionObservation: state.getObservation },
}) }));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => ({
  session: { tenantId: state.tenant, subject: state.subject, clusterIds: [state.cluster], roles: ["operator"] },
  hasClusterScope: (id: string) => id === state.cluster,
}) }));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => ({ clusters: [{ id: state.cluster, tenant_id: state.tenant, external_cluster_key: "synthetic-cluster" }] }) }));
vi.mock("@/hooks/useWorkflowProgress", () => ({ useWorkflowProgress: () => ({ events: [], transport: "polling" }) }));
vi.mock("@/preferences/OperatorPreferences", () => ({ useOperatorPreferences: () => ({ locale: "en-US", formatDateTime: (value: string) => value }) }));

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime("2026-11-17T10:00:00Z");
  state.subject = "operator";
  state.getObservation.mockReset().mockResolvedValue(observationFixture().view);
});
afterEach(() => { vi.useRealTimers(); });

function mount(extra = "") {
  const id = observationFixture().view.binding.execution_id;
  window.history.replaceState(null, "", `/changes/executions/${id}?cluster_id=${state.cluster}${extra}`);
  const router = createBrowserRouter([{
    path: "/",
    element: <WorkspaceScopeProvider><Outlet /></WorkspaceScopeProvider>,
    children: [{ path: "/changes/executions/:executionId", element: <IdentityFixture /> }],
  }]);
  const root = render(<RouterProvider router={router} />);
  const navigate = (href: string) => {
    window.history.pushState(null, "", href);
    window.dispatchEvent(new PopStateEvent("popstate"));
  };
  return { navigate, root, id };
}

function IdentityFixture() {
  const [, renderOwner] = useState(0);
  return <><button onClick={() => { state.subject = "different-reader"; renderOwner((value) => value + 1); }}>Switch authenticated reader</button><ExecutionPage /></>;
}

describe("owned execution observation route", () => {
  it("reads a historical deep link but never grants recovery or a target-write action", async () => {
    mount();
    expect(await screen.findByRole("heading", { name: "Business recovery" })).toBeVisible();
    expect(screen.getByText("Independent business-recovery observation is missing.")).toBeVisible();
    expect(state.getObservation).toHaveBeenCalledWith(observationFixture().view.binding.execution_id, expect.any(AbortSignal));
    expect(screen.queryByRole("button", { name: /execute|retry write|compensate/i })).not.toBeInTheDocument();
  });

  it("rejects a foreign receipt before showing any retained step or timeline", async () => {
    const { view } = observationFixture();
    view.binding.tenant_id = "00000000-0000-4000-8000-000000000099";
    state.getObservation.mockResolvedValue(view);
    mount();
    expect(await screen.findByRole("alert")).toHaveTextContent("does not match the current identity");
    expect(screen.queryByText(view.steps[0].target)).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Recorded execution timeline" })).not.toBeInTheDocument();
  });

  it("keeps a wrong explicitly selected resource behind the actual scope boundary", async () => {
    mount("&resource_kind=broker&resource_key=different-broker");
    expect(await screen.findByRole("alert")).toHaveTextContent("does not match the selected scope");
    expect(screen.queryByRole("heading", { name: "Business recovery" })).not.toBeInTheDocument();
  });

  it("aborts the old window request and ignores its response after new-owner success", async () => {
    let finish!: (value: ReturnType<typeof observationFixture>["view"]) => void;
    state.getObservation.mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }));
    const { navigate, id } = mount("&window_start=2026-11-17T09:00:00Z&window_end=2026-11-17T10:00:00Z");
    await waitFor(() => expect(state.getObservation).toHaveBeenCalledTimes(1));
    const signal = state.getObservation.mock.calls[0][1];
    const fresh = observationFixture().view;
    fresh.binding.state = "succeeded";
    state.getObservation.mockResolvedValue(fresh);
    await act(async () => navigate(`/changes/executions/${id}?cluster_id=${state.cluster}&window_start=2026-11-17T08:00:00Z&window_end=2026-11-17T09:00:00Z`));
    expect(signal.aborted).toBe(true);
    expect(await screen.findByText("Successful execution state recorded")).toBeVisible();
    await act(async () => finish(observationFixture().view));
    expect(screen.getByText("Successful execution state recorded")).toBeVisible();
  });

  it("retains labelled history on a network error, hides current facts, and recovers only by read", async () => {
    const user = userEvent.setup();
    state.getObservation.mockResolvedValueOnce(observationFixture().view)
      .mockRejectedValueOnce(new TypeError("private transport address"))
      .mockResolvedValue(observationFixture().view);
    mount();
    await screen.findByRole("heading", { name: "Business recovery" });
    await user.click(screen.getByRole("button", { name: "Read-only refresh" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("could not be read");
    expect(screen.queryByText("private transport address")).not.toBeInTheDocument();
    expect(screen.queryByText("This snapshot expired. Read it again for the current state.")).not.toBeInTheDocument();
    expect(screen.queryByText("Independent business-recovery observation is missing.")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Recorded execution timeline" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Read-only refresh" }));
    expect(await screen.findByText("Independent business-recovery observation is missing.")).toBeVisible();
    expect(state.getObservation).toHaveBeenCalledTimes(3);
  });

  it("cancels a replaced identity and never displays its ignored late response", async () => {
    const user = userEvent.setup();
    let finish!: (value: ReturnType<typeof observationFixture>["view"]) => void;
    state.getObservation.mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }));
    mount();
    await waitFor(() => expect(state.getObservation).toHaveBeenCalledTimes(1));
    const signal = state.getObservation.mock.calls[0][1];
    state.getObservation.mockRejectedValue(new ApiError(403, "forbidden", "private-denial"));
    await user.click(screen.getByRole("button", { name: "Switch authenticated reader" }));
    expect(signal.aborted).toBe(true);
    await screen.findByRole("alert");
    await act(async () => finish(observationFixture().view));
    expect(screen.queryByRole("heading", { name: "Business recovery" })).not.toBeInTheDocument();
    expect(screen.queryByText("private-denial")).not.toBeInTheDocument();
  });

  it("removes old history after authoritative scope revocation", async () => {
    const user = userEvent.setup();
    mount();
    await screen.findByRole("heading", { name: "Business recovery" });
    state.getObservation.mockRejectedValue(new ApiError(403, "forbidden", "private server diagnostic"));
    await user.click(screen.getByRole("button", { name: "Read-only refresh" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("cannot read this execution");
    expect(screen.queryByRole("heading", { name: "Recorded execution timeline" })).not.toBeInTheDocument();
    expect(screen.queryByText("private server diagnostic")).not.toBeInTheDocument();
  });
});
