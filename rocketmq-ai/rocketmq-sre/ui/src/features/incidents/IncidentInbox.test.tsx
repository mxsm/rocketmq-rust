import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { CollectionEnvelope, IncidentView } from "@/api/types";
import {
  DEMO_CLUSTER_ID,
  DEMO_TENANT_ID,
  phase1Incidents,
} from "@/data/phase1Demo";
import { IncidentsPage } from "@/pages/WorkflowPages";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";

interface RegisteredTool {
  name: string;
  execute: (
    input: unknown,
    options: { signal: AbortSignal },
  ) => Promise<unknown>;
}
const registerTool =
  vi.fn<
    (tool: RegisteredTool, options: { signal: AbortSignal }) => Promise<void>
  >();
const listIncidents =
  vi.fn<
    (
      clusterId: string,
      signal?: AbortSignal,
    ) => Promise<CollectionEnvelope<IncidentView>>
  >();
const api = { listIncidents };
let allowed: boolean;
let identity: string;
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    status: "authenticated",
    session: {
      tenantId: DEMO_TENANT_ID,
      clusterIds: allowed ? [DEMO_CLUSTER_ID] : [],
    },
  }),
}));
vi.mock("@/context/WorkspaceScopeContext", () => ({
  useWorkspaceScope: () => ({ scopeKey: identity, invalid: false }),
}));
vi.mock("@/hooks/useClusterScope", () => ({
  useClusterScope: () => ({
    clusterId: DEMO_CLUSTER_ID,
    clusters: [
      {
        id: DEMO_CLUSTER_ID,
        external_cluster_key: "rmq-prod-cn",
        environment: "demo",
      },
    ],
    setClusterId: vi.fn(),
  }),
}));
vi.mock("@/hooks/useWorkflowProgress", () => ({
  useWorkflowProgress: () => ({ transport: "sse" }),
}));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => ({ api }) }));

function page() {
  return (
    <MemoryRouter>
      <OperatorPreferencesProvider>
        <IncidentsPage />
      </OperatorPreferencesProvider>
    </MemoryRouter>
  );
}

beforeEach(() => {
  allowed = true;
  identity = "tenant/operator-a/cluster";
  listIncidents.mockReset().mockResolvedValue(structuredClone(phase1Incidents));
  registerTool.mockReset().mockResolvedValue(undefined);
  window.localStorage.clear();
  window.localStorage.setItem(
    "rocketmq-sre-operator-preferences.v1",
    JSON.stringify({ locale: "en-US", timeZone: "UTC" }),
  );
});

afterEach(() => {
  Reflect.deleteProperty(document, "modelContext");
  vi.unstubAllGlobals();
  window.localStorage.clear();
});

describe("incident queue", () => {
  it("keeps the manual timeline usable without WebMCP, with real local dates, filters and accessible detail links", async () => {
    window.localStorage.setItem(
      "rocketmq-sre-operator-preferences.v1",
      JSON.stringify({ locale: "en-US", timeZone: "Asia/Shanghai" }),
    );
    const records = structuredClone(phase1Incidents);
    records.items[1].incident.updated_at = "2026-07-27T20:10:00Z";
    listIncidents.mockResolvedValue(records);
    const user = userEvent.setup();
    render(page());
    const first = await screen.findByRole("link", {
      name: /View incident · orders/,
    });
    expect(first).toHaveAttribute(
      "href",
      "/incidents/" + records.items[0].incident.id,
    );
    const rows = screen.getAllByRole("listitem");
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByText("16:42")).toBeInTheDocument();
    expect(within(rows[0]).getByText("Partial evidence")).toBeInTheDocument();
    expect(within(rows[1]).getByText("Stale evidence")).toBeInTheDocument();
    expect(within(rows[1]).getByText("04:10")).toBeInTheDocument();
    const dates = screen.getByRole("combobox", {
      name: /Incident update date/,
    });
    await user.selectOptions(dates, "2026-07-28");
    expect(screen.getAllByRole("listitem")).toHaveLength(1);
    expect(
      screen.getByRole("link", { name: /Controller/ }),
    ).toBeInTheDocument();
    await user.type(screen.getByRole("searchbox"), "orders");
    expect(
      screen.getByText("No incidents match these filters"),
    ).toBeInTheDocument();
    await user.selectOptions(dates, "");
    expect(
      screen.getByRole("link", { name: /View incident · orders/ }),
    ).toBeInTheDocument();
  });

  it("applies visible filters to scoped tool reads, rejects invalid inputs and discards revoked in-flight results", async () => {
    vi.stubGlobal("isSecureContext", true);
    Object.defineProperty(document, "modelContext", {
      configurable: true,
      value: { registerTool },
    });
    const view = render(page());
    const firstTool = registerTool.mock.calls[0][0];
    const call = { signal: new AbortController().signal };
    await expect(
      firstTool.execute({ cluster_id: "another-cluster" }, call),
    ).rejects.toThrow("empty object");
    const user = userEvent.setup();
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Severity" }),
      "critical",
    );
    expect(registerTool.mock.calls[0][1].signal.aborted).toBe(true);
    const tool = registerTool.mock.calls.at(-1)![0];
    const result = await tool.execute({}, call);
    expect(result).toMatchObject({
      incidents: [
        {
          id: phase1Incidents.items[0].incident.id,
          severity: "critical",
          partial: true,
        },
      ],
    });
    expect(JSON.stringify(result)).not.toContain("messaging-platform");
    expect(listIncidents).toHaveBeenLastCalledWith(
      DEMO_CLUSTER_ID,
      expect.any(AbortSignal),
    );
    listIncidents.mockRejectedValueOnce(
      new Error("sensitive backend error detail"),
    );
    await expect(tool.execute({}, call)).rejects.toThrow(
      /^The scoped incident queue is unavailable\.$/,
    );

    let finish!: (value: CollectionEnvelope<IncidentView>) => void;
    listIncidents.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const pending = tool.execute({}, call);
    identity = "tenant/operator-b/cluster";
    view.rerender(page());
    const aborted = expect(pending).rejects.toMatchObject({
      name: "AbortError",
    });
    await act(async () => {
      finish(phase1Incidents);
    });
    await aborted;
    const registration = registerTool.mock.calls.at(-1)![1].signal;
    view.unmount();
    expect(registration.aborted).toBe(true);
  });

  it("does not offer tools or issue reads for an unauthorized cluster", async () => {
    allowed = false;
    vi.stubGlobal("isSecureContext", true);
    Object.defineProperty(document, "modelContext", {
      configurable: true,
      value: { registerTool },
    });
    render(page());
    await waitFor(() =>
      expect(screen.getByRole("alert")).toBeInTheDocument(),
    );
    expect(registerTool).not.toHaveBeenCalled();
    expect(listIncidents).not.toHaveBeenCalled();
  });
});
