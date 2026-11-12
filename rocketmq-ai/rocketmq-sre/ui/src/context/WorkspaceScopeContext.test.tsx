import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Link, MemoryRouter, useLocation } from "react-router-dom";

import { useClusterScope } from "@/hooks/useClusterScope";
import {
  WorkspaceScopeProvider,
  useWorkspaceScope,
} from "./WorkspaceScopeContext";

const A = "10000000-0000-4000-8000-000000000001";
const B = "10000000-0000-4000-8000-000000000002";
const TENANT = "00000000-0000-4000-8000-000000000001";
const state = vi.hoisted(() => ({
  session: {
    tenantId: "",
    subject: "operator",
    clusterIds: [] as string[],
    roles: ["reader"],
  },
  clusters: [] as Array<{ id: string; tenant_id: string }>,
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: state.session,
    hasClusterScope: (id: string) => state.session.clusterIds.includes(id),
  }),
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ clusters: state.clusters }),
}));

function Probe() {
  const workspace = useWorkspaceScope();
  const legacy = useClusterScope();
  const location = useLocation();
  return (
    <>
      <span data-testid="workspace">{workspace.clusterId}</span>
      <span data-testid="legacy">{legacy.clusterId}</span>
      <span data-testid="key">{workspace.scopeKey}</span>
      <span data-testid="invalid">{String(workspace.invalid)}</span>
      <span data-testid="location">
        {location.pathname}
        {location.search}
      </span>
      <button onClick={() => legacy.setClusterId(B)} type="button">
        choose B
      </button>
      <button
        onClick={() =>
          workspace.setScope({
            clusterId: B,
            resourceKind: "topic",
            resourceKey: "orders",
            window: {
              start: "2026-11-11T10:00:00Z",
              end: "2026-11-11T11:00:00Z",
            },
          })
        }
        type="button"
      >
        set scope
      </button>
      <Link to="/evidence?tab=metadata">evidence</Link>
    </>
  );
}
function Fixture({ initial = "/ask" }: { initial?: string }) {
  return (
    <MemoryRouter initialEntries={[initial]}>
      <WorkspaceScopeProvider>
        <Probe />
      </WorkspaceScopeProvider>
    </MemoryRouter>
  );
}

describe("global workspace scope", () => {
  beforeEach(() => {
    state.session = {
      tenantId: TENANT,
      subject: "operator",
      clusterIds: [A, B],
      roles: ["reader"],
    };
    state.clusters = [A, B].map((id) => ({ id, tenant_id: TENANT }));
  });
  it("shares legacy selector changes and keeps cluster/resource/window on page navigation", async () => {
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "choose B" }));
    expect(screen.getByTestId("workspace")).toHaveTextContent(B);
    expect(screen.getByTestId("legacy")).toHaveTextContent(B);
    await user.click(screen.getByRole("button", { name: "set scope" }));
    await user.click(screen.getByRole("link", { name: "evidence" }));
    const location = new URL(
      screen.getByTestId("location").textContent ?? "",
      "http://router.local",
    );
    expect(location.pathname).toBe("/evidence");
    expect(location.searchParams.get("cluster_id")).toBe(B);
    expect(location.searchParams.get("resource_key")).toBe("orders");
    expect(location.searchParams.get("window_start")).toBe(
      "2026-11-11T10:00:00Z",
    );
    expect(location.searchParams.get("tab")).toBe("metadata");
  });
  it("changes the ownership key and rejects the old scope when authorization changes", () => {
    const { rerender } = render(<Fixture initial={`/?cluster_id=${B}`} />);
    const old = screen.getByTestId("key").textContent;
    state.session = { ...state.session, subject: "new-user", clusterIds: [A] };
    rerender(<Fixture initial={`/?cluster_id=${B}`} />);
    expect(screen.getByTestId("key").textContent).not.toBe(old);
    expect(screen.getByTestId("workspace")).toBeEmptyDOMElement();
    expect(screen.getByTestId("invalid")).toHaveTextContent("true");
  });
});
