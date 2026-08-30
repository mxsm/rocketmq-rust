import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";

import {
  createBrowserRouter,
  Link,
  MemoryRouter,
  NavLink,
  Outlet,
  RouterProvider,
  useLocation,
  useParams,
  useSearchParams,
} from "./router";

describe("bounded browser router", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
  });

  it("matches route parameters and renders them through the layout outlet", () => {
    window.history.replaceState(null, "", "/clusters/cluster%20a");
    const router = createBrowserRouter([
      {
        path: "/",
        element: (
          <main>
            <span>application shell</span>
            <Outlet />
          </main>
        ),
        children: [
          {
            path: "/clusters/:clusterId",
            element: <ClusterRoute />,
          },
        ],
      },
    ]);

    render(<RouterProvider router={router} />);

    expect(screen.getByText("application shell")).toBeVisible();
    expect(screen.getByText("cluster a")).toBeVisible();
  });

  it("updates browser history when an internal link is followed", async () => {
    const user = userEvent.setup();
    const router = createBrowserRouter([
      {
        path: "/",
        element: <Outlet />,
        children: [
          {
            index: true,
            element: <Link to="/clusters/primary">open cluster</Link>,
          },
          {
            path: "/clusters/:clusterId",
            element: <ClusterRoute />,
          },
        ],
      },
    ]);
    render(<RouterProvider router={router} />);

    await user.click(screen.getByRole("link", { name: "open cluster" }));

    expect(window.location.pathname).toBe("/clusters/primary");
    expect(screen.getByText("primary")).toBeVisible();
  });

  it("tracks active links and replaces search parameters in memory", async () => {
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={["/changes/calendar?owner=platform"]}>
        <NavLink
          className={({ isActive }) => (isActive ? "active" : undefined)}
          to="/changes"
        >
          changes
        </NavLink>
        <SearchParameters />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "changes" })).toHaveClass(
      "active",
    );
    expect(screen.getByText("platform")).toBeVisible();

    await user.click(screen.getByRole("button", { name: "show sre" }));

    expect(screen.getByText("sre")).toBeVisible();
    expect(screen.getByTestId("location")).toHaveTextContent(
      "/changes/calendar?owner=sre",
    );
  });

  it("renders unsafe link targets as disabled without leaving the app", async () => {
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={["/models"]}>
        <Link to="https://untrusted.example/models">external model</Link>
        <Link to="//untrusted.example/models">protocol relative model</Link>
        <LocationValue />
      </MemoryRouter>,
    );

    const links = [
      screen.getByRole("link", { name: "external model" }),
      screen.getByRole("link", { name: "protocol relative model" }),
    ];
    for (const link of links) {
      expect(link).toHaveAttribute("aria-disabled", "true");
      expect(link).toHaveAttribute("href", "#");
      await user.click(link);
    }
    expect(screen.getByTestId("location")).toHaveTextContent("/models");
  });
});

function ClusterRoute() {
  const { clusterId } = useParams<{ clusterId: string }>();
  return <span>{clusterId}</span>;
}

function SearchParameters() {
  const [searchParams, setSearchParams] = useSearchParams();
  return (
    <>
      <span>{searchParams.get("owner")}</span>
      <button
        onClick={() => setSearchParams({ owner: "sre" }, { replace: true })}
        type="button"
      >
        show sre
      </button>
      <LocationValue />
    </>
  );
}

function LocationValue() {
  const location = useLocation();
  return (
    <span data-testid="location">
      {location.pathname}
      {location.search}
    </span>
  );
}
