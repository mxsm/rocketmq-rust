import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { AuthProvider } from "@/auth/AuthContext";
import { DEMO_CLUSTER_ID } from "@/data/phase1Demo";
import { SreDataProvider } from "@/data/SreDataContext";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";

import { AppLayout } from "./AppLayout";

it("opens the calendar in the change sidebar and carries the workspace window into investigations", async () => {
  const user = userEvent.setup();
  const start = "2026-07-27T00:00:00.000Z";
  const end = "2026-07-28T00:00:00.000Z";
  const search = new URLSearchParams({
    demo: "1", cluster_id: DEMO_CLUSTER_ID, window_start: start, window_end: end,
  });
  window.history.replaceState(null, "", `/changes/calendar?${search}`);
  const router = createBrowserRouter([{
    element: <AppLayout />,
    children: [
      { path: "/changes/calendar", element: <h1>Calendar</h1> },
      { path: "/investigations", element: <h1>Investigations</h1> },
      { path: "/ask", element: <h1>Ask SRE</h1> },
    ],
  }]);
  render(
    <OperatorPreferencesProvider>
      <AuthProvider>
        <SreDataProvider>
          <RouterProvider router={router} />
        </SreDataProvider>
      </AuthProvider>
    </OperatorPreferencesProvider>,
  );

  await screen.findByRole("heading", { name: "Calendar" });
  const nav = within(screen.getByRole("navigation", { name: "主导航" }));
  expect(nav.getByRole("link", { name: "变更" })).toHaveAttribute("aria-current", "location");
  expect(nav.getByRole("link", { name: "变更日历" })).toHaveAttribute("aria-current", "page");
  const destinations = [
    ["变更中心", "/changes"],
    ["Runbook 库", "/changes/runbooks"],
    ["排程队列", "/changes/schedules"],
    ["发布护航", "/changes/releases"],
    ["企业集成", "/changes/integrations"],
    ["资源隔离", "/changes/quarantines"],
  ];
  for (const [name, pathname] of destinations) {
    const link = nav.getByRole<HTMLAnchorElement>("link", { name });
    expect(new URL(link.href).pathname).toBe(pathname);
  }

  await user.click(nav.getByRole("link", { name: "调查" }));
  expect(await screen.findByRole("heading", { name: "Investigations" })).toBeVisible();
  await user.click(nav.getByRole("link", { name: "Ask SRE" }));
  expect(await screen.findByRole("heading", { name: "Ask SRE" })).toBeVisible();
  expect(nav.getByRole("link", { name: "Ask SRE" })).toHaveAttribute("aria-current", "page");
  const target = new URL(window.location.href);
  expect(target.pathname).toBe("/ask");
  expect(target.searchParams.get("cluster_id")).toBe(DEMO_CLUSTER_ID);
  expect(target.searchParams.get("window_start")).toBe(start);
  expect(target.searchParams.get("window_end")).toBe(end);
});
