import {
  BookOpenCheck,
  Compass,
  Gauge,
  Settings2,
  ShieldCheck,
  Workflow,
} from "lucide-react";

import type { WorkspaceMessageKey } from "@/i18n/workspaceMessages";

interface NavigationItem {
  to: string;
  label: WorkspaceMessageKey;
  end?: boolean;
}
export const workspaceNavigation = [
  {
    id: "duty",
    label: "duty",
    icon: Gauge,
    to: "/",
    items: [
      { to: "/", label: "overview", end: true },
      { to: "/incidents", label: "incidents" },
      { to: "/operations", label: "operations" },
      { to: "/action-items", label: "actions" },
    ],
  },
  {
    id: "investigations",
    label: "investigations",
    icon: Compass,
    to: "/investigations",
    items: [
      { to: "/investigations", label: "workspace" },
      { to: "/ask", label: "ask" },
      { to: "/assets", label: "assets" },
      { to: "/topology", label: "topology" },
      { to: "/evidence", label: "evidence" },
      { to: "/journeys", label: "journeys" },
    ],
  },
  {
    id: "changes",
    label: "changes",
    icon: Workflow,
    to: "/changes",
    items: [
      { to: "/changes", label: "changeCenter", end: true },
      { to: "/changes/runbooks", label: "runbooks" },
      { to: "/changes/calendar", label: "calendar" },
      { to: "/changes/schedules", label: "schedules" },
      { to: "/changes/releases", label: "releases" },
      { to: "/changes/integrations", label: "integrations" },
      { to: "/changes/quarantines", label: "quarantines" },
    ],
  },
  {
    id: "reliability",
    label: "reliability",
    icon: ShieldCheck,
    to: "/fleet",
    items: [
      { to: "/fleet", label: "fleet", end: true },
      { to: "/fleet/compliance", label: "compliance" },
      { to: "/fleet/dr", label: "dr" },
      { to: "/inspections", label: "inspections" },
      { to: "/forecasts", label: "forecasts" },
      { to: "/coverage", label: "coverage" },
    ],
  },
  {
    id: "knowledge",
    label: "knowledge",
    icon: BookOpenCheck,
    to: "/knowledge",
    items: [{ to: "/knowledge", label: "knowledgeBase" }],
  },
  {
    id: "settings",
    label: "settings",
    icon: Settings2,
    to: "/system",
    items: [
      { to: "/clusters", label: "clusters" },
      { to: "/models", label: "models" },
      { to: "/governance", label: "governance" },
      { to: "/finops", label: "finops" },
      { to: "/autonomy", label: "autonomy" },
      { to: "/system", label: "system" },
    ],
  },
] satisfies Array<{
  id: string;
  label: WorkspaceMessageKey;
  icon: typeof Gauge;
  to: string;
  items: NavigationItem[];
}>;

export function navigationForPath(pathname: string) {
  if (pathname.startsWith("/conversations/")) return workspaceNavigation[1];
  if (pathname.startsWith("/postmortems/")) return workspaceNavigation[0];
  return (
    workspaceNavigation.find((group) =>
      group.items.some(
        (item) =>
          pathname === item.to ||
          (item.to !== "/" && pathname.startsWith(`${item.to}/`)),
      ),
    ) ?? workspaceNavigation[0]
  );
}
