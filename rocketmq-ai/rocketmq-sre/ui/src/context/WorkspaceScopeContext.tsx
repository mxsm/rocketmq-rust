import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { useSreData } from "@/data/SreDataContext";
import {
  parseAbsoluteWindow,
  parseReadOnlyUrlContext,
  projectWorkspaceSearch,
  type AbsoluteWindow,
  type WorkspaceScope,
} from "./workspaceScope";

interface WorkspaceScopeValue extends WorkspaceScope {
  invalid: boolean;
  hasExplicitCluster: boolean;
  identityKey: string;
  knowledgeFocus: string;
  scopeKey: string;
  setClusterId: (clusterId: string) => void;
  setScope: (scope: WorkspaceScope, intent?: { knowledgeFocus: string }) => void;
  setWindow: (window: AbsoluteWindow) => void;
  reset: () => void;
  searchFor: (search?: string) => string;
}

const WorkspaceScopeContext = createContext<WorkspaceScopeValue | undefined>(
  undefined,
);

export function WorkspaceScopeProvider({ children }: PropsWithChildren) {
  const { clusters } = useSreData();
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [defaultWindow] = useState<AbsoluteWindow>(() => {
    const end = new Date();
    return {
      start: new Date(end.getTime() - 24 * 60 * 60_000).toISOString(),
      end: end.toISOString(),
    };
  });
  const allowedClusters = useMemo(
    () =>
      clusters.filter(
        (cluster) =>
          auth.hasClusterScope(cluster.id) &&
          cluster.tenant_id === auth.session?.tenantId,
      ),
    [auth, clusters],
  );
  const parsed = useMemo(
    () =>
      parseReadOnlyUrlContext(
        location.search,
        allowedClusters.map((cluster) => cluster.id),
      ),
    [allowedClusters, location.search],
  );
  const parsedWindow = useMemo(
    () => parseAbsoluteWindow(location.search),
    [location.search],
  );
  const invalid = parsed.status === "invalid" || parsedWindow === "invalid";
  const clusterId = invalid
    ? ""
    : parsed.status === "valid"
      ? parsed.context.clusterId
      : (allowedClusters[0]?.id ?? "");
  const resourceKind =
    parsed.status === "valid" ? parsed.context.resourceKind : undefined;
  const resourceKey =
    parsed.status === "valid" ? parsed.context.resourceKey : undefined;
  const window =
    parsedWindow && parsedWindow !== "invalid" ? parsedWindow : defaultWindow;
  const identityKey = JSON.stringify([
    auth.session?.tenantId,
    auth.session?.subject,
    [...(auth.session?.clusterIds ?? [])].sort(),
    [...(auth.session?.roles ?? [])].sort(),
  ]);
  const focusValues = new URLSearchParams(location.search).getAll("focus");
  const rawFocus = focusValues.length === 1 ? focusValues[0] : focusValues.length ? "invalid" : "";
  const focusIdentity = auth.session ? identityKey : undefined;
  // This owner outlives the outlet's loading/scope remounts. It retains only an
  // entry identity, never item data; a new explicit navigation is a new intent.
  const focusEntry = useRef({ location, identity: focusIdentity, active: true });
  if (focusEntry.current.location !== location) {
    focusEntry.current = { location, identity: focusIdentity, active: true };
  } else if (focusEntry.current.identity !== focusIdentity) {
    const initialIdentity = focusEntry.current.identity === undefined && focusEntry.current.active;
    focusEntry.current = { location, identity: focusIdentity, active: initialIdentity };
  }
  const knowledgeFocus = location.pathname === "/knowledge" && focusEntry.current.active ? rawFocus : "";
  const currentNavigation = useRef({ location, identityKey });
  currentNavigation.current = { location, identityKey };
  useEffect(() => {
    const entry = focusEntry.current;
    if (entry.location !== location || entry.active || location.pathname !== "/knowledge") return;
    const params = new URLSearchParams(location.search);
    if (!params.has("focus")) return;
    params.delete("focus");
    const encoded = params.toString();
    navigate({ pathname: location.pathname, search: encoded ? `?${encoded}` : "", hash: location.hash }, { replace: true });
  }, [focusIdentity, location, navigate]);
  const setScope = useCallback(
    (next: WorkspaceScope, intent?: { knowledgeFocus: string }) => {
      if (!allowedClusters.some((cluster) => cluster.id === next.clusterId))
        return;
      if (intent && (location.pathname !== "/knowledge" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(intent.knowledgeFocus) ||
        currentNavigation.current.location !== location || currentNavigation.current.identityKey !== identityKey)) return;
      const projected = new URLSearchParams(projectWorkspaceSearch(location.search, next));
      // Ordinary scope changes consume the old detail link. Only the explicit,
      // authorized knowledge switch carries that item into the same navigation.
      if (location.pathname === "/knowledge") {
        projected.delete("focus");
        if (intent) projected.set("focus", intent.knowledgeFocus);
      }
      // Evidence and revision bindings belong to the previous scope selection.
      for (const key of [
        "evidence_id",
        "investigation_id",
        "revision_id",
        "run_id",
        "run_revision_id",
        "resource",
      ]) {
        projected.delete(key);
      }
      const search = `?${projected.toString()}`;
      if (
        parseAbsoluteWindow(search) === "invalid" ||
        parseReadOnlyUrlContext(
          search,
          allowedClusters.map((cluster) => cluster.id),
        ).status !== "valid"
      )
        return;
      navigate({ pathname: location.pathname, search, hash: location.hash });
    },
    [
      allowedClusters,
      identityKey,
      location,
      navigate,
    ],
  );
  const scope = useMemo<WorkspaceScope>(
    () => ({ clusterId, resourceKind, resourceKey, window }),
    [clusterId, resourceKey, resourceKind, window],
  );
  const value = useMemo<WorkspaceScopeValue>(
    () => ({
      ...scope,
      invalid,
      identityKey,
      knowledgeFocus,
      hasExplicitCluster: new URLSearchParams(location.search).has(
        "cluster_id",
      ),
      scopeKey: JSON.stringify([
        identityKey,
        clusterId,
        resourceKind,
        resourceKey,
        window,
      ]),
      setScope,
      setClusterId: (next) => setScope({ clusterId: next, window }),
      setWindow: (next) => setScope({ ...scope, window: next }),
      reset: () =>
        setScope({
          clusterId: allowedClusters[0]?.id ?? "",
          window: defaultWindow,
        }),
      searchFor: (search = "") => projectWorkspaceSearch(search, scope),
    }),
    [
      allowedClusters,
      clusterId,
      defaultWindow,
      identityKey,
      knowledgeFocus,
      invalid,
      location.search,
      resourceKey,
      resourceKind,
      scope,
      setScope,
      window,
    ],
  );

  return (
    <WorkspaceScopeContext.Provider value={value}>
      {children}
    </WorkspaceScopeContext.Provider>
  );
}

export function useOptionalWorkspaceScope() {
  return useContext(WorkspaceScopeContext);
}

export function useWorkspaceScope() {
  const value = useOptionalWorkspaceScope();
  if (!value)
    throw new Error(
      "useWorkspaceScope must be used within WorkspaceScopeProvider",
    );
  return value;
}
