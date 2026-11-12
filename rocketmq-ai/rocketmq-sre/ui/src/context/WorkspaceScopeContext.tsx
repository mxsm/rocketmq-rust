import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useMemo,
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
  scopeKey: string;
  setClusterId: (clusterId: string) => void;
  setScope: (scope: WorkspaceScope) => void;
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
  const setScope = useCallback(
    (next: WorkspaceScope) => {
      if (!allowedClusters.some((cluster) => cluster.id === next.clusterId))
        return;
      const projected = new URLSearchParams(projectWorkspaceSearch(location.search, next));
      // Evidence and revision bindings belong to the previous scope selection.
      for (const key of [
        "evidence_id",
        "investigation_id",
        "revision_id",
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
      location.hash,
      location.pathname,
      location.search,
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
