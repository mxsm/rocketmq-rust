import { useMemo, useState } from "react";
import { useLocation } from "react-router-dom";

import { useOptionalWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { parseReadOnlyUrlContext } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";

export {
  parseReadOnlyUrlContext,
  withoutReadOnlyUrlContext,
} from "@/context/workspaceScope";
export type {
  ReadOnlyUrlContext,
  UrlContextResult,
} from "@/context/workspaceScope";

/** Legacy selectors share the workspace scope when mounted in the application shell. */
export function useClusterScope() {
  const { clusters } = useSreData();
  const workspace = useOptionalWorkspaceScope();
  const location = useLocation();
  const [localClusterId, setLocalClusterId] = useState("");
  const parsed = useMemo(
    () =>
      parseReadOnlyUrlContext(
        location.search,
        clusters.map((cluster) => cluster.id),
      ),
    [clusters, location.search],
  );
  const clusterId =
    workspace?.clusterId ??
    (parsed.status === "valid"
      ? parsed.context.clusterId
      : (clusters.find((cluster) => cluster.id === localClusterId)?.id ??
        clusters[0]?.id ??
        ""));
  return useMemo(
    () => ({
      clusterId,
      setClusterId: workspace?.setClusterId ?? setLocalClusterId,
      cluster: clusters.find((item) => item.id === clusterId),
      clusters,
      urlContext: workspace
        ? {
            clusterId,
            resourceKind: workspace.resourceKind,
            resourceKey: workspace.resourceKey,
          }
        : parsed.status === "valid"
          ? parsed.context
          : undefined,
    }),
    [clusterId, clusters, parsed, workspace],
  );
}
