import { type ReactNode, useEffect } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { Button } from "@/components/ui/button";
import { useOptionalWorkspaceScope } from "@/context/WorkspaceScopeContext";
import {
  parseReadOnlyUrlContext,
  projectWorkspaceSearch,
  resourcePath,
  type WorkspaceScope,
} from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

interface ScopedObject {
  tenant_id: string;
  cluster_id: string;
  resource?: string;
}

/** Object IDs never override an explicitly selected or unauthorized scope. */
export function ObjectScopeBoundary({
  object,
  children,
  resourcePolicy = "exact",
}: {
  object?: ScopedObject;
  children: ReactNode;
  /** Run children must verify the fetched Run's parent and effective scope before exposing controls. */
  resourcePolicy?: "exact" | "defer_to_run";
}) {
  const auth = useAuth();
  const { clusters } = useSreData();
  const scope = useOptionalWorkspaceScope();
  const location = useLocation();
  const navigate = useNavigate();
  const english = useOperatorPreferences().locale === "en-US";
  const authorized = Boolean(
    object &&
      object.tenant_id === auth.session?.tenantId &&
      auth.hasClusterScope(object.cluster_id) &&
      clusters.some(
        (cluster) =>
          cluster.id === object.cluster_id &&
          cluster.tenant_id === object.tenant_id,
      ),
  );
  const objectResource = object?.resource;
  const objectCluster = object?.cluster_id;
  const objectScope =
    scope && objectCluster
      ? resourcePolicy === "defer_to_run"
        ? {
            clusterId: objectCluster,
            resourceKind: scope.resourceKind,
            resourceKey: scope.resourceKey,
            window: scope.window,
          }
        : scopeForObject(objectCluster, objectResource, scope.window)
      : undefined;
  const alignSearch = objectScope
    ? projectWorkspaceSearch(location.search, objectScope)
    : undefined;
  const shouldAlign = Boolean(
    object &&
      authorized &&
      scope &&
      !scope.invalid &&
      !scope.hasExplicitCluster,
  );

  useEffect(() => {
    if (shouldAlign && alignSearch !== undefined) {
      // Replacing preserves browser Back for legacy links without a scope query.
      navigate(
        {
          pathname: location.pathname,
          search: alignSearch,
          hash: location.hash,
        },
        { replace: true },
      );
    }
  }, [alignSearch, location.hash, location.pathname, navigate, shouldAlign]);

  if (!object) return children;

  if (!authorized) {
    return (
      <div className="page">
        <section className="state-panel" role="alert">
          <h2>
            {english
              ? "Object is outside your authorized scope"
              : "对象不在当前授权范围内"}
          </h2>
          <p>
            {english
              ? "Return to an authorized workspace to continue."
              : "请返回授权工作区后继续。"}
          </p>
          <Button asChild variant="outline">
            <Link to="/investigations">
              {english ? "Back to investigations" : "返回调查列表"}
            </Link>
          </Button>
        </section>
      </div>
    );
  }

  if (shouldAlign) {
    return (
      <div className="page">
        <div className="state-panel" role="status">
          {english
            ? "Aligning the workspace with the authorized object…"
            : "正在按授权对象对齐工作区范围…"}
        </div>
      </div>
    );
  }

  const requestedResource = scope ? resourcePath(scope) : undefined;
  const resourceMismatch =
    resourcePolicy === "exact" &&
    scope?.resourceKind !== "cluster" &&
    requestedResource !== undefined &&
    requestedResource !== object.resource;
  if (
    scope &&
    (scope.invalid || scope.clusterId !== object.cluster_id || resourceMismatch)
  ) {
    return (
      <div className="page">
        <section className="state-panel" role="alert">
          <h2>
            {english
              ? "This object does not match the selected scope"
              : "此对象与当前选择的范围不一致"}
          </h2>
          <p>
            {english
              ? "Align the workspace with this object or return to the investigation list."
              : "可对齐到此对象的范围，或返回调查列表。"}
          </p>
          <div className="button-row">
            {objectScope && (
              <Button onClick={() => scope.setScope(objectScope)} type="button">
                {english ? "Align to object" : "对齐到对象范围"}
              </Button>
            )}
            <Button asChild variant="outline">
              <Link to="/investigations">
                {english ? "Back to investigations" : "返回调查列表"}
              </Link>
            </Button>
          </div>
        </section>
      </div>
    );
  }

  return children;
}

function scopeForObject(
  clusterId: string,
  resource: string | undefined,
  window: WorkspaceScope["window"],
): WorkspaceScope {
  const params = new URLSearchParams({ cluster_id: clusterId });
  const separator = resource?.indexOf("/") ?? -1;
  if (resource && separator > 0) {
    params.set("resource_kind", resource.slice(0, separator));
    params.set("resource_key", resource.slice(separator + 1));
  }
  const parsed = parseReadOnlyUrlContext(params.toString(), [clusterId]);
  return {
    clusterId,
    ...(parsed.status === "valid" ? parsed.context : {}),
    window,
  };
}
