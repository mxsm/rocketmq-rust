import { useCallback } from "react";

import { ApiError } from "@/api/client";
import { useAuth } from "@/auth/AuthContext";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import {
  metadataInScope,
  toEvidenceMetadata,
  type EvidenceAccess,
} from "./evidencePresentation";

export const SELECTED_RESOURCE: EvidenceAccess = { kind: "selected_resource" };
export class EvidenceScopeError extends Error {}

export function useEvidenceMetadata(
  evidenceId: string | undefined,
  access: EvidenceAccess = SELECTED_RESOURCE,
) {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const tenantId = auth.session?.tenantId ?? "";
  const authorized = auth.hasClusterScope(scope.clusterId);
  const clusterId = scope.clusterId;
  const accessKey = JSON.stringify(access);
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!evidenceId || !clusterId || !tenantId || !authorized)
        return undefined;
      const evidence = await api.getEvidence(evidenceId, signal);
      if (
        !metadataInScope(
          evidence,
          evidenceId,
          {
            clusterId,
            resourceKind: scope.resourceKind,
            resourceKey: scope.resourceKey,
            window: scope.window,
          },
          tenantId,
          JSON.parse(accessKey) as EvidenceAccess,
        )
      )
        throw new EvidenceScopeError();
      return {
        ...toEvidenceMetadata(evidence),
        ownerIdentity: scope.identityKey,
      };
    },
    [
      accessKey,
      api,
      authorized,
      clusterId,
      evidenceId,
      scope.identityKey,
      scope.resourceKey,
      scope.resourceKind,
      scope.window,
      tenantId,
    ],
  );
  return useAsyncResource(load);
}

export function evidenceErrorKind(error: unknown) {
  if (error instanceof EvidenceScopeError) return "mismatch" as const;
  if (error instanceof ApiError && [401, 403].includes(error.status))
    return "denied" as const;
  if (error instanceof ApiError && error.status === 404)
    return "notFound" as const;
  return "unavailable" as const;
}
