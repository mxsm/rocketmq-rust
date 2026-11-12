import { useCallback } from "react";
import type { CoverageRevisionBinding } from "@/api/types";

import { useAuth } from "@/auth/AuthContext";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { scopedCapabilityProjection } from "./capabilityVerification";

export class CapabilityScopeError extends Error {}

export function useCapabilityQuality(binding?: CoverageRevisionBinding) {
  const { api } = useSreData();
  const scope = useWorkspaceScope();
  const tenantId = useAuth().session?.tenantId;
  const investigationId = binding?.investigationId;
  const revisionId = binding?.revisionId;
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!scope.clusterId || !tenantId) return undefined;
      const requestBinding =
        investigationId && revisionId
          ? { investigationId, revisionId }
          : undefined;
      const matrix = await api.getCoverage(
        signal,
        scope.clusterId,
        requestBinding,
      );
      const quality = matrix.quality;
      // This hook requests cluster or saved-revision scope, never a canonical target.
      if (
        quality &&
        (quality.schema_version !== "rocketmq-sre.capability-quality.v1" ||
          quality.cluster_id !== scope.clusterId ||
          quality.tenant_id !== tenantId ||
          quality.resource != null ||
          quality.execution_eligible !== false ||
          (requestBinding &&
            (quality.investigation_id !== investigationId ||
              quality.revision_id !== revisionId)) ||
          (!requestBinding &&
            (quality.investigation_id != null || quality.revision_id != null)))
      )
        throw new CapabilityScopeError();
      return {
        quality: quality ? scopedCapabilityProjection(quality) : undefined,
        ownerIdentity: scope.identityKey,
      };
    },
    [
      api,
      investigationId,
      revisionId,
      scope.clusterId,
      scope.identityKey,
      tenantId,
    ],
  );
  const resource = useAsyncResource(load);
  return {
    ...resource,
    projection: resource.error ? undefined : resource.data?.quality,
  };
}
