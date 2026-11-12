import { useCallback } from "react";

import type { InvestigationDiagnosisRevision } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import {
  toEvidenceMetadata,
  type EvidenceMetadata,
} from "./evidencePresentation";
import { EvidenceScopeError, evidenceErrorKind } from "./useEvidenceMetadata";
export type EvidenceReadFailure = ReturnType<typeof evidenceErrorKind>;

/** Metadata is bounded to saved references, with four concurrent reads at most. */
export function useRevisionEvidenceMetadata(
  revision: InvestigationDiagnosisRevision | undefined,
  requestedIds: readonly string[],
  assessmentKey?: string,
) {
  const { api } = useSreData();
  const scope = useWorkspaceScope();
  const tenantId = useAuth().session?.tenantId;
  const idsKey = JSON.stringify(
    [...new Set(requestedIds)]
      .filter((id) => revision?.evidence_ids.includes(id))
      .slice(0, 128),
  );
  const revisionId = revision?.id;
  const load = useCallback(
    async (signal: AbortSignal) => {
      const metadata: Record<string, EvidenceMetadata> = {};
      const failures: Record<string, EvidenceReadFailure> = {};
      if (!tenantId || !scope.clusterId)
        return {
          metadata,
          failures,
          ownerIdentity: scope.identityKey,
          revisionId,
          assessmentKey,
        };
      const ids = JSON.parse(idsKey) as string[];
      for (let index = 0; index < ids.length; index += 4) {
        if (signal.aborted) throw new DOMException("Aborted", "AbortError");
        const results = await Promise.allSettled(
          ids.slice(index, index + 4).map(async (id) => {
            const evidence = await api.getEvidence(id, signal);
            if (
              evidence.evidence_id !== id ||
              evidence.tenant_id !== tenantId ||
              evidence.cluster_id !== scope.clusterId
            )
              throw new EvidenceScopeError();
            return toEvidenceMetadata(evidence);
          }),
        );
        results.forEach((result, offset) => {
          const id = ids[index + offset];
          if (result.status === "fulfilled") metadata[id] = result.value;
          else failures[id] = evidenceErrorKind(result.reason);
        });
      }
      return {
        metadata,
        failures,
        ownerIdentity: scope.identityKey,
        revisionId,
        assessmentKey,
      };
    },
    [
      api,
      assessmentKey,
      idsKey,
      revisionId,
      scope.clusterId,
      scope.identityKey,
      tenantId,
    ],
  );
  return useAsyncResource(load);
}
