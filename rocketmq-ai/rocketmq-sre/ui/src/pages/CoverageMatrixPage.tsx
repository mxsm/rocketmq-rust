import { RefreshCw } from "lucide-react";
import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import type { CoverageRevisionBinding } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { DataState } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { CapabilityQualityMatrix } from "@/features/capabilities/CapabilityQualityMatrix";
import {
  CapabilityScopeError,
  useCapabilityQuality,
} from "@/features/capabilities/useCapabilityQuality";
import { ObjectScopeBoundary } from "@/features/scope/ObjectScopeBoundary";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useCapabilityMessages } from "@/i18n/capabilityMessages";

export function CoverageMatrixPage() {
  const [params] = useSearchParams();
  const m = useCapabilityMessages();
  const investigations = params.getAll("investigation_id");
  const revisions = params.getAll("revision_id");
  const hasBinding = investigations.length > 0 || revisions.length > 0;
  const validId = (value: string) =>
    value.length > 0 && value.length <= 128 && !/\s/.test(value);
  if (
    params.has("resource") ||
    (hasBinding &&
      (investigations.length !== 1 ||
        revisions.length !== 1 ||
        !validId(investigations[0]) ||
        !validId(revisions[0])))
  ) {
    return (
      <p className="page inline-alert warning" role="alert">
        {m.invalidBinding}
      </p>
    );
  }
  return hasBinding ? (
    <RevisionCoverage
      binding={{ investigationId: investigations[0], revisionId: revisions[0] }}
    />
  ) : (
    <CoverageContent />
  );
}

function RevisionCoverage({ binding }: { binding: CoverageRevisionBinding }) {
  const { api } = useSreData();
  const m = useCapabilityMessages();
  const { investigationId, revisionId } = binding;
  const load = useCallback(
    (signal: AbortSignal) => api.getInvestigation(investigationId, signal),
    [api, investigationId],
  );
  const resource = useAsyncResource(load);
  const revision = resource.data?.diagnosis_revisions.find(
    (item) => item.id === revisionId && item.investigation_id === investigationId,
  );
  if (!resource.data || resource.error) {
    return (
      <div className="page">
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && !resource.data}
          onRetry={resource.reload}
        />
      </div>
    );
  }
  if (resource.data.investigation.id !== investigationId || !revision) {
    return (
      <p className="page inline-alert warning" role="alert">
        {m.bindingUnavailable}
      </p>
    );
  }
  return (
    <ObjectScopeBoundary object={resource.data.investigation}>
      <CoverageContent
        binding={binding}
        revisionLabel={`Revision ${revision.revision} · ${resource.data.investigation.title}`}
        resourceLabel={resource.data.investigation.resource ?? m.clusterScope}
      />
    </ObjectScopeBoundary>
  );
}

function CoverageContent({
  binding,
  revisionLabel,
  resourceLabel,
}: {
  binding?: CoverageRevisionBinding;
  revisionLabel?: string;
  resourceLabel?: string;
}) {
  const resource = useCapabilityQuality(binding);
  const m = useCapabilityMessages();
  return (
    <div className="page coverage-page">
      <PageHeader
        eyebrow="CAPABILITY & EVIDENCE QUALITY"
        title={m.title}
        description={m.description}
        actions={
          <Button
            disabled={resource.loading}
            onClick={resource.reload}
            size="sm"
            variant="outline"
          >
            <RefreshCw size={14} />
            {m.refresh}
          </Button>
        }
      />
      <p className="investigation-mode">{m.noExecution}</p>
      {binding && (
        <p className="inline-alert" role="status">
          {m.boundRevision}: {revisionLabel} · {m.investigationScope}:{" "}
          {resourceLabel}
        </p>
      )}
      {resource.error instanceof CapabilityScopeError ? (
        <p className="inline-alert warning" role="alert">
          {m.scopeMismatch}
        </p>
      ) : (
        <DataState
          loading={resource.loading && !resource.projection}
          error={resource.error}
          empty={!resource.loading && !resource.error && !resource.projection}
          emptyTitle={m.noProjection}
          emptyDescription={m.noProjectionHint}
          onRetry={resource.reload}
        />
      )}
      {resource.projection && (
        <CapabilityQualityMatrix projection={resource.projection} />
      )}
    </div>
  );
}
