import { RefreshCw } from "lucide-react";

import { PageHeader } from "@/components/PageHeader";
import { DataState } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { CapabilityQualityMatrix } from "@/features/capabilities/CapabilityQualityMatrix";
import {
  CapabilityScopeError,
  useCapabilityQuality,
} from "@/features/capabilities/useCapabilityQuality";
import { useCapabilityMessages } from "@/i18n/capabilityMessages";

export function CoverageMatrixPage() {
  const resource = useCapabilityQuality();
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
