import { DatabaseZap } from "lucide-react";

import { DataSurface } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { useEvidenceMessages } from "@/i18n/evidenceMessages";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";

export function EvidenceReferencePanel({
  evidenceId,
  onOpen,
}: {
  evidenceId?: string;
  onOpen: (id: string) => void;
}) {
  const m = useWorkspaceMessages();
  const e = useEvidenceMessages();
  return (
    <DataSurface title={m.provenance}>
      <p className="workspace-empty">{m.selectEvidence}</p>
      <div className="investigation-related-links">
        <Button
          disabled={!evidenceId}
          onClick={() => {
            if (evidenceId) onOpen(evidenceId);
          }}
          size="sm"
          variant="outline"
        >
          <DatabaseZap size={14} />
          {e.view}
        </Button>
      </div>
    </DataSurface>
  );
}
