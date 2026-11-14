import { useLocation, useNavigate } from "react-router-dom";

import type { InvestigationRun } from "@/api/types";
import { DataSurface } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { useRunMessages } from "@/i18n/runMessages";
import { useRunLabel } from "./runLabels";
import { selectionSearch } from "./runPresentation";

export function RunAssessment({ run }: { run: InvestigationRun }) {
  const m = useRunMessages();
  const label = useRunLabel();
  const location = useLocation();
  const navigate = useNavigate();
  const assessment = run.assessments
    .filter((item) => item.control_epoch === run.control_epoch)
    .at(-1);
  if (!assessment) return null;
  const conclusion = assessment.conclusion;
  return (
    <DataSurface title={m.conclusion} description={m.advisory}>
      <p>{assessment.summary}</p>
      {conclusion.kind === "root_cause" && (
        <p>
          <strong>{conclusion.statement}</strong>
        </p>
      )}
      {conclusion.kind === "healthy" && (
        <p>
          <strong>{label("healthy")}</strong>
        </p>
      )}
      {conclusion.kind === "inconclusive" && (
        <>
          <p>{label(conclusion.reason)}</p>
          <ul>
            {conclusion.missing_requirements.map((requirement) => (
              <li
                key={`${requirement.pack.pack_id}:${requirement.requirement_id}`}
              >
                {requirement.pack.pack_id} · {requirement.requirement_id}
              </li>
            ))}
          </ul>
        </>
      )}
      {assessment.run_revision_id &&
        run.pack_results?.some(
          (result) => result.run_revision_id === assessment.run_revision_id,
        ) && (
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              navigate({
                pathname: location.pathname,
                search: selectionSearch(
                  location.search,
                  run.id,
                  assessment.run_revision_id!,
                ),
                hash: location.hash,
              })
            }
          >
            {m.openRevision}
          </Button>
        )}
      <details className="run-audit">
        <summary>{m.audit}</summary>
        <p className="run-id">
          Assessment {assessment.id}
          <br />
          Revision{" "}
          {assessment.run_revision_id ?? assessment.diagnosis_revision_id}
        </p>
      </details>
    </DataSurface>
  );
}
