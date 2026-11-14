import { useCallback, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import type { InvestigationPackResult, InvestigationRun } from "@/api/types";
import { DataSurface } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { EvidenceDrawer } from "@/features/evidence/EvidenceDrawer";
import type { EvidenceFreshnessPolicy } from "@/features/evidence/evidencePresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useRunMessages } from "@/i18n/runMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import {
  RunScopeError,
  sameRunScope,
  selectionSearch,
} from "./runPresentation";
import { useRunLabel } from "./runLabels";

export function RunRevision({
  run,
  revisionId,
  observedAt,
}: {
  run: InvestigationRun;
  revisionId?: string;
  observedAt?: string;
}) {
  const m = useRunMessages();
  const label = useRunLabel();
  const location = useLocation();
  const navigate = useNavigate();
  const { formatDateTime } = useOperatorPreferences();
  const results = [...(run.pack_results ?? [])].sort(
    (left, right) => Date.parse(right.created_at) - Date.parse(left.created_at),
  );
  const selected = revisionId
    ? results.find((result) => result.run_revision_id === revisionId)
    : results[0];
  return (
    <DataSurface title={m.revisions} description={m.advisory}>
      {!results.length && <p>{m.noRevisions}</p>}
      {results.length > 0 && (
        <label>
          {m.openRevision}
          <select
            aria-label={m.openRevision}
            value={selected?.run_revision_id ?? ""}
            onChange={(event) =>
              navigate({
                pathname: location.pathname,
                search: selectionSearch(
                  location.search,
                  run.id,
                  event.target.value,
                ),
                hash: location.hash,
              })
            }
          >
            {!selected && <option value="">{m.scopeFailure}</option>}
            {results.map((result) => (
              <option
                key={result.run_revision_id}
                value={result.run_revision_id}
              >
                {result.pack.pack_id} · {formatDateTime(result.created_at)} ·{" "}
                {label(result.status)}
              </option>
            ))}
          </select>
        </label>
      )}
      {revisionId && !selected && <p role="alert">{m.scopeFailure}</p>}
      {selected && (
        <RunRevisionBody
          key={`${run.id}:${selected.run_revision_id}:${selected.revision_digest}`}
          runId={run.id}
          result={selected}
          observedAt={observedAt}
        />
      )}
    </DataSurface>
  );
}

function RunRevisionBody({
  runId,
  result,
  observedAt,
}: {
  runId: string;
  result: InvestigationPackResult;
  observedAt?: string;
}) {
  const m = useRunMessages();
  const label = useRunLabel();
  const { api } = useSreData();
  const { formatDateTime } = useOperatorPreferences();
  const {
    run_revision_id: id,
    pack,
    scope,
    step_id: stepId,
    attempt_id: attemptId,
    control_epoch: epoch,
    status,
    partial,
  } = result;
  const evidenceKey = JSON.stringify([...result.evidence_ids].sort());
  const scopeKey = JSON.stringify(scope);
  const fingerprint = result.input_fingerprint;
  const load = useCallback(
    async (signal: AbortSignal) => {
      const revision = await api.getInvestigationRunRevision(runId, id, signal);
      if (
        revision.schema_version !==
          "rocketmq-sre.investigation-run-revision.v1" ||
        revision.id !== id ||
        revision.run_id !== runId ||
        !sameRunScope(revision.scope, JSON.parse(scopeKey)) ||
        revision.control_epoch !== epoch ||
        revision.pack.pack_id !== pack.pack_id ||
        revision.pack.pack_version !== pack.pack_version ||
        revision.step_id !== stepId ||
        revision.attempt_id !== attemptId ||
        revision.status !== status ||
        revision.partial !== partial ||
        revision.input_fingerprint !== fingerprint ||
        JSON.stringify([...revision.evidence_ids].sort()) !== evidenceKey ||
        !Array.isArray(revision.findings) ||
        !Array.isArray(revision.requirements) ||
        revision.findings.some((finding) =>
          [
            ...finding.supporting_evidence_ids,
            ...finding.counter_evidence_ids,
          ].some((evidenceId) => !revision.evidence_ids.includes(evidenceId)),
        ) ||
        revision.requirements.some((requirement) =>
          requirement.evidence_ids.some(
            (evidenceId) => !revision.evidence_ids.includes(evidenceId),
          ),
        )
      )
        throw new RunScopeError();
      return revision;
    },
    [
      api,
      attemptId,
      epoch,
      evidenceKey,
      fingerprint,
      id,
      pack.pack_id,
      pack.pack_version,
      partial,
      runId,
      scopeKey,
      status,
      stepId,
    ],
  );
  const resource = useAsyncResource(load);
  const revision = resource.error ? undefined : resource.data;
  const [drawer, setDrawer] = useState<{
    id: string;
    policy?: EvidenceFreshnessPolicy;
  }>();
  const open = (evidenceId: string) => {
    if (!revision?.evidence_ids.includes(evidenceId)) return;
    const matches = revision.requirements.filter(
      (requirement) =>
        requirement.evidence_ids.includes(evidenceId) &&
        requirement.max_age_seconds > 0,
    );
    const maxAge = matches.length
      ? Math.min(...matches.map((requirement) => requirement.max_age_seconds))
      : undefined;
    setDrawer({
      id: evidenceId,
      policy:
        observedAt && maxAge
          ? {
              maxAgeSeconds: maxAge,
              asOf: observedAt,
              sourceRef: revision.pack.pack_id,
            }
          : undefined,
    });
  };
  return (
    <div className="run-revision">
      {resource.loading && !revision && <p role="status">{m.loading}</p>}
      {Boolean(resource.error) && (
        <div role="alert">
          {resource.error instanceof RunScopeError
            ? m.scopeFailure
            : m.loadFailure}
          <Button
            size="sm"
            variant="outline"
            onClick={() => void resource.reload()}
          >
            {m.retry}
          </Button>
        </div>
      )}
      {revision && (
        <>
          <div className="run-step-heading">
            <h3>{m.conclusion}</h3>
            <Badge variant={revision.partial ? "warning" : "outline"}>
              {label(revision.status)} ·{" "}
              {revision.partial ? m.partial : m.complete}
            </Badge>
          </div>
          <p className="muted">
            {formatDateTime(revision.created_at)} · {revision.pack.pack_id}{" "}
            {revision.pack.pack_version}
          </p>
          {revision.findings.map((finding, index) => (
            <article
              className="run-finding"
              key={`${finding.reason_code}:${index}`}
            >
              <h4>{finding.root_cause}</h4>
              <p>{label(finding.outcome)}</p>
              <div className="run-evidence-links">
                {finding.supporting_evidence_ids.map((evidenceId) => (
                  <Button
                    size="sm"
                    variant="outline"
                    key={evidenceId}
                    onClick={() => open(evidenceId)}
                  >
                    {m.supporting} · {evidenceId.slice(0, 8)}
                  </Button>
                ))}
                {finding.counter_evidence_ids.map((evidenceId) => (
                  <Button
                    size="sm"
                    variant="outline"
                    key={`counter:${evidenceId}`}
                    onClick={() => open(evidenceId)}
                  >
                    {m.counter} · {evidenceId.slice(0, 8)}
                  </Button>
                ))}
              </div>
              <details className="run-audit">
                <summary>{m.audit}</summary>
                {finding.reason_code}
              </details>
            </article>
          ))}
          <h3>{m.missing}</h3>
          <p className="muted">{m.savedQuality}</p>
          <ul className="run-requirements">
            {revision.requirements.map((requirement) => (
              <li key={requirement.id}>
                <div className="run-step-heading">
                  <strong>{requirement.id}</strong>
                  <Badge
                    variant={
                      requirement.quality === "available"
                        ? "outline"
                        : "warning"
                    }
                  >
                    {label(requirement.quality)}
                  </Badge>
                </div>
                <p>
                  {requirement.source} · {requirement.resource_prefix}
                </p>
                <p>{label(requirement.reason_code)}</p>
                {requirement.missing_fields.length > 0 && (
                  <p>{requirement.missing_fields.join(", ")}</p>
                )}
              </li>
            ))}
          </ul>
          <div className="run-evidence-links">
            {revision.evidence_ids.map((evidenceId) => (
              <Button
                key={evidenceId}
                size="sm"
                variant="outline"
                onClick={() => open(evidenceId)}
              >
                {m.evidence} · {evidenceId.slice(0, 8)}
              </Button>
            ))}
          </div>
          <details className="run-audit">
            <summary>{m.audit}</summary>
            <p className="run-id">
              Revision {revision.id}
              <br />
              Step {revision.step_id}
              <br />
              Attempt {revision.attempt_id}
            </p>
            <p className="run-window">
              {revision.scope.time_range.start} →{" "}
              {revision.scope.time_range.end}
            </p>
          </details>
          {drawer && (
            <EvidenceDrawer
              evidenceId={drawer.id}
              freshnessPolicy={drawer.policy}
              access={{
                kind: "revision_evidence",
                revisionId: revision.id,
                evidenceIds: revision.evidence_ids,
              }}
              onClose={() => setDrawer(undefined)}
            />
          )}
        </>
      )}
    </div>
  );
}
