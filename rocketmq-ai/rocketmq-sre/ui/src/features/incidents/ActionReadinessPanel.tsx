import type { ActionReadinessRequest, ActionReadinessView } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useActionReadinessMessages } from "@/i18n/actionReadinessMessages";
import { useActionReadinessLabels } from "@/i18n/actionReadinessReasons";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

import {
  readinessMatches,
  readinessStages,
  type ReadinessOwner,
} from "./actionReadinessPresentation";
import { useReadinessExpiry } from "./useReadinessExpiry";

type ReadinessError = "read" | "forbidden" | "scope" | "conflict";

export function ActionReadinessPanel({
  view,
  request,
  owner,
  loading = false,
  disabled = false,
  error,
  onRefresh,
}: {
  view?: ActionReadinessView;
  request: ActionReadinessRequest;
  owner: ReadinessOwner;
  loading?: boolean;
  disabled?: boolean;
  error?: ReadinessError;
  onRefresh?: () => void;
}) {
  const m = useActionReadinessMessages();
  const label = useActionReadinessLabels();
  const { formatDateTime } = useOperatorPreferences();
  const current = view && readinessMatches(view, request, owner) ? view : undefined;
  const expired = useReadinessExpiry(current);
  const invalid = Boolean(view && !current);
  const title = {
    descriptor: m.descriptor,
    handler: m.handler,
    enabled: m.enabled,
    target: m.targetSupported,
    evidence: m.evidenceValid,
    policy: m.policy,
    approval: m.approval,
    qualification: m.qualification,
    compensation: m.compensation,
  };
  const status = {
    satisfied: m.satisfied,
    blocked: m.blocked,
    unknown: m.unknown,
    not_applicable: m.notApplicable,
  };
  const tone = {
    satisfied: "success",
    blocked: "destructive",
    unknown: "warning",
    not_applicable: "secondary",
  } as const;
  const summary = current && {
    satisfied: m.conditionsSatisfied,
    blocked: m.blocked,
    unknown: m.unknown,
    awaiting_approval: m.awaitingApproval,
  }[current.execution_readiness];
  const failure = invalid || error === "scope"
    ? m.scopeMismatch
    : error === "forbidden"
      ? m.forbidden
      : error === "conflict"
        ? m.conflict
        : error ? m.readFailure : undefined;

  return (
    <section className="action-readiness-panel" aria-label={m.readinessTitle}>
      <header className="action-readiness-heading">
        <div>
          <h3>{m.readinessTitle}</h3>
          <p>{m.readinessHint}</p>
        </div>
        {current && !expired && (
          <Badge variant={current.execution_readiness === "satisfied" ? "success" : "warning"}>
            {summary}
          </Badge>
        )}
        {onRefresh && (
          <Button disabled={loading || disabled} onClick={onRefresh} variant="outline" size="sm">
            {current ? m.refreshShort : m.refresh}
          </Button>
        )}
      </header>
      {loading && <p role="status">{m.loading}</p>}
      {failure && <p className="inline-alert warning" role="alert">{failure}</p>}
      {expired && <p className="inline-alert warning" role="alert">{m.expired}</p>}
      {!loading && !failure && !current && <p className="muted">{m.notRead}</p>}
      {current && (
        <>
          <dl className="action-readiness-times">
            <div>
              <dt>{m.observedAt}</dt>
              <dd>{formatDateTime(current.observed_at)}</dd>
            </div>
            <div>
              <dt>{m.validUntil}</dt>
              <dd>{current.valid_until ? formatDateTime(current.valid_until) : m.awaitingServer}</dd>
            </div>
          </dl>
          {!expired && (
            <ol className="action-readiness-stages">
              {readinessStages.map((key) => {
                const fact = current.stages[key];
                return (
                  <li key={key}>
                    <div className="action-readiness-stage-title">
                      <h4>{title[key]}</h4>
                      <Badge variant={tone[fact.state]}>{status[fact.state]}</Badge>
                    </div>
                    {fact.reason_codes.length > 0 && (
                      <ul>
                        {fact.reason_codes.map((reason, index) => (
                          <li key={`${reason}-${index}`}>{label.reason(reason)}</li>
                        ))}
                      </ul>
                    )}
                  </li>
                );
              })}
            </ol>
          )}
          <p className="muted action-readiness-authority">{m.dispatchHint}</p>
          <details className="action-readiness-audit">
            <summary>{m.audit}</summary>
            <dl>
              <div><dt>{m.revision}</dt><dd>{current.binding.diagnosis_revision_id}</dd></div>
              <div><dt>{m.resource}</dt><dd>{current.binding.resource || m.awaitingServer}</dd></div>
              <div><dt>{m.policyVersion}</dt><dd>{current.binding.policy_version}</dd></div>
              <div><dt>{m.parametersDigest}</dt><dd>{current.binding.parameters_digest ?? m.awaitingServer}</dd></div>
              <div><dt>{m.bindingDigest}</dt><dd>{current.binding.binding_digest}</dd></div>
              {current.binding.precondition_evidence && (
                <div><dt>{m.proof}</dt><dd>{current.binding.precondition_evidence.evidence_id}</dd></div>
              )}
            </dl>
            {!expired && readinessStages.map((key) => (
              <div className="action-readiness-source" key={key}>
                <strong>{title[key]} · {label.source(current.stages[key].source)}</strong>
                {current.stages[key].observed_at && (
                  <span>{m.observedAt} · {formatDateTime(current.stages[key].observed_at!)}</span>
                )}
              </div>
            ))}
          </details>
        </>
      )}
    </section>
  );
}
