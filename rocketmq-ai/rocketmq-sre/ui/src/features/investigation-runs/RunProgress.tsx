import type { InvestigationRun, InvestigationRunControls } from "@/api/types";
import { DataSurface, DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { useRunMessages } from "@/i18n/runMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import { budgetRemaining } from "./runPresentation";
import { useRunLabel } from "./runLabels";

export function RunProgress({
  run,
  controls,
}: {
  run: InvestigationRun;
  controls?: InvestigationRunControls;
}) {
  const m = useRunMessages();
  const label = useRunLabel();
  const { formatDateTime } = useOperatorPreferences();
  const usage =
    controls?.run.revision === run.revision ? controls.budget_usage : undefined;
  const costs = [
    { key: "read_queries", title: m.readQueries },
    { key: "model_calls", title: m.modelCalls },
    { key: "evidence_items", title: m.evidenceItems },
    { key: "output_bytes", title: m.outputBytes },
  ] as const;
  const costDescription = (cost: InvestigationRun["budget"]["limit"]) =>
    costs.map(({ key, title }) => `${title}: ${cost[key]}`).join(" · ");
  return (
    <>
      <DataSurface title={m.budget} description={m.budgetHint}>
        <div className="run-table-scroll">
          <table className="run-budget-table">
            <thead>
              <tr>
                <th scope="col">{m.budget}</th>
                <th scope="col">{m.limit}</th>
                <th scope="col">{m.settled}</th>
                <th scope="col">{m.reserved}</th>
                <th scope="col">{m.remaining}</th>
              </tr>
            </thead>
            <tbody>
              {costs.map(({ key, title }) => (
                <tr key={key}>
                  <th scope="row">{title}</th>
                  <td>{run.budget.limit[key]}</td>
                  <td>{usage ? usage.settled[key] : "—"}</td>
                  <td>{usage ? usage.reserved[key] : "—"}</td>
                  <td>
                    {usage
                      ? (budgetRemaining(
                          run.budget.limit[key],
                          usage.settled[key],
                          usage.reserved[key],
                        ) ?? "—")
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <DefinitionGrid
          items={[
            {
              label: m.maxSteps,
              value: `${run.steps.reduce((total, step) => total + step.attempts.length, 0)} / ${run.budget.max_steps}`,
            },
            { label: m.maxInFlight, value: run.budget.max_in_flight },
            { label: m.maxAttempts, value: run.budget.max_attempts_per_step },
            { label: m.maxRetries, value: run.budget.max_query_retries },
          ]}
        />
      </DataSurface>
      <DataSurface title={m.steps}>
        {!run.steps.length && <p className="muted">{m.noSteps}</p>}
        <ol className="run-steps">
          {run.steps.map((step, index) => (
            <li key={step.id}>
              <div className="run-step-heading">
                <strong>
                  {index + 1}.{" "}
                  {step.spec.work.kind === "collect_evidence"
                    ? m.collect
                    : step.spec.work.kind === "evaluate_rules"
                      ? m.evaluate
                      : m.explain}
                </strong>
                <span>
                  {step.spec.pack.pack_id} · {step.spec.pack.pack_version}
                </span>
              </div>
              {step.spec.work.kind === "collect_evidence" && (
                <p className="muted">{step.spec.work.requirement_id}</p>
              )}
              {step.attempts.map((attempt, attemptIndex) => (
                <article className="run-attempt" key={attempt.id}>
                  <div className="run-step-heading">
                    <strong>
                      {m.attempt} {attemptIndex + 1}
                    </strong>
                    <Badge
                      variant={attempt.interruption ? "warning" : "outline"}
                    >
                      {attempt.interruption
                        ? m.unknown
                        : attempt.result
                          ? m[attempt.result.outcome]
                          : m.inFlight}
                    </Badge>
                  </div>
                  {attempt.interruption && (
                    <p>{label(attempt.interruption.reason)}</p>
                  )}
                  {attempt.disposition === "superseded" && (
                    <p>{m.superseded}</p>
                  )}
                  {attempt.result?.failure && (
                    <p>{label(attempt.result.failure)}</p>
                  )}
                  <p className="muted">
                    {formatDateTime(attempt.admitted_at)} · {m.epoch}{" "}
                    {attempt.control_epoch}
                  </p>
                  <p>
                    {m.reservation}: {costDescription(attempt.reservation)}
                  </p>
                  <p>
                    {m.actual}:{" "}
                    {attempt.result
                      ? costDescription(attempt.result.actual_cost)
                      : "—"}
                  </p>
                  <details className="run-audit">
                    <summary>{m.audit}</summary>
                    <p className="run-id">
                      Step {step.id}
                      <br />
                      Attempt {attempt.id}
                    </p>
                  </details>
                </article>
              ))}
            </li>
          ))}
        </ol>
      </DataSurface>
    </>
  );
}
