import type { ExecutionObservationView } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { shortDigest } from "@/features/plans/planPresentation";
import { useExecutionObservationMessages } from "@/i18n/executionObservationMessages";
import { ExecutionFact } from "./ExecutionObservationPanel";

export function ExecutionTimeline({ view, transport, current }: {
  view: ExecutionObservationView;
  transport: "connecting" | "sse" | "polling";
  current: boolean;
}) {
  const { m, state, reason, formatDateTime } = useExecutionObservationMessages();
  const binding = view.binding;
  const verificationCount = view.steps.reduce((count, step) => count + step.verifications.length, 0);
  const eventNames: Record<ExecutionObservationView["timeline"][number]["kind"], string> = {
    accepted: m.eventAccepted, state_changed: m.eventState, intent_persisted: m.eventIntent,
    result_recorded: m.eventResult, verification_captured: m.eventCapture,
    verification_completed: m.eventVerification, manual_takeover_required: m.eventManual,
    quarantine_created: m.eventQuarantine, quarantine_cleared: m.eventQuarantineCleared, other: m.eventOther,
  };
  return <>
    <section className="execution-layer-grid" aria-label={m.title}>
      <article className="data-surface"><h2>{m.submission}</h2><Badge variant="secondary">{m.accepted}</Badge>
        <p>{m.requester} · {binding.requested_by}</p><time>{formatDateTime(binding.accepted_at)}</time>
      </article>
      <article className="data-surface"><h2>{m.action}</h2>
        <Badge variant="secondary">{state(binding.state)}</Badge><p>{m.recordedState}</p>
      </article>
      <article className="data-surface"><h2>{m.verification}</h2><p>{m.stepRecords}</p>
        {verificationCount ? <Badge variant="secondary">{m.verificationRecords} · {verificationCount}</Badge> : <p>{m.noWindowRecords}</p>}
        <p>{m.historical}</p>
      </article>
      <article className="data-surface"><h2>{m.business}</h2>
        <ExecutionFact fact={view.business_recovery} current={current} />
      </article>
    </section>
    <section className="data-surface execution-recorded-timeline">
      <header className="surface-heading"><div><h2>{m.timeline}</h2><p>{m.timelineHint}</p><p>{m.updateHint}</p></div>
        <Badge variant="secondary">{m.transport} · {transport.toUpperCase()}</Badge>
      </header>
      <dl className="execution-observation-values">
        <div><dt>{m.executionId}</dt><dd>{shortDigest(binding.execution_id)}</dd></div>
        <div><dt>{m.asOf}</dt><dd>{formatDateTime(view.observed_at)}</dd></div>
        <div><dt>{m.validUntil}</dt><dd>{formatDateTime(view.valid_until)}</dd></div>
      </dl>
      {view.timeline_partial && <p className="inline-alert warning">{m.partial}</p>}
      <details className="execution-history-quality"><summary>{m.historyRead}</summary><ExecutionFact fact={view.history} current={current} /></details>
      {!view.timeline.length ? <p className="compact-empty">{m.noEvents}</p> : <ol className="execution-record-list" aria-label={m.timeline}>
        {view.timeline.map((event) => <li key={event.sequence}>
          <span className="execution-record-order">{event.sequence}</span>
          <div><strong>{eventNames[event.kind] ?? m.eventOther}</strong>
            {event.state && <span>{state(event.state)}</span>}
            <p>{reason(event.reason)}</p><time>{formatDateTime(event.occurred_at)}</time>
          </div>
          {event.step_id && <code>{shortDigest(event.step_id)}</code>}
        </li>)}
      </ol>}
    </section>
  </>;
}
