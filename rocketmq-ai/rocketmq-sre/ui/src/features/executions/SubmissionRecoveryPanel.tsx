import { Button } from "@/components/ui/button";
import { useExecutionObservationMessages } from "@/i18n/executionObservationMessages";
import type { useExecutionSubmission } from "./useExecutionSubmission";

export function SubmissionRecoveryPanel({ submission, onOpen }: {
  submission: ReturnType<typeof useExecutionSubmission>;
  onOpen: (executionId: string) => void;
}) {
  const { m, state, formatDateTime } = useExecutionObservationMessages();
  const uncertain = submission.intent.status === "uncertain";
  const acceptedId = submission.current?.acceptedId;
  return <section className="data-surface submission-recovery-panel">
    <header className="surface-heading"><div><h2>{uncertain ? m.uncertainTitle : m.acceptedReferences}</h2>
      <p>{uncertain ? m.uncertainHint : m.discoverHint}</p></div>
      <Button variant="outline" size="sm" disabled={submission.busy || submission.discoveryLoading} onClick={() => void submission.refresh()}>{m.refresh}</Button>
    </header>
    {submission.discoveryLoading && <p role="status">{m.discoverPending}</p>}
    {Boolean(submission.discoveryError) && <p className="inline-alert warning" role="alert">{m.discoverError}</p>}
    {submission.intent.status === "unavailable" && <p className="inline-alert warning" role="alert">{m.storageUnavailable}</p>}
    {submission.page && !submission.fresh && <p className="inline-alert warning">{m.expired}</p>}
    {submission.page?.partial && <p className="inline-alert warning">{m.partial}</p>}
    {uncertain && <>
      <p className="permission-copy">{m.noRepeat}</p>
      <Button variant="outline" disabled={submission.busy} onClick={() => void submission.lookup()}>
        {submission.busy ? m.lookupPending : m.lookup}
      </Button>
    </>}
    {submission.current?.notObserved && <p role="status">{m.noReceipt}</p>}
    {submission.current?.error && <p role="alert" className="inline-alert warning">
      {submission.current.error === "storage" ? m.storageUnavailable : submission.current.error === "scope" ? m.mismatch : submission.current.error === "forbidden" ? m.forbidden : m.lookupError}
    </p>}
    {acceptedId && <Button variant="outline" onClick={() => onOpen(acceptedId)}>{m.openExecution}</Button>}
    {submission.page?.items.length ? <ul className="accepted-execution-references">
      {submission.page.items.map((receipt) => <li key={receipt.execution_id}>
        <div><strong>{state(receipt.state)}</strong><small>{m.requester} · {receipt.requested_by} · {formatDateTime(receipt.accepted_at)}</small></div>
        <Button size="sm" variant="outline" onClick={() => onOpen(receipt.execution_id)}>{m.openExecution}</Button>
      </li>)}
    </ul> : null}
  </section>;
}
