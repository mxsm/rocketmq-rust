import { Filter } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { auditApi } from '../api/audit_api';
import { ApiClientError } from '../api/client';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import { Button } from '../components/ui/Button';
import { Input } from '../components/ui/Input';
import type { AuditEventPage, AuditOutcome } from '../types/audit';

const pageSize = 50;

export default function AuditPage() {
  const [actor, setActor] = useState('');
  const [action, setAction] = useState('');
  const [outcome, setOutcome] = useState<AuditOutcome | ''>('');
  const [environmentId, setEnvironmentId] = useState('');
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');
  const [page, setPage] = useState<AuditEventPage>({ events: [] });
  const [cursorHistory, setCursorHistory] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (cursor?: string, resetHistory = false) => {
    setLoading(true);
    setError(null);
    try {
      const next = await auditApi.listEvents({
        actor: actor.trim() || undefined,
        action: action.trim() || undefined,
        outcome: outcome || undefined,
        environmentId: environmentId.trim() || undefined,
        startMs: localDateTimeToMillis(start),
        endMs: localDateTimeToMillis(end),
        cursor,
        limit: pageSize
      });
      setPage(next);
      if (resetHistory) setCursorHistory([]);
    } catch (cause) {
      setError(errorMessage(cause, 'Unable to load audit events.'));
    } finally {
      setLoading(false);
    }
  }, [action, actor, end, environmentId, outcome, start]);

  useEffect(() => { void load(undefined, true); }, [load]);

  return (
    <>
      <PageHeader title="Audit" description="Append-only operational history. Read access is not audited." actions={<RefreshButton onRefresh={() => void load(undefined, true)} />} />
      <section className="table-shell audit-admin-shell">
        <div className="audit-filter-row">
          <Input value={actor} placeholder="Actor" aria-label="Actor" onChange={(event) => setActor(event.target.value)} />
          <Input value={action} placeholder="Action code" aria-label="Action code" onChange={(event) => setAction(event.target.value)} />
          <Input value={environmentId} placeholder="Environment ID" aria-label="Environment ID" onChange={(event) => setEnvironmentId(event.target.value)} />
          <Input type="datetime-local" value={start} aria-label="Start time" onChange={(event) => setStart(event.target.value)} />
          <Input type="datetime-local" value={end} aria-label="End time" onChange={(event) => setEnd(event.target.value)} />
          <select value={outcome} aria-label="Outcome" onChange={(event) => setOutcome(event.target.value as AuditOutcome | '')}>
            <option value="">All outcomes</option><option value="succeeded">Succeeded</option><option value="rejected">Rejected</option><option value="failed">Failed</option>
          </select>
          <Button type="button" variant="secondary" onClick={() => void load(undefined, true)}><Filter size={15} aria-hidden="true" /> Filter</Button>
        </div>
        {loading ? <LoadingState label="Loading audit events" /> : null}
        {!loading && error ? <ErrorState message={error} onRetry={() => void load(undefined, true)} retryLabel="Retry audit" /> : null}
        {!loading && !error ? (
          <>
            <div className="table-scroll"><table>
              <thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Resource</th><th>Outcome</th><th>Detail</th></tr></thead>
              <tbody>{page.events.map((event) => (
                <tr key={event.eventId}>
                  <td>{formatTime(event.createdAtMs)}</td><td>{event.actor}</td><td><code>{event.action}</code></td>
                  <td>{event.resourceName ?? event.resourceType}</td><td><span className={`audit-outcome audit-outcome-${event.outcome}`}>{event.outcome}</span></td>
                  <td><code className="audit-detail">{safeJson(event.detail)}</code></td>
                </tr>
              ))}</tbody>
            </table></div>
            {page.events.length === 0 ? <EmptyState title="No audit events match these filters" /> : null}
            <div className="table-footer"><span>{page.events.length} events</span><div className="pagination">
              <Button type="button" variant="secondary" disabled={cursorHistory.length === 0} onClick={() => {
                const previous = cursorHistory.slice(0, -1); setCursorHistory(previous); void load(previous[previous.length - 1]);
              }}>Previous</Button>
              <Button type="button" variant="secondary" disabled={!page.nextCursor} onClick={() => {
                if (!page.nextCursor) return; setCursorHistory((history) => [...history, page.nextCursor!]); void load(page.nextCursor);
              }}>Next</Button>
            </div></div>
          </>
        ) : null}
      </section>
    </>
  );
}

function safeJson(value: unknown) {
  if (value === null || value === undefined) return '—';
  try { return JSON.stringify(value); } catch { return '—'; }
}

function formatTime(value: number) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Unknown' : date.toLocaleString();
}

function localDateTimeToMillis(value: string) {
  if (!value) return undefined;
  const milliseconds = new Date(value).getTime();
  return Number.isFinite(milliseconds) ? milliseconds : undefined;
}

function errorMessage(error: unknown, fallback: string) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_UNAVAILABLE') return `${error.message} Retry when storage is available.`;
  return error instanceof Error ? error.message : fallback;
}
