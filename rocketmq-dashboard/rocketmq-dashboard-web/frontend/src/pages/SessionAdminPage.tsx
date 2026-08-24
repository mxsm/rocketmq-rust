import { LogOut, ShieldCheck } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { auditApi } from '../api/audit_api';
import { ApiClientError, isAppliedAuditFailure } from '../api/client';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import { Button } from '../components/ui/Button';
import { Input } from '../components/ui/Input';
import type { SessionListPage } from '../types/audit';

const pageSize = 50;

interface RevokeErrorState {
  message: string;
  retryable: boolean;
}

export default function SessionAdminPage() {
  const [username, setUsername] = useState('');
  const [appliedUsername, setAppliedUsername] = useState('');
  const [page, setPage] = useState<SessionListPage>({ items: [] });
  const [cursorHistory, setCursorHistory] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [revoking, setRevoking] = useState(false);
  const [revokeError, setRevokeError] = useState<RevokeErrorState | null>(null);

  const load = useCallback(async (exactUsername: string, cursor?: string, resetHistory = false) => {
    setLoading(true);
    setError(null);
    try {
      const next = await auditApi.listSessions({ username: exactUsername || undefined, cursor, limit: pageSize });
      setPage(next);
      if (resetHistory) setCursorHistory([]);
    } catch (cause) {
      setError(errorMessage(cause, 'Unable to load active sessions.'));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(appliedUsername, undefined, true); }, [appliedUsername, load]);

  const revokeAll = async () => {
    const exactUsername = username.trim();
    if (!exactUsername) return;
    setRevoking(true);
    setRevokeError(null);
    try {
      await auditApi.revokeAllSessions(exactUsername);
      await load(appliedUsername, undefined, true);
    } catch (cause) {
      if (isAppliedAuditFailure(cause)) {
        // The business mutation is durable. Reload authoritative state but
        // retain the terminal warning instead of offering a second revoke.
        await load(appliedUsername, undefined, true);
      }
      setRevokeError({
        message: errorMessage(cause, 'Unable to revoke sessions.'),
        // The backend has already applied the mutation. Retrying would create
        // another revoke-all audit decision rather than repairing its audit.
        retryable: !isAppliedAuditFailure(cause)
      });
    } finally {
      setRevoking(false);
    }
  };

  const next = () => {
    if (!page.nextCursor) return;
    setCursorHistory((history) => [...history, page.nextCursor!]);
    void load(appliedUsername, page.nextCursor);
  };
  const previous = () => {
    if (cursorHistory.length === 0) return;
    const previousHistory = cursorHistory.slice(0, -1);
    const cursor = previousHistory[previousHistory.length - 1];
    setCursorHistory(previousHistory);
    void load(appliedUsername, cursor);
  };

  return (
    <>
      <PageHeader
        title="Sessions"
        description="Review active and revoked dashboard sign-ins. Session credentials are never shown."
        actions={<RefreshButton onRefresh={() => void load(appliedUsername, undefined, true)} />}
      />
      <section className="table-shell audit-admin-shell">
        <div className="audit-filter-row">
          <Input value={username} placeholder="Exact username" aria-label="Exact username" onChange={(event) => setUsername(event.target.value)} />
          <Button
            type="button"
            variant="secondary"
            onClick={() => {
              const nextUsername = username.trim();
              if (nextUsername === appliedUsername) {
                void load(nextUsername, undefined, true);
              } else {
                setAppliedUsername(nextUsername);
              }
            }}
          >
            Filter
          </Button>
          <ConfirmDialog
            title="Revoke all sessions?"
            description={username.trim()
              ? `Revoke every active session for ${username.trim()}. The current session is included when it belongs to this user.`
              : 'Enter the exact username before revoking sessions.'}
            confirmLabel={revoking ? 'Revoking' : 'Revoke all'}
            onConfirm={() => { void revokeAll(); }}
          >
            <Button type="button" variant="destructive" disabled={!username.trim() || revoking}>
              <LogOut size={15} aria-hidden="true" /> Revoke all
            </Button>
          </ConfirmDialog>
        </div>
        {revokeError ? (
          <ErrorState
            message={revokeError.message}
            onRetry={revokeError.retryable ? () => void revokeAll() : undefined}
            retryLabel="Retry revoke"
          />
        ) : null}
        {loading ? <LoadingState label="Loading dashboard sessions" /> : null}
        {!loading && error ? <ErrorState message={error} onRetry={() => void load(appliedUsername, undefined, true)} retryLabel="Retry sessions" /> : null}
        {!loading && !error ? (
          <>
            <div className="table-scroll">
              <table>
                <thead><tr><th>User</th><th>Created</th><th>Expires</th><th>Last seen</th><th>Status</th></tr></thead>
                <tbody>
                  {page.items.map((session) => (
                    <tr key={session.sessionId}>
                      <td><div className="audit-actor"><ShieldCheck size={14} aria-hidden="true" /> {session.username}{session.current ? ' (current)' : ''}</div></td>
                      <td>{formatTime(session.createdAtMs)}</td>
                      <td>{formatTime(session.expiresAtMs)}</td>
                      <td>{formatTime(session.lastSeenAtMs)}</td>
                      <td>{sessionStatus(session)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {page.items.length === 0 ? <EmptyState title="No sessions match this filter" /> : null}
            <div className="table-footer">
              <span>{page.items.length} sessions</span>
              <div className="pagination">
                <Button type="button" variant="secondary" disabled={cursorHistory.length === 0} onClick={previous}>Previous</Button>
                <Button type="button" variant="secondary" disabled={!page.nextCursor} onClick={next}>Next</Button>
              </div>
            </div>
          </>
        ) : null}
      </section>
    </>
  );
}

function formatTime(value: number) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Unknown' : date.toLocaleString();
}

function sessionStatus(session: SessionListPage['items'][number]) {
  if (session.revokedAtMs) return `Revoked ${formatTime(session.revokedAtMs)}`;
  if (Date.now() >= session.expiresAtMs) return 'Expired';
  return 'Active';
}

function errorMessage(error: unknown, fallback: string) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_UNAVAILABLE') return `${error.message} Retry when storage is available.`;
  return error instanceof Error ? error.message : fallback;
}
