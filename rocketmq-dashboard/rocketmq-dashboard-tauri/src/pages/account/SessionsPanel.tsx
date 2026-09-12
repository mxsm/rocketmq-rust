import { useCallback } from 'react';
import { ArrowRight, Info, LogOut } from 'lucide-react';
import { AuthService } from '../../services/auth.service';
import { useReadResource } from '../../hooks/useReadResource';
import { useAppStore, useNavigationState } from '../../stores/app.store';
import { usePageRefresh } from '../../app/layout/pageToolbar';
import { Button } from '../../components/ui/LegacyButton';
import { PageState } from '../../components/layout/PageState';
import { useSessionRevoke } from './SessionRevokeProvider';
import { SessionTable } from './SessionTable';
import { sessionCursorHistory } from './sessionModel';
import './sessions.css';

function SessionsToolbar({ refresh, pending, refreshedAt, username }: { refresh: () => void; pending: boolean; refreshedAt: number | null; username: string }) {
    const revoke = useSessionRevoke();
    usePageRefresh({ refresh, pending, refreshedAt, actions: <Button variant="danger" icon={LogOut} disabled={revoke.pending} onClick={() => revoke.open(username)}>Sign out all sessions</Button> });
    return null;
}
function SessionList({ username }: { username: string }) {
    const app = useAppStore();
    const isPage = app.activeTab === 'Sessions';
    const revoke = useSessionRevoke();
    const [location, setLocation] = useNavigationState(`sessions:${username}`, () => ({ cursors: [undefined] as Array<string | undefined>, generation: 0 }));
    const cursor = location.cursors[location.cursors.length - 1];
    const load = useCallback(() => AuthService.listSessions(username, cursor), [username, cursor, location.generation]);
    const page = useReadResource(load, 'Account sessions could not be loaded.');
    const refresh = useCallback(() => setLocation(previous => ({ cursors: [undefined], generation: previous.generation + 1 })), [setLocation]);
    const next = page.data?.nextCursor ?? null;
    const busy = page.pending || revoke.pending;
    return <section className={`ops-sessions ${isPage ? 'ops-sessions-page' : ''}`} id="sessions" aria-label="Account sessions">
        {isPage ? <SessionsToolbar refresh={refresh} pending={busy} refreshedAt={page.receivedAt} username={username} /> :
            <><h2 className="ops-session-embedded-heading">Sessions</h2><div className="ops-session-inline-actions"><Button variant="outline" disabled={busy} onClick={refresh}>Refresh sessions</Button><Button variant="danger" icon={LogOut} disabled={revoke.pending} onClick={() => revoke.open(username)}>Sign out all sessions</Button></div></>}
        <div className="ops-session-note"><Info aria-hidden="true" size={22} /><p>Local dashboard sessions for <strong>{username}</strong>. Activity updates the last visit without extending expiry. Signing out all sessions includes this session.</p></div>
        {page.pending && <PageState kind="loading" title="Loading account sessions" />}
        {page.error && <PageState kind="error" title="Session query failed" description={page.error} action={<Button variant="outline" disabled={busy} onClick={() => { void page.read(); }}>Retry this page</Button>} />}
        {(page.data || location.cursors.length > 1) && <div className="ops-session-panel">
            {page.data && <div className="ops-session-table-scroll" role="region" tabIndex={0} aria-label="Session records" aria-busy={page.pending}><SessionTable items={page.data.items} username={username} /></div>}
            {!page.pending && !page.error && page.data?.items.length === 0 && <PageState kind="empty" title="No sessions on this page" description="Refresh to load the current session records." />}
            <nav className="ops-session-pagination" aria-label="Session pagination">
                <Button variant="outline" disabled={busy || location.cursors.length === 1} onClick={() => setLocation(previous => ({ ...previous, cursors: previous.cursors.slice(0, -1) }))}>Previous</Button>
                <span>Page {location.cursors.length}</span>
                <Button variant="outline" disabled={busy || Boolean(page.error) || !next || location.cursors.includes(next)} onClick={() => setLocation(previous => ({ ...previous, cursors: sessionCursorHistory(previous.cursors, next) }))}>Next</Button>
                {page.data && <span className="ops-session-count">{page.data.items.length} {page.data.items.length === 1 ? 'record' : 'records'} on this page</span>}
            </nav>
        </div>}
        {isPage && <div className="ops-session-account"><div><h2>Account</h2><p>Manage your account password and security settings.</p></div>
            <Button variant="ghost" icon={ArrowRight} onClick={() => app.setActiveTab('Account')}>Manage account</Button></div>}
    </section>;
}
export function SessionsPanel({ username }: { username: string }) {
    return <SessionList key={username} username={username} />;
}
