import { useEffect, useRef, useState } from 'react';
import { AuthService, type SessionPage } from '../../services/auth.service';
import { dashboardErrorMessage } from '../../services/invoke';
import { Button } from '../../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { SignOutConfirmDialog } from '../../features/auth';

const timestamp = (value: number) => new Date(value).toLocaleString();

export const SessionsPanel = ({ username }: { username: string }) => {
    const [page, setPage] = useState<SessionPage | null>(null);
    const [cursors, setCursors] = useState<Array<string | undefined>>([undefined]);
    const [refresh, setRefresh] = useState(0);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [confirm, setConfirm] = useState(false);
    const [revoking, setRevoking] = useState(false);
    const mounted = useRef(false);
    const cursor = cursors[cursors.length - 1];

    useEffect(() => {
        mounted.current = true;
        return () => { mounted.current = false; };
    }, []);

    useEffect(() => {
        let active = true;
        setLoading(true);
        setError('');
        setPage(null);
        void AuthService.listSessions(username, cursor).then((result) => {
            if (active) setPage(result);
        }).catch((failure: unknown) => {
            if (active) setError(dashboardErrorMessage(failure, 'Could not load sessions.'));
        }).finally(() => { if (active) setLoading(false); });
        return () => { active = false; };
    }, [username, cursor, refresh]);

    const revoke = async () => {
        setRevoking(true);
        setError('');
        try {
            await AuthService.revokeUserSessions(username);
        } catch (failure) {
            if (mounted.current) setError(dashboardErrorMessage(failure, 'Could not revoke sessions.'));
        } finally {
            if (mounted.current) { setRevoking(false); setConfirm(false); }
        }
    };

    return (
        <Card className="ops-card account-card" id="sessions">
            <CardHeader>
                <CardTitle>Sessions</CardTitle>
                <CardDescription>Sessions for {username}. Activity updates the last visit, without extending expiry.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 !pb-8">
                <div className="flex flex-wrap gap-3">
                    <Button variant="outline" disabled={loading || revoking} onClick={() => { setCursors([undefined]); setRefresh((value) => value + 1); }}>Refresh sessions</Button>
                    <Button variant="outline" className="ops-button ops-button-danger" disabled={revoking} onClick={() => setConfirm(true)}>Sign out all sessions</Button>
                </div>
                {error && <p role="alert" className="text-sm text-red-600">{error}</p>}
                {loading ? <p role="status">Loading sessions¡­</p> : page && (
                    <>
                        <div className="overflow-x-auto">
                            <table className="w-full text-left text-sm">
                                <caption className="sr-only">Local account sessions for {username}</caption>
                                <thead><tr>{['Session', 'Created', 'Expires', 'Last visit', 'Status'].map((label) => <th key={label} scope="col" className="p-3">{label}</th>)}</tr></thead>
                                <tbody>{page.items.map((session) => (
                                    <tr key={session.id} className="border-t border-gray-200 dark:border-gray-700">
                                        <td className="p-3"><span className="font-mono" title={session.id}>{session.id.slice(0, 8)}</span>{session.current && <span className="ml-2 text-sky-600">Current</span>}</td>
                                        <td className="p-3 whitespace-nowrap">{timestamp(session.createdAtMs)}</td>
                                        <td className="p-3 whitespace-nowrap">{timestamp(session.expiresAtMs)}</td>
                                        <td className="p-3 whitespace-nowrap">{timestamp(session.lastSeenAtMs)}</td>
                                        <td className="p-3">{session.revokedAtMs !== null ? `Revoked ${timestamp(session.revokedAtMs)}` : session.expiresAtMs <= Date.now() ? 'Expired' : 'Active'}</td>
                                    </tr>
                                ))}</tbody>
                            </table>
                            {page.items.length === 0 && <p className="p-3">No sessions found.</p>}
                        </div>
                        <div className="flex items-center gap-3">
                            <Button variant="outline" disabled={cursors.length === 1} onClick={() => setCursors((values) => values.slice(0, -1))}>Previous</Button>
                            <span>Page {cursors.length}</span>
                            <Button variant="outline" disabled={!page.nextCursor} onClick={() => { if (page.nextCursor) setCursors((values) => [...values, page.nextCursor!]); }}>Next</Button>
                        </div>
                    </>
                )}
                <SignOutConfirmDialog open={confirm} isSubmitting={revoking} title="Sign out all sessions?"
                    description={`All sessions for ${username}, including this one, will be revoked. Sign in again to continue.`}
                    onCancel={() => { if (!revoking) setConfirm(false); }} onConfirm={() => void revoke()} />
            </CardContent>
        </Card>
    );
};
