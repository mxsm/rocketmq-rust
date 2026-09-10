import { useEffect, useState, type FormEvent } from 'react';
import { queryAuditEvents, type AuditPage as AuditResultPage, type AuditQuery, type AuditOutcome } from '../../services/audit.service';
import { dashboardErrorMessage } from '../../services/invoke';
import { Button } from '../../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';

export const AuditPage = () => {
    const [query, setQuery] = useState<AuditQuery>({ limit: 50 });
    const [cursors, setCursors] = useState<Array<string | undefined>>([undefined]);
    const [refresh, setRefresh] = useState(0);
    const [page, setPage] = useState<AuditResultPage | null>(null);
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(true);
    const cursor = cursors[cursors.length - 1];
    useEffect(() => {
        let active = true;
        setLoading(true); setError(''); setPage(null);
        void queryAuditEvents({ ...query, cursor }).then((result) => { if (active) setPage(result); })
            .catch((failure: unknown) => { if (active) setError(dashboardErrorMessage(failure, 'Could not load audit events.')); })
            .finally(() => { if (active) setLoading(false); });
        return () => { active = false; };
    }, [query, cursor, refresh]);

    const apply = (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const data = new FormData(event.currentTarget);
        const text = (key: string) => String(data.get(key) ?? '').trim() || undefined;
        const from = text('from'); const to = text('to');
        const fromMs = from ? new Date(from).getTime() : undefined;
        const toMs = to ? new Date(to).getTime() : undefined;
        if ((fromMs !== undefined && !Number.isFinite(fromMs)) || (toMs !== undefined && !Number.isFinite(toMs)) || (fromMs !== undefined && toMs !== undefined && fromMs > toMs)) {
            setError('Choose a valid time range.'); return;
        }
        setCursors([undefined]);
        setQuery({ fromMs, toMs, actor: text('actor'), action: text('action'), outcome: text('outcome') as AuditOutcome | undefined, environmentId: text('environmentId'), limit: 50 });
    };
    const inputClass = 'mt-1 w-full rounded-lg border border-gray-300 bg-transparent px-3 py-2 dark:border-gray-600';
    return <Card className="ops-card">
        <CardHeader><CardTitle>Audit events</CardTitle><CardDescription>Terminal outcomes of local and RocketMQ administration. Unknown means the remote result could not be confirmed; review the actual resource before resubmitting.</CardDescription></CardHeader>
        <CardContent className="space-y-5">
            <form onSubmit={apply} className="grid gap-3 md:grid-cols-3">
                <label className="text-sm">From<input name="from" type="datetime-local" className={inputClass} /></label>
                <label className="text-sm">To<input name="to" type="datetime-local" className={inputClass} /></label>
                <label className="text-sm">Actor<input name="actor" placeholder="Exact username" className={inputClass} /></label>
                <label className="text-sm">Action<input name="action" placeholder="e.g. topic.delete" className={inputClass} /></label>
                <label className="text-sm">Outcome<select name="outcome" className={inputClass}><option value="">All outcomes</option>{['success', 'rejected', 'failed', 'partial', 'unknown'].map((outcome) => <option key={outcome}>{outcome}</option>)}</select></label>
                <label className="text-sm">Environment ID<input name="environmentId" placeholder="Exact ID, if recorded" className={inputClass} /></label>
                <div className="flex gap-3"><Button type="submit" disabled={loading}>Apply filters</Button><Button type="button" variant="outline" disabled={loading} onClick={() => { setCursors([undefined]); setRefresh((value) => value + 1); }}>Refresh</Button></div>
            </form>
            {error && <p role="alert" className="text-red-600">{error}</p>}
            {loading ? <p role="status">Loading audit events¡­</p> : page && <>
                <div className="overflow-x-auto"><table className="w-full text-left text-sm">
                    <caption className="sr-only">Filtered audit events, newest first</caption>
                    <thead><tr>{['Time', 'Actor', 'Action', 'Resource', 'Environment', 'Outcome', 'Details'].map((label) => <th scope="col" className="p-3" key={label}>{label}</th>)}</tr></thead>
                    <tbody>{page.items.map((event) => <tr key={event.eventId} className="border-t border-gray-200 dark:border-gray-700">
                        <td className="p-3 whitespace-nowrap">{new Date(event.createdAtMs).toLocaleString()}</td><td className="p-3">{event.actor ?? 'Unauthenticated'}</td><td className="p-3 font-mono">{event.action}</td>
                        <td className="p-3">{event.resourceType}{event.resourceName && `: ${event.resourceName}`}</td><td className="p-3">{event.environmentId ?? 'Not recorded'}</td><td className="p-3 font-medium">{event.outcome}</td>
                        <td className="p-3"><details><summary className="cursor-pointer">View receipt</summary><p className="mt-2 break-all font-mono text-xs">Request: {event.requestId}</p>{event.detail.resultUnknown && <p className="text-amber-700">Remote outcome unknown. No automatic retry was performed.</p>}{event.detail.errorCode && <p>{event.detail.errorCode}</p>}{event.detail.successCount !== undefined && <p>Succeeded: {event.detail.successCount}</p>}{event.detail.failureCount !== undefined && <p>Failed: {event.detail.failureCount}</p>}</details></td>
                    </tr>)}</tbody>
                </table>{page.items.length === 0 && <p className="p-3">No matching audit events.</p>}</div>
                <div className="flex items-center gap-3"><Button variant="outline" disabled={cursors.length === 1} onClick={() => setCursors((values) => values.slice(0, -1))}>Previous</Button><span>Page {cursors.length}</span><Button variant="outline" disabled={!page.nextCursor} onClick={() => { const next = page.nextCursor; if (next) setCursors((values) => [...values, next]); }}>Next</Button></div>
            </>}
        </CardContent>
    </Card>;
};
