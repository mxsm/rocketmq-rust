import React, { useEffect, useRef, useState } from 'react';
import { MonitorService, type MonitorRule, type SaveMonitorRule } from '../../services/monitor.service';
import { dashboardErrorMessage, isDashboardClientError } from '../../services/invoke';

const emptyDraft = (): SaveMonitorRule => ({ consumerGroup: '', minCount: 0, maxDiffTotal: 0, expectedRevision: 0 });

export const MonitorPage = () => {
    const [rules, setRules] = useState<MonitorRule[]>([]);
    const [filter, setFilter] = useState('');
    const [draft, setDraft] = useState<SaveMonitorRule | null>(null);
    const [deleting, setDeleting] = useState<MonitorRule | null>(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState('');
    const [notice, setNotice] = useState('');
    const alive = useRef(true);
    const generation = useRef(0);
    const refresh = async () => {
        const request = ++generation.current;
        setBusy(true);
        setError('');
        try {
            const current = await MonitorService.list();
            if (alive.current && request === generation.current) setRules(current);
        } catch (failure) {
            if (alive.current && request === generation.current) setError(dashboardErrorMessage(failure, 'Rules could not be loaded.'));
        } finally {
            if (alive.current && request === generation.current) setBusy(false);
        }
    };
    useEffect(() => { alive.current = true; void refresh(); return () => { alive.current = false; generation.current++; }; }, []);

    const mutate = async (operation: () => Promise<unknown>) => {
        setBusy(true);
        setError('');
        setNotice('');
        try {
            await operation();
            if (!alive.current) return;
            setDraft(null);
            setDeleting(null);
            setNotice('Rule change saved.');
            await refresh();
        } catch (failure) {
            if (!alive.current) return;
            if (isDashboardClientError(failure) && failure.code === 'dashboard.monitor_conflict') {
                // Refresh the table, retaining the draft and its old revision until explicit review.
                await refresh();
                if (!alive.current) return;
                setDeleting(null);
                setNotice('The rule changed. Your draft is retained. Choose Edit on the current row to load its version, or New rule if it was deleted.');
            }
            setError(dashboardErrorMessage(failure, 'The rule change failed.'));
        } finally {
            if (alive.current) setBusy(false);
        }
    };
    const valid = draft && draft.consumerGroup.length > 0 && draft.consumerGroup.length <= 255 && !/\s/.test(draft.consumerGroup) &&
        Number.isSafeInteger(draft.minCount) && draft.minCount >= 0 && Number.isSafeInteger(draft.maxDiffTotal) && draft.maxDiffTotal >= 0;
    const inputClass = 'rounded border bg-transparent px-3 py-2';
    return <section className="p-6 space-y-4">
        <h2 className="text-xl font-semibold">Consumer Monitor rules</h2>
        <p className="text-sm text-gray-500">Rules belong to the selected NameServer environment. This page manages thresholds; it does not evaluate alerts or send notifications.</p>
        <div className="flex gap-3 items-center">
            <input className={inputClass} aria-label="Filter Consumer group" placeholder="Filter Consumer group" value={filter} onChange={event => setFilter(event.target.value)} />
            <button disabled={busy} onClick={() => void refresh()}>Refresh</button>
            <button disabled={busy} onClick={() => { setDraft(emptyDraft()); setDeleting(null); setNotice(''); }}>New rule</button>
        </div>
        {busy && <p role="status">Loading…</p>}
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {notice && <p role="status" className="text-amber-600">{notice}</p>}
        <table className="w-full text-left text-sm"><thead><tr><th>Consumer group</th><th>Min count</th><th>Max total lag</th><th>Revision</th><th>Updated</th><th>Actions</th></tr></thead>
            <tbody>{rules.filter(rule => rule.consumerGroup.toLowerCase().includes(filter.toLowerCase())).map(rule => <tr key={rule.consumerGroup} className="border-t">
                <td className="py-3">{rule.consumerGroup}</td><td>{rule.minCount}</td><td>{rule.maxDiffTotal}</td><td>{rule.revision}</td><td>{new Date(rule.updatedAtMs).toLocaleString()}</td>
                <td className="space-x-3"><button disabled={busy} onClick={() => { setDraft({ consumerGroup: rule.consumerGroup, minCount: rule.minCount, maxDiffTotal: rule.maxDiffTotal, expectedRevision: rule.revision }); setDeleting(null); setNotice('Current version loaded for review.'); }}>Edit</button><button disabled={busy} onClick={() => { setDeleting(rule); setDraft(null); }}>Delete</button></td>
            </tr>)}</tbody>
        </table>
        {!busy && !error && rules.length === 0 && <p>No rules in this environment.</p>}
        {draft && <form className="rounded border p-4 space-y-3" onSubmit={event => { event.preventDefault(); if (valid && !busy) void mutate(() => MonitorService.save(draft)); }}>
            <h3 className="font-semibold">{draft.expectedRevision === 0 ? 'New rule' : `Edit revision ${draft.expectedRevision}`}</h3>
            <label className="flex gap-3 items-center">Consumer group<input required maxLength={255} disabled={busy || draft.expectedRevision !== 0} className={inputClass} value={draft.consumerGroup} onChange={event => setDraft({ ...draft, consumerGroup: event.target.value })} /></label>
            <label className="flex gap-3 items-center">Min count<input required disabled={busy} className={inputClass} type="number" min="0" step="1" value={Number.isNaN(draft.minCount) ? '' : draft.minCount} onChange={event => setDraft({ ...draft, minCount: event.target.valueAsNumber })} /></label>
            <label className="flex gap-3 items-center">Max total lag<input required disabled={busy} className={inputClass} type="number" min="0" step="1" value={Number.isNaN(draft.maxDiffTotal) ? '' : draft.maxDiffTotal} onChange={event => setDraft({ ...draft, maxDiffTotal: event.target.valueAsNumber })} /></label>
            <div className="flex gap-3"><button type="submit" disabled={busy || !valid}>Save rule</button><button type="button" disabled={busy} onClick={() => setDraft(null)}>Cancel</button></div>
        </form>}
        {deleting && <div role="alertdialog" aria-label="Confirm rule deletion" className="rounded border p-4 space-y-3"><p>Delete {deleting.consumerGroup}, revision {deleting.revision}, from this environment?</p><div className="flex gap-3"><button disabled={busy} onClick={() => void mutate(() => MonitorService.delete(deleting))}>Confirm delete</button><button disabled={busy} onClick={() => setDeleting(null)}>Cancel</button></div></div>}
    </section>;
};
