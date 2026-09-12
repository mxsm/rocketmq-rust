import { useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { Plus, Info } from 'lucide-react';
import { ConnectionStore } from '../../services/connection.store';
import { getConnectionSettings } from '../../services/connection.service';
import { useReadResource } from '../../hooks/useReadResource';
import { useNavigationState } from '../../stores/app.store';
import { usePageRefresh } from '../../app/layout/pageToolbar';
import { Button } from '../../components/ui/LegacyButton';
import { Input } from '../../components/ui/LegacyInput';
import { PageState } from '../../components/layout/PageState';
import { useMonitorActions } from './MonitorActionProvider';
import { useMonitorRules } from './useMonitorRules';
import { MonitorEditor, monitorNotice } from './MonitorEditor';
import { MonitorResult } from './MonitorResult';
import { MonitorDeleteDialog } from './MonitorDeleteDialog';
import { applyMonitorReceipt, monitorContextMatches, monitorDraft, monitorSaveRequest, type MonitorContext, type MonitorDraft, type MonitorTarget } from './monitorModel';
import './monitor.css';

function MonitorWorkspace({ context }: { context: MonitorContext }) {
    const rules = useMonitorRules(context);
    const actions = useMonitorActions();
    const [filter, setFilter] = useNavigationState('monitorFilter', '');
    // Revision-scoped drafts cannot be restored under a different connection configuration.
    const [draft, setDraft] = useNavigationState<MonitorDraft | null>(`monitorDraft:${context.environmentId}:${context.revision}`, null);
    const [deleting, setDeleting] = useState<MonitorTarget | null>(null);
    const [error, setError] = useState('');
    const appliedReceipt = useRef<string | null>(null);
    const alive = useRef(true);
    const newButton = useRef<HTMLButtonElement>(null);
    useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
    useEffect(() => {
        const receipt = actions.receipt;
        if (!receipt || appliedReceipt.current === receipt.target.id || !monitorContextMatches(context, receipt.target.context)) return;
        appliedReceipt.current = receipt.target.id;
        setDraft(previous => applyMonitorReceipt(previous, receipt, context));
        // Invalidate the read started before the write; never reuse its stale in-flight result.
        void rules.afterWrite();
    }, [actions.receipt, context.environmentId, context.revision, rules.afterWrite]);
    usePageRefresh({ refresh: rules.refresh, pending: rules.pending || Boolean(actions.pending), refreshedAt: rules.receivedAt });
    const busy = Boolean(actions.pending);
    const current = rules.data?.find(rule => rule.consumerGroup === draft?.consumerGroup) ?? null;
    const filtered = (rules.data ?? []).filter(rule => rule.consumerGroup.toLowerCase().includes(filter.toLowerCase()));
    const closeEditor = () => { setDraft(null); setError(''); newButton.current?.focus(); };
    const submit = async (target: MonitorTarget) => {
        if (busy || !rules.ready || !monitorContextMatches(context, ConnectionStore.getSnapshot())) return;
        setError('');
        try {
            setDraft(previous => previous?.id === target.draftId ? { ...previous, lastAttemptId: target.id } : previous);
            await actions.submit(target);
        } catch (failure) {
            if (alive.current) setError(failure instanceof Error ? failure.message : 'The rule operation could not be started.');
        } finally {
            if (alive.current) setDeleting(null);
        }
    };
    const save = () => {
        if (!draft) return;
        try {
            const request = monitorSaveRequest(draft);
            void submit({ kind: 'save', id: crypto.randomUUID(), draftId: draft.id, context, request });
        } catch (failure) { setError(failure instanceof Error ? failure.message : 'Check the rule values.'); }
    };
    const deletionCurrent = deleting && rules.ready && monitorContextMatches(deleting.context, ConnectionStore.getSnapshot()) &&
        rules.data?.some(rule => rule.consumerGroup === deleting.request.consumerGroup && rule.revision === deleting.request.expectedRevision);
    return <div className="ops-monitors">
        <section className="ops-monitor-controls" aria-label="Monitor rule filters">
            <div className="ops-monitor-environment"><small>Environment</small><strong tabIndex={0}>{context.environmentId}</strong></div>
            <Input aria-label="Search consumer group" type="search" placeholder="Search consumer group…" value={filter} onChange={event => setFilter(event.target.value)} />
            <Button ref={newButton} icon={Plus} disabled={!rules.ready || busy || Boolean(draft)} onClick={() => { setDraft(monitorDraft(null)); setError(''); }}>New rule</Button>
        </section>
        {actions.pending && <PageState kind="loading" title="Applying monitor rule change" description={`Target: ${actions.pending.request.consumerGroup} · Environment: ${actions.pending.context.environmentId}. The request will not retry automatically.`} />}
        {error && <PageState kind="error" title="Rule change could not start" description={error} />}
        {rules.pending && !rules.data && <PageState kind="loading" title="Loading monitor rules" />}
        {rules.error && <PageState kind="error" title={rules.data ? 'Rule refresh failed; previous observation retained' : 'Rules could not be loaded'} description={rules.error}
            action={<Button variant="outline" disabled={busy || rules.pending} onClick={() => { void rules.refresh(); }}>Retry rule list</Button>} />}
        {rules.data && <div className="ops-monitor-table-wrap" tabIndex={0} role="region" aria-label="Consumer monitor rules" aria-busy={rules.pending}>
            <table className="ops-monitor-table"><thead><tr><th scope="col">Consumer group</th><th scope="col">Minimum online clients</th><th scope="col">Maximum lag</th><th scope="col">Revision</th><th scope="col">Updated</th><th scope="col">Actions</th></tr></thead>
                <tbody>{filtered.map(rule => <tr key={rule.consumerGroup} data-selected={draft?.consumerGroup === rule.consumerGroup}>
                    <td><div className="ops-monitor-group" tabIndex={0}>{rule.consumerGroup}</div></td><td>{rule.minCount}</td><td>{rule.maxDiffTotal}</td><td>{rule.revision}</td>
                    <td>{Number.isFinite(rule.updatedAtMs) && !Number.isNaN(new Date(rule.updatedAtMs).getTime()) ? <time dateTime={new Date(rule.updatedAtMs).toISOString()}>{new Date(rule.updatedAtMs).toLocaleString()}</time> : 'Unknown'}</td>
                    <td><Button variant="outline" disabled={!rules.ready || busy || Boolean(draft)} aria-label={`Edit ${rule.consumerGroup}`} onClick={() => { setDraft(monitorDraft(rule)); setError(''); }}>Edit</Button></td>
                </tr>)}</tbody>
            </table>
        </div>}
        {rules.ready && filtered.length === 0 && <PageState kind="empty" title={rules.data?.length ? 'No matching consumer groups' : 'No rules in this environment'} description={rules.data?.length ? 'Change the search to see other rules.' : 'Create a rule to store thresholds for a consumer group.'} />}
        {draft ? <MonitorEditor draft={draft} current={current} ready={rules.ready} pending={busy} onChange={setDraft} onSave={save} onCancel={closeEditor}
            onDelete={() => setDeleting({ kind: 'delete', id: crypto.randomUUID(), draftId: draft.id, context: { ...context },
                request: { consumerGroup: draft.consumerGroup, expectedRevision: draft.expectedRevision } })} /> :
            <p className="ops-monitor-notice"><Info aria-hidden="true" size={18} />{monitorNotice}</p>}
        {actions.receipt && <MonitorResult receipt={actions.receipt} context={context} />}
        {deleting && <MonitorDeleteDialog target={deleting} current={Boolean(deletionCurrent)} pending={busy} close={() => setDeleting(null)} submit={() => submit(deleting)} />}
    </div>;
}

export function MonitorPage() {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const bootstrap = useReadResource(settings ? null : getConnectionSettings, 'Connection settings could not be loaded.');
    if (!settings) return bootstrap.error
        ? <PageState kind="error" title="Environment unavailable" description={bootstrap.error} action={<Button variant="outline" onClick={() => { void bootstrap.read(); }}>Retry connection</Button>} />
        : <PageState kind="loading" title="Loading environment" />;
    if (!settings.environmentId) return <PageState kind="empty" title="Select a NameServer environment" description="Monitor rules are stored separately for each environment. Select one using the environment toolbar." />;
    return <MonitorWorkspace key={`${settings.environmentId}:${settings.revision}`} context={{ environmentId: settings.environmentId, revision: settings.revision }} />;
}
