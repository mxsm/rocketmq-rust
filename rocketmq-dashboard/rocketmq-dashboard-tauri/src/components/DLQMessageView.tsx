import { useCallback, useEffect, useId, useMemo, useState, useSyncExternalStore } from 'react';
import { Download, Send } from 'lucide-react';
import { ConnectionStore } from '../services/connection.store';
import { useNavigationState } from '../stores/app.store';
import { useConsumerCatalog } from '../features/consumer/hooks/useConsumerCatalog';
import { buildDlqQuery, createDlqQueryController, dlqGroup, type DlqQueryDraft } from '../features/dlq/dlqQuery';
import { messageLookup, messageTimestamp, visibleMessageText } from '../features/message/messageModel';
import { useDlqActions } from '../features/dlq/dlqActionContext';
import { failedDlqSelection } from '../features/dlq/receipts';
import { DlqReceiptPanel } from '../features/dlq/components/DlqReceiptPanel';
import { DlqMessageDialog } from '../features/dlq/components/DlqMessageDialog';
import type { DlqMessageSummary } from '../features/dlq/types/dlq.types';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Pagination } from './Pagination';
import '../features/message/message.css';
import '../features/dlq/dlq.css';

const localTime = (timestamp: number) => new Date(timestamp - new Date(timestamp).getTimezoneOffset() * 60_000).toISOString().slice(0, 19);
const initialDraft = (): DlqQueryDraft => ({ mode: 'key', consumerGroup: '', key: '', messageId: '', begin: localTime(Date.now() - 3 * 3_600_000), end: localTime(Date.now()) });
const modes = [['key', 'By Key'], ['id', 'By ID'], ['time', 'By Time']] as const;
const rowId = (message: DlqMessageSummary) => messageLookup(message).messageId;

export function DLQMessageView() {
    const [draft, storeDraft] = useNavigationState<DlqQueryDraft>('dlqQuery', initialDraft);
    const [clientId, setClientId] = useNavigationState<string>('dlqClient', () => '');
    const [selected, setSelected] = useState<Set<string>>(new Set());
    const [detail, setDetail] = useState<DlqMessageSummary | null>(null);
    const [localPage, setLocalPage] = useState(1);
    const [validation, setValidation] = useState('');
    const [reviewNote, setReviewNote] = useState('');
    const [hasSearched, setHasSearched] = useState(false);
    const catalog = useConsumerCatalog({ mode: 'name_server' });
    const groupsId = useId();
    const actions = useDlqActions();
    const context = useMemo(() => {
        const settings = ConnectionStore.getSnapshot();
        return { revision: settings?.revision ?? 0, environmentId: settings?.environmentId ?? null };
    }, []);
    const controller = useMemo(() => {
        return createDlqQueryController(() => {
            const current = ConnectionStore.getSnapshot();
            return current?.revision === context.revision && current?.environmentId === context.environmentId;
        });
    }, [context]);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useEffect(() => { controller.start(); return controller.stop; }, [controller]);
    useEffect(() => { setSelected(new Set()); setReviewNote(''); }, [state.result, actions.receipt]);
    const update = (patch: Partial<DlqQueryDraft>) => {
        controller.reset(); setSelected(new Set()); setDetail(null); setLocalPage(1); setValidation(''); setReviewNote(''); setHasSearched(false);
        storeDraft({ ...draft, ...patch });
    };
    const query = useCallback(() => {
        try {
            const request = buildDlqQuery(draft);
            setValidation(''); setHasSearched(true); setLocalPage(1); setDetail(null); setSelected(new Set());
            void controller.read(request, 1, true);
        } catch (error) { setValidation((error as Error).message); }
    }, [controller, draft]);
    const refresh = useCallback(() => { void catalog.refresh(); if (hasSearched) query(); }, [catalog.refresh, hasSearched, query]);
    usePageRefresh({ refresh, pending: catalog.pending || state.pending, refreshedAt: state.receivedAt });
    const result = state.result;
    const group = result ? dlqGroup(result.query.topic) : '';
    const timeMode = result?.query.mode === 'time';
    const pageCount = timeMode ? result.totalPages : Math.ceil((result?.items.length ?? 0) / 12);
    const page = timeMode ? result.page : Math.min(localPage, Math.max(1, pageCount));
    const items = result ? timeMode ? result.items : result.items.slice((page - 1) * 12, page * 12) : [];
    const rows = [...new Map(items.map(message => [rowId(message), message])).values()];
    const selectedRows = rows.filter(message => selected.has(rowId(message)));
    const blocked = state.pending || Boolean(state.error);
    const failed = actions.receipt ? failedDlqSelection(actions.receipt, ConnectionStore.getSnapshot()?.environmentId ?? null, group, rows) : new Set<string>();
    const allSelected = rows.length > 0 && selectedRows.length === rows.length;
    return <div className="ops-dlq">
        <section className="ops-dlq-query" aria-label="Dead-letter query"><form onSubmit={event => { event.preventDefault(); query(); }}>
            <Input label="Consumer group" list={groupsId} placeholder="Choose or enter a group" value={draft.consumerGroup} onChange={event => update({ consumerGroup: event.target.value })} required />
            <datalist id={groupsId}>{catalog.data?.items.filter(item => item.category !== 'SYSTEM').map(item => <option key={item.rawGroupName} value={item.rawGroupName} />)}</datalist>
            <div className="ops-dlq-mode"><span>Query mode</span><div className="ops-message-modes" role="group" aria-label="DLQ query mode">{modes.map(([mode, label]) => <Button key={mode} variant={draft.mode === mode ? 'primary' : 'ghost'} aria-pressed={draft.mode === mode} onClick={() => { if (mode !== draft.mode) update({ mode }); }}>{label}</Button>)}</div></div>
            {draft.mode === 'key' && <Input label="Key" value={draft.key} onChange={event => update({ key: event.target.value })} required />}
            {draft.mode === 'id' && <Input label="Message ID" value={draft.messageId} onChange={event => update({ messageId: event.target.value })} required />}
            {draft.mode === 'time' && <><Input label="Begin" type="datetime-local" step={1} value={draft.begin} onChange={event => update({ begin: event.target.value })} required />
                <Input label="End" type="datetime-local" step={1} value={draft.end} onChange={event => update({ end: event.target.value })} required /></>}
            <Input label="Client ID (optional)" placeholder="Selected by Broker" value={clientId} onChange={event => setClientId(event.target.value)} />
            <Button type="submit" disabled={state.pending}>{state.pending ? 'Querying…' : 'Query'}</Button>
        </form>
            <p className="ops-message-note">{draft.mode === 'time' ? `Local time: ${Intl.DateTimeFormat().resolvedOptions().timeZone}. Refresh starts a new scan.` : draft.mode === 'key' ? 'Key lookup returns up to 64 indexed messages.' : 'Query a DLQ physical or unique message ID.'} Client ID applies only to resend.</p>
            {catalog.error && <p className="ops-message-note">Consumer suggestions are unavailable. Manual group input remains available.</p>}
            {validation && <PageState kind="error" title="Check query conditions" description={validation} />}
        </section>
        <PageSection title={result ? `${result.total.toLocaleString()} messages found` : 'Dead-letter messages'} description={result ? `Consumer group: ${group} · Last successful query: ${new Date(state.receivedAt!).toLocaleString()}` : 'Query a Consumer group to inspect its dead-letter queue.'}>
            {state.pending && <PageState kind="loading" title="Reading dead-letter messages" />}
            {state.error && <PageState kind="error" title="DLQ query failed" description={state.error + (result ? ' Showing the last successful result; actions are disabled.' : '')} />}
            {!hasSearched && <PageState kind="empty" title="No query yet" />}
            {result && <>
                <div className="ops-message-scroll" role="region" aria-label="Dead-letter results" tabIndex={0}><table><thead><tr>
                    <th scope="col" className="ops-dlq-check"><input type="checkbox" aria-label="Select all messages on this page" checked={allSelected} disabled={blocked || !rows.length} onChange={() => { setReviewNote(''); setSelected(allSelected ? new Set() : new Set(rows.map(rowId))); }} /></th>
                    {['DLQ request ID', 'DLQ Topic', 'Store time', 'Actions'].map(label => <th scope="col" key={label}>{label}</th>)}
                </tr></thead><tbody>{rows.map(message => <tr key={rowId(message)} data-selected={selected.has(rowId(message))}>
                    <td className="ops-dlq-check"><input type="checkbox" aria-label={'Select message ' + rowId(message)} checked={selected.has(rowId(message))} disabled={blocked} onChange={() => { setReviewNote(''); setSelected(current => { const next = new Set(current); if (next.has(rowId(message))) next.delete(rowId(message)); else next.add(rowId(message)); return next; }); }} /></td>
                    <th scope="row"><button type="button" className="ops-message-choice" disabled={blocked} onClick={() => setDetail(message)} title={rowId(message)}>{visibleMessageText(rowId(message))}</button></th>
                    <td>{visibleMessageText(message.topic)}</td>
                    <td>{messageTimestamp(message.storeTimestamp)}</td><td><div className="ops-dlq-row-actions"><Button variant="outline" disabled={blocked} onClick={() => setDetail(message)}>Detail</Button>
                        <Button variant="outline" disabled={blocked} onClick={() => actions.open({ action: 'resend', group, messages: [message], clientId, context })}>Resend</Button>
                        <Button variant="outline" disabled={blocked} onClick={() => actions.open({ action: 'export', group, messages: [message], clientId: '', context })}>Export</Button></div></td>
                </tr>)}</tbody></table></div>
                {!rows.length && <PageState kind="empty" title="No matching dead-letter messages" />}
                {pageCount > 1 && <Pagination currentPage={page} totalPages={pageCount} disabled={state.pending} onPageChange={value => { setSelected(new Set()); setDetail(null); if (timeMode) void controller.read(result.query, value); else setLocalPage(value); }} />}
                <div className="ops-dlq-selection"><span>{selectedRows.length} selected</span><div className="ops-message-actions"><Button icon={Send} disabled={blocked || !selectedRows.length} onClick={() => actions.open({ action: 'resend', group, messages: selectedRows, clientId, context })}>Resend selected</Button>
                    <Button variant="outline" icon={Download} disabled={blocked || !selectedRows.length} onClick={() => actions.open({ action: 'export', group, messages: selectedRows, clientId: '', context })}>Export CSV</Button></div></div>
                {reviewNote && <p role="status" className="ops-message-note">{reviewNote}</p>}
            </>}
        </PageSection>
        <DlqReceiptPanel receipt={actions.receipt} canReview={!blocked && failed.size > 0} review={() => { setSelected(failed); setReviewNote(`${failed.size} failed targets selected. Review the targets and confirm before resending.`); }} />
        {detail && <DlqMessageDialog message={detail} group={group} close={() => setDetail(null)} />}
    </div>;
}
