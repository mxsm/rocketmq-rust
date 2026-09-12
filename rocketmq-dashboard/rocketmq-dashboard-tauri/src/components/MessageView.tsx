import { useCallback, useEffect, useId, useMemo, useState, useSyncExternalStore } from 'react';
import { Copy, Search } from 'lucide-react';
import { ConnectionStore } from '../services/connection.store';
import { MessageService } from '../services/message.service';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import { buildMessageQuery, createMessageQueryController, type MessageQueryDraft } from '../features/message/messageQuery';
import { messageIdentity, messageTimestamp, visibleMessageText } from '../features/message/messageModel';
import { MessageInspector, copyMessageValue } from '../features/message/components/MessageInspector';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Pagination } from './Pagination';
import '../features/message/message.css';

const localTime = (timestamp: number) => {
    const date = new Date(timestamp);
    return new Date(timestamp - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 19);
};
const initialDraft = (): MessageQueryDraft => ({ mode: 'key', topic: '', key: '', messageId: '', begin: localTime(Date.now() - 3_600_000), end: localTime(Date.now()) });
const modes = [['key', 'By Key'], ['id', 'By ID'], ['time', 'By Time']] as const;

export function MessageView() {
    const { navigation } = useAppStore();
    const target = navigation.target?.kind === 'message' ? navigation.target : null;
    const [draft, storeDraft] = useNavigationState<MessageQueryDraft>('messageQuery', () => target
        ? { ...initialDraft(), mode: 'id', topic: target.topic, messageId: target.name } : initialDraft());
    const [selection, setSelection] = useState<string | null>(null);
    const [localPage, setLocalPage] = useState(1);
    const [validation, setValidation] = useState('');
    const [hasSearched, setHasSearched] = useState(false);
    const topics = useTopicCatalog();
    const topicsId = useId();
    const controller = useMemo(() => {
        const original = ConnectionStore.getSnapshot();
        return createMessageQueryController(MessageService, () => {
            const current = ConnectionStore.getSnapshot();
            return current?.revision === original?.revision && current?.environmentId === original?.environmentId;
        });
    }, []);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useEffect(() => { controller.start(); return controller.stop; }, [controller]);
    const update = (patch: Partial<MessageQueryDraft>) => {
        controller.reset(); setSelection(null); setLocalPage(1); setValidation(''); setHasSearched(false);
        storeDraft({ ...draft, ...patch });
    };
    const query = useCallback(() => {
        try {
            const request = buildMessageQuery(draft);
            setValidation(''); setHasSearched(true); setLocalPage(1);
            void controller.read(request, 1, true);
        } catch (error) { setValidation((error as Error).message); }
    }, [controller, draft]);
    const refresh = useCallback(() => { void topics.refresh(); if (hasSearched) query(); }, [topics.refresh, hasSearched, query]);
    usePageRefresh({ refresh, pending: topics.pending || state.pending, refreshedAt: state.receivedAt });
    const result = state.result;
    const selected = result?.items.find(item => messageIdentity(item) === selection);
    useEffect(() => { if (selection === null && result?.items[0]) setSelection(messageIdentity(result.items[0])); }, [selection, result]);
    const timeMode = result?.query.mode === 'time';
    const pageCount = timeMode ? result.totalPages : Math.ceil((result?.items.length ?? 0) / 12);
    const page = timeMode ? result.page : Math.min(localPage, Math.max(1, pageCount));
    const rows = result ? timeMode ? result.items : result.items.slice((page - 1) * 12, page * 12) : [];
    return <div className="ops-messages">
        <section className="ops-message-query" aria-label="Message query">
            <div className="ops-message-query-heading"><div className="ops-message-modes" role="group" aria-label="Message query mode">{modes.map(([mode, label]) => <Button key={mode} variant={draft.mode === mode ? 'primary' : 'ghost'} aria-pressed={draft.mode === mode} onClick={() => { if (mode !== draft.mode) update({ mode }); }}>{label}</Button>)}</div>
                {draft.mode === 'key' && <p className="ops-message-note">Key lookup returns up to 64 indexed messages.</p>}
                {draft.mode === 'time' && <p className="ops-message-note">Local time: {Intl.DateTimeFormat().resolvedOptions().timeZone}. Refresh starts a new scan.</p>}
            </div>
            <form className="ops-message-query-fields" onSubmit={event => { event.preventDefault(); query(); }}>
                <Input label="Topic" placeholder="Choose or enter a Topic" list={topicsId} value={draft.topic} onChange={event => update({ topic: event.target.value })} required />
                <datalist id={topicsId}>{topics.data?.items.map(item => <option key={item.topic} value={item.topic} />)}</datalist>
                {draft.mode === 'key' && <Input label="Key" value={draft.key} onChange={event => update({ key: event.target.value })} required />}
                {draft.mode === 'id' && <Input label="Message ID" value={draft.messageId} onChange={event => update({ messageId: event.target.value })} required />}
                {draft.mode === 'time' && <><Input label="Begin" type="datetime-local" step={1} value={draft.begin} onChange={event => update({ begin: event.target.value })} required />
                    <Input label="End" type="datetime-local" step={1} value={draft.end} onChange={event => update({ end: event.target.value })} required /></>}
                <Button type="submit" icon={Search} disabled={state.pending}>{state.pending ? 'Querying…' : 'Query'}</Button>
            </form>
            {draft.mode === 'id' && <p className="ops-message-note">A physical message ID contains a Broker address that must be reachable from this app. Unique IDs may resolve to a different physical ID.</p>}
            {topics.error && <p className="ops-message-note">Topic suggestions are unavailable. Manual Topic input remains available.</p>}
            {validation && <PageState kind="error" title="Check query conditions" description={validation} />}
        </section>
        <PageSection title="Query results" description={result ? `${result.total.toLocaleString()} ${result.total === 1 ? 'message' : 'messages'} · Topic: ${result.query.topic} · Last successful query: ${new Date(state.receivedAt!).toLocaleString()}` : 'Run a query to inspect matching messages.'}>
            {state.pending && <PageState kind="loading" title="Reading messages" />}
            {state.error && <PageState kind="error" title="Message query failed" description={state.error + (result ? ' Showing the last successful result.' : '')} />}
            {!hasSearched && <PageState kind="empty" title="No query yet" description="Select a query mode and enter its required conditions." />}
            {result && <>
                <div className="ops-message-scroll" role="region" aria-label="Message results" tabIndex={0}><table><thead><tr>
                    {['Message ID', 'Topic', 'Tags', 'Keys', 'Store time'].map(label => <th scope="col" key={label}>{label}</th>)}
                </tr></thead><tbody>{rows.map(message => <tr key={messageIdentity(message)} data-selected={messageIdentity(message) === selection}>
                    <th scope="row"><div className="ops-message-result-id"><button type="button" className="ops-message-choice" title={message.msgId} aria-pressed={messageIdentity(message) === selection} onClick={() => setSelection(messageIdentity(message))}>{visibleMessageText(message.msgId)}</button>
                        <Button variant="ghost" icon={Copy} aria-label={'Copy message ID ' + message.msgId} onClick={() => { void copyMessageValue(message.msgId, 'Message ID'); }} /></div></th>
                    <td>{visibleMessageText(message.topic)}</td><td>{visibleMessageText(message.tags || 'Not reported')}</td><td>{visibleMessageText(message.keys || 'Not reported')}</td><td>{messageTimestamp(message.storeTimestamp)}</td>
                </tr>)}</tbody></table></div>
                {!rows.length && <PageState kind="empty" title="No matching messages returned" description="The query completed without reporting matching messages on this page." />}
                {pageCount > 1 && <Pagination currentPage={page} totalPages={pageCount} disabled={state.pending} onPageChange={value => { if (timeMode) void controller.read(result.query, value); else setLocalPage(value); }} />}
            </>}
        </PageSection>
        {selected ? <MessageInspector key={messageIdentity(selected)} message={selected} disabled={state.pending || Boolean(state.error)} />
            : result && <PageState kind="empty" title={selection ? 'Selected message is not in the returned page' : 'Select a message'} description="Choose a returned record explicitly to inspect its body and delivery metadata." />}
    </div>;
}
