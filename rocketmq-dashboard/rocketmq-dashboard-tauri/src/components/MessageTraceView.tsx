import { useCallback, useEffect, useId, useMemo, useState, useSyncExternalStore } from 'react';
import { Search } from 'lucide-react';
import { ConnectionStore } from '../services/connection.store';
import { MessageService } from '../services/message.service';
import { MessageTraceService } from '../services/message-trace.service';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import { buildTraceQuery, traceCandidateIdentity, type TraceQueryDraft } from '../features/message-trace/traceModel';
import { createTraceQueryController } from '../features/message-trace/traceQuery';
import { TraceWorkspace } from '../features/message-trace/components/TraceWorkspace';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { PageState } from './layout/PageState';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import '../features/message-trace/trace.css';

const DEFAULT_TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';

export function MessageTraceView() {
    const { navigation } = useAppStore();
    const target = navigation.target?.kind === 'trace' ? navigation.target : null;
    const [draft, storeDraft] = useNavigationState<TraceQueryDraft>('traceQuery', () => ({
        mode: 'id', traceTopic: DEFAULT_TRACE_TOPIC, messageId: target?.name ?? '', topic: target?.topic ?? '', key: '',
    }));
    const [selection, setSelection] = useState<string | null>(null);
    const [validation, setValidation] = useState('');
    const [hasSearched, setHasSearched] = useState(false);
    const topics = useTopicCatalog();
    const topicsId = useId();
    const traceTopics = useMemo(() => [...new Set([DEFAULT_TRACE_TOPIC, ...(topics.data?.items ?? []).map(item => item.topic)])], [topics.data]);
    const controller = useMemo(() => {
        const original = ConnectionStore.getSnapshot();
        return createTraceQueryController({ queryMessageByTopicKey: MessageService.queryMessageByTopicKey, queryMessageTraceById: MessageTraceService.queryMessageTraceById }, () => {
            const current = ConnectionStore.getSnapshot();
            return current?.revision === original?.revision && current?.environmentId === original?.environmentId;
        });
    }, []);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useEffect(() => { controller.start(); return controller.stop; }, [controller]);
    const update = (patch: Partial<TraceQueryDraft>) => {
        controller.reset(); setSelection(null); setValidation(''); setHasSearched(false);
        storeDraft({ ...draft, ...patch });
    };
    const query = useCallback(() => {
        try {
            const request = buildTraceQuery(draft);
            setValidation(''); setHasSearched(true);
            void controller.read(request);
        } catch (error) { setValidation((error as Error).message); }
    }, [controller, draft]);
    const refresh = useCallback(() => { void topics.refresh(); if (hasSearched) query(); }, [topics.refresh, hasSearched, query]);
    usePageRefresh({ refresh, pending: topics.pending || state.pending, refreshedAt: state.receivedAt });
    const result = state.result;
    useEffect(() => { if (selection === null && result?.items[0]) setSelection(traceCandidateIdentity(result.items[0])); }, [selection, result]);
    const selected = result?.items.find(item => traceCandidateIdentity(item) === selection);
    return <div className="ops-trace">
        <section className="ops-trace-query" aria-label="Trace query">
            <div className="ops-trace-query-heading"><div className="ops-trace-modes" role="group" aria-label="Trace query mode">
                {(['id', 'key'] as const).map(mode => <Button key={mode} variant={draft.mode === mode ? 'primary' : 'ghost'} aria-pressed={draft.mode === mode} onClick={() => { if (mode !== draft.mode) update({ mode }); }}>{mode === 'id' ? 'Message ID' : 'Message Key'}</Button>)}
            </div><p className="ops-trace-note">{draft.mode === 'id' ? 'Events correlated by producer unique ID.' : 'Resolve up to 64 indexed messages from a business Topic and Key.'}</p></div>
            <form className="ops-trace-fields" onSubmit={event => { event.preventDefault(); query(); }}>
                <Input label="Trace Topic" list={topicsId} value={draft.traceTopic} onChange={event => update({ traceTopic: event.target.value })} required />
                <datalist id={topicsId}>{traceTopics.map(topic => <option key={topic} value={topic} />)}</datalist>
                {draft.mode === 'id' ? <Input label="Message ID (producer unique ID)" value={draft.messageId} onChange={event => update({ messageId: event.target.value })} required /> : <>
                    <Input label="Business Topic" list={topicsId} value={draft.topic} onChange={event => update({ topic: event.target.value })} required />
                    <Input label="Message Key" value={draft.key} onChange={event => update({ key: event.target.value })} required />
                </>}
                <Button type="submit" icon={Search} disabled={state.pending}>{state.pending ? 'Querying…' : 'Query trace'}</Button>
            </form>
            {topics.error && <p className="ops-trace-note">Topic suggestions are unavailable. Enter the Trace Topic and business Topic manually.</p>}
            {validation && <PageState kind="error" title="Check trace query" description={validation} />}
        </section>
        {state.pending && <PageState kind="loading" title="Querying trace candidates" description={result ? 'Showing the last successful query while refreshing.' : undefined} />}
        {state.error && <PageState kind="error" title="Trace query unavailable" description={state.error + (result ? ' Showing the last successful query; message navigation is paused.' : ' Check the Trace Topic and whether tracing was enabled for this message.')} />}
        {!hasSearched && <PageState kind="empty" title="Query a message trace" description="Enter a producer unique ID, or use a business Topic and Key to find candidate messages. No query runs until you submit." />}
        {result && result.items.length === 0 && <PageState kind="empty" title="No trace candidates returned" description="The query completed without matching records. Missing trace data does not prove that a message was not delivered." />}
        {result && result.items.length > 0 && (result.items.length > 1 || selection && !selected) && <div className="ops-trace-candidates">
            <label className="ops-trace-select">Message to inspect
                <select value={selected ? selection! : ''} disabled={state.pending} onChange={event => setSelection(event.target.value)}>
                    <option value="" disabled>Select a returned message</option>
                    {result.items.map(message => <option key={traceCandidateIdentity(message)} value={traceCandidateIdentity(message)}>{message.msgId} · {message.topic || 'Topic not reported'}</option>)}
                </select>
            </label><p className="ops-trace-note">{result.items.length} unique message identities returned. Physical copies with the same Topic and unique ID share a trace.</p>
        </div>}
        {selected && result && <TraceWorkspace key={traceCandidateIdentity(selected)} message={selected} traceTopic={result.query.traceTopic} queryResult={result} disabled={state.pending || Boolean(state.error)} />}
        {result && result.items.length > 0 && selection && !selected && <PageState kind="empty" title="Selected message is no longer reported" description="Choose a returned message to continue; selection has not moved to another identity." />}
        {state.receivedAt != null && <p className="ops-trace-note">Last successful candidate query: {new Date(state.receivedAt).toLocaleString()}</p>}
    </div>;
}
