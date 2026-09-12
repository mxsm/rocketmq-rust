import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowRight, Check, CircleHelp, Copy, RefreshCw, X } from 'lucide-react';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { Button } from '../../../components/ui/LegacyButton';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../../components/ui/tabs';
import { useReadResource } from '../../../hooks/useReadResource';
import { MessageTraceService } from '../../../services/message-trace.service';
import { useAppStore } from '../../../stores/app.store';
import { copyMessageValue } from '../../message/components/MessageInspector';
import { messageNumber, visibleMessageText } from '../../message/messageModel';
import type { MessageSummary } from '../../message/types/message.types';
import { checkTraceDetail, traceDuration, traceEvents, traceStatus, traceTimestamp } from '../traceModel';
import type { TraceResult } from '../traceQuery';
import type { MessageTraceDetail, MessageTraceNode } from '../types/message-trace.types';

type EventView = 'timeline' | 'consumers' | 'transactions';
type TraceEvent = ReturnType<typeof traceEvents>[number];

export function TraceWorkspace({ message, traceTopic, queryResult, disabled }: {
    message: MessageSummary; traceTopic: string; queryResult: TraceResult; disabled: boolean;
}) {
    const load = useCallback(async () => checkTraceDetail(await MessageTraceService.viewMessageTraceDetail({ traceTopic, messageId: message.msgId }), traceTopic, message), [traceTopic, message.msgId, message.topic, queryResult]);
    const resource = useReadResource(load, 'Trace events could not be read.');
    const [view, setView] = useState<EventView>('timeline');
    const [selection, setSelection] = useState<string | null>(null);
    const data = resource.data;
    const groups = useMemo(() => data ? [
        { key: 'timeline', label: '', events: traceEvents(data.timeline) },
        ...data.consumerGroups.map((group, index) => ({ key: `consumer:${index}`, label: group.consumerGroup || 'Group not reported', events: traceEvents(group.nodes) })),
        { key: 'transactions', label: '', events: traceEvents(data.transactionChecks) },
    ] : [], [data]);
    const visibleGroups = groups.filter(group => view === 'consumers' ? group.key.startsWith('consumer:') : group.key === view);
    const events = visibleGroups.flatMap(group => group.events);
    useEffect(() => { if (selection === null && events[0]) setSelection(events[0].id); }, [selection, events]);
    const selected = events.find(event => event.id === selection);
    const blocked = disabled || resource.pending || Boolean(resource.error);
    const topic = data?.topic || message.topic;
    return <>
        {resource.pending && <PageState kind="loading" title="Reading trace events" description={data ? 'Showing the last successful event read while refreshing.' : undefined} />}
        {resource.error && <PageState kind="error" title="Trace events unavailable" description={resource.error + (data ? ' Showing the last successful read; message navigation is paused.' : '')} />}
        <div className="ops-trace-workspace">
            <PageSection title="Trace timeline" description={<span title={message.msgId}>Events for message <span className="ops-trace-identity">{visibleMessageText(message.msgId)}</span></span>}
                action={<Button variant="ghost" icon={RefreshCw} aria-label="Refresh trace events" disabled={disabled || resource.pending} onClick={() => { void resource.read(); }} />}>
                <Tabs value={view} onValueChange={value => { setView(value as EventView); setSelection(null); }}>
                    <TabsList className="ops-tabs-underlined" aria-label="Trace event view"><TabsTrigger value="timeline">All events</TabsTrigger><TabsTrigger value="consumers">Consumer groups</TabsTrigger><TabsTrigger value="transactions">Transactions</TabsTrigger></TabsList>
                    {(['timeline', 'consumers', 'transactions'] as const).map(value => <TabsContent key={value} value={value}>
                        <div className="ops-trace-event-list">
                            {visibleGroups.map(group => <div key={group.key}>{group.label && <h3 className="ops-trace-group-heading">{visibleMessageText(group.label)} <span>{group.events.length} events</span></h3>}
                                <ol aria-label={group.label || (view === 'transactions' ? 'Transaction events' : 'All trace events')}>
                                    {group.events.map(event => <TraceEventItem key={event.id} event={event} selected={selection === event.id} topic={topic} onSelect={() => setSelection(event.id)} />)}
                                </ol>
                            </div>)}
                            {data && !events.length && <PageState kind="empty" title="No events returned in this view" description="No delivery outcome can be inferred from missing trace records." />}
                        </div>
                    </TabsContent>)}
                </Tabs>
            </PageSection>
            <PageSection title="Event details" description="Details for the selected event.">
                {selected && data ? <EventDetails node={selected.node} detail={data} topic={topic} disabled={blocked} /> : <PageState kind="empty" title={selection ? 'Selected event is no longer reported' : 'Select an event'} description="Choose an event in the timeline to inspect its reported fields." />}
            </PageSection>
        </div>
        {data && <details className="ops-trace-summary"><summary>Message and producer summary</summary><TraceSummary detail={data} />
            <p className="ops-trace-note">Observed span covers returned event timestamps. It is not an end-to-end delivery latency or proof of complete trace coverage.</p>
        </details>}
        {resource.receivedAt != null && <p className="ops-trace-note">Last successful event read: {new Date(resource.receivedAt).toLocaleString()} · Event times: {Intl.DateTimeFormat().resolvedOptions().timeZone}</p>}
    </>;
}

function TraceStatus({ status }: { status?: string | null }) {
    const kind = traceStatus(status);
    return <StatusBadge tone={kind === 'success' ? 'success' : kind === 'failed' ? 'danger' : 'neutral'}>{kind === 'success' ? 'Success' : kind === 'failed' ? 'Failed' : status ? `Unknown (${visibleMessageText(status)})` : 'Unknown'}</StatusBadge>;
}

function TraceEventItem({ event, selected, topic, onSelect }: { event: TraceEvent; selected: boolean; topic: string; onSelect: () => void }) {
    const { node } = event;
    const kind = traceStatus(node.status), Icon = kind === 'success' ? Check : kind === 'failed' ? X : CircleHelp;
    return <li className="ops-trace-event" data-selected={selected} data-status={kind}>
        <button type="button" aria-pressed={selected} className="ops-trace-event-button" onClick={onSelect}>
            <span className="ops-trace-marker"><Icon aria-hidden="true" /></span>
            <span className="ops-trace-event-content"><span className="ops-trace-event-heading"><strong>{visibleMessageText(node.traceType || 'Unreported event')}</strong><TraceStatus status={node.status} /></span>
                <time>{traceTimestamp(node.timestamp)}</time>
                <span className="ops-trace-event-fields">{[['Group', node.groupName], ['Topic', topic], ['Client', node.clientHost]].map(([label, value]) => <span key={label}><span>{label}</span><span title={value}>{visibleMessageText(value || 'Not reported')}</span></span>)}</span>
            </span>
        </button>
    </li>;
}

function EventDetails({ node, detail, topic, disabled }: { node: MessageTraceNode; detail: MessageTraceDetail; topic: string; disabled: boolean }) {
    const { openMessage } = useAppStore();
    return <div className="ops-trace-detail">
        <dl className="ops-trace-properties">{[['Event', node.traceType], ['Group', node.groupName], ['Topic', topic], ['Client', node.clientHost], ['Timestamp', traceTimestamp(node.timestamp)]].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value || 'Not reported')}</dd></div>)}
            <div><dt>Status</dt><dd><TraceStatus status={node.status} /></dd></div>
        </dl>
        <div className="ops-trace-actions"><Button variant="ghost" disabled={disabled || !topic} onClick={() => { if (topic) openMessage(detail.msgId, topic); }}>View message<ArrowRight aria-hidden="true" /></Button>
            <Button variant="ghost" icon={Copy} onClick={() => { void copyMessageValue(detail.msgId, 'Trace message ID'); }}>Copy ID</Button></div>
        {!topic && <p className="ops-trace-note">The business Topic was not reported, so message navigation is unavailable.</p>}
        <details className="ops-trace-metadata"><summary>Event metadata</summary><dl className="ops-trace-properties">{[
            ['Role', node.role], ['Store host', node.storeHost], ['Cost time', traceDuration(node.costTime)], ['Retry count', messageNumber(node.retryTimes)],
            ['From transaction check', node.fromTransactionCheck ? 'Yes' : 'No'],
        ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value || 'Not reported')}</dd></div>)}</dl></details>
    </div>;
}

function TraceSummary({ detail }: { detail: MessageTraceDetail }) {
    return <dl className="ops-trace-properties ops-trace-summary-fields">{[
        ['Message ID', detail.msgId], ['Trace Topic', detail.traceTopic], ['Business Topic', detail.topic], ['Tags', detail.tags], ['Keys', detail.keys],
        ['Store host', detail.storeHost], ['First event', traceTimestamp(detail.minTimestamp)], ['Last event', traceTimestamp(detail.maxTimestamp)],
        ['Observed span', traceDuration(detail.totalSpanMs)], ['Producer group', detail.producerGroup], ['Producer event', detail.producerTraceType],
        ['Producer client', detail.producerClientHost], ['Producer store host', detail.producerStoreHost], ['Producer timestamp', traceTimestamp(detail.producerTimestamp)],
        ['Producer cost time', traceDuration(detail.producerCostTime)], ['Timeline events', String(detail.timeline.length)],
        ['Consumer groups', String(detail.consumerGroups.length)], ['Transaction events', String(detail.transactionChecks.length)],
    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value && value.length > 200 ? 0 : undefined}>{visibleMessageText(value || 'Not reported')}</dd></div>)}<div><dt>Producer status</dt><dd><TraceStatus status={detail.producerStatus} /></dd></div></dl>;
}
