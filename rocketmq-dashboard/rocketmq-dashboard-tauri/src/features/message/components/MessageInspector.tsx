import { useCallback, useMemo, useState, type ReactNode } from 'react';
import { Activity, Copy, Play, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { MessageService } from '../../../services/message.service';
import { useReadResource } from '../../../hooks/useReadResource';
import { useAppStore } from '../../../stores/app.store';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../../components/ui/tabs';
import type { MessageDetail, MessageSummary } from '../types/message.types';
import { checkMessageDetail, messageLookup, messageNumber, messageTimestamp, traceMessageIdentity, visibleMessageText } from '../messageModel';
import { useDirectConsume } from '../messageActionContext';
import '../message.css';

export async function copyMessageValue(value: string, label: string) {
    try { await navigator.clipboard.writeText(value); toast.success(label + ' copied'); }
    catch { toast.error(label + ' could not be copied'); }
}

export function MessageInspector({ message, disabled = false, loadDetail, actions }: {
    message: MessageSummary; disabled?: boolean; loadDetail?: () => Promise<MessageDetail>; actions?: ReactNode;
}) {
    const { openTrace } = useAppStore();
    const consume = useDirectConsume();
    const request = messageLookup(message);
    const load = useCallback(async () => checkMessageDetail(await (loadDetail ? loadDetail() : MessageService.viewMessageDetail(request)), message), [request.topic, request.messageId, loadDetail]);
    const detail = useReadResource(load, 'Message detail could not be read.');
    const [tab, setTab] = useState('body');
    const blocked = disabled || detail.pending || Boolean(detail.error) || !detail.data;
    return <PageSection title="Message detail" description="Selected message data and delivery metadata." className="ops-message-inspector"
        action={<div className="ops-message-actions"><Button variant="outline" icon={RefreshCw} disabled={detail.pending} onClick={() => { void detail.read(); }}>Refresh detail</Button>
            {actions === undefined ? <><Button variant="outline" icon={Activity} disabled={blocked} onClick={() => { if (detail.data) openTrace(traceMessageIdentity(detail.data, message), message.topic); }}>View trace</Button>
            <Button variant="outline" icon={Play} disabled={blocked} onClick={() => consume(message)}>Direct consume</Button></> : actions}</div>}>
        {detail.pending && <PageState kind="loading" title="Reading message detail" />}
        {detail.error && <PageState kind="error" title="Message detail unavailable" description={detail.error + (detail.data ? ' Showing the last successful read.' : '')} />}
        {detail.data && <>
            <div className="ops-message-identity" aria-label="Message coordinates">{[
                ['Message ID', detail.data.msgId], ['Broker', detail.data.storeHost || 'Not reported'],
                ['Queue', messageNumber(detail.data.queueId)], ['Offset', messageNumber(detail.data.queueOffset)],
            ].map(([label, value]) => <div key={label}><span>{label}</span><strong title={value}>{visibleMessageText(value)}</strong></div>)}</div>
            <Tabs value={tab} onValueChange={setTab}>
                <TabsList className="ops-tabs-underlined" aria-label="Message detail view"><TabsTrigger value="body">Body</TabsTrigger><TabsTrigger value="properties">Properties</TabsTrigger><TabsTrigger value="delivery">Delivery</TabsTrigger></TabsList>
                <TabsContent value="body"><MessageBody detail={detail.data} /></TabsContent>
                <TabsContent value="properties"><MessageProperties detail={detail.data} /></TabsContent>
                <TabsContent value="delivery"><MessageDelivery detail={detail.data} message={message} disabled={blocked} showActions={actions === undefined} /></TabsContent>
            </Tabs>
            <p className="ops-message-note">Last successful detail read: {new Date(detail.receivedAt!).toLocaleString()}</p>
        </>}
    </PageSection>;
}

function MessageBody({ detail }: { detail: MessageDetail }) {
    const text = detail.bodyText;
    const isText = text != null;
    const body = isText ? text : detail.bodyBase64;
    const display = body == null ? null : visibleMessageText(body);
    return <div className="ops-message-body">
        <div className="ops-message-body-toolbar"><span>{body == null ? 'Body' : isText ? 'UTF-8 body' : 'Base64 body'}</span>
            <Button variant="outline" icon={Copy} disabled={body == null} onClick={() => { if (body != null) void copyMessageValue(body, 'Message body'); }}>Copy body</Button></div>
        {!isText && body != null && <p className="ops-message-note">The payload is not UTF-8 text. Showing the returned Base64 value.</p>}
        {body != null && display !== body && <p className="ops-message-note">Control characters are escaped for display. Copy preserves the original value.</p>}
        {body == null ? <PageState kind="empty" title="Message body was not returned" /> : <pre tabIndex={0} aria-label="Message body">{display === '' ? '(empty body)' : display}</pre>}
    </div>;
}

function MessageProperties({ detail }: { detail: MessageDetail }) {
    const [filter, setFilter] = useState('');
    const entries = useMemo(() => Object.entries(detail.properties).filter(([key]) => key.toLowerCase().includes(filter.toLowerCase())), [detail.properties, filter]);
    return <div className="ops-message-stack"><Input label="Filter property names" value={filter} onChange={event => setFilter(event.target.value)} />
        <div className="ops-message-scroll" role="region" aria-label="Message properties" tabIndex={0}><table><thead><tr><th scope="col">Property</th><th scope="col">Value</th><th scope="col">Copy</th></tr></thead><tbody>
            {entries.map(([key, value]) => <tr key={key}><th scope="row">{visibleMessageText(key)}</th><td><pre tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value)}</pre></td>
                <td><Button variant="outline" icon={Copy} aria-label={'Copy property ' + key} onClick={() => { void copyMessageValue(value, 'Property'); }}>Copy</Button></td></tr>)}
        </tbody></table></div>{!entries.length && <PageState kind="empty" title="No properties match" />}
    </div>;
}

function MessageDelivery({ detail, message, disabled, showActions }: { detail: MessageDetail; message: MessageSummary; disabled: boolean; showActions: boolean }) {
    const consume = useDirectConsume();
    return <div className="ops-message-stack"><dl className="ops-message-properties">{[
        ['Message ID', detail.msgId], ['Query ID', message.queryMsgId || message.msgId], ['Topic', detail.topic],
        ['Born host', detail.bornHost || 'Not reported'], ['Store host', detail.storeHost || 'Not reported'],
        ['Born time', messageTimestamp(detail.bornTimestamp)], ['Store time', messageTimestamp(detail.storeTimestamp)],
        ['Queue ID', messageNumber(detail.queueId)], ['Queue offset', messageNumber(detail.queueOffset)],
        ['Store size (bytes)', messageNumber(detail.storeSize)], ['Reconsume times', messageNumber(detail.reconsumeTimes)],
        ['Body CRC', messageNumber(detail.bodyCrc)], ['System flag', messageNumber(detail.sysFlag)], ['Flag', messageNumber(detail.flag)],
        ['Prepared transaction offset', messageNumber(detail.preparedTransactionOffset)],
    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value)}
        {['Message ID', 'Query ID', 'Topic'].includes(label) && <Button variant="ghost" icon={Copy} aria-label={'Copy ' + label} onClick={() => { void copyMessageValue(value, label); }}>Copy</Button>}</dd></div>)}</dl>
        <h3>Consumer delivery records</h3>
        {detail.messageTrackList?.length ? <div className="ops-message-scroll" role="region" aria-label="Consumer delivery records" tabIndex={0}><table><thead><tr><th scope="col">Consumer group</th><th scope="col">Reported state</th><th scope="col">Detail</th>{showActions && <th scope="col">Action</th>}</tr></thead><tbody>
            {detail.messageTrackList.map((track, index) => <tr key={track.consumerGroup + ':' + index}><th scope="row">{track.consumerGroup}</th><td>{track.trackType || 'Unknown'}</td><td>{track.exceptionDesc || 'None reported'}</td>
                {showActions && <td><Button variant="outline" disabled={disabled} onClick={() => consume(message, track.consumerGroup)}>Direct consume</Button></td>}</tr>)}
        </tbody></table></div> : <PageState kind="empty" title="No Consumer delivery records returned" description="No delivery state can be inferred from missing records." />}
    </div>;
}
