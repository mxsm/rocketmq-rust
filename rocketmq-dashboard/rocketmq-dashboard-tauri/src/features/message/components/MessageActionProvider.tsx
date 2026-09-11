import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { MessageActionContext, type OpenDirectConsume } from '../messageActionContext';
import { ConnectionStore } from '../../../services/connection.store';
import { MessageService } from '../../../services/message.service';
import { useOperationOwner } from '../../../hooks/useOperationOwner';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { PageState } from '../../../components/layout/PageState';
import { checkMessageDetail, messageLookup, visibleMessageText } from '../messageModel';
import type { MessageDetail, MessageDirectConsumeRequest, MessageDirectConsumeResult, MessageSummary } from '../types/message.types';
import '../message.css';

interface Request { message: MessageSummary; group: string; revision: number; environmentId: string | null; trigger: HTMLElement | null; }

function DirectConsumeDialog({ request, close }: { request: Request; close: () => void }) {
    const owner = useOperationOwner(request.revision, request.environmentId);
    const [detail, setDetail] = useState<MessageDetail | null>(null);
    const [group, setGroup] = useState(request.group);
    const [client, setClient] = useState('');
    const [review, setReview] = useState<MessageDirectConsumeRequest | null>(null);
    const [submitted, setSubmitted] = useState<MessageDirectConsumeRequest | null>(null);
    const [receipt, setReceipt] = useState<MessageDirectConsumeResult | null>(null);
    const [unconfirmed, setUnconfirmed] = useState(false);
    const body = useRef<HTMLDivElement>(null);
    const load = async () => {
        const result = await owner.controller.read(async () => checkMessageDetail(await MessageService.viewMessageDetail(messageLookup(request.message)), request.message), 'Current message targets could not be read.');
        if (result) setDetail(result);
    };
    useEffect(() => { void load(); }, [owner.controller]);
    useEffect(() => { body.current?.scrollTo({ top: 0 }); }, [review, receipt, owner.state.error, owner.contextChanged]);
    const groups = [...new Set(detail?.messageTrackList?.map(track => track.consumerGroup).filter(Boolean) ?? [])];
    const submit = async () => {
        if (owner.blocked || submitted || !detail) return;
        if (!review) {
            if (!groups.includes(group)) { owner.controller.validationError('Select a Consumer group from the current delivery records.'); return; }
            setReview({ topic: detail.topic, messageId: detail.msgId, consumerGroup: group, clientId: client.trim() || undefined });
            owner.controller.validationError('');
            return;
        }
        setSubmitted(review);
        const result = await owner.controller.write(() => MessageService.consumeMessageDirectly(review), 'Direct consume was not confirmed. Inspect the Consumer before another operation.');
        setReview(null);
        setUnconfirmed(result === null);
        if (result) setReceipt(result);
    };
    const matches = receipt && submitted && receipt.topic === submitted.topic && receipt.msgId === submitted.messageId && receipt.consumerGroup === submitted.consumerGroup;
    return <Dialog open onOpenChange={open => { if (!open && !owner.busy) close(); }}><DialogContent className="ops-message-operation" showCloseButton={!owner.busy}
        onEscapeKeyDown={event => { if (owner.busy) event.preventDefault(); }} onInteractOutside={event => { if (owner.busy) event.preventDefault(); }}
        onCloseAutoFocus={event => { event.preventDefault(); if (request.trigger?.isConnected) request.trigger.focus(); else document.getElementById('main-content')?.focus(); }}>
        <DialogHeader><DialogTitle>Direct consume</DialogTitle><DialogDescription>Review one message and its Consumer target before requesting consumption.</DialogDescription></DialogHeader>
        <p className="ops-message-note">Environment {request.environmentId ?? 'Not configured'} · Connection revision {request.revision}</p>
        <form className="ops-message-operation-form" onSubmit={event => { event.preventDefault(); event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus(); void submit(); }}>
            <div className="ops-message-operation-body" ref={body}>
                {owner.contextChanged && <PageState kind="stale" title="Connection context changed" description="This operation and any accepted result remain attached to the original environment. Reopen to operate in the current context." />}
                {owner.state.error && <PageState kind="error" title="Direct consume could not complete" description={owner.state.error} />}
                {owner.state.operation === 'read' && <PageState kind="loading" title="Reading current message targets" />}
                <dl className="ops-message-properties"><div><dt>Topic</dt><dd>{request.message.topic}</dd></div><div><dt>Query ID</dt><dd>{messageLookup(request.message).messageId}</dd></div></dl>
                {!detail && <Button variant="outline" disabled={owner.blocked} onClick={() => { void load(); }}>Retry target read</Button>}
                {detail && !review && !submitted && <fieldset className="ops-message-fields" disabled={owner.blocked}>
                    <label className="ops-message-select"><span>Consumer group</span><select value={group} onChange={event => setGroup(event.target.value)}>
                        <option value="">Choose a Consumer group</option>{request.group && !groups.includes(request.group) && <option value={request.group} disabled>{request.group} (no longer reported)</option>}
                        {groups.map(value => <option key={value}>{value}</option>)}
                    </select></label>
                    {!groups.length && <PageState kind="empty" title="No Consumer targets were returned" description="Refresh the message after a Consumer delivery record is available." />}
                    <Input label="Client ID (optional)" value={client} onChange={event => setClient(event.target.value)} />
                    <p className="ops-message-note">Leave Client ID empty to let the Broker select an eligible client in this group. An explicit ID targets that client.</p>
                </fieldset>}
                {(review || submitted) && <section aria-label="Direct consume target"><h3>{submitted ? 'Submitted target' : 'Confirm direct consume'}</h3>
                    <dl className="ops-message-properties">{[
                        ['Message ID', (review || submitted)!.messageId], ['Consumer group', (review || submitted)!.consumerGroup],
                        ['Client', (review || submitted)!.clientId || 'Selected by Broker'],
                    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value)}</dd></div>)}</dl>
                    {!submitted && <p>This may invoke the Consumer again for an already processed message. The result will remain here for inspection.</p>}
                </section>}
                {unconfirmed && <PageState kind="partial" title="Consumption result is unconfirmed" description="The Consumer may already have run. Inspect its state before submitting another operation; this dialog will not retry." />}
                {receipt && <section role="status"><h3>{matches ? receipt.success ? 'Direct consume acknowledged' : 'Direct consume not confirmed' : 'Receipt target differs from the request'}</h3>
                    <p>{receipt.message}</p><dl className="ops-message-properties">{[
                        ['Returned Topic', receipt.topic], ['Returned message ID', receipt.msgId], ['Returned group', receipt.consumerGroup],
                        ['Consume result', receipt.consumeResult || 'Not reported'], ['Remark', receipt.remark || 'Not reported'],
                    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{visibleMessageText(value)}</dd></div>)}</dl></section>}
            </div>
            <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={close}>Close</Button>
                {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to target</Button>}
                <Button variant="danger" type="submit" disabled={owner.blocked || Boolean(submitted) || !detail || !groups.includes(group)}>{owner.state.operation === 'write' ? 'Requesting…' : review ? 'Confirm direct consume' : 'Review target'}</Button>
            </DialogFooter>
        </form>
    </DialogContent></Dialog>;
}

export function MessageActionProvider({ children }: { children: ReactNode }) {
    const [request, setRequest] = useState<Request | null>(null);
    const open = useCallback<OpenDirectConsume>((message, group = '') => {
        const settings = ConnectionStore.getSnapshot();
        if (!settings) return;
        const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        setRequest(current => current ?? { message: structuredClone(message), group, revision: settings.revision, environmentId: settings.environmentId, trigger });
    }, []);
    return <MessageActionContext.Provider value={open}>{children}{request && <DirectConsumeDialog request={request} close={() => setRequest(null)} />}</MessageActionContext.Provider>;
}
