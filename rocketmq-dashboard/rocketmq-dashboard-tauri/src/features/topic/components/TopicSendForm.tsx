import { useEffect, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import { useAppStore } from '../../../stores/app.store';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { DialogFooter } from '../../../components/ui/dialog';
import { TopicActionBody } from './TopicActionFrame';
import { TopicProperties } from './TopicDetails';
import type { TopicListItem, TopicSendMessageResult, SendTopicMessageRequest } from '../types/topic.types';
import type { TopicOperationOwner } from '../hooks/useTopicOperation';

export function TopicSendForm({ topic, owner, onClose }: { topic: TopicListItem; owner: TopicOperationOwner; onClose: () => void }) {
    const { setActiveTab } = useAppStore();
    const [messageType, setMessageType] = useState<string | null>(null);
    const [draft, setDraft] = useState<SendTopicMessageRequest>({ topic: topic.topic, key: '', tag: '', messageBody: '', traceEnabled: false });
    const [review, setReview] = useState<SendTopicMessageRequest | null>(null);
    const [receipt, setReceipt] = useState<TopicSendMessageResult | null>(null);
    const [submitted, setSubmitted] = useState(false);
    const loadType = async () => {
        const config = await owner.controller.read(() => TopicService.getTopicConfig({ topic: topic.topic }), 'Topic configuration must be read before sending.');
        if (config?.topicName === topic.topic) setMessageType(config.messageType || 'UNSPECIFIED');
        else if (config) owner.controller.validationError('Returned Topic does not match this send target.');
    };
    useEffect(() => { void loadType(); }, [topic.topic, owner.controller]);
    const submit = async () => {
        if (owner.blocked || submitted || !messageType) return;
        if (!draft.messageBody.trim()) { owner.controller.validationError('Enter a message body.'); return; }
        if (!review) { setReview({ ...draft }); return; }
        setSubmitted(true);
        const result = await owner.controller.write(() => TopicService.sendTopicMessage(review), 'Send outcome was not confirmed. Inspect the Topic before manually sending again.');
        if (result) setReceipt(result);
        setReview(null);
    };
    return <form className="ops-topic-action-form" onSubmit={event => {
        event.preventDefault();
        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
        void submit();
    }}>
        <TopicActionBody owner={owner} reveal={receipt ?? review}>
            {!messageType && !owner.busy && <Button variant="outline" disabled={owner.contextChanged} onClick={() => void loadType()}>Retry configuration read</Button>}
            <p className="ops-topic-note">{messageType === 'TRANSACTION' ? 'Transaction test send commits its local transaction immediately.' : 'Message type: ' + (messageType ?? 'Not read') + '. This test send does not retry automatically.'}</p>
            {!receipt && <fieldset className="ops-topic-fields" disabled={owner.blocked || submitted || Boolean(review)}>
                <div className="ops-topic-form-grid"><Input label="Key" value={draft.key} onChange={event => setDraft({ ...draft, key: event.target.value })} />
                    <Input label="Tag" value={draft.tag} onChange={event => setDraft({ ...draft, tag: event.target.value })} /></div>
                <label className="ops-topic-select"><span>Message body</span><textarea rows={8} value={draft.messageBody} onChange={event => setDraft({ ...draft, messageBody: event.target.value })} spellCheck={false} required /></label>
                <label className="ops-topic-check"><input type="checkbox" checked={draft.traceEnabled} onChange={event => setDraft({ ...draft, traceEnabled: event.target.checked })} />Enable message trace</label>
            </fieldset>}
            {review && <p role="status">Confirm sending one message to <strong>{review.topic}</strong>. Trace: {review.traceEnabled ? 'on' : 'off'}. Review the body above before sending.</p>}
            {receipt && <section role="status" aria-label="Topic send receipt"><h3>{receipt.success ? 'Message sent' : 'Send needs review'} · {receipt.sendStatus}</h3>
                <TopicProperties rows={[
                    ['Topic', receipt.topic], ['Send status', receipt.sendStatus], ['Message ID', receipt.messageId ?? 'Not returned'],
                    ['Broker', receipt.brokerName ?? 'Not returned'], ['Queue ID', receipt.queueId ?? 'Not returned'], ['Queue offset', receipt.queueOffset],
                    ['Transaction ID', receipt.transactionId ?? 'Not returned'], ['Region ID', receipt.regionId ?? 'Not returned'],
                    ['Local transaction state', receipt.localTransactionState ?? 'Not returned'],
                ]} />
                <p className="ops-topic-note">Keep this receipt when investigating an uncertain send. Sending again may create a duplicate.</p>
                <Button variant="outline" onClick={() => { onClose(); setActiveTab('Message'); }}>Open Messages</Button>
            </section>}
        </TopicActionBody>
        <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to message</Button>}
            <Button type="submit" disabled={owner.blocked || submitted || !messageType}>{owner.state.operation === 'write' ? 'Sending…' : review ? 'Confirm send' : 'Review message'}</Button>
        </DialogFooter>
    </form>;
}
