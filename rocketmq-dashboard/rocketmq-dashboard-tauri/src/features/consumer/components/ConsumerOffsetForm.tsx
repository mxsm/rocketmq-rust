import { useEffect, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import { TopicService } from '../../../services/topic.service';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { DialogFooter } from '../../../components/ui/dialog';
import { PageState } from '../../../components/layout/PageState';
import type { OperationOwner } from '../../../hooks/useOperationOwner';
import type { ConsumerAction } from '../consumerActionContext';
import type { ResetOffsetRequest, TopicMutationResult } from '../../topic/types/topic.types';
import { currentLocalMinute, offsetResetRequest } from '../../topic/offsetReset';
import { checkConsumerIdentity } from '../consumerModel';
import { ConsumerActionBody } from './ConsumerActionFrame';
import { ConsumerJson, ConsumerProperties } from './ConsumerDetails';

export function ConsumerOffsetForm({ action, owner, onClose }: { action: Extract<ConsumerAction, { kind: 'reset' }>; owner: OperationOwner; onClose: () => void }) {
    const group = action.consumer.rawGroupName;
    const [ready, setReady] = useState(false);
    const [time, setTime] = useState(currentLocalMinute);
    const [force, setForce] = useState(false);
    const [review, setReview] = useState<ResetOffsetRequest | null>(null);
    const [submitted, setSubmitted] = useState(false);
    const [unconfirmed, setUnconfirmed] = useState(false);
    const [receipt, setReceipt] = useState<TopicMutationResult | null>(null);
    const [progress, setProgress] = useState<unknown>(null);
    const readProgress = async () => {
        const value = checkConsumerIdentity(await ConsumerService.queryConsumerTopicDetail({ consumerGroup: group, scope: { mode: 'name_server' } }), group);
        const topic = value.topics.find(item => item.topic === action.topic);
        if (!topic) throw new Error('This Topic is no longer returned for the selected group.');
        return { consumerGroup: value.consumerGroup, topic };
    };
    const load = async () => { const value = await owner.controller.read(readProgress, 'Current progress could not be verified.'); if (value) setReady(true); };
    useEffect(() => { void load(); }, [group, action.topic, owner.controller]);
    const submit = async () => {
        if (owner.blocked || submitted || !ready) return;
        if (!review) {
            try { setReview(offsetResetRequest(action.topic, group, time, force)); owner.controller.validationError(''); }
            catch (error) { owner.controller.validationError((error as Error).message); }
            return;
        }
        const request = review;
        setSubmitted(true);
        const result = await owner.controller.write(() => TopicService.resetConsumerOffset(request), 'Offset reset was not confirmed. Inspect progress before another operation.');
        setReview(null);
        setUnconfirmed(result === null);
        if (!result) return;
        setReceipt(result);
        if (!owner.controller.isCurrent()) return;
        const observed = await owner.controller.read(readProgress, 'The reset receipt is retained, but current progress could not be read.');
        if (observed) setProgress(observed);
    };
    return <form className="ops-consumer-form" onSubmit={event => { event.preventDefault(); event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus(); void submit(); }}>
        <ConsumerActionBody owner={owner} reveal={review ?? receipt}>
            <ConsumerProperties rows={ [['Consumer group', group], ['Topic', action.topic], ['Scope', 'NameServer']] } />
            {!ready && <Button variant="outline" disabled={owner.blocked} onClick={() => void load()}>Read current progress</Button>}
            <fieldset className="ops-consumer-fields" disabled={owner.blocked || submitted || Boolean(review) || !ready}>
                <Input label={'Local date and time (' + Intl.DateTimeFormat().resolvedOptions().timeZone + ')'} type="datetime-local" required value={time} onChange={event => setTime(event.target.value)} />
                <label className="ops-consumer-check"><input type="checkbox" checked={force} onChange={event => setForce(event.target.checked)} />Force reset (allows moving offsets backwards)</label>
            </fieldset>
            {review && <section role="alert"><h3>Confirm offset reset</h3><ConsumerProperties rows={ [['Timestamp', new Date(review.resetTime).toLocaleString()], ['Timestamp (ms)', review.resetTime], ['Force', String(review.force)]] } />
                <p>This can skip messages or cause them to be consumed again.</p></section>}
            {receipt && <section role="status"><h3>{receipt.success ? 'Reset acknowledged' : 'Reset not confirmed'}</h3><p>{receipt.message}</p>{receipt.affectedQueues != null && <p>Affected queues: {receipt.affectedQueues}</p>}</section>}
            {unconfirmed && <p role="alert">The offset reset was not confirmed. Offsets may have changed. Close this dialog and inspect current progress before another reset.</p>}
            {receipt && progress === null && !owner.busy && <PageState kind="partial" title="Reset receipt retained; progress unavailable" description={owner.contextChanged ? 'The environment or scope changed. Progress is not read from another context.' : 'Read progress before another offset operation.'} />}
            {progress !== null && <ConsumerJson label="Progress read after reset" value={progress} open />}
        </ConsumerActionBody><DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to editing</Button>}
            <Button variant="danger" type="submit" disabled={owner.blocked || submitted || !ready}>{owner.state.operation === 'write' ? 'Resetting…' : review ? 'Confirm offset reset' : 'Review offset reset'}</Button>
        </DialogFooter>
    </form>;
}
