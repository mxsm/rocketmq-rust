import { useEffect, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import { ConsumerService } from '../../../services/consumer.service';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { DialogFooter } from '../../../components/ui/dialog';
import { PageState } from '../../../components/layout/PageState';
import { currentLocalMinute, offsetResetRequest } from '../offsetReset';
import type { ResetOffsetRequest, TopicMutationResult } from '../types/topic.types';
import type { TopicOperationOwner } from '../hooks/useTopicOperation';
import { TopicActionBody } from './TopicActionFrame';
import { TopicProperties } from './TopicDetails';

export function TopicOffsetForm({ topic, skip, owner, onClose }: {
    topic: string; skip: boolean; owner: TopicOperationOwner; onClose: () => void;
}) {
    const [groups, setGroups] = useState<string[] | null>(null);
    const [group, setGroup] = useState('');
    const [time, setTime] = useState(currentLocalMinute);
    const [force, setForce] = useState(false);
    const [review, setReview] = useState<ResetOffsetRequest | null>(null);
    const [submitted, setSubmitted] = useState(false);
    const [receipt, setReceipt] = useState<TopicMutationResult | null>(null);
    const [progress, setProgress] = useState<unknown>(null);
    const loadGroups = async () => {
        const result = await owner.controller.read(async () => {
            const value = await TopicService.getTopicConsumerGroups({ topic });
            if (value.topic !== topic) throw new Error('Unexpected Topic response');
            return value.consumerGroups;
        }, 'Consumer groups could not be read.');
        if (result) setGroups(result);
    };
    useEffect(() => { void loadGroups(); }, [topic, owner.controller]);
    const disabled = owner.blocked || submitted || Boolean(review);
    const submit = async () => {
        if (owner.blocked || submitted || !groups?.includes(group)) return;
        if (!review) {
            try {
                setReview(skip ? { topic, consumerGroupList: [group], resetTime: -1, force: true }
                    : offsetResetRequest(topic, group, time, force));
                owner.controller.validationError('');
            } catch (error) { owner.controller.validationError((error as Error).message); }
            return;
        }
        const request = review;
        setSubmitted(true);
        const result = await owner.controller.write(() => skip ? TopicService.skipMessageAccumulate(request) : TopicService.resetConsumerOffset(request),
            'Offset change was not confirmed. Inspect Consumer progress before another operation.');
        setReview(null);
        if (!result) return;
        setReceipt(result);
        if (!owner.controller.isCurrent()) return;
        const observed = await owner.controller.read(async () => {
            const value = await ConsumerService.queryConsumerTopicDetail({ consumerGroup: group, scope: { mode: 'name_server' } });
            if (value.consumerGroup !== group) throw new Error('Unexpected Consumer response');
            return { consumerGroup: value.consumerGroup, topics: value.topics.filter(item => item.topic === topic) };
        }, 'The operation receipt is retained, but progress could not be read. Inspect progress before another operation.');
        if (observed) setProgress(observed);
    };
    return <form className="ops-topic-action-form" onSubmit={event => {
        event.preventDefault();
        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
        void submit();
    }}>
        <TopicActionBody owner={owner} reveal={review ?? receipt}>
            <p>{skip ? 'Skip accumulated messages by moving the selected group to the latest offset.' : 'Reset offsets for this Topic and the selected Consumer group.'} Query scope: NameServer.</p>
            {groups === null && !owner.busy && <Button variant="outline" disabled={owner.contextChanged} onClick={() => void loadGroups()}>Retry group read</Button>}
            {groups?.length === 0 && <PageState kind="empty" title="No Consumer groups returned for this Topic" />}
            <fieldset disabled={disabled} className="ops-topic-fields">
                <label className="ops-topic-select"><span>Consumer group</span><select value={group} onChange={event => setGroup(event.target.value)} required>
                    <option value="">Select a group</option>{groups?.map(name => <option key={name}>{name}</option>)}
                </select></label>
                {!skip && <><Input label={'Local date and time (' + Intl.DateTimeFormat().resolvedOptions().timeZone + ')'} type="datetime-local"
                    value={time} onChange={event => setTime(event.target.value)} required />
                    <label className="ops-topic-check"><input type="checkbox" checked={force} onChange={event => setForce(event.target.checked)} />Force reset (allows moving offsets backwards)</label></>}
            </fieldset>
            {review && <section role="alert"><h3>Confirm {skip ? 'skip accumulated messages' : 'offset reset'}</h3>
                <TopicProperties rows={[['Topic', review.topic], ['Consumer group', group], ['Target offset', skip ? 'Latest position' : new Date(review.resetTime).toLocaleString()],
                    ...(!skip ? [['Timestamp (ms)', review.resetTime] as [string, number]] : []), ['Force', String(review.force)]]} />
                <p>Changing offsets can skip messages or cause them to be consumed again.</p>
            </section>}
            {receipt && <section role="status" className="ops-topic-receipt"><h3>{receipt.success ? 'Offset change acknowledged' : 'Offset change not confirmed'}</h3>
                <p>{receipt.message}</p>{receipt.affectedQueues != null && <p>Affected queues: {receipt.affectedQueues}</p>}
                <p>This result belongs to {topic} / {group}. Another operation requires a fresh review.</p></section>}
            {receipt && progress === null && !owner.busy && <PageState kind="partial" title="Operation receipt retained; progress unavailable"
                description={owner.contextChanged ? 'The connection changed. Progress is not queried in another environment.' : 'The progress read did not complete. Inspect Consumer progress before another offset change.'} />}
            {progress !== null && <details open><summary>Progress read after operation</summary><pre className="ops-topic-code" tabIndex={0}>{JSON.stringify(progress, null, 2)}</pre></details>}
        </TopicActionBody>
        <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to editing</Button>}
            <Button variant="danger" type="submit" disabled={owner.blocked || submitted || !group || !groups?.includes(group)}>
                {owner.state.operation === 'write' ? 'Applying…' : review ? (skip ? 'Confirm skip' : 'Confirm offset reset') : 'Review offset change'}
            </Button>
        </DialogFooter>
    </form>;
}
