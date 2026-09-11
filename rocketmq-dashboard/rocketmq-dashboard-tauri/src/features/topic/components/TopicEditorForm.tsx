import { useEffect, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { DialogFooter } from '../../../components/ui/dialog';
import { TopicActionBody } from './TopicActionFrame';
import { TopicProperties } from './TopicDetails';
import { TopicMutationReceipt } from './TopicMutationReceipt';
import { validateTopicDraft, failedTopicDraft } from '../topicModel';
import { TOPIC_MESSAGE_TYPE_OPTIONS, type TopicConfigRequest, type TopicBatchResult } from '../types/topic.types';
import type { TopicAction } from '../topicActionContext';
import type { TopicOperationOwner } from '../hooks/useTopicOperation';

export function TopicEditorForm({ action, owner, onClose }: {
    action: Extract<TopicAction, { kind: 'create' | 'edit' }>; owner: TopicOperationOwner; onClose: () => void;
}) {
    const [draft, setDraft] = useState<TopicConfigRequest>({ topicName: action.kind === 'edit' ? action.topic.topic : '',
        clusterNameList: [], brokerNameList: [], readQueueNums: 4, writeQueueNums: 4, perm: 6, order: false, messageType: 'UNSPECIFIED' });
    const [ready, setReady] = useState(action.kind === 'create');
    const [mode, setMode] = useState<'create' | 'update'>(action.kind === 'create' ? 'create' : 'update');
    const [review, setReview] = useState<TopicConfigRequest | null>(null);
    const [receipts, setReceipts] = useState<TopicBatchResult[]>([]);
    const [locked, setLocked] = useState(false);
    const [retryNames, setRetryNames] = useState<string[] | null>(null);
    const [sourceNote, setSourceNote] = useState('');
    const [allBrokers, setAllBrokers] = useState(false);
    const loadSeed = async () => {
        if (action.kind !== 'edit') return;
        const config = await owner.controller.read(() => TopicService.getTopicConfig({ topic: action.topic.topic, brokerName: action.brokerName }),
            'Topic configuration could not be read. Retry before editing.');
        if (!config) return;
        if (config.topicName !== action.topic.topic || (action.brokerName && config.brokerName !== action.brokerName)) {
            owner.controller.validationError('The returned Topic or Broker did not match the selected target.'); return;
        }
        setDraft({ topicName: config.topicName,
            clusterNameList: action.brokerName ? action.targets.filter(target => target.brokerNames.includes(action.brokerName!)).map(target => target.clusterName) : config.clusterNameList,
            brokerNameList: action.brokerName ? [action.brokerName] : config.brokerNameList,
            readQueueNums: config.readQueueNums, writeQueueNums: config.writeQueueNums, perm: config.perm, order: config.order,
            messageType: config.messageType || 'UNSPECIFIED' });
        setSourceNote('Values loaded from ' + config.brokerName + (config.inconsistentFields.length ? '. Differing fields: ' + config.inconsistentFields.join(', ') : '.'));
        setReady(true);
    };
    useEffect(() => { void loadSeed(); }, [action, owner.controller]);
    const brokers = [...new Set(action.targets.filter(target => draft.clusterNameList.includes(target.clusterName)).flatMap(target => target.brokerNames))];
    const disabled = owner.blocked || locked || !ready || Boolean(review);
    const change = (patch: Partial<TopicConfigRequest>) => { if (!disabled) setDraft(previous => ({ ...previous, ...patch })); };
    const toggleCluster = (name: string) => {
        const clusterNameList = draft.clusterNameList.includes(name) ? draft.clusterNameList.filter(value => value !== name) : [...draft.clusterNameList, name];
        const available = action.targets.filter(target => clusterNameList.includes(target.clusterName)).flatMap(target => target.brokerNames);
        change({ clusterNameList, brokerNameList: draft.brokerNameList.filter(value => available.includes(value)) });
    };
    const submit = async () => {
        if (owner.blocked || locked || !ready) return;
        if (!review) {
            try {
                if (!allBrokers && !draft.brokerNameList.length) throw new Error('Select at least one Broker or explicitly choose all Brokers.');
                const request = validateTopicDraft({ ...draft, brokerNameList: allBrokers ? brokers : draft.brokerNameList }, action.targets);
                if (!request.brokerNameList.length) throw new Error('The selected clusters have no Broker targets.');
                setReview(request); setDraft(request); owner.controller.validationError('');
            }
            catch (error) { owner.controller.validationError((error as Error).message); }
            return;
        }
        const request = review;
        setLocked(true);
        const result = await owner.controller.write(() => TopicService.createOrUpdateTopic(request, mode),
            'Topic write was not confirmed. Inspect the current Topic configuration before retrying.');
        setReview(null);
        if (result) setReceipts(previous => [...previous, result]);
    };
    const reviewFailed = (result: TopicBatchResult) => {
        if (!owner.controller.isCurrent() || owner.busy) return;
        try {
            const next = failedTopicDraft(result, draft, action.targets);
            setDraft(next.request); setMode(next.mode); setAllBrokers(false); setRetryNames(next.request.brokerNameList); setLocked(false); setReview(null);
            owner.controller.validationError('');
        } catch (error) { owner.controller.validationError((error as Error).message); }
    };
    return <form className="ops-topic-action-form" onSubmit={event => {
        event.preventDefault();
        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
        void submit();
    }}>
        <TopicActionBody owner={owner} reveal={review ?? receipts.length}>
            {sourceNote && <p className="ops-topic-note">{sourceNote}</p>}
            {!ready && !owner.busy && <Button variant="outline" disabled={owner.contextChanged} onClick={() => void loadSeed()}>Retry configuration read</Button>}
            {ready && !review && !locked && <fieldset disabled={disabled} className="ops-topic-fields">
                <Input label="Topic name" value={draft.topicName} readOnly={action.kind === 'edit' || receipts.length > 0} onChange={event => change({ topicName: event.target.value })} required />
                <fieldset className="ops-topic-targets"><legend>Cluster targets</legend>{action.targets.map(target => <label key={target.clusterName}>
                    <input type="checkbox" checked={draft.clusterNameList.includes(target.clusterName)} disabled={Boolean(retryNames)}
                        onChange={() => toggleCluster(target.clusterName)} />{target.clusterName}</label>)}
                    {!action.targets.length && <p>No cluster targets available. Refresh the Topic catalog.</p>}
                </fieldset>
                <fieldset className="ops-topic-targets"><legend>Broker targets</legend>
                    <label><input type="checkbox" checked={allBrokers} disabled={Boolean(retryNames) || !draft.clusterNameList.length}
                        onChange={event => { setAllBrokers(event.target.checked); change({ brokerNameList: event.target.checked ? brokers : [] }); }} />All Brokers in selected clusters</label>
                    {brokers.map(name => <label key={name}><input type="checkbox" checked={allBrokers || draft.brokerNameList.includes(name)} disabled={Boolean(retryNames) || allBrokers}
                        onChange={() => change({ brokerNameList: draft.brokerNameList.includes(name) ? draft.brokerNameList.filter(value => value !== name) : [...draft.brokerNameList, name] })} />{name}</label>)}
                </fieldset>
                {retryNames && <p className="ops-topic-note">Reviewing failed Brokers only: {retryNames.join(', ')}. Completed targets remain excluded.</p>}
                <div className="ops-topic-form-grid">
                    <Input label="Read queues" type="number" min={1} max={2147483647} step={1} value={Number.isNaN(draft.readQueueNums) ? '' : draft.readQueueNums} onChange={event => change({ readQueueNums: event.target.valueAsNumber })} required />
                    <Input label="Write queues" type="number" min={1} max={2147483647} step={1} value={Number.isNaN(draft.writeQueueNums) ? '' : draft.writeQueueNums} onChange={event => change({ writeQueueNums: event.target.valueAsNumber })} required />
                    <Input label="Permission (0–7)" type="number" min={0} max={7} step={1} value={Number.isNaN(draft.perm) ? '' : draft.perm} onChange={event => change({ perm: event.target.valueAsNumber })} required />
                    <label className="ops-topic-select"><span>Message type</span><select value={draft.messageType ?? 'UNSPECIFIED'} onChange={event => change({ messageType: event.target.value })}>
                        {TOPIC_MESSAGE_TYPE_OPTIONS.map(value => <option key={value}>{value}</option>)}</select></label>
                </div>
                <label className="ops-topic-check"><input type="checkbox" checked={draft.order} onChange={event => change({ order: event.target.checked })} />Ordered routing</label>
            </fieldset>}
            {review && <section aria-label="Review Topic change"><h3>{mode === 'create' ? 'Create' : 'Update'} {review.topicName}</h3>
                <TopicProperties rows={ [['Clusters', review.clusterNameList.join(', ')], ['Brokers', review.brokerNameList.join(', ') || 'All Brokers in selected clusters'],
                    ['Read / write queues', review.readQueueNums + ' / ' + review.writeQueueNums], ['Permission', review.perm],
                    ['Message type', review.messageType], ['Ordered routing', review.order ? 'On' : 'Off']] } />
                <p className="ops-topic-note">This request applies the displayed configuration to these targets. Partial outcomes are reported individually.</p>
            </section>}
            {receipts.map((result, index) => <TopicMutationReceipt key={index} result={result}
                onReviewFailed={index === receipts.length - 1 && !owner.blocked && locked ? () => reviewFailed(result) : undefined} />)}
            {locked && !review && <details><summary>Submitted configuration</summary><TopicProperties rows={[
                ['Topic', draft.topicName], ['Clusters', draft.clusterNameList.join(', ')], ['Brokers', draft.brokerNameList.join(', ')],
                ['Read / write queues', draft.readQueueNums + ' / ' + draft.writeQueueNums], ['Permission', draft.perm],
                ['Message type', draft.messageType], ['Ordered routing', draft.order ? 'On' : 'Off'],
            ]} /></details>}
        </TopicActionBody>
        <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to editing</Button>}
            <Button type="submit" disabled={owner.blocked || locked || !ready}>{owner.state.operation === 'write' ? 'Applying…' : review ? 'Confirm and apply' : 'Review Topic change'}</Button>
        </DialogFooter>
    </form>;
}
