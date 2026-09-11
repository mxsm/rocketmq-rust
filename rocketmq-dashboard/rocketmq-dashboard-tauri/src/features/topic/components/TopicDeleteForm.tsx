import { useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import { Button } from '../../../components/ui/LegacyButton';
import { DialogFooter } from '../../../components/ui/dialog';
import { TopicMutationReceipt } from './TopicMutationReceipt';
import { TopicActionBody } from './TopicActionFrame';
import type { TopicBatchResult, TopicTargetReceipt } from '../types/topic.types';
import type { TopicAction } from '../topicActionContext';
import type { TopicOperationOwner } from '../hooks/useTopicOperation';

export function TopicDeleteForm({ action, owner, onClose }: {
    action: Extract<TopicAction, { kind: 'delete' | 'delete_broker' }>;
    owner: TopicOperationOwner; onClose: () => void;
}) {
    const [cluster, setCluster] = useState('');
    const [confirmation, setConfirmation] = useState(false);
    const [receipts, setReceipts] = useState<TopicBatchResult[]>([]);
    const [locked, setLocked] = useState(false);
    const [reviewingFailed, setReviewingFailed] = useState(false);
    const brokerName = action.kind === 'delete_broker' ? action.brokerName : null;
    const reviewFailed = (target: TopicTargetReceipt) => {
        if (owner.blocked || target.success || target.kind !== 'cluster') return;
        setCluster(target.name); setLocked(false); setReviewingFailed(true); setConfirmation(false);
    };
    const submit = async () => {
        if (owner.blocked || locked) return;
        if (!confirmation) { setConfirmation(true); return; }
        setLocked(true);
        const result = await owner.controller.write(() => brokerName
            ? TopicService.deleteTopicByBroker({ topic: action.topic.topic, brokerName })
            : TopicService.deleteTopic({ topic: action.topic.topic, clusterName: cluster || undefined }),
        'Deletion was not confirmed. Inspect the Topic and its route before manually retrying.');
        setConfirmation(false);
        if (result) setReceipts(previous => [...previous, result]);
    };
    return <form className="ops-topic-action-form" onSubmit={event => {
        event.preventDefault();
        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
        void submit();
    }}>
        <TopicActionBody owner={owner} reveal={confirmation ? 'review' : receipts.length}>
            <p>Delete <strong>{action.topic.topic}</strong>{brokerName ? ' from Broker ' + brokerName + ' only. Its NameServer mapping is retained.' : ' from the selected cluster scope.'}</p>
            {!brokerName && <label className="ops-topic-select"><span>Cluster scope</span><select value={cluster} disabled={owner.blocked || locked || reviewingFailed || confirmation} onChange={event => setCluster(event.target.value)}>
                <option value="">All discovered clusters</option>{[...new Set([...action.topic.clusters, ...(cluster ? [cluster] : [])])].map(name => <option key={name}>{name}</option>)}
            </select></label>}
            <p className="ops-topic-note">Known clusters: {action.topic.clusters.join(', ') || 'No route reported'}. The backend revalidates the current route before deletion.</p>
            {confirmation && <div role="alert"><strong>Confirm deletion</strong><p>Topic: {action.topic.topic}</p><p>Target: {brokerName ?? (cluster || 'All discovered clusters')}</p>
                <p>This cannot be undone by this dialog. Partial cluster outcomes require state verification.</p></div>}
            {receipts.map((result, index) => <TopicMutationReceipt key={index} result={result} onReviewTarget={!brokerName && index === receipts.length - 1 && locked && !owner.blocked ? reviewFailed : undefined} />)}
        </TopicActionBody>
        <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {confirmation && <Button variant="outline" disabled={owner.blocked} onClick={() => setConfirmation(false)}>Back to scope</Button>}
            <Button variant="danger" type="submit" disabled={owner.blocked || locked}>{owner.state.operation === 'write' ? 'Deleting…' : confirmation ? 'Confirm deletion' : 'Review deletion'}</Button>
        </DialogFooter>
    </form>;
}
