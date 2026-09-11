import { useEffect, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import { Button } from '../../../components/ui/LegacyButton';
import { DialogFooter } from '../../../components/ui/dialog';
import type { OperationOwner } from '../../../hooks/useOperationOwner';
import type { ConsumerAction } from '../consumerActionContext';
import type { ConsumerMutationResult } from '../types/consumer.types';
import { isReadOnlyConsumer } from '../mutation';
import { reviewedFailedBrokers } from '../consumerModel';
import { ConsumerActionBody } from './ConsumerActionFrame';
import { ConsumerMutationReceipt } from './ConsumerMutationReceipt';
import { ConsumerProperties } from './ConsumerDetails';

export function ConsumerDeleteForm({ action, owner, onClose }: { action: Extract<ConsumerAction, { kind: 'delete' }>; owner: OperationOwner; onClose: () => void }) {
    const group = action.consumer.rawGroupName;
    const [available, setAvailable] = useState<string[] | null>(null);
    const [selected, setSelected] = useState<string[]>([]);
    const [review, setReview] = useState<string[] | null>(null);
    const [locked, setLocked] = useState(false);
    const [unconfirmed, setUnconfirmed] = useState(false);
    const [retryOnly, setRetryOnly] = useState(false);
    const [receipts, setReceipts] = useState<ConsumerMutationResult[]>([]);
    const load = async (receipt?: ConsumerMutationResult) => {
        if (receipt && unconfirmed) return;
        const result = await owner.controller.read(async () => {
            const value = await ConsumerService.refreshConsumerGroup({ consumerGroup: group, scope: { mode: 'name_server' } });
            if (value.rawGroupName !== group || isReadOnlyConsumer(value)) throw new Error('The group is unavailable or protected.');
            return { available: value.brokerNames, selected: receipt ? reviewedFailedBrokers(receipt, group, 'delete', value.brokerNames, selected) : value.brokerNames };
        }, 'Current group targets could not be verified. Previous results remain available.');
        if (!result) return;
        setAvailable(result.available); setSelected(result.selected); setLocked(false); setRetryOnly(Boolean(receipt)); setReview(null);
    };
    useEffect(() => { void load(); }, [group, owner.controller]);
    const submit = async () => {
        if (owner.blocked || locked || !available) return;
        if (!review) {
            if (!selected.length || selected.some(name => !available.includes(name))) { owner.controller.validationError('Select at least one currently available Broker.'); return; }
            setReview([...selected]); owner.controller.validationError(''); return;
        }
        const brokerNameList = review;
        setLocked(true);
        const result = await owner.controller.write(() => ConsumerService.deleteConsumerGroup({ consumerGroup: group, brokerNameList }), 'Deletion was not confirmed. Inspect current group membership before another operation.');
        setReview(null);
        setUnconfirmed(result === null);
        if (result) setReceipts(values => [...values, result]);
    };
    return <form className="ops-consumer-form" onSubmit={event => { event.preventDefault(); event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus(); void submit(); }}>
        <ConsumerActionBody owner={owner} reveal={review ?? receipts.length}>
            <p>Delete the group from the selected Brokers. Retry/DLQ Topic cleanup runs only after deletion succeeds on every authoritative Broker; its outcome is reported separately.</p>
            {available === null && <Button variant="outline" disabled={owner.blocked} onClick={() => void load()}>Read current group targets</Button>}
            {!locked && !review && available && <fieldset className="ops-consumer-targets" disabled={owner.blocked || retryOnly}><legend>Broker targets</legend>{available.map(name => <label key={name}>
                <input type="checkbox" checked={selected.includes(name)} onChange={() => setSelected(values => values.includes(name) ? values.filter(value => value !== name) : [...values, name])} />{name}</label>)}</fieldset>}
            {retryOnly && !locked && <p>Only failed Brokers are selected: {selected.join(', ')}.</p>}
            {review && <section role="alert"><h3>Confirm deletion</h3><ConsumerProperties rows={ [['Consumer group', group], ['Brokers', review.join(', ')]] } /><p>Existing successful changes will remain applied if another target fails.</p></section>}
            {unconfirmed && <p role="alert">The last deletion was not confirmed. Some targets may have changed. Close this dialog and inspect current group membership before another operation.</p>}
            {receipts.map((receipt, index) => <ConsumerMutationReceipt key={index} result={receipt} disabled={owner.blocked || unconfirmed || !locked || index !== receipts.length - 1} onReviewFailed={() => void load(receipt)} />)}
            {locked && !review && <ConsumerProperties rows={ [['Submitted group', group], ['Submitted Brokers', selected.join(', ')]] } />}
        </ConsumerActionBody><DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to selection</Button>}
            <Button variant="danger" type="submit" disabled={owner.blocked || locked || !available}>{owner.state.operation === 'write' ? 'Deleting…' : review ? 'Confirm deletion' : 'Review deletion'}</Button>
        </DialogFooter>
    </form>;
}
