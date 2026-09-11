import { useEffect, useState } from 'react';
import { ClusterService } from '../../../services/cluster.service';
import { ConsumerService } from '../../../services/consumer.service';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { DialogFooter } from '../../../components/ui/dialog';
import type { OperationOwner } from '../../../hooks/useOperationOwner';
import type { ClusterBrokerCardItem } from '../../cluster/types/cluster.types';
import type { ConsumerAction } from '../consumerActionContext';
import type { ConsumerCreateOrUpdateRequest, ConsumerMutationResult } from '../types/consumer.types';
import { checkConsumerIdentity, consumerDraftFromConfig, consumerNumberFields, consumerSwitches, newConsumerDraft, reviewedFailedBrokers, validateConsumerDraft } from '../consumerModel';
import { ConsumerActionBody } from './ConsumerActionFrame';
import { ConsumerJson, ConsumerProperties } from './ConsumerDetails';
import { ConsumerMutationReceipt } from './ConsumerMutationReceipt';

export function ConsumerEditorForm({ action, owner, onClose }: { action: Extract<ConsumerAction, { kind: 'create' | 'edit' }>; owner: OperationOwner; onClose: () => void }) {
    const [targets, setTargets] = useState<ClusterBrokerCardItem[] | null>(null);
    const [draft, setDraft] = useState(() => newConsumerDraft(action.kind === 'edit' ? action.consumer.rawGroupName : ''));
    const [source, setSource] = useState(action.kind === 'edit' ? action.address ?? '' : '');
    const [seeded, setSeeded] = useState(action.kind === 'create');
    const [review, setReview] = useState<ConsumerCreateOrUpdateRequest | null>(null);
    const [receipts, setReceipts] = useState<ConsumerMutationResult[]>([]);
    const [locked, setLocked] = useState(false);
    const [unconfirmed, setUnconfirmed] = useState(false);
    const [retryOnly, setRetryOnly] = useState(false);
    const [sourceNote, setSourceNote] = useState('');
    const load = async () => {
        const result = await owner.controller.read(async () => {
            const inventory = await ClusterService.getClusterHomePage({ forceRefresh: true });
            if (action.kind !== 'edit' || !source) return { items: inventory.items, config: null };
            const config = checkConsumerIdentity(await ConsumerService.queryConsumerConfig({ consumerGroup: action.consumer.rawGroupName, address: source }), action.consumer.rawGroupName);
            if (config.brokerAddress !== source || !inventory.items.some(item => item.brokerName === config.brokerName && item.address === source)) throw new Error('The selected Broker no longer matches this configuration source.');
            return { items: inventory.items, config };
        }, 'Editor data could not be read. Choose a current Broker configuration before editing.');
        if (!result) return;
        setTargets(result.items);
        if (result.config) { setDraft(consumerDraftFromConfig(result.config)); setSeeded(true); setSourceNote('Values loaded from ' + result.config.brokerName + ' at ' + source + '. Other Brokers may have different values.'); }
    };
    useEffect(() => { void load(); }, [action, owner.controller, source]);
    const disabled = owner.blocked || locked || Boolean(review);
    const update = (patch: Partial<ConsumerCreateOrUpdateRequest>) => { if (!disabled) setDraft(value => ({ ...value, ...patch })); };
    const brokers = [...new Set(targets?.map(item => item.brokerName) ?? [])].sort();
    const submit = async () => {
        if (owner.blocked || locked || !seeded || !targets) return;
        if (!review) {
            try { const next = validateConsumerDraft(draft, targets); setDraft(next); setReview(next); owner.controller.validationError(''); }
            catch (error) { owner.controller.validationError((error as Error).message); }
            return;
        }
        setLocked(true);
        const result = await owner.controller.write(() => ConsumerService.createOrUpdateConsumerGroup(review), 'Group write was not confirmed. Inspect configuration before another operation.');
        setReview(null);
        setUnconfirmed(result === null);
        if (result) setReceipts(values => [...values, result]);
    };
    const reviewFailed = async (receipt: ConsumerMutationResult) => {
        if (owner.blocked || unconfirmed) return;
        const refreshed = await owner.controller.read(async () => {
            const inventory = await ClusterService.getClusterHomePage({ forceRefresh: true });
            const names = reviewedFailedBrokers(receipt, draft.consumerGroup, 'upsert', inventory.items.map(item => item.brokerName), draft.brokerNameList);
            return { targets: inventory.items, names };
        }, 'Failed targets could not be verified. The previous receipt is retained.');
        if (!refreshed) return;
        setTargets(refreshed.targets); setDraft(value => ({ ...value, clusterNameList: [], brokerNameList: refreshed.names }));
        setRetryOnly(true); setLocked(false); setReview(null);
    };
    return <form className="ops-consumer-form" onSubmit={event => { event.preventDefault(); event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus(); void submit(); }}>
        <ConsumerActionBody owner={owner} reveal={review ?? receipts.length}>
            {sourceNote && <p className="ops-consumer-note">{sourceNote}</p>}
            {!locked && !review && <fieldset className="ops-consumer-fields" disabled={disabled}>
                {action.kind === 'edit' && <label className="ops-consumer-select"><span>Configuration source</span><select value={source} disabled={retryOnly} onChange={event => { setSource(event.target.value); setSeeded(false); setDraft(newConsumerDraft(action.consumer.rawGroupName)); setSourceNote(''); }}>
                    <option value="">Choose a Broker to load current configuration</option>{source && !action.consumer.brokerAddresses.includes(source) && <option value={source}>{source}</option>}{action.consumer.brokerAddresses.map(address => <option key={address}>{address}</option>)}</select></label>}
                {(!targets || !seeded) && <Button variant="outline" disabled={owner.busy || (action.kind === 'edit' && !source)} onClick={() => void load()}>Read current configuration</Button>}
                {seeded && <><Input label="Consumer group" value={draft.consumerGroup} readOnly={action.kind === 'edit' || receipts.length > 0} onChange={event => update({ consumerGroup: event.target.value })} required />
                    <fieldset className="ops-consumer-targets" disabled={retryOnly}><legend>Explicit Broker targets</legend>
                        <div className="ops-consumer-actions">{[...new Set(targets?.map(item => item.clusterName) ?? [])].map(cluster => <Button key={cluster} variant="outline" onClick={() => update({ brokerNameList: [...new Set([...draft.brokerNameList, ...targets!.filter(item => item.clusterName === cluster).map(item => item.brokerName)])] })}>Select {cluster} Brokers</Button>)}</div>
                        {brokers.map(name => <label key={name}><input type="checkbox" checked={draft.brokerNameList.includes(name)} onChange={() => update({ brokerNameList: draft.brokerNameList.includes(name) ? draft.brokerNameList.filter(value => value !== name) : [...draft.brokerNameList, name] })} />{name}</label>)}
                    </fieldset>
                    {retryOnly && <p className="ops-consumer-note">Reviewing failed Brokers only: {draft.brokerNameList.join(', ')}. Completed targets are excluded.</p>}
                    <div className="ops-consumer-form-grid">{consumerNumberFields.map(([field, label, min, max]) => <Input key={field} label={label} type="number" min={min} max={max} step={1} required value={Number.isNaN(draft[field]) ? '' : draft[field]} onChange={event => update({ [field]: event.target.valueAsNumber })} />)}</div>
                    {consumerSwitches.map(([field, label]) => <label key={field} className="ops-consumer-check"><input type="checkbox" checked={draft[field]} onChange={event => update({ [field]: event.target.checked })} />{label}</label>)}
                </>}
            </fieldset>}
            {review && <section aria-label="Review Consumer change"><h3>Confirm group configuration</h3><ConsumerProperties rows={[
                ['Consumer group', review.consumerGroup], ['Brokers', review.brokerNameList.join(', ')], ...consumerNumberFields.map(([field, label]): [string, number] => [label, review[field]]),
                ...consumerSwitches.map(([field, label]): [string, string] => [label, review[field] ? 'Enabled' : 'Disabled']),
            ]} /><p>The displayed configuration will be applied to these explicit Brokers.</p></section>}
            {unconfirmed && <p role="alert">The last write was not confirmed. Some targets may have changed. Close this dialog and inspect current Broker configuration before another operation.</p>}
            {receipts.map((receipt, index) => <ConsumerMutationReceipt key={index} result={receipt} disabled={owner.blocked || unconfirmed || !locked || index !== receipts.length - 1} onReviewFailed={() => void reviewFailed(receipt)} />)}
            {locked && <ConsumerJson label="Submitted configuration" value={draft} />}
        </ConsumerActionBody><DialogFooter><Button variant="outline" disabled={owner.busy} onClick={onClose}>Close</Button>
            {review && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to editing</Button>}
            <Button type="submit" disabled={owner.blocked || locked || !seeded || !targets}>{owner.state.operation === 'write' ? 'Applying…' : review ? 'Confirm and apply' : 'Review group change'}</Button>
        </DialogFooter>
    </form>;
}
