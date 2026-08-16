import { useEffect, useState } from 'react';
import { brokerApi } from '../api/broker_api';
import { consumerApi } from '../api/consumer_api';
import { useConsumerQueryScope } from '../pages/consumers/ConsumerQueryScopeProvider';
import type { ConsumerGroupListItem, ConsumerOperationResult, ConsumerUpsertRequest } from '../types/consumer';
import { Button } from './ui/Button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface ConsumerMutationDialogProps {
  open: boolean;
  mode: 'create' | 'edit';
  consumer?: ConsumerGroupListItem | null;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: ConsumerOperationResult) => void;
}

const defaultForm = (): Omit<ConsumerUpsertRequest, 'consumerGroup'> => ({
  clusterNameList: [],
  brokerNameList: [],
  consumeEnable: true,
  consumeFromMinEnable: true,
  consumeBroadcastEnable: false,
  consumeMessageOrderly: false,
  retryQueueNums: 1,
  retryMaxTimes: 16,
  brokerId: 0,
  whichBrokerWhenConsumeSlowly: 1,
  notifyConsumerIdsChangedEnable: true,
  groupSysFlag: 0,
  consumeTimeoutMinute: 15
});

export default function ConsumerMutationDialog({
  open,
  mode,
  consumer,
  onOpenChange,
  onSucceeded
}: ConsumerMutationDialogProps) {
  const { scope } = useConsumerQueryScope();
  const [group, setGroup] = useState(consumer?.rawGroupName ?? '');
  const [form, setForm] = useState(defaultForm());
  const [brokers, setBrokers] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setGroup(consumer?.rawGroupName ?? '');
    setForm(defaultForm());
    setError(null);
    let cancelled = false;
    void (async () => {
      try {
        const result = await brokerApi.list();
        if (cancelled) return;
        const names = Array.from(new Set(result.items.map((broker) => broker.brokerName))).sort();
        setBrokers(names);
        if (mode === 'edit' && consumer) {
          const config = await consumerApi.config(consumer.rawGroupName, scope);
          if (cancelled) return;
          const effective = config.effective;
          if (effective) {
            setForm({
              clusterNameList: [],
              brokerNameList: config.targets
                .filter((target) => target.config !== null)
                .map((target) => target.brokerName),
              consumeEnable: effective.consumeEnable,
              consumeFromMinEnable: effective.consumeFromMinEnable,
              consumeBroadcastEnable: effective.consumeBroadcastEnable,
              consumeMessageOrderly: effective.consumeMessageOrderly,
              retryQueueNums: effective.retryQueueNums,
              retryMaxTimes: effective.retryMaxTimes,
              brokerId: effective.brokerId,
              whichBrokerWhenConsumeSlowly: effective.whichBrokerWhenConsumeSlowly,
              notifyConsumerIdsChangedEnable: effective.notifyConsumerIdsChangedEnable,
              groupSysFlag: effective.groupSysFlag,
              consumeTimeoutMinute: effective.consumeTimeoutMinute
            });
          }
        }
      } catch (reason) {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, consumer]);

  const submit = async () => {
    if (mode === 'create' && !group.trim()) {
      setError('Consumer group is required.');
      return;
    }
    if (form.brokerNameList.length === 0) {
      setError('Select at least one broker target.');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const payload: ConsumerUpsertRequest = {
        ...(mode === 'create' ? { consumerGroup: group.trim() } : {}),
        ...form
      };
      const result = mode === 'create'
        ? await consumerApi.create(payload)
        : await consumerApi.update(group, payload);
      onSucceeded(result);
      onOpenChange(false);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  };

  const setBrokerTarget = (brokerName: string, selected: boolean) => {
    setForm((current) => ({
      ...current,
      brokerNameList: selected
        ? Array.from(new Set([...current.brokerNameList, brokerName]))
        : current.brokerNameList.filter((value) => value !== brokerName)
    }));
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{mode === 'create' ? 'Create consumer group' : `Edit ${consumer?.rawGroupName ?? group}`}</DialogTitle>
          <DialogDescription>
            Configure subscription group targets and broker-side limits.
          </DialogDescription>
        </DialogHeader>

        <div className="consumer-mutation-form">
          {mode === 'create' ? (
            <div className="field">
              <Label htmlFor="consumer-mutation-group">Consumer group</Label>
              <Input id="consumer-mutation-group" value={group} onChange={(event) => setGroup(event.target.value)} />
            </div>
          ) : null}

          <div className="field">
            <Label>Broker targets</Label>
            <div className="consumer-target-checkboxes">
              {brokers.map((broker) => (
                <label key={broker} className="compact-check">
                  <input
                    type="checkbox"
                    checked={form.brokerNameList.includes(broker)}
                    onChange={(event) => setBrokerTarget(broker, event.target.checked)}
                  />
                  {broker}
                </label>
              ))}
            </div>
          </div>

          <div className="form-grid consumer-mutation-grid">
            <NumberField label="Retry queue nums" value={form.retryQueueNums} onChange={(value) => setForm((current) => ({ ...current, retryQueueNums: value }))} />
            <NumberField label="Retry max times" value={form.retryMaxTimes} onChange={(value) => setForm((current) => ({ ...current, retryMaxTimes: value }))} />
            <NumberField label="Broker id" value={form.brokerId} onChange={(value) => setForm((current) => ({ ...current, brokerId: value }))} />
            <NumberField label="Slow broker id" value={form.whichBrokerWhenConsumeSlowly} onChange={(value) => setForm((current) => ({ ...current, whichBrokerWhenConsumeSlowly: value }))} />
            <NumberField label="Consume timeout minutes" value={form.consumeTimeoutMinute} onChange={(value) => setForm((current) => ({ ...current, consumeTimeoutMinute: value }))} />
          </div>

          <div className="consumer-mutation-flags">
            <CheckboxField label="Consume enabled" checked={form.consumeEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeEnable: checked }))} />
            <CheckboxField label="Consume from minimum" checked={form.consumeFromMinEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeFromMinEnable: checked }))} />
            <CheckboxField label="Broadcast consume" checked={form.consumeBroadcastEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeBroadcastEnable: checked }))} />
            <CheckboxField label="Orderly consume" checked={form.consumeMessageOrderly} onChange={(checked) => setForm((current) => ({ ...current, consumeMessageOrderly: checked }))} />
          </div>
        </div>

        {error ? <div className="inline-validation" role="alert">{error}</div> : null}

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button type="button" disabled={loading} onClick={() => void submit()}>
            {loading ? 'Saving' : mode === 'create' ? 'Create group' : 'Update group'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function NumberField({ label, value, onChange }: { label: string; value: number; onChange: (value: number) => void }) {
  const id = `consumer-${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  return (
    <div className="field">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="number"
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </div>
  );
}

function CheckboxField({ label, checked, onChange }: { label: string; checked: boolean; onChange: (checked: boolean) => void }) {
  return (
    <label className="compact-check">
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
      {label}
    </label>
  );
}
