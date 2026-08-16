import { useEffect, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupListItem, ConsumerOperationResult } from '../types/consumer';
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

interface ConsumerDeleteDialogProps {
  open: boolean;
  consumer: ConsumerGroupListItem | null;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: ConsumerOperationResult) => void;
}

export default function ConsumerDeleteDialog({
  open,
  consumer,
  onOpenChange,
  onSucceeded
}: ConsumerDeleteDialogProps) {
  const group = consumer?.rawGroupName ?? '';
  const [brokers, setBrokers] = useState<string[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [confirmation, setConfirmation] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !consumer) return;
    setSelected([]);
    setConfirmation('');
    setError(null);
    let cancelled = false;
    consumerApi
      .brokers(group)
      .then((result) => {
        if (cancelled) return;
        setBrokers(result.items.map((item) => item.brokerName).sort());
      })
      .catch((reason: unknown) => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      });
    return () => {
      cancelled = true;
    };
  }, [open, consumer, group]);

  const toggle = (brokerName: string, checked: boolean) => {
    setSelected((current) => checked
      ? Array.from(new Set([...current, brokerName]))
      : current.filter((value) => value !== brokerName));
  };

  const submit = async () => {
    if (selected.length === 0) {
      setError('Select at least one broker target.');
      return;
    }
    if (confirmation.trim() !== group) {
      setError(`Type the exact group name "${group}" to confirm.`);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const result = await consumerApi.delete(group, { brokerNames: selected });
      onSucceeded(result);
      onOpenChange(false);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete consumer group</DialogTitle>
          <DialogDescription>
            Delete {group} from selected broker targets. This is destructive and cannot be undone.
          </DialogDescription>
        </DialogHeader>

        <div className="consumer-delete-targets">
          {brokers.map((broker) => (
            <label key={broker} className="compact-check">
              <input
                type="checkbox"
                checked={selected.includes(broker)}
                onChange={(event) => toggle(broker, event.target.checked)}
              />
              {broker}
            </label>
          ))}
        </div>

        <div className="field">
          <label htmlFor="consumer-delete-confirm">Confirm consumer group</label>
          <Input
            id="consumer-delete-confirm"
            value={confirmation}
            onChange={(event) => setConfirmation(event.target.value)}
          />
        </div>

        {error ? <div className="inline-validation" role="alert">{error}</div> : null}

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button type="button" variant="destructive" disabled={loading || selected.length === 0 || confirmation.trim() !== group} onClick={() => void submit()}>
            {loading ? 'Deleting' : 'Delete consumer group'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
