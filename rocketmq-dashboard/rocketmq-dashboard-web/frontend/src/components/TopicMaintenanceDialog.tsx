import * as Dialog from '@radix-ui/react-dialog';
import { RotateCcw, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupInfo } from '../types/consumer';
import ConfirmDialog from './ConfirmDialog';
import StatusBadge from './StatusBadge';
import { Button } from './ui/Button';

interface TopicMaintenanceDialogProps {
  topic: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  consumerGroups: ConsumerGroupInfo[];
  onMutationFinished: () => void;
}

export default function TopicMaintenanceDialog({
  topic,
  open,
  onOpenChange,
  consumerGroups,
  onMutationFinished
}: TopicMaintenanceDialogProps) {
  const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
  const [resetTime, setResetTime] = useState(() => toDateTimeLocal(Date.now()));
  const [force, setForce] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const sortedGroups = useMemo(() => [...consumerGroups].sort((left, right) => left.group.localeCompare(right.group)), [consumerGroups]);

  useEffect(() => {
    if (!open) return;
    setSelectedGroups([]);
    setResetTime(toDateTimeLocal(Date.now()));
    setForce(true);
    setError(null);
    setResult(null);
    setSubmitting(false);
  }, [open, topic]);

  const resetOffset = () => {
    if (!topic) return;
    if (selectedGroups.length === 0) {
      setError('Please select at least one consumer group.');
      return;
    }
    const resetTimestamp = new Date(resetTime).getTime();
    if (!Number.isFinite(resetTimestamp)) {
      setError('Please select a valid reset time.');
      return;
    }
    setSubmitting(true);
    setError(null);
    Promise.all(selectedGroups.map((group) => consumerApi.resetOffset(group, { topic, resetTimestamp, force })))
      .then(() => {
        setResult(`Reset offset submitted for ${selectedGroups.length} consumer group(s).`);
        onMutationFinished();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setSubmitting(false));
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content topic-modal">
          <div className="drawer-header">
            <div>
              <Dialog.Title className="dialog-title">
                <RotateCcw size={18} aria-hidden="true" />
                Reset Consumer Offset
              </Dialog.Title>
              <Dialog.Description asChild>
                <p className="dialog-description">Reset selected consumer groups to the specified timestamp for this topic.</p>
              </Dialog.Description>
              <div className="drawer-meta">
                <StatusBadge status={topic ?? 'topic pending'} tone="success" />
              </div>
            </div>
            <Dialog.Close asChild>
              <Button type="button" variant="ghost" size="icon" title="Close" aria-label="Close">
                <X size={15} aria-hidden="true" />
              </Button>
            </Dialog.Close>
          </div>

          <div className="form-grid topic-form-grid">
            <label className="field field-wide">
              Consumer groups
              <select
                className="multi-select"
                multiple
                value={selectedGroups}
                onChange={(event) => setSelectedGroups(Array.from(event.target.selectedOptions).map((option) => option.value))}
              >
                {sortedGroups.map((group) => (
                  <option key={group.group} value={group.group}>
                    {group.group}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              Reset time
              <input type="datetime-local" value={resetTime} onChange={(event) => setResetTime(event.target.value)} />
            </label>
            <label className="compact-check topic-force-check">
              <input type="checkbox" checked={force} onChange={(event) => setForce(event.target.checked)} />
              Force reset
            </label>
          </div>
          {error ? <div className="notice notice-danger">{error}</div> : null}
          {result ? <div className="notice notice-success">{result}</div> : null}

          <div className="dialog-actions">
            <Dialog.Close asChild>
              <Button type="button" variant="secondary">Close</Button>
            </Dialog.Close>
            <ConfirmDialog
              title="Reset consumer offset"
              description={`Reset ${selectedGroups.length || 'selected'} consumer group(s) for ${topic}?`}
              confirmLabel="Reset"
              onConfirm={resetOffset}
            >
              <Button type="button" variant="destructive" disabled={submitting}>
                <RotateCcw size={15} aria-hidden="true" /> {submitting ? 'Resetting' : 'Reset'}
              </Button>
            </ConfirmDialog>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function toDateTimeLocal(timestamp: number) {
  const date = new Date(timestamp);
  const offset = date.getTimezoneOffset();
  return new Date(date.getTime() - offset * 60_000).toISOString().slice(0, 16);
}
