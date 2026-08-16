import { useEffect, useId, useState } from 'react';
import EmptyState from '../../components/EmptyState';
import ErrorState from '../../components/ErrorState';
import LoadingState from '../../components/LoadingState';
import { Button } from '../../components/ui/Button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from '../../components/ui/Dialog';
import { Label } from '../../components/ui/Label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/Select';
import type { TopicConsumerView } from '../../types/topic';

export type TopicConsumerActionKind = 'reset' | 'skip';

interface TopicConsumerActionDialogProps {
  open: boolean;
  kind: TopicConsumerActionKind;
  topicName: string;
  consumers: TopicConsumerView[];
  loading: boolean;
  error: string | null;
  onRetry: () => void;
  onSelect: (consumerGroup: string) => void;
  onOpenChange: (open: boolean) => void;
}

export default function TopicConsumerActionDialog({
  open,
  kind,
  topicName,
  consumers,
  loading,
  error,
  onRetry,
  onSelect,
  onOpenChange
}: TopicConsumerActionDialogProps) {
  const [consumerGroup, setConsumerGroup] = useState('');
  const consumerId = useId();
  const actionLabel = kind === 'reset' ? 'reset consumer offset' : 'skip accumulated messages';

  useEffect(() => {
    setConsumerGroup('');
  }, [kind, open, topicName]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Choose consumer group for {actionLabel}</DialogTitle>
          <DialogDescription>
            Select a consumer group currently subscribed to {topicName}. No group is selected automatically.
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={(event) => { event.preventDefault(); if (consumerGroup) onSelect(consumerGroup); }}>
          {loading ? <LoadingState label="Loading topic consumers" /> : null}
          {!loading && error ? (
            <ErrorState message={error} onRetry={onRetry} retryLabel="Retry consumers" />
          ) : null}
          {!loading && !error && consumers.length === 0 ? (
            <div>
              <EmptyState title="No consumers" detail="No consumers subscribe to this topic." />
              <Button type="button" variant="outline" onClick={onRetry}>Reload consumers</Button>
            </div>
          ) : null}
          {!loading && !error && consumers.length > 0 ? (
            <div className="field">
              <Label htmlFor={consumerId}>Consumer group</Label>
              <Select value={consumerGroup} onValueChange={setConsumerGroup}>
                <SelectTrigger id={consumerId} aria-label="Consumer group">
                  <SelectValue placeholder="Choose a consumer group" />
                </SelectTrigger>
                <SelectContent>
                  {consumers.map((consumer) => (
                    <SelectItem key={consumer.consumerGroup} value={consumer.consumerGroup}>
                      {consumer.consumerGroup}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          ) : null}

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button type="submit" disabled={!consumerGroup}>Continue to {kind}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
