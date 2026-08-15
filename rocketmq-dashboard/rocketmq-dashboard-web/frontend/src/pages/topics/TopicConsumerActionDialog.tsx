import { useEffect, useState } from 'react';
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
          <label className="native-filter-field">
            <span>Consumer group</span>
            <select
              className="ui-select-native"
              aria-label="Consumer group"
              value={consumerGroup}
              onChange={(event) => setConsumerGroup(event.target.value)}
            >
              <option value="">Choose a consumer group</option>
              {consumers.map((consumer) => (
                <option key={consumer.consumerGroup} value={consumer.consumerGroup}>
                  {consumer.consumerGroup}
                </option>
              ))}
            </select>
          </label>
        ) : null}

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button
            type="button"
            disabled={!consumerGroup}
            onClick={() => onSelect(consumerGroup)}
          >
            Continue to {kind}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
