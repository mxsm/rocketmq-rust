import { Trash2 } from 'lucide-react';
import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { topicApi } from '../api/topic_api';
import type { TopicInfo, TopicOperationResult } from '../types/topic';
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from './ui/AlertDialog';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { Label } from './ui/Label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/Select';

export interface TopicDeleteDialogProps {
  open: boolean;
  topic: TopicInfo | null;
  mode: 'broker' | 'topic';
  brokerName?: string;
  onOpenChange: (open: boolean) => void;
  onResult?: (result: TopicOperationResult) => void;
  onSucceeded: (result: TopicOperationResult) => void;
}

interface DeleteSnapshot {
  generation: number;
  topicName: string;
  mode: 'broker' | 'topic';
  brokerName?: string;
}

export default function TopicDeleteDialog({
  open,
  topic,
  mode,
  brokerName,
  onOpenChange,
  onResult,
  onSucceeded
}: TopicDeleteDialogProps) {
  const topicName = topic?.topic ?? '';
  const [selectedBroker, setSelectedBroker] = useState(() => initialBroker(topic, brokerName));
  const [confirmationText, setConfirmationText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TopicOperationResult | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingRef = useRef<number | null>(null);
  const requestRef = useRef(0);
  const generationRef = useRef(0);
  const mountedRef = useRef(false);
  const openRef = useRef(open);
  const topicNameRef = useRef(topicName);
  const modeRef = useRef(mode);
  const selectedBrokerRef = useRef(selectedBroker);
  const inputId = useId();
  const brokerId = useId();
  const authoritativeClusters = topic?.clusters.filter((cluster) => cluster.trim().length > 0) ?? [];

  openRef.current = open;
  topicNameRef.current = topicName;
  modeRef.current = mode;
  selectedBrokerRef.current = selectedBroker;

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      generationRef.current += 1;
    };
  }, []);

  useEffect(() => {
    generationRef.current += 1;
    setSelectedBroker(initialBroker(topic, brokerName));
    setConfirmationText('');
    setError(null);
    setResult(null);
    if (pendingRef.current === null) setSubmitting(false);
  }, [brokerName, mode, open, topic, topicName]);

  const invalidateAndClose = () => {
    generationRef.current += 1;
    onOpenChange(false);
  };

  const canDelete = Boolean(
    topic
    && !topic.systemTopic
    && confirmationText === topicName
    && (mode === 'topic' ? authoritativeClusters.length > 0 : topic.brokers.includes(selectedBroker))
  );

  const isCurrentPresentation = (snapshot: DeleteSnapshot) => (
    mountedRef.current
    && openRef.current
    && generationRef.current === snapshot.generation
    && topicNameRef.current === snapshot.topicName
    && modeRef.current === snapshot.mode
    && (snapshot.mode === 'topic' || selectedBrokerRef.current === snapshot.brokerName)
  );

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!topic || !canDelete || pendingRef.current !== null) return;

    const snapshot: DeleteSnapshot = {
      generation: generationRef.current,
      topicName,
      mode,
      brokerName: mode === 'broker' ? selectedBroker : undefined
    };
    if (snapshot.mode === 'broker' && !topic.brokers.includes(snapshot.brokerName ?? '')) return;

    const requestId = requestRef.current + 1;
    requestRef.current = requestId;
    pendingRef.current = requestId;
    setSubmitting(true);
    setError(null);
    setResult(null);

    try {
      const nextResult = snapshot.mode === 'broker'
        ? await topicApi.deleteFromBroker(snapshot.topicName, snapshot.brokerName!)
        : await topicApi.delete(snapshot.topicName);
      if (!isCurrentPresentation(snapshot)) return;

      onResult?.(nextResult);
      if (nextResult.success) {
        onSucceeded(nextResult);
        onOpenChange(false);
      } else {
        setResult(nextResult);
      }
    } catch (requestError) {
      if (!isCurrentPresentation(snapshot)) return;
      setError(requestError instanceof Error ? requestError.message : 'Unable to delete the topic.');
    } finally {
      if (pendingRef.current === requestId) {
        pendingRef.current = null;
        if (mountedRef.current) setSubmitting(false);
      }
    }
  };

  const title = mode === 'broker' ? 'Delete topic from broker' : 'Delete topic';
  const actionLabel = mode === 'broker' ? 'Delete from broker' : 'Delete topic';
  const unavailableReason = !topic
    ? 'Topic metadata is unavailable.'
    : topic.systemTopic
      ? 'System topics cannot be deleted.'
      : mode === 'broker' && topic.brokers.length === 0
        ? 'No broker targets are available for this topic.'
        : mode === 'topic' && authoritativeClusters.length === 0
          ? 'No authoritative cluster targets are available for this topic.'
        : null;

  return (
    <AlertDialog open={open} onOpenChange={(nextOpen) => { if (!nextOpen) invalidateAndClose(); }}>
      <AlertDialogContent className="entity-mutation-dialog">
        <AlertDialogTitle>{title}</AlertDialogTitle>
        <AlertDialogDescription>
          {mode === 'broker'
            ? `Remove ${topicName || 'this topic'} from one broker in its current route.`
            : `Remove ${topicName || 'this topic'} across its resolved cluster and broker scope.`}
        </AlertDialogDescription>

        <form noValidate onSubmit={(event) => void submit(event)}>
          {mode === 'broker' ? (
            <div className="field">
              <Label htmlFor={brokerId}>Broker</Label>
              <Select
                value={selectedBroker}
                disabled={submitting || !topic || topic.brokers.length === 0}
                onValueChange={setSelectedBroker}
              >
                <SelectTrigger id={brokerId} aria-label="Broker">
                  <SelectValue placeholder="Select a broker" />
                </SelectTrigger>
                <SelectContent>
                  {(topic?.brokers ?? []).map((name) => (
                    <SelectItem key={name} value={name}>{name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          ) : topic ? (
            <dl className="detail-list">
              <div><dt>Clusters</dt><dd>{authoritativeClusters.join(', ') || 'None reported'}</dd></div>
              <div><dt>Brokers</dt><dd>{topic.brokers.join(', ') || 'None reported'}</dd></div>
            </dl>
          ) : null}

          <div className="field">
            <Label htmlFor={inputId}>Confirm topic name</Label>
            <Input
              id={inputId}
              value={confirmationText}
              disabled={submitting || !topic || topic.systemTopic}
              autoComplete="off"
              onChange={(event) => setConfirmationText(event.target.value)}
            />
            <span className="ui-dialog-description">
              Type {topicName || 'the exact topic name'} exactly to confirm this destructive operation.
            </span>
          </div>

          {unavailableReason ? <div className="notice notice-danger" role="alert">{unavailableReason}</div> : null}
          {error ? <div className="notice notice-danger" role="alert">{error}</div> : null}
          {result ? <OperationResult result={result} /> : null}

          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel type="button" disabled={false}>Cancel</AlertDialogCancel>
            <Button
              type="submit"
              variant="destructive"
              disabled={!canDelete || submitting}
              aria-busy={submitting}
            >
              <Trash2 size={15} aria-hidden="true" /> {submitting ? 'Deleting' : actionLabel}
            </Button>
          </div>
        </form>
      </AlertDialogContent>
    </AlertDialog>
  );
}

function initialBroker(topic: TopicInfo | null, brokerName?: string) {
  return brokerName && topic?.brokers.includes(brokerName) ? brokerName : '';
}

function OperationResult({ result }: { result: TopicOperationResult }) {
  return (
    <div className={`notice ${result.success ? 'notice-success' : 'notice-danger'}`} role={result.success ? 'status' : 'alert'}>
      <strong>{result.message}</strong>
      <dl className="detail-list">
        <div><dt>Target count</dt><dd>{result.targetCount}</dd></div>
      </dl>
      <ul className="topic-operation-targets">
        {result.targets.map((target) => (
          <li key={target.target}>
            <strong>{target.target}</strong>
            <span>{target.success ? 'Succeeded' : 'Failed'}</span>
            <span>{target.message}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
