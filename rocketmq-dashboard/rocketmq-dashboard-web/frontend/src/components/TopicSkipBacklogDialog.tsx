import { SkipForward } from 'lucide-react';
import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { topicApi } from '../api/topic_api';
import { handleAppliedAuditFailure } from '../api/client';
import type { TopicOffsetResult, TopicSkipOffsetRequest } from '../types/topic';
import { Button } from './ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface TopicSkipBacklogDialogProps {
  open: boolean;
  topic: string;
  consumerGroup: string;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: TopicOffsetResult) => void;
  onAppliedAuditFailure?: () => Promise<void> | void;
}

interface SkipSnapshot {
  generation: number;
  topic: string;
  consumerGroup: string;
  request: TopicSkipOffsetRequest;
}

export default function TopicSkipBacklogDialog({
  open,
  topic,
  consumerGroup,
  onOpenChange,
  onSucceeded,
  onAppliedAuditFailure
}: TopicSkipBacklogDialogProps) {
  const [confirmationText, setConfirmationText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TopicOffsetResult | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingRef = useRef<number | null>(null);
  const requestRef = useRef(0);
  const generationRef = useRef(0);
  const mountedRef = useRef(false);
  const openRef = useRef(open);
  const topicRef = useRef(topic);
  const consumerGroupRef = useRef(consumerGroup);
  const actionButtonRef = useRef<HTMLButtonElement>(null);
  const formId = useId();

  openRef.current = open;
  topicRef.current = topic;
  consumerGroupRef.current = consumerGroup;

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      generationRef.current += 1;
    };
  }, []);

  useEffect(() => {
    generationRef.current += 1;
    setError(null);
    setResult(null);
    if (pendingRef.current === null) setSubmitting(false);
    if (open) setConfirmationText('');
  }, [consumerGroup, open, topic]);

  const invalidateAndClose = () => {
    generationRef.current += 1;
    onOpenChange(false);
  };

  const focusAction = () => {
    window.setTimeout(() => actionButtonRef.current?.focus(), 0);
  };

  const isCurrentPresentation = (snapshot: SkipSnapshot) => (
    mountedRef.current
    && openRef.current
    && generationRef.current === snapshot.generation
    && topicRef.current === snapshot.topic
    && consumerGroupRef.current === snapshot.consumerGroup
  );

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (confirmationText !== consumerGroup || pendingRef.current !== null) return;

    const snapshot: SkipSnapshot = {
      generation: generationRef.current,
      topic,
      consumerGroup,
      request: { consumerGroup }
    };
    const requestId = requestRef.current + 1;
    requestRef.current = requestId;
    pendingRef.current = requestId;
    setSubmitting(true);
    setError(null);
    setResult(null);

    try {
      const nextResult = await topicApi.skipBacklog(snapshot.topic, snapshot.request);
      if (!isCurrentPresentation(snapshot)) return;

      setResult(nextResult);
      if (nextResult.success) {
        setConfirmationText('');
        onSucceeded(nextResult);
      }
      focusAction();
    } catch (requestError) {
      if (!isCurrentPresentation(snapshot)) return;

      if (await handleAppliedAuditFailure(requestError, {
        onApplied: () => {
          invalidateAndClose();
          setError(null);
          setResult(null);
        },
        refresh: onAppliedAuditFailure
      })) return;

      setError(requestError instanceof Error ? requestError.message : 'Unable to skip accumulated messages.');
      focusAction();
    } finally {
      if (pendingRef.current === requestId) {
        pendingRef.current = null;
        if (mountedRef.current) setSubmitting(false);
      }
    }
  };

  return (
    <Dialog open={open} onOpenChange={(nextOpen) => { if (!nextOpen) invalidateAndClose(); }}>
      <DialogContent className="entity-mutation-dialog">
        <DialogHeader>
          <DialogTitle>Skip accumulated messages</DialogTitle>
          <DialogDescription>
            Unread messages currently in the backlog will be skipped for this consumer group. This discarded backlog cannot be consumed afterward.
          </DialogDescription>
        </DialogHeader>

        <form noValidate onSubmit={(event) => void submit(event)}>
          <div className="form-grid topic-form-grid">
            <div className="field">
              <Label htmlFor={`${formId}-topic`}>Topic</Label>
              <Input id={`${formId}-topic`} value={topic} disabled />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-group`}>Consumer group</Label>
              <Input id={`${formId}-group`} value={consumerGroup} disabled />
            </div>
            <div className="field field-wide">
              <Label htmlFor={`${formId}-confirmation`}>Confirm consumer group</Label>
              <Input
                id={`${formId}-confirmation`}
                value={confirmationText}
                disabled={submitting}
                autoComplete="off"
                onChange={(event) => setConfirmationText(event.target.value)}
              />
              <span className="ui-dialog-description">Type {consumerGroup} exactly to confirm this destructive operation.</span>
            </div>
          </div>

          {error ? <div className="notice notice-danger" role="alert">{error}</div> : null}
          {result ? <OffsetResult result={result} /> : null}

          <DialogFooter>
            <Button type="button" variant="secondary" onClick={invalidateAndClose}>Cancel</Button>
            <Button
              ref={actionButtonRef}
              type="submit"
              variant="destructive"
              disabled={submitting || confirmationText !== consumerGroup}
              aria-busy={submitting}
            >
              <SkipForward size={15} aria-hidden="true" />
              Skip accumulated messages
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function OffsetResult({ result }: { result: TopicOffsetResult }) {
  return (
    <div className={`notice ${result.success ? 'notice-success' : 'notice-danger'}`} role={result.success ? 'status' : 'alert'}>
      <strong>{result.message}</strong>
      <dl className="detail-list">
        <div><dt>Affected queues</dt><dd>{result.affectedQueueCount}</dd></div>
        <div><dt>Applied time</dt><dd>{new Date(result.appliedTimestamp).toLocaleString()}</dd></div>
      </dl>
    </div>
  );
}
