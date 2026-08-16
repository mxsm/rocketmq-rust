import { RotateCcw } from 'lucide-react';
import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { topicApi } from '../api/topic_api';
import type { TopicOffsetResult, TopicResetOffsetRequest } from '../types/topic';
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from './ui/AlertDialog';
import { Button } from './ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface TopicResetOffsetDialogProps {
  open: boolean;
  topic: string;
  consumerGroup: string;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: TopicOffsetResult) => void;
}

interface ResetConfirmation {
  generation: number;
  topic: string;
  consumerGroup: string;
  request: TopicResetOffsetRequest;
  localTimeLabel: string;
}

export interface LocalDateTimeFields {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
}

export default function TopicResetOffsetDialog({
  open,
  topic,
  consumerGroup,
  onOpenChange,
  onSucceeded
}: TopicResetOffsetDialogProps) {
  const [resetTime, setResetTime] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TopicOffsetResult | null>(null);
  const [confirmation, setConfirmation] = useState<ResetConfirmation | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingRef = useRef<number | null>(null);
  const requestRef = useRef(0);
  const generationRef = useRef(0);
  const mountedRef = useRef(false);
  const openRef = useRef(open);
  const topicRef = useRef(topic);
  const consumerGroupRef = useRef(consumerGroup);
  const reviewButtonRef = useRef<HTMLButtonElement>(null);
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
    setConfirmation(null);
    setError(null);
    setResult(null);
    if (pendingRef.current === null) setSubmitting(false);
    if (open) setResetTime('');
  }, [consumerGroup, open, topic]);

  const invalidateAndClose = () => {
    generationRef.current += 1;
    setConfirmation(null);
    onOpenChange(false);
  };

  const reviewReset = (event: FormEvent) => {
    event.preventDefault();
    const resetTimestamp = parseLocalDateTime(resetTime);
    if (resetTimestamp === null) {
      setError('Select a valid reset time.');
      setResult(null);
      return;
    }

    setError(null);
    setResult(null);
    setConfirmation({
      generation: generationRef.current,
      topic,
      consumerGroup,
      request: { consumerGroup, resetTimestamp, force: true },
      localTimeLabel: new Date(resetTimestamp).toLocaleString()
    });
  };

  const focusReview = () => {
    window.setTimeout(() => reviewButtonRef.current?.focus(), 0);
  };

  const isCurrentPresentation = (snapshot: ResetConfirmation) => (
    mountedRef.current
    && openRef.current
    && generationRef.current === snapshot.generation
    && topicRef.current === snapshot.topic
    && consumerGroupRef.current === snapshot.consumerGroup
  );

  const submit = async () => {
    const snapshot = confirmation;
    if (!snapshot || !isCurrentPresentation(snapshot)) return;
    if (pendingRef.current !== null) return;

    const requestId = requestRef.current + 1;
    requestRef.current = requestId;
    pendingRef.current = requestId;
    setSubmitting(true);
    setError(null);
    setResult(null);

    try {
      const nextResult = await topicApi.resetOffset(snapshot.topic, snapshot.request);
      if (!isCurrentPresentation(snapshot)) return;

      setConfirmation(null);
      setResult(nextResult);
      if (nextResult.success) onSucceeded(nextResult);
      focusReview();
    } catch (requestError) {
      if (!isCurrentPresentation(snapshot)) return;

      setConfirmation(null);
      setError(requestError instanceof Error ? requestError.message : 'Unable to reset the consumer offset.');
      focusReview();
    } finally {
      if (pendingRef.current === requestId) {
        pendingRef.current = null;
        if (mountedRef.current) setSubmitting(false);
      }
    }
  };

  return (
    <>
      <Dialog open={open} onOpenChange={(nextOpen) => { if (!nextOpen) invalidateAndClose(); }}>
        <DialogContent className="entity-mutation-dialog">
          <DialogHeader>
            <DialogTitle>Reset consumer offset</DialogTitle>
            <DialogDescription>
              Move this consumer group to an explicit local date and time with force enabled.
            </DialogDescription>
          </DialogHeader>

          <form noValidate onSubmit={reviewReset}>
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
                <Label htmlFor={`${formId}-time`}>Reset time</Label>
                <Input
                  id={`${formId}-time`}
                  type="datetime-local"
                  value={resetTime}
                  disabled={submitting}
                  onChange={(event) => setResetTime(event.target.value)}
                />
              </div>
            </div>

            {error ? <div className="notice notice-danger" role="alert">{error}</div> : null}
            {result ? <OffsetResult result={result} successLabel="Offset reset completed." /> : null}

            <DialogFooter>
              <Button type="button" variant="secondary" onClick={invalidateAndClose}>Cancel</Button>
              <Button ref={reviewButtonRef} type="submit" disabled={submitting}>
                <RotateCcw size={15} aria-hidden="true" /> Review reset
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(nextOpen) => {
          if (!nextOpen && !submitting) {
            setConfirmation(null);
            focusReview();
          }
        }}
      >
        <AlertDialogContent>
          <AlertDialogTitle>Reset consumer offset?</AlertDialogTitle>
          <AlertDialogDescription>
            Reset {confirmation?.consumerGroup ?? consumerGroup} on {confirmation?.topic ?? topic} to{' '}
            {confirmation?.localTimeLabel ?? 'the selected local time'} with force enabled?
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={submitting}>Cancel</AlertDialogCancel>
            <Button type="button" disabled={submitting} onClick={() => void submit()}>
              {submitting ? 'Resetting' : 'Reset offset'}
            </Button>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

export function parseLocalDateTime(value: string): number | null {
  const match = /^(\d{4,6})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
  if (!match) return null;

  const fields: LocalDateTimeFields = {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
    hour: Number(match[4]),
    minute: Number(match[5]),
    second: Number(match[6] ?? 0)
  };
  if (
    fields.month < 1 || fields.month > 12
    || fields.day < 1 || fields.day > 31
    || fields.hour < 0 || fields.hour > 23
    || fields.minute < 0 || fields.minute > 59
    || fields.second < 0 || fields.second > 59
  ) return null;

  const localDate = new Date(
    fields.year,
    fields.month - 1,
    fields.day,
    fields.hour,
    fields.minute,
    fields.second,
    0
  );
  const resolvedFields: LocalDateTimeFields = {
    year: localDate.getFullYear(),
    month: localDate.getMonth() + 1,
    day: localDate.getDate(),
    hour: localDate.getHours(),
    minute: localDate.getMinutes(),
    second: localDate.getSeconds()
  };
  const timestamp = localDate.getTime();
  if (
    !hasExactLocalDateTimeFields(fields, resolvedFields)
    || !Number.isSafeInteger(timestamp)
    || timestamp < 0
  ) return null;

  return timestamp;
}

export function hasExactLocalDateTimeFields(
  expected: LocalDateTimeFields,
  resolved: LocalDateTimeFields
): boolean {
  return expected.year === resolved.year
    && expected.month === resolved.month
    && expected.day === resolved.day
    && expected.hour === resolved.hour
    && expected.minute === resolved.minute
    && expected.second === resolved.second;
}

function OffsetResult({ result, successLabel }: { result: TopicOffsetResult; successLabel: string }) {
  return (
    <div className={`notice ${result.success ? 'notice-success' : 'notice-danger'}`} role={result.success ? 'status' : 'alert'}>
      <strong>{result.success ? successLabel : result.message}</strong>
      <dl className="detail-list">
        <div><dt>Affected queues</dt><dd>{result.affectedQueueCount}</dd></div>
        <div><dt>Applied time</dt><dd>{new Date(result.appliedTimestamp).toLocaleString()}</dd></div>
      </dl>
    </div>
  );
}
