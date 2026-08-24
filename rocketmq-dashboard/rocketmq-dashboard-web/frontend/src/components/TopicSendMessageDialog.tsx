import { Send } from 'lucide-react';
import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { topicApi } from '../api/topic_api';
import { handleAppliedAuditFailure } from '../api/client';
import type { TopicSendResultView, TopicTestMessageRequest } from '../types/topic';
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

interface TopicSendMessageDialogProps {
  open: boolean;
  topic: string;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: TopicSendResultView) => void;
  onAppliedAuditFailure?: () => Promise<void> | void;
}

interface SendForm {
  key: string;
  tag: string;
  messageBody: string;
  traceEnabled: boolean;
}

interface SendConfirmation {
  generation: number;
  topic: string;
  request: TopicTestMessageRequest;
}

const emptyForm = (): SendForm => ({ key: '', tag: '', messageBody: '', traceEnabled: false });

export default function TopicSendMessageDialog({
  open,
  topic,
  onOpenChange,
  onSucceeded,
  onAppliedAuditFailure
}: TopicSendMessageDialogProps) {
  const [form, setForm] = useState<SendForm>(emptyForm);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TopicSendResultView | null>(null);
  const [confirmation, setConfirmation] = useState<SendConfirmation | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingRef = useRef<number | null>(null);
  const requestRef = useRef(0);
  const generationRef = useRef(0);
  const mountedRef = useRef(false);
  const openRef = useRef(open);
  const topicRef = useRef(topic);
  const reviewButtonRef = useRef<HTMLButtonElement>(null);
  const formId = useId();

  openRef.current = open;
  topicRef.current = topic;

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
    if (open) setForm(emptyForm());
  }, [open, topic]);

  const invalidateAndClose = () => {
    generationRef.current += 1;
    setConfirmation(null);
    onOpenChange(false);
  };

  const reviewSend = (event: FormEvent) => {
    event.preventDefault();
    if (!form.messageBody.trim()) {
      setError('Message body is required.');
      setResult(null);
      return;
    }

    setError(null);
    setResult(null);
    setConfirmation({
      generation: generationRef.current,
      topic,
      request: {
        key: form.key,
        tag: form.tag,
        messageBody: form.messageBody,
        traceEnabled: form.traceEnabled
      }
    });
  };

  const focusReview = () => {
    window.setTimeout(() => reviewButtonRef.current?.focus(), 0);
  };

  const isCurrentPresentation = (snapshot: SendConfirmation) => (
    mountedRef.current
    && openRef.current
    && generationRef.current === snapshot.generation
    && topicRef.current === snapshot.topic
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
      const nextResult = await topicApi.sendTestMessage(snapshot.topic, snapshot.request);
      if (!isCurrentPresentation(snapshot)) return;

      setConfirmation(null);
      setResult(nextResult);
      if (nextResult.success) onSucceeded(nextResult);
      focusReview();
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

      setConfirmation(null);
      setError(requestError instanceof Error ? requestError.message : 'Unable to send the test message.');
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
            <DialogTitle>Send test message</DialogTitle>
            <DialogDescription>
              Send one broker-backed test message and inspect the complete broker outcome.
            </DialogDescription>
          </DialogHeader>

          <form noValidate onSubmit={reviewSend}>
            <div className="form-grid topic-form-grid">
              <div className="field field-wide">
                <Label htmlFor={`${formId}-topic`}>Topic</Label>
                <Input id={`${formId}-topic`} value={topic} disabled />
              </div>
              <div className="field">
                <Label htmlFor={`${formId}-key`}>Key</Label>
                <Input
                  id={`${formId}-key`}
                  value={form.key}
                  disabled={submitting}
                  onChange={(event) => setForm((current) => ({ ...current, key: event.target.value }))}
                />
              </div>
              <div className="field">
                <Label htmlFor={`${formId}-tag`}>Tag</Label>
                <Input
                  id={`${formId}-tag`}
                  value={form.tag}
                  disabled={submitting}
                  onChange={(event) => setForm((current) => ({ ...current, tag: event.target.value }))}
                />
              </div>
              <div className="field field-wide">
                <Label htmlFor={`${formId}-body`}>Message body</Label>
                <textarea
                  id={`${formId}-body`}
                  className="ui-input"
                  rows={8}
                  value={form.messageBody}
                  disabled={submitting}
                  onChange={(event) => setForm((current) => ({ ...current, messageBody: event.target.value }))}
                />
              </div>
              <label className="compact-check" htmlFor={`${formId}-trace`}>
                <input
                  id={`${formId}-trace`}
                  type="checkbox"
                  checked={form.traceEnabled}
                  disabled={submitting}
                  onChange={(event) => setForm((current) => ({ ...current, traceEnabled: event.target.checked }))}
                />
                Enable trace
              </label>
            </div>

            {error ? <div className="notice notice-danger" role="alert">{error}</div> : null}
            {result ? <SendResult result={result} /> : null}

            <DialogFooter>
              <Button type="button" variant="secondary" onClick={invalidateAndClose}>Cancel</Button>
              <Button ref={reviewButtonRef} type="submit" disabled={submitting}>
                <Send size={15} aria-hidden="true" /> Review send
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
          <AlertDialogTitle>Send test message?</AlertDialogTitle>
          <AlertDialogDescription>
            Send the captured message to {confirmation?.topic ?? topic}? The exact key, tag, body, and trace choice shown in the form will be used.
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={submitting}>Cancel</AlertDialogCancel>
            <Button type="button" disabled={submitting} onClick={() => void submit()}>
              {submitting ? 'Sending' : 'Send test message'}
            </Button>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function SendResult({ result }: { result: TopicSendResultView }) {
  return (
    <div className={`notice ${result.success ? 'notice-success' : 'notice-danger'}`} role={result.success ? 'status' : 'alert'}>
      <strong>{result.success ? 'Message sent.' : `Broker send did not succeed: ${result.sendStatus}`}</strong>
      <dl className="detail-list">
        <ResultField label="Send status" value={result.sendStatus} />
        <ResultField label="Message ID" value={result.messageId} />
        <ResultField label="Broker" value={result.brokerName} />
        <ResultField label="Queue ID" value={result.queueId} />
        <ResultField label="Queue offset" value={result.queueOffset} />
        <ResultField label="Transaction ID" value={result.transactionId} />
        <ResultField label="Region" value={result.regionId} />
        <ResultField label="Local transaction state" value={result.localTransactionState} />
      </dl>
    </div>
  );
}

function ResultField({ label, value }: { label: string; value: string | number | null }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value ?? 'Not reported'}</dd>
    </div>
  );
}
