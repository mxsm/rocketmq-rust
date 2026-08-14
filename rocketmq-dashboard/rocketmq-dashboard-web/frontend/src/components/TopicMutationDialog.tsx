import { Save } from 'lucide-react';
import { useEffect, useId, useState } from 'react';
import type { TopicMutationRequest } from '../types/topic';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from './ui/AlertDialog';
import { Button } from './ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface TopicMutationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: TopicMutationRequest) => Promise<void>;
}

interface TopicFormState {
  topic: string;
  clusterNames: string;
  brokerNames: string;
  readQueueCount: number;
  writeQueueCount: number;
  perm: number;
  messageType: string;
  ordered: boolean;
}

const MAX_U32 = 4_294_967_295;

export default function TopicMutationDialog({ open, onOpenChange, onSubmit }: TopicMutationDialogProps) {
  const [form, setForm] = useState<TopicFormState>(toFormState);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const formId = useId();

  useEffect(() => {
    if (open) {
      setForm(toFormState());
      setError(null);
      setSubmitting(false);
      setConfirmOpen(false);
    }
  }, [open]);

  const reviewChanges = () => {
    if (!form.topic.trim()) {
      setError('Topic name cannot be empty.');
      return;
    }
    if (splitCsv(form.clusterNames).length === 0 && splitCsv(form.brokerNames).length === 0) {
      setError('Choose at least one cluster or broker target.');
      return;
    }
    if (!isPositiveU32(form.readQueueCount) || !isPositiveU32(form.writeQueueCount)) {
      setError('Queue counts must be positive 32-bit integers.');
      return;
    }
    if (!Number.isInteger(form.perm) || form.perm < 0 || form.perm > 7) {
      setError('Permission must be an integer between 0 and 7.');
      return;
    }
    setError(null);
    setConfirmOpen(true);
  };

  const submit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      await onSubmit({
        topic: form.topic.trim(),
        readQueueCount: Number(form.readQueueCount),
        writeQueueCount: Number(form.writeQueueCount),
        perm: Number(form.perm),
        brokerNameList: splitCsv(form.brokerNames),
        clusterNameList: splitCsv(form.clusterNames),
        order: form.ordered,
        messageType: form.messageType
      });
      setConfirmOpen(false);
      onOpenChange(false);
    } catch (requestError) {
      setConfirmOpen(false);
      setError(requestError instanceof Error ? requestError.message : 'Unable to save the topic.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="entity-mutation-dialog">
          <DialogHeader>
            <DialogTitle>Create topic</DialogTitle>
            <DialogDescription>
              Configure queue capacity, placement, permissions, and the message contract.
            </DialogDescription>
          </DialogHeader>

          <div className="form-grid topic-form-grid">
            <div className="field field-wide">
              <Label htmlFor={`${formId}-topic`}>Topic name</Label>
              <Input
                id={`${formId}-topic`}
                value={form.topic}
                onChange={(event) => setForm((value) => ({ ...value, topic: event.target.value }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-clusters`}>Cluster names</Label>
              <Input
                id={`${formId}-clusters`}
                value={form.clusterNames}
                placeholder="Comma-separated target clusters"
                onChange={(event) => setForm((value) => ({ ...value, clusterNames: event.target.value }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-brokers`}>Broker names</Label>
              <Input
                id={`${formId}-brokers`}
                value={form.brokerNames}
                placeholder="Comma-separated target brokers"
                onChange={(event) => setForm((value) => ({ ...value, brokerNames: event.target.value }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-write-queues`}>Write queue count</Label>
              <Input
                id={`${formId}-write-queues`}
                type="number"
                min="1"
                value={form.writeQueueCount}
                onChange={(event) => setForm((value) => ({ ...value, writeQueueCount: Number(event.target.value) }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-read-queues`}>Read queue count</Label>
              <Input
                id={`${formId}-read-queues`}
                type="number"
                min="1"
                value={form.readQueueCount}
                onChange={(event) => setForm((value) => ({ ...value, readQueueCount: Number(event.target.value) }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-permission`}>Permission</Label>
              <Input
                id={`${formId}-permission`}
                type="number"
                min="0"
                max="7"
                value={form.perm}
                onChange={(event) => setForm((value) => ({ ...value, perm: Number(event.target.value) }))}
              />
            </div>
            <div className="field">
              <Label htmlFor={`${formId}-message-type`}>Message type</Label>
              <select
                id={`${formId}-message-type`}
                className="ui-select-native"
                value={form.messageType}
                onChange={(event) => setForm((value) => ({ ...value, messageType: event.target.value }))}
              >
                <option value="NORMAL">NORMAL</option>
                <option value="FIFO">FIFO</option>
                <option value="DELAY">DELAY</option>
                <option value="TRANSACTION">TRANSACTION</option>
              </select>
            </div>
            <label className="compact-check" htmlFor={`${formId}-ordered`}>
              <input
                id={`${formId}-ordered`}
                type="checkbox"
                checked={form.ordered}
                onChange={(event) => setForm((value) => ({ ...value, ordered: event.target.checked }))}
              />
              Ordered topic
            </label>
          </div>

          {error ? <div className="inline-validation" role="status">{error}</div> : null}
          <DialogFooter>
            <Button type="button" variant="secondary" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button type="button" onClick={reviewChanges} disabled={submitting}>
              <Save size={15} aria-hidden="true" /> Save topic
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogTitle>Create topic?</AlertDialogTitle>
          <AlertDialogDescription>
            Create {form.topic.trim() || 'this topic'} on the selected cluster and broker targets with the current queue and permission settings?
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={submitting}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={submitting}
              onClick={(event) => {
                event.preventDefault();
                void submit();
              }}
            >
              {submitting ? 'Creating' : 'Create topic'}
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function toFormState(): TopicFormState {
  return {
    topic: '',
    clusterNames: '',
    brokerNames: '',
    readQueueCount: 8,
    writeQueueCount: 8,
    perm: 6,
    messageType: 'NORMAL',
    ordered: false
  };
}

function isPositiveU32(value: number) {
  return Number.isInteger(value) && value > 0 && value <= MAX_U32;
}

function splitCsv(value: string) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}
