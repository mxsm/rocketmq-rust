import * as Dialog from '@radix-ui/react-dialog';
import { Save, X } from 'lucide-react';
import { useEffect, useState } from 'react';
import type { TopicInfo, TopicMutationRequest } from '../types/topic';
import ConfirmDialog from './ConfirmDialog';
import StatusBadge from './StatusBadge';

interface TopicMutationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialTopic?: TopicInfo | null;
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

export default function TopicMutationDialog({ open, onOpenChange, initialTopic, onSubmit }: TopicMutationDialogProps) {
  const [form, setForm] = useState<TopicFormState>(() => toFormState(initialTopic));
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (open) {
      setForm(toFormState(initialTopic));
      setError(null);
      setSubmitting(false);
    }
  }, [initialTopic, open]);

  const submit = () => {
    if (!form.topic.trim()) {
      setError('Topic name cannot be empty.');
      return;
    }
    if (form.readQueueCount <= 0 || form.writeQueueCount <= 0) {
      setError('Queue counts must be greater than zero.');
      return;
    }
    setSubmitting(true);
    setError(null);
    onSubmit({
      topic: form.topic.trim(),
      readQueueCount: Number(form.readQueueCount),
      writeQueueCount: Number(form.writeQueueCount),
      perm: Number(form.perm),
      brokerNameList: splitCsv(form.brokerNames),
      clusterNameList: splitCsv(form.clusterNames),
      order: form.ordered,
      messageType: form.messageType
    })
      .then(() => onOpenChange(false))
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
              <Dialog.Title>{initialTopic ? 'Update Topic' : 'Add / Update Topic'}</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status="Java /topic/createOrUpdate.do parity" tone="success" />
              </div>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>

          <div className="form-grid topic-form-grid">
            <label className="field field-wide">
              Topic name
              <input value={form.topic} disabled={Boolean(initialTopic)} onChange={(event) => setForm((value) => ({ ...value, topic: event.target.value }))} />
            </label>
            <label className="field">
              Cluster names
              <input
                value={form.clusterNames}
                placeholder="empty means all clusters"
                onChange={(event) => setForm((value) => ({ ...value, clusterNames: event.target.value }))}
              />
            </label>
            <label className="field">
              Broker names
              <input
                value={form.brokerNames}
                placeholder="empty means all brokers"
                onChange={(event) => setForm((value) => ({ ...value, brokerNames: event.target.value }))}
              />
            </label>
            <label className="field">
              Write Queue Nums
              <input
                type="number"
                min="1"
                value={form.writeQueueCount}
                onChange={(event) => setForm((value) => ({ ...value, writeQueueCount: Number(event.target.value) }))}
              />
            </label>
            <label className="field">
              Read Queue Nums
              <input
                type="number"
                min="1"
                value={form.readQueueCount}
                onChange={(event) => setForm((value) => ({ ...value, readQueueCount: Number(event.target.value) }))}
              />
            </label>
            <label className="field">
              Perm
              <input type="number" min="0" value={form.perm} onChange={(event) => setForm((value) => ({ ...value, perm: Number(event.target.value) }))} />
            </label>
            <label className="field">
              Message type
              <select value={form.messageType} onChange={(event) => setForm((value) => ({ ...value, messageType: event.target.value }))}>
                <option value="NORMAL">NORMAL</option>
                <option value="FIFO">FIFO</option>
                <option value="DELAY">DELAY</option>
                <option value="TRANSACTION">TRANSACTION</option>
              </select>
            </label>
            <label className="compact-check">
              <input type="checkbox" checked={form.ordered} onChange={(event) => setForm((value) => ({ ...value, ordered: event.target.checked }))} />
              Ordered topic
            </label>
          </div>

          {error ? <div className="notice notice-danger">{error}</div> : null}
          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
            <ConfirmDialog title="Save topic" description={`Apply topic metadata for ${form.topic || 'new topic'}?`} confirmLabel="Save" onConfirm={submit}>
              <button type="button" className="button" disabled={submitting}>
                <Save size={15} aria-hidden="true" /> {submitting ? 'Saving' : 'Save topic'}
              </button>
            </ConfirmDialog>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function toFormState(topic: TopicInfo | null | undefined): TopicFormState {
  return {
    topic: topic?.topic ?? '',
    clusterNames: '',
    brokerNames: topic?.brokerName ?? '',
    readQueueCount: topic?.readQueueCount ?? 8,
    writeQueueCount: topic?.writeQueueCount ?? 8,
    perm: topic?.perm ?? 6,
    messageType: normalizeMessageType(topic?.category),
    ordered: false
  };
}

function normalizeMessageType(category: string | undefined) {
  const normalized = category?.toUpperCase() ?? 'NORMAL';
  if (['FIFO', 'DELAY', 'TRANSACTION'].includes(normalized)) return normalized;
  return 'NORMAL';
}

function splitCsv(value: string) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}
