import * as Dialog from '@radix-ui/react-dialog';
import { FastForward, RotateCcw, Send, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupInfo } from '../types/consumer';
import ConfirmDialog from './ConfirmDialog';
import StatusBadge from './StatusBadge';

export type TopicMaintenanceMode = 'send' | 'reset' | 'skip';

interface TopicMaintenanceDialogProps {
  topic: string | null;
  mode: TopicMaintenanceMode;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  consumerGroups: ConsumerGroupInfo[];
  onMutationFinished: () => void;
}

const modeCopy = {
  send: {
    title: 'Send Message',
    description: 'Java API /topic/sendTopicMessage.do sends a test message to the selected topic.',
    icon: Send
  },
  reset: {
    title: 'Reset Consumer Offset',
    description: 'Reset selected consumer groups to the specified timestamp for this topic.',
    icon: RotateCcw
  },
  skip: {
    title: 'Skip Message Accumulate',
    description: 'Java API /consumer/skipAccumulate.do moves selected consumer groups to the latest offset.',
    icon: FastForward
  }
};

export default function TopicMaintenanceDialog({
  topic,
  mode,
  open,
  onOpenChange,
  consumerGroups,
  onMutationFinished
}: TopicMaintenanceDialogProps) {
  const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
  const [resetTime, setResetTime] = useState(() => toDateTimeLocal(Date.now()));
  const [force, setForce] = useState(true);
  const [tag, setTag] = useState('');
  const [messageKey, setMessageKey] = useState('');
  const [messageBody, setMessageBody] = useState('');
  const [traceEnabled, setTraceEnabled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const copy = modeCopy[mode];
  const Icon = copy.icon;
  const sortedGroups = useMemo(() => [...consumerGroups].sort((left, right) => left.group.localeCompare(right.group)), [consumerGroups]);

  useEffect(() => {
    if (!open) return;
    setSelectedGroups([]);
    setResetTime(toDateTimeLocal(Date.now()));
    setForce(true);
    setTag('');
    setMessageKey('');
    setMessageBody('');
    setTraceEnabled(false);
    setError(null);
    setResult(null);
    setSubmitting(false);
  }, [mode, open, topic]);

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

  const notImplementedMessage =
    mode === 'send'
      ? 'Rust backend does not expose /api/topics/:topic/send-message yet. Keep this Java-compatible dialog as an M2 entry point.'
      : 'Rust backend does not expose /api/consumers/:group/skip-accumulate yet. Keep this Java-compatible dialog as an M2 entry point.';

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content topic-modal">
          <div className="drawer-header">
            <div>
              <Dialog.Title className="dialog-title">
                <Icon size={18} aria-hidden="true" />
                {copy.title}
              </Dialog.Title>
              <p className="dialog-description">{copy.description}</p>
              <div className="drawer-meta">
                <StatusBadge status={topic ?? 'topic pending'} tone="success" />
                {mode !== 'reset' ? <StatusBadge status="M2 backend API pending" tone="warning" /> : null}
              </div>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>

          {mode === 'send' ? (
            <div className="form-grid topic-form-grid">
              <label className="field field-wide">
                Topic
                <input value={topic ?? ''} disabled />
              </label>
              <label className="field">
                Tag
                <input value={tag} onChange={(event) => setTag(event.target.value)} />
              </label>
              <label className="field">
                Key
                <input value={messageKey} onChange={(event) => setMessageKey(event.target.value)} />
              </label>
              <label className="field field-wide">
                Message body
                <textarea value={messageBody} onChange={(event) => setMessageBody(event.target.value)} />
              </label>
              <label className="compact-check">
                <input type="checkbox" checked={traceEnabled} onChange={(event) => setTraceEnabled(event.target.checked)} />
                Enable message trace
              </label>
            </div>
          ) : null}

          {mode === 'reset' || mode === 'skip' ? (
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
              {mode === 'reset' ? (
                <>
                  <label className="field">
                    Reset time
                    <input type="datetime-local" value={resetTime} onChange={(event) => setResetTime(event.target.value)} />
                  </label>
                  <label className="compact-check topic-force-check">
                    <input type="checkbox" checked={force} onChange={(event) => setForce(event.target.checked)} />
                    Force reset
                  </label>
                </>
              ) : null}
            </div>
          ) : null}

          {mode !== 'reset' ? <div className="notice notice-danger">{notImplementedMessage}</div> : null}
          {error ? <div className="notice notice-danger">{error}</div> : null}
          {result ? <div className="notice notice-success">{result}</div> : null}

          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Close</Dialog.Close>
            {mode === 'reset' ? (
              <ConfirmDialog
                title="Reset consumer offset"
                description={`Reset ${selectedGroups.length || 'selected'} consumer group(s) for ${topic}?`}
                confirmLabel="Reset"
                onConfirm={resetOffset}
              >
                <button type="button" className="button button-danger" disabled={submitting}>
                  <RotateCcw size={15} aria-hidden="true" /> {submitting ? 'Resetting' : 'Reset'}
                </button>
              </ConfirmDialog>
            ) : (
              <button type="button" className="button button-secondary" disabled>
                {mode === 'send' ? <Send size={15} aria-hidden="true" /> : <FastForward size={15} aria-hidden="true" />}
                Backend pending
              </button>
            )}
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
