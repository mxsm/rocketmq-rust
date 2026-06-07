import * as Dialog from '@radix-ui/react-dialog';
import { Save, X } from 'lucide-react';
import { useEffect, useState } from 'react';
import type { ConsumerGroupInfo } from '../types/consumer';

interface ConsumerMutationDialogProps {
  open: boolean;
  consumer?: ConsumerGroupInfo | null;
  onOpenChange: (open: boolean) => void;
}

interface ConsumerDraft {
  group: string;
  brokerNames: string;
  clusterNames: string;
  retryQueueNums: number;
  consumeEnable: boolean;
  broadcastEnable: boolean;
}

export default function ConsumerMutationDialog({ open, consumer, onOpenChange }: ConsumerMutationDialogProps) {
  const [draft, setDraft] = useState<ConsumerDraft>(() => defaultDraft(consumer));
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setDraft(defaultDraft(consumer));
    setNotice(null);
  }, [consumer, open]);

  const submitPending = () => {
    setNotice(
      'Rust backend does not expose /api/consumers create-or-update yet. This dialog mirrors Java /consumer/createOrUpdate.do for the next admin facade step.'
    );
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content consumer-modal">
          <div className="drawer-header">
            <div>
              <Dialog.Title>{consumer ? 'Update Consumer Group' : 'Add / Update Consumer Group'}</Dialog.Title>
              <Dialog.Description className="dialog-description">
                Java-compatible subscription group config form. Save is intentionally blocked until Rust exposes the admin mutation.
              </Dialog.Description>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>

          {notice ? <div className="notice notice-warning">{notice}</div> : null}

          <div className="form-grid consumer-form-grid">
            <label className="field field-wide">
              SubscriptionGroup
              <input
                value={draft.group}
                placeholder="please_rename_unique_group_name_4"
                onChange={(event) => setDraft((value) => ({ ...value, group: event.target.value }))}
              />
            </label>
            <label className="field">
              Broker names
              <input value={draft.brokerNames} placeholder="broker-a,broker-b" onChange={(event) => setDraft((value) => ({ ...value, brokerNames: event.target.value }))} />
            </label>
            <label className="field">
              Cluster names
              <input value={draft.clusterNames} placeholder="DefaultCluster" onChange={(event) => setDraft((value) => ({ ...value, clusterNames: event.target.value }))} />
            </label>
            <label className="field">
              Retry Queue Nums
              <input
                type="number"
                min="0"
                value={draft.retryQueueNums}
                onChange={(event) => setDraft((value) => ({ ...value, retryQueueNums: Number(event.target.value) }))}
              />
            </label>
            <label className="compact-check topic-type-check">
              <input type="checkbox" checked={draft.consumeEnable} onChange={(event) => setDraft((value) => ({ ...value, consumeEnable: event.target.checked }))} />
              Consume enable
            </label>
            <label className="compact-check topic-type-check">
              <input type="checkbox" checked={draft.broadcastEnable} onChange={(event) => setDraft((value) => ({ ...value, broadcastEnable: event.target.checked }))} />
              Broadcast enable
            </label>
          </div>

          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
            <button type="button" className="button" onClick={submitPending}>
              <Save size={15} aria-hidden="true" /> Save
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function defaultDraft(consumer?: ConsumerGroupInfo | null): ConsumerDraft {
  return {
    group: consumer?.group ?? '',
    brokerNames: '',
    clusterNames: '',
    retryQueueNums: 1,
    consumeEnable: true,
    broadcastEnable: false
  };
}
