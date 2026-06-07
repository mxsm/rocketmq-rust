import * as Dialog from '@radix-ui/react-dialog';
import { Pencil, RefreshCw, Save, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { brokerApi } from '../api/broker_api';
import type { BrokerConfigView, BrokerRuntimeStats } from '../types/broker';
import ConfirmDialog from './ConfirmDialog';
import ErrorState from './ErrorState';
import KeyValueTable from './KeyValueTable';
import LoadingState from './LoadingState';
import StatusBadge from './StatusBadge';

type BrokerDrawerTab = 'status' | 'config';

interface BrokerInspectorDrawerProps {
  brokerName: string | null;
  initialTab: BrokerDrawerTab;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function toRows(entries: Record<string, string> | undefined) {
  return Object.entries(entries ?? {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => ({ key, value }));
}

export default function BrokerInspectorDrawer({ brokerName, initialTab, open, onOpenChange }: BrokerInspectorDrawerProps) {
  const [activeTab, setActiveTab] = useState<BrokerDrawerTab>(initialTab);
  const [runtime, setRuntime] = useState<BrokerRuntimeStats | null>(null);
  const [config, setConfig] = useState<BrokerConfigView | null>(null);
  const [configDraft, setConfigDraft] = useState('{}');
  const [editing, setEditing] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const runtimeRows = useMemo(() => toRows(runtime?.entries), [runtime]);
  const configRows = useMemo(() => toRows(config?.entries), [config]);

  const load = () => {
    if (!brokerName) return;
    setLoading(true);
    setError(null);
    Promise.all([brokerApi.runtime(brokerName), brokerApi.config(brokerName)])
      .then(([runtimeData, configData]) => {
        setRuntime(runtimeData);
        setConfig(configData);
        setConfigDraft(JSON.stringify(configData.entries, null, 2));
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (open) {
      setActiveTab(initialTab);
      setEditing(false);
      load();
    }
  }, [brokerName, initialTab, open]);

  const saveConfig = () => {
    if (!brokerName) return;
    let entries: Record<string, string>;
    try {
      const parsed = JSON.parse(configDraft) as Record<string, unknown>;
      entries = Object.fromEntries(Object.entries(parsed).map(([key, value]) => [key, String(value)]));
    } catch {
      setError('Broker config must be a JSON object.');
      return;
    }

    brokerApi
      .updateConfig(brokerName, { entries })
      .then(() => {
        setEditing(false);
        load();
      })
      .catch((requestError: Error) => setError(requestError.message));
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content broker-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Broker Inspector</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={brokerName ?? 'unknown'} tone="success" />
                <StatusBadge status={runtime?.address ?? config?.address ?? 'address pending'} />
              </div>
            </div>
            <div className="action-row">
              <button type="button" className="icon-button" title="Refresh broker" onClick={load}>
                <RefreshCw size={15} aria-hidden="true" />
              </button>
              <Dialog.Close className="icon-button" title="Close">
                <X size={15} aria-hidden="true" />
              </Dialog.Close>
            </div>
          </div>

          <div className="drawer-tabs">
            <button type="button" className={activeTab === 'status' ? 'active' : ''} onClick={() => setActiveTab('status')}>
              Status
            </button>
            <button type="button" className={activeTab === 'config' ? 'active' : ''} onClick={() => setActiveTab('config')}>
              Config
            </button>
          </div>

          {loading ? <LoadingState label="Loading broker" /> : null}
          {error ? <ErrorState message={error} onRetry={load} /> : null}

          {!loading && !error && activeTab === 'status' ? <KeyValueTable rows={runtimeRows} emptyTitle="No runtime stats" /> : null}
          {!loading && !error && activeTab === 'config' ? (
            <div className="drawer-section">
              <div className="drawer-section-actions">
                <StatusBadge status={`${configRows.length} config items`} />
                <button type="button" className="button button-secondary" onClick={() => setEditing((value) => !value)}>
                  <Pencil size={15} aria-hidden="true" /> {editing ? 'Preview' : 'Edit'}
                </button>
              </div>
              {editing ? (
                <>
                  <textarea className="config-editor" value={configDraft} onChange={(event) => setConfigDraft(event.target.value)} />
                  <div className="dialog-actions">
                    <ConfirmDialog
                      title="Update broker config"
                      description={`Apply config changes to ${brokerName}?`}
                      confirmLabel="Update"
                      onConfirm={saveConfig}
                    >
                      <button type="button" className="button">
                        <Save size={15} aria-hidden="true" /> Update
                      </button>
                    </ConfirmDialog>
                  </div>
                </>
              ) : (
                <KeyValueTable rows={configRows} emptyTitle="No broker config" />
              )}
            </div>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
