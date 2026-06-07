import * as Dialog from '@radix-ui/react-dialog';
import { Pencil } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { brokerApi } from '../api/broker_api';
import ConfirmDialog from '../components/ConfirmDialog';
import ErrorState from '../components/ErrorState';
import KeyValueTable from '../components/KeyValueTable';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import type { BrokerConfigView, BrokerRuntimeStats } from '../types/broker';

interface EntryRow {
  key: string;
  value: string;
}

export default function BrokerDetailPage() {
  const { brokerName = '' } = useParams();
  const [runtime, setRuntime] = useState<BrokerRuntimeStats | null>(null);
  const [config, setConfig] = useState<BrokerConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [configDraft, setConfigDraft] = useState('{}');

  const load = () => {
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
    load();
  }, [brokerName]);

  if (loading) return <LoadingState label="Loading broker" />;
  if (error) return <ErrorState message={error} onRetry={load} />;

  const runtimeRows: EntryRow[] = Object.entries(runtime?.entries ?? {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => ({ key, value }));
  const configRows: EntryRow[] = Object.entries(config?.entries ?? {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => ({ key, value }));
  const saveConfig = () => {
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
    <>
      <PageHeader
        title={brokerName}
        description="Broker runtime and configuration details."
        actions={
          <button type="button" className="button" onClick={() => setEditing(true)}>
            <Pencil size={15} aria-hidden="true" /> Edit config
          </button>
        }
      />
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Status</h2>
            <p>Runtime key/value stats.</p>
          </div>
          <button type="button" className="button button-secondary" onClick={load}>
            Refresh
          </button>
        </div>
        <KeyValueTable rows={runtimeRows} emptyTitle="No runtime stats" />
      </section>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Config</h2>
            <p>Broker configuration key/value entries.</p>
          </div>
        </div>
        <KeyValueTable rows={configRows} emptyTitle="No broker config" />
      </section>
      <Dialog.Root open={editing} onOpenChange={setEditing}>
        <Dialog.Portal>
          <Dialog.Overlay className="dialog-overlay" />
          <Dialog.Content className="dialog-content">
            <Dialog.Title>Edit Broker Config</Dialog.Title>
            <textarea className="config-editor" value={configDraft} onChange={(event) => setConfigDraft(event.target.value)} />
            <div className="dialog-actions">
              <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
              <ConfirmDialog
                title="Update broker config"
                description={`Apply config changes to ${brokerName}?`}
                confirmLabel="Update"
                onConfirm={saveConfig}
              >
                <button type="button" className="button">
                  Update
                </button>
              </ConfirmDialog>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  );
}
