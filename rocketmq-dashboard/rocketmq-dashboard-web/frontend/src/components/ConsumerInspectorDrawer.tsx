import * as Dialog from '@radix-ui/react-dialog';
import { RefreshCw, Settings2, Users, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupInfo, ConsumerProgress } from '../types/consumer';
import DataTable, { type DataTableColumn } from './DataTable';
import ErrorState from './ErrorState';
import KeyValueTable from './KeyValueTable';
import LoadingState from './LoadingState';
import StatusBadge from './StatusBadge';

export type ConsumerInspectorTab = 'client' | 'detail' | 'config';

interface ConsumerInspectorDrawerProps {
  consumer: ConsumerGroupInfo | null;
  initialTab: ConsumerInspectorTab;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRefreshGroup: (group: string) => void;
}

const progressColumns: DataTableColumn<ConsumerProgress['queues'][number]>[] = [
  { header: 'Topic', render: (row) => <code>{row.topic}</code>, width: '260px' },
  { header: 'Broker', render: (row) => row.brokerName, width: '180px' },
  { header: 'Queue', render: (row) => row.queueId, width: '90px' },
  { header: 'Broker Offset', render: (row) => row.brokerOffset, width: '140px' },
  { header: 'Consumer Offset', render: (row) => row.consumerOffset, width: '150px' },
  { header: 'Diff', render: (row) => <StatusBadge status={String(row.diff)} tone={row.diff > 0 ? 'warning' : 'success'} />, width: '100px' }
];

export default function ConsumerInspectorDrawer({
  consumer,
  initialTab,
  open,
  onOpenChange,
  onRefreshGroup
}: ConsumerInspectorDrawerProps) {
  const [activeTab, setActiveTab] = useState<ConsumerInspectorTab>(initialTab);
  const [progress, setProgress] = useState<ConsumerProgress | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadProgress = () => {
    if (!consumer) return;
    setLoading(true);
    setError(null);
    consumerApi
      .progress(consumer.group)
      .then(setProgress)
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (!open) return;
    setActiveTab(initialTab);
    setProgress(null);
    loadProgress();
  }, [initialTab, open, consumer?.group]);

  const configRows = useMemo(
    () =>
      [
        { key: 'subscriptionGroup', value: consumer?.group ?? '' },
        { key: 'consumeType', value: consumer?.consumeType ?? 'UNKNOWN' },
        { key: 'messageModel', value: consumer?.messageModel ?? 'UNKNOWN' },
        { key: 'clientCount', value: String(consumer?.clientCount ?? 0) },
        { key: 'diffTotal', value: String(progress?.diffTotal ?? consumer?.diffTotal ?? 0) },
        { key: 'topicCount', value: String(progress?.topicCount ?? 0) },
        { key: 'javaConfigApi', value: '/consumer/examineSubscriptionGroupConfig.query' },
        { key: 'rustStatus', value: 'pending admin facade support for subscription group config mutation' }
      ].filter((row) => row.value !== ''),
    [consumer, progress]
  );

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content consumer-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Consumer Inspector</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={consumer?.group ?? 'group pending'} tone="success" />
                <StatusBadge status={normalizeText(consumer?.consumeType)} />
                <StatusBadge status={normalizeText(consumer?.messageModel)} />
              </div>
            </div>
            <div className="action-row">
              <button type="button" className="icon-button" title="Refresh consumer" onClick={loadProgress}>
                <RefreshCw size={15} aria-hidden="true" />
              </button>
              <Dialog.Close className="icon-button" title="Close">
                <X size={15} aria-hidden="true" />
              </Dialog.Close>
            </div>
          </div>

          <div className="drawer-tabs consumer-drawer-tabs">
            <button type="button" className={activeTab === 'client' ? 'active' : ''} onClick={() => setActiveTab('client')}>
              CLIENT
            </button>
            <button type="button" className={activeTab === 'detail' ? 'active' : ''} onClick={() => setActiveTab('detail')}>
              CONSUME DETAIL
            </button>
            <button type="button" className={activeTab === 'config' ? 'active' : ''} onClick={() => setActiveTab('config')}>
              Config
            </button>
          </div>

          {loading ? <LoadingState label="Loading consumer progress" /> : null}
          {error ? <ErrorState message={error} onRetry={loadProgress} /> : null}

          {!loading && !error && activeTab === 'client' ? (
            <section className="drawer-section">
              <div className="consumer-detail-metrics">
                <div>
                  <span>Quantity</span>
                  <strong>{consumer?.clientCount ?? 0}</strong>
                </div>
                <div>
                  <span>Topics</span>
                  <strong>{progress?.topicCount ?? 0}</strong>
                </div>
                <div>
                  <span>Delay</span>
                  <strong>{progress?.diffTotal ?? consumer?.diffTotal ?? 0}</strong>
                </div>
              </div>
              <div className="pending-callout">
                <strong>Client connection API pending</strong>
                <span>
                  Java uses /consumer/consumerConnection.query and /consumer/consumerRunningInfo.query for CLIENT details. The Rust backend currently exposes group
                  summary and queue progress only.
                </span>
              </div>
            </section>
          ) : null}

          {!error && activeTab === 'detail' && !loading ? (
            <DataTable
              rows={progress?.queues ?? []}
              columns={progressColumns}
              getRowId={(row) => `${row.topic}-${row.brokerName}-${row.queueId}`}
              searchPlaceholder="Search topic, broker, or queue"
              emptyTitle="No consume progress"
              onRefresh={loadProgress}
            />
          ) : null}

          {!loading && !error && activeTab === 'config' ? (
            <section className="drawer-section">
              <KeyValueTable rows={configRows} emptyTitle="No consumer config" />
              <div className="drawer-section-actions">
                <button type="button" className="button button-secondary" onClick={() => consumer && onRefreshGroup(consumer.group)}>
                  <RefreshCw size={15} aria-hidden="true" /> REFRESH
                </button>
                <button type="button" className="button" disabled title="Rust backend does not expose subscription group config mutation yet">
                  <Settings2 size={15} aria-hidden="true" /> Save Config
                </button>
              </div>
            </section>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function normalizeText(value: string | undefined) {
  return value?.replace(/^CONSUME_/, '').replace(/^MESSAGE_MODEL_/, '') || 'UNKNOWN';
}
