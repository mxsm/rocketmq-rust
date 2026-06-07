import * as Dialog from '@radix-ui/react-dialog';
import { RefreshCw, Save, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import type { ConsumerQueueProgress } from '../types/consumer';
import type { TopicInfo, TopicMutationRequest, TopicRouteInfo, TopicStatsInfo } from '../types/topic';
import ConfirmDialog from './ConfirmDialog';
import DataTable, { type DataTableColumn } from './DataTable';
import EmptyState from './EmptyState';
import ErrorState from './ErrorState';
import KeyValueTable from './KeyValueTable';
import LoadingState from './LoadingState';
import StatusBadge from './StatusBadge';

export type TopicInspectorTab = 'status' | 'router' | 'consumers' | 'config';

interface TopicInspectorDrawerProps {
  topic: TopicInfo | null;
  initialTab: TopicInspectorTab;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onTopicUpdated: () => void;
}

interface ConsumerTopicProgress {
  group: string;
  diffTotal: number;
  queues: ConsumerQueueProgress[];
}

interface TopicConfigDraft {
  readQueueCount: number;
  writeQueueCount: number;
  perm: number;
  brokerNames: string;
  clusterNames: string;
  messageType: string;
  ordered: boolean;
}

const consumerColumns: DataTableColumn<ConsumerTopicProgress>[] = [
  { header: 'Consumer Group', render: (row) => <code>{row.group}</code>, width: '280px' },
  { header: 'Lag', render: (row) => row.diffTotal, width: '100px' },
  { header: 'Queues', render: (row) => row.queues.length, width: '100px' },
  {
    header: 'Brokers',
    render: (row) => Array.from(new Set(row.queues.map((queue) => queue.brokerName))).join(', ') || 'none'
  }
];

export default function TopicInspectorDrawer({
  topic,
  initialTab,
  open,
  onOpenChange,
  onTopicUpdated
}: TopicInspectorDrawerProps) {
  const [activeTab, setActiveTab] = useState<TopicInspectorTab>(initialTab);
  const [route, setRoute] = useState<TopicRouteInfo | null>(null);
  const [stats, setStats] = useState<TopicStatsInfo | null>(null);
  const [consumers, setConsumers] = useState<ConsumerTopicProgress[]>([]);
  const [draft, setDraft] = useState<TopicConfigDraft>(defaultDraft(null));
  const [loading, setLoading] = useState(false);
  const [consumerLoading, setConsumerLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const statsRows = useMemo(
    () =>
      [
        { key: 'topic', value: topic?.topic ?? '' },
        { key: 'category', value: topic?.category ?? '' },
        { key: 'queueCount', value: String(stats?.queueCount ?? 0) },
        { key: 'totalMinOffset', value: String(stats?.totalMinOffset ?? 0) },
        { key: 'totalMaxOffset', value: String(stats?.totalMaxOffset ?? 0) },
        { key: 'readQueueCount', value: String(topic?.readQueueCount ?? 0) },
        { key: 'writeQueueCount', value: String(topic?.writeQueueCount ?? 0) },
        { key: 'perm', value: String(topic?.perm ?? '') }
      ].filter((row) => row.value !== ''),
    [stats, topic]
  );

  const routeRows = route?.queues ?? [];
  const brokerRows = route?.brokers ?? [];

  const loadTopic = () => {
    if (!topic) return;
    setLoading(true);
    setError(null);
    setNotice(null);
    Promise.all([topicApi.stats(topic.topic), topicApi.route(topic.topic)])
      .then(([statsData, routeData]) => {
        setStats(statsData);
        setRoute(routeData);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  const loadConsumers = () => {
    if (!topic) return;
    setConsumerLoading(true);
    setError(null);
    consumerApi
      .list()
      .then((list) =>
        Promise.allSettled(
          list.items.map((consumer) =>
            consumerApi.progress(consumer.group).then((progress) => ({
              group: consumer.group,
              queues: progress.queues.filter((queue) => queue.topic === topic.topic)
            }))
          )
        )
      )
      .then((results) => {
        const rows = results
          .filter((result): result is PromiseFulfilledResult<{ group: string; queues: ConsumerQueueProgress[] }> => result.status === 'fulfilled')
          .map((result) => result.value)
          .filter((result) => result.queues.length > 0)
          .map((result) => ({
            group: result.group,
            queues: result.queues,
            diffTotal: result.queues.reduce((sum, queue) => sum + queue.diff, 0)
          }));
        setConsumers(rows);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setConsumerLoading(false));
  };

  useEffect(() => {
    if (!open) return;
    setActiveTab(initialTab);
    setDraft(defaultDraft(topic));
    setConsumers([]);
    loadTopic();
  }, [initialTab, open, topic]);

  useEffect(() => {
    if (open && activeTab === 'consumers' && consumers.length === 0 && !consumerLoading) {
      loadConsumers();
    }
  }, [activeTab, open]);

  const saveConfig = () => {
    if (!topic) return;
    const request: TopicMutationRequest = {
      topic: topic.topic,
      readQueueCount: Number(draft.readQueueCount),
      writeQueueCount: Number(draft.writeQueueCount),
      perm: Number(draft.perm),
      brokerNameList: splitCsv(draft.brokerNames),
      clusterNameList: splitCsv(draft.clusterNames),
      order: draft.ordered,
      messageType: draft.messageType
    };
    topicApi
      .update(topic.topic, request)
      .then(() => {
        setNotice('Topic config updated.');
        onTopicUpdated();
        loadTopic();
      })
      .catch((requestError: Error) => setError(requestError.message));
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content topic-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Topic Inspector</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={topic?.topic ?? 'topic pending'} tone="success" />
                <StatusBadge status={topic?.category ?? 'unknown'} />
              </div>
            </div>
            <div className="action-row">
              <button type="button" className="icon-button" title="Refresh topic" onClick={activeTab === 'consumers' ? loadConsumers : loadTopic}>
                <RefreshCw size={15} aria-hidden="true" />
              </button>
              <Dialog.Close className="icon-button" title="Close">
                <X size={15} aria-hidden="true" />
              </Dialog.Close>
            </div>
          </div>

          <div className="drawer-tabs topic-drawer-tabs">
            <button type="button" className={activeTab === 'status' ? 'active' : ''} onClick={() => setActiveTab('status')}>
              Status
            </button>
            <button type="button" className={activeTab === 'router' ? 'active' : ''} onClick={() => setActiveTab('router')}>
              Router
            </button>
            <button type="button" className={activeTab === 'consumers' ? 'active' : ''} onClick={() => setActiveTab('consumers')}>
              Consumer Manage
            </button>
            <button type="button" className={activeTab === 'config' ? 'active' : ''} onClick={() => setActiveTab('config')}>
              Topic Config
            </button>
          </div>

          {notice ? <div className="notice notice-success">{notice}</div> : null}
          {loading && activeTab !== 'consumers' ? <LoadingState label="Loading topic" /> : null}
          {error ? <ErrorState message={error} onRetry={activeTab === 'consumers' ? loadConsumers : loadTopic} /> : null}

          {!loading && !error && activeTab === 'status' ? <KeyValueTable rows={statsRows} emptyTitle="No topic status" /> : null}

          {!loading && !error && activeTab === 'router' ? (
            <div className="drawer-section">
              <section className="topic-section">
                <div className="drawer-section-actions">
                  <h3>Broker Datas</h3>
                  <StatusBadge status={`${brokerRows.length} brokers`} />
                </div>
                {brokerRows.length === 0 ? (
                  <EmptyState title="No route brokers" />
                ) : (
                  <div className="topic-route-grid">
                    {brokerRows.map((broker) => (
                      <div className="topic-route-card" key={broker.brokerName}>
                        <strong>{broker.brokerName}</strong>
                        <code>{broker.brokerAddrs.join(', ') || 'no address'}</code>
                      </div>
                    ))}
                  </div>
                )}
              </section>
              <DataTable
                rows={routeRows}
                columns={[
                  { header: 'Broker', render: (row) => row.brokerName },
                  { header: 'Read Queue Nums', render: (row) => row.readQueueNums },
                  { header: 'Write Queue Nums', render: (row) => row.writeQueueNums },
                  { header: 'Perm', render: (row) => <code>{row.perm}</code> }
                ]}
                getRowId={(row) => `${row.brokerName}-${row.readQueueNums}-${row.writeQueueNums}`}
                searchPlaceholder="Search route queues"
                emptyTitle="No queue datas"
              />
            </div>
          ) : null}

          {!error && activeTab === 'consumers' ? (
            consumerLoading ? (
              <LoadingState label="Loading topic consumers" />
            ) : (
              <DataTable
                rows={consumers}
                columns={consumerColumns}
                getRowId={(row) => row.group}
                searchPlaceholder="Search consumer group"
                emptyTitle="No consumer group for this topic"
                onRefresh={loadConsumers}
              />
            )
          ) : null}

          {!loading && !error && activeTab === 'config' ? (
            <div className="drawer-section">
              <div className="form-grid">
                <label className="field">
                  Topic name
                  <input value={topic?.topic ?? ''} disabled />
                </label>
                <label className="field">
                  Message type
                  <select value={draft.messageType} onChange={(event) => setDraft((value) => ({ ...value, messageType: event.target.value }))}>
                    <option value="NORMAL">NORMAL</option>
                    <option value="FIFO">FIFO</option>
                    <option value="DELAY">DELAY</option>
                    <option value="TRANSACTION">TRANSACTION</option>
                  </select>
                </label>
                <label className="field">
                  Write Queue Nums
                  <input
                    type="number"
                    min="1"
                    value={draft.writeQueueCount}
                    onChange={(event) => setDraft((value) => ({ ...value, writeQueueCount: Number(event.target.value) }))}
                  />
                </label>
                <label className="field">
                  Read Queue Nums
                  <input
                    type="number"
                    min="1"
                    value={draft.readQueueCount}
                    onChange={(event) => setDraft((value) => ({ ...value, readQueueCount: Number(event.target.value) }))}
                  />
                </label>
                <label className="field">
                  Perm
                  <input type="number" min="0" value={draft.perm} onChange={(event) => setDraft((value) => ({ ...value, perm: Number(event.target.value) }))} />
                </label>
                <label className="field">
                  Broker names
                  <input value={draft.brokerNames} placeholder="broker-a,broker-b" onChange={(event) => setDraft((value) => ({ ...value, brokerNames: event.target.value }))} />
                </label>
                <label className="field field-wide">
                  Cluster names
                  <input value={draft.clusterNames} placeholder="DefaultCluster" onChange={(event) => setDraft((value) => ({ ...value, clusterNames: event.target.value }))} />
                </label>
                <label className="compact-check">
                  <input type="checkbox" checked={draft.ordered} onChange={(event) => setDraft((value) => ({ ...value, ordered: event.target.checked }))} />
                  Ordered topic
                </label>
              </div>
              <div className="dialog-actions">
                <ConfirmDialog title="Update topic config" description={`Apply topic config changes to ${topic?.topic}?`} confirmLabel="Update" onConfirm={saveConfig}>
                  <button type="button" className="button">
                    <Save size={15} aria-hidden="true" /> Update
                  </button>
                </ConfirmDialog>
              </div>
            </div>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function defaultDraft(topic: TopicInfo | null): TopicConfigDraft {
  return {
    readQueueCount: topic?.readQueueCount ?? 8,
    writeQueueCount: topic?.writeQueueCount ?? 8,
    perm: topic?.perm ?? 6,
    brokerNames: topic?.brokerName ?? '',
    clusterNames: '',
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
