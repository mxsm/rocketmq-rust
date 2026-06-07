import { Activity, FastForward, GitBranch, Plus, RefreshCw, RotateCcw, Search, Send, Settings2, Trash2, Users } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusBadge from '../components/StatusBadge';
import TopicInspectorDrawer, { type TopicInspectorTab } from '../components/TopicInspectorDrawer';
import TopicMaintenanceDialog, { type TopicMaintenanceMode } from '../components/TopicMaintenanceDialog';
import TopicMutationDialog from '../components/TopicMutationDialog';
import type { ConsumerGroupInfo } from '../types/consumer';
import type { TopicInfo, TopicListView, TopicMutationRequest } from '../types/topic';

const topicTypeFilters = [
  { key: 'normal', label: 'NORMAL', defaultChecked: true },
  { key: 'delay', label: 'Delay', defaultChecked: true },
  { key: 'fifo', label: 'FIFO', defaultChecked: false },
  { key: 'transaction', label: 'TRANSACTION', defaultChecked: false },
  { key: 'unspecified', label: 'UNSPECIFIED', defaultChecked: false },
  { key: 'retry', label: 'RETRY', defaultChecked: true },
  { key: 'dlq', label: 'DLQ', defaultChecked: true },
  { key: 'system', label: 'SYSTEM', defaultChecked: true }
] as const;

type TopicTypeKey = (typeof topicTypeFilters)[number]['key'];

type TopicTypeFilterState = Record<TopicTypeKey, boolean>;

const pageSize = 10;

function defaultFilters(): TopicTypeFilterState {
  return Object.fromEntries(topicTypeFilters.map((item) => [item.key, item.defaultChecked])) as TopicTypeFilterState;
}

export default function TopicListPage() {
  const [data, setData] = useState<TopicListView | null>(null);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroupInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [topicQuery, setTopicQuery] = useState('');
  const [filters, setFilters] = useState<TopicTypeFilterState>(() => defaultFilters());
  const [mutationOpen, setMutationOpen] = useState(false);
  const [inspectorTopic, setInspectorTopic] = useState<TopicInfo | null>(null);
  const [inspectorTab, setInspectorTab] = useState<TopicInspectorTab>('status');
  const [maintenanceTopic, setMaintenanceTopic] = useState<string | null>(null);
  const [maintenanceMode, setMaintenanceMode] = useState<TopicMaintenanceMode>('reset');
  const [page, setPage] = useState(1);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([topicApi.list(), consumerApi.list().catch(() => ({ items: [], total: 0 }))])
      .then(([topicData, consumerData]) => {
        setData(topicData);
        setConsumerGroups(consumerData.items);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const filteredTopics = useMemo(() => {
    const query = topicQuery.trim().toLowerCase();
    const enabledTypes = new Set(
      Object.entries(filters)
        .filter(([, enabled]) => enabled)
        .map(([key]) => key)
    );
    return (data?.items ?? []).filter((topic) => {
      const matchesQuery = !query || topic.topic.toLowerCase().includes(query);
      const matchesType = enabledTypes.size === 0 || enabledTypes.has(inferTopicType(topic));
      return matchesQuery && matchesType;
    });
  }, [data?.items, filters, topicQuery]);

  const topicCounts = useMemo(() => {
    const items = data?.items ?? [];
    const system = items.filter(isSystemTopic).length;
    return {
      total: items.length,
      visible: filteredTopics.length,
      system,
      application: Math.max(0, items.length - system)
    };
  }, [data?.items, filteredTopics.length]);

  const pageCount = Math.max(1, Math.ceil(filteredTopics.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const visibleTopics = useMemo(
    () => filteredTopics.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    [currentPage, filteredTopics]
  );

  useEffect(() => {
    setPage(1);
  }, [filters, topicQuery]);

  const openInspector = (topic: TopicInfo, tab: TopicInspectorTab) => {
    setInspectorTopic(topic);
    setInspectorTab(tab);
  };

  const openMaintenance = (topic: string, mode: TopicMaintenanceMode) => {
    setMaintenanceTopic(topic);
    setMaintenanceMode(mode);
  };

  const saveTopic = (request: TopicMutationRequest) =>
    topicApi.create(request).then(() => {
      setNotice(`Topic ${request.topic} saved.`);
      load();
    });

  const deleteTopic = (topic: string) => {
    topicApi
      .delete(topic)
      .then(() => {
        setNotice(`Topic ${topic} deleted.`);
        load();
      })
      .catch((requestError: Error) => setError(requestError.message));
  };

  if (loading) {
    return <LoadingState label="Loading topics" />;
  }

  if (error) {
    return <ErrorState message={error} onRetry={load} />;
  }

  return (
    <>
      <PageHeader
        title="Topic"
        description="Java Dashboard topic parity with Rust Web API backed status, route, consumer progress, config, and guarded maintenance flows."
        actions={
          <div className="action-row">
            <button type="button" className="button" onClick={() => setMutationOpen(true)}>
              <Plus size={15} aria-hidden="true" /> Add / Update
            </button>
            <button type="button" className="button button-secondary" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" /> Refresh
            </button>
          </div>
        }
      />

      {notice ? <div className="notice notice-success">{notice}</div> : null}

      <section className="topic-filter-panel">
        <label className="field topic-filter-search">
          Topic
          <input value={topicQuery} placeholder="Topic name" onChange={(event) => setTopicQuery(event.target.value)} />
        </label>
        <div className="topic-type-filters" aria-label="Topic type filters">
          {topicTypeFilters.map((filter) => (
            <label className="compact-check topic-type-check" key={filter.key}>
              <input
                type="checkbox"
                checked={filters[filter.key]}
                onChange={(event) => setFilters((value) => ({ ...value, [filter.key]: event.target.checked }))}
              />
              {filter.label}
            </label>
          ))}
        </div>
      </section>

      <section className="topic-list-panel">
        <div className="topic-list-toolbar">
          <div>
            <h2>Topic inventory</h2>
            <p>
              {topicCounts.visible} visible / {topicCounts.total} total
            </p>
          </div>
          <div className="topic-list-summary">
            <StatusBadge status={`${topicCounts.system} system`} />
            <StatusBadge status={`${topicCounts.application} non-system`} tone="success" />
            <button type="button" className="icon-button" title="Refresh topics" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" />
            </button>
          </div>
        </div>
        <div className="topic-list-search">
          <Search size={16} aria-hidden="true" />
          <input value={topicQuery} placeholder="Search topic inventory" onChange={(event) => setTopicQuery(event.target.value)} />
        </div>
        <div className="topic-inventory-scroll">
          <table className="topic-inventory-table">
            <thead>
              <tr>
                <th>Topic</th>
                <th>Type</th>
                <th>Queues</th>
                <th>Perm</th>
                <th>Policy</th>
                <th>Operation</th>
              </tr>
            </thead>
            <tbody>
              {visibleTopics.map((row) => (
                <tr className={isSystemTopic(row) ? 'topic-row-system' : 'topic-row-application'} key={row.topic}>
                  <td>
                    <div className="topic-name-cell">
                      <Link to={`/topics/${encodeURIComponent(row.topic)}`}>{row.topic}</Link>
                      <span>{topicDescription(row)}</span>
                    </div>
                  </td>
                  <td>
                    <StatusBadge status={inferTopicType(row).toUpperCase()} tone={topicTone(row)} />
                  </td>
                  <td>
                    <span className="topic-queue-cell">
                      <strong>{row.readQueueCount}</strong>
                      <span>/</span>
                      <strong>{row.writeQueueCount}</strong>
                    </span>
                  </td>
                  <td>
                    <code>{row.perm}</code>
                  </td>
                  <td>
                    <StatusBadge status={isSystemTopic(row) ? '4 safe actions' : 'full actions'} tone={isSystemTopic(row) ? 'neutral' : 'success'} />
                  </td>
                  <td>{renderTopicActions(row, openInspector, openMaintenance, deleteTopic)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {visibleTopics.length === 0 ? <EmptyState title="No topics match the current filters" /> : null}
        <div className="topic-list-footer">
          <span>
            {filteredTopics.length} rows / page {currentPage} of {pageCount}
          </span>
          <div className="pagination">
            <button type="button" className="button button-secondary" disabled={currentPage <= 1} onClick={() => setPage(currentPage - 1)}>
              Previous
            </button>
            <button type="button" className="button button-secondary" disabled={currentPage >= pageCount} onClick={() => setPage(currentPage + 1)}>
              Next
            </button>
          </div>
        </div>
      </section>

      <TopicMutationDialog open={mutationOpen} onOpenChange={setMutationOpen} onSubmit={saveTopic} />
      <TopicInspectorDrawer
        open={inspectorTopic !== null}
        topic={inspectorTopic}
        initialTab={inspectorTab}
        onOpenChange={(open) => {
          if (!open) setInspectorTopic(null);
        }}
        onTopicUpdated={load}
      />
      <TopicMaintenanceDialog
        open={maintenanceTopic !== null}
        topic={maintenanceTopic}
        mode={maintenanceMode}
        consumerGroups={consumerGroups}
        onOpenChange={(open) => {
          if (!open) setMaintenanceTopic(null);
        }}
        onMutationFinished={load}
      />
    </>
  );
}

function inferTopicType(topic: TopicInfo): TopicTypeKey {
  if (isSystemTopic(topic)) return 'system';
  if (topic.topic.startsWith('%RETRY%')) return 'retry';
  if (topic.topic.startsWith('%DLQ%')) return 'dlq';
  if (topic.topic.includes('TRANS')) return 'transaction';
  if (topic.topic.includes('SCHEDULE')) return 'delay';
  const explicit = topic.category.toLowerCase();
  if (topicTypeFilters.some((filter) => filter.key === explicit)) return explicit as TopicTypeKey;
  return 'normal';
}

function isSystemTopic(topic: TopicInfo) {
  const topicName = topic.topic;
  const upperTopic = topicName.toUpperCase();
  return (
    topicName.startsWith('%SYS%') ||
    upperTopic.startsWith('RMQ_SYS_') ||
    topicName.startsWith('rmq_sys_') ||
    upperTopic.startsWith('SCHEDULE_TOPIC_') ||
    upperTopic.startsWith('DEFAULTCLUSTER') ||
    upperTopic.startsWith('BROKER-') ||
    upperTopic.endsWith('_REPLY_TOPIC') ||
    upperTopic === 'TRANS_CHECK_MAX_TIME_TOPIC' ||
    upperTopic === 'CHECKPOINT_TOPIC' ||
    upperTopic === 'SELF_TEST_TOPIC' ||
    upperTopic === 'DEFAULTHEARTBEATSYNCERTOPIC' ||
    upperTopic === 'TBW102' ||
    upperTopic === 'OFFSET_MOVED_EVENT' ||
    upperTopic === 'BENCHMARKTEST'
  );
}

function topicTone(topic: TopicInfo) {
  const type = inferTopicType(topic);
  if (type === 'system') return 'neutral';
  if (type === 'retry' || type === 'dlq') return 'warning';
  if (type === 'transaction') return 'danger';
  return 'success';
}

function topicDescription(topic: TopicInfo) {
  const type = inferTopicType(topic);
  if (type === 'retry') return 'retry maintenance topic';
  if (type === 'dlq') return 'dead-letter queue topic';
  if (type === 'system') return 'system topic';
  return `${topic.readQueueCount}/${topic.writeQueueCount} queues`;
}

function renderTopicActions(
  row: TopicInfo,
  openInspector: (topic: TopicInfo, tab: TopicInspectorTab) => void,
  openMaintenance: (topic: string, mode: TopicMaintenanceMode) => void,
  deleteTopic: (topic: string) => void
) {
  const systemTopic = isSystemTopic(row);
  return (
    <div className="topic-action-grid">
      <button type="button" className="button button-secondary topic-action-button" onClick={() => openInspector(row, 'status')}>
        <Activity size={14} aria-hidden="true" /> Status
      </button>
      <button type="button" className="button button-secondary topic-action-button" onClick={() => openInspector(row, 'router')}>
        <GitBranch size={14} aria-hidden="true" /> Router
      </button>
      <button type="button" className="button button-secondary topic-action-button" onClick={() => openInspector(row, 'consumers')}>
        <Users size={14} aria-hidden="true" /> Consumer Manage
      </button>
      <button type="button" className="button topic-action-button" onClick={() => openInspector(row, 'config')}>
        <Settings2 size={14} aria-hidden="true" /> Topic Config
      </button>
      {!systemTopic ? (
        <>
          <button type="button" className="button button-secondary topic-action-button" onClick={() => openMaintenance(row.topic, 'send')}>
            <Send size={14} aria-hidden="true" /> Send Message
          </button>
          <button type="button" className="button button-danger topic-action-button" onClick={() => openMaintenance(row.topic, 'reset')}>
            <RotateCcw size={14} aria-hidden="true" /> Reset Consumer Offset
          </button>
          <button type="button" className="button button-danger topic-action-button" onClick={() => openMaintenance(row.topic, 'skip')}>
            <FastForward size={14} aria-hidden="true" /> Skip Message Accumulate
          </button>
          <ConfirmDialog
            title="Delete topic"
            description={`Delete topic ${row.topic}? This operation changes cluster metadata.`}
            confirmLabel="Delete"
            onConfirm={() => deleteTopic(row.topic)}
          >
            <button type="button" className="button button-danger topic-action-button">
              <Trash2 size={14} aria-hidden="true" /> Delete
            </button>
          </ConfirmDialog>
        </>
      ) : null}
    </div>
  );
}
