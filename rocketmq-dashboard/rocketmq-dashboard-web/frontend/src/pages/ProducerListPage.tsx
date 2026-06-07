import * as Dialog from '@radix-ui/react-dialog';
import { Copy, DatabaseZap, RefreshCw, Search, Server, Users, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { producerApi } from '../api/producer_api';
import { topicApi } from '../api/topic_api';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SelectMenu from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { ProducerConnectionInfo, ProducerConnectionView, ProducerInfo } from '../types/producer';
import type { TopicInfo } from '../types/topic';

const pageSize = 10;

export default function ProducerListPage() {
  const [items, setItems] = useState<ProducerInfo[]>([]);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [connection, setConnection] = useState<ProducerConnectionView | null>(null);
  const [selectedConnection, setSelectedConnection] = useState<ProducerConnectionInfo | null>(null);
  const [topic, setTopic] = useState('');
  const [group, setGroup] = useState('');
  const [groupQuery, setGroupQuery] = useState('');
  const [tableQuery, setTableQuery] = useState('');
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [querying, setQuerying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([producerApi.list(), topicApi.list().catch(() => ({ items: [], total: 0 }))])
      .then(([producerItems, topicData]) => {
        setItems(producerItems);
        setTopics(topicData.items);
        setTopic((value) => value || producerItems[0]?.topic || topicData.items[0]?.topic || '');
        setGroup((value) => value || producerItems[0]?.producerGroup || '');
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const filteredProducerGroups = useMemo(() => {
    const normalized = groupQuery.trim().toLowerCase();
    return items.filter((item) => {
      if (!normalized) return true;
      return `${item.topic} ${item.producerGroup}`.toLowerCase().includes(normalized);
    });
  }, [groupQuery, items]);

  const filteredConnections = useMemo(() => {
    const normalized = tableQuery.trim().toLowerCase();
    const rows = connection?.connections ?? [];
    if (!normalized) return rows;
    return rows.filter((row) => `${row.clientId} ${row.clientAddr} ${row.language} ${row.version}`.toLowerCase().includes(normalized));
  }, [connection?.connections, tableQuery]);

  const pageCount = Math.max(1, Math.ceil(filteredConnections.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const visibleConnections = useMemo(
    () => filteredConnections.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    [currentPage, filteredConnections]
  );

  useEffect(() => {
    setPage(1);
  }, [tableQuery, connection]);

  const queryConnections = () => {
    if (!topic.trim() || !group.trim()) {
      setNotice('TOPIC and PRODUCER_GROUP are required.');
      return;
    }
    setQuerying(true);
    setError(null);
    setNotice(null);
    producerApi
      .connections(topic.trim(), group.trim())
      .then((data) => {
        setConnection(data);
        setNotice(`Producer query completed. ${data.connections.length} connection(s) found.`);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setQuerying(false));
  };

  const useProducerGroup = (item: ProducerInfo) => {
    setTopic((value) => item.topic || value || topics[0]?.topic || '');
    setGroup(item.producerGroup);
    setNotice(`Loaded ${item.producerGroup}. Run SEARCH to query live connections.`);
  };

  const copyConnection = (row: ProducerConnectionInfo) => {
    void navigator.clipboard?.writeText(`${row.clientId}\t${row.clientAddr}\t${row.language}\t${row.version}`);
    setNotice('Producer connection copied.');
  };

  if (loading) return <LoadingState label="Loading producers" />;
  if (error) return <ErrorState message={error} onRetry={load} />;

  return (
    <>
      <PageHeader
        title="Producer"
        description="Producer connection lookup by topic and producer group."
        actions={
          <div className="action-row">
            <button type="button" className="button button-secondary" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" /> Refresh
            </button>
          </div>
        }
      />

      {notice ? <div className="notice notice-warning">{notice}</div> : null}

      <section className="producer-query-panel">
        <label className="producer-query-field">
          <span>
            <strong>*</strong> TOPIC
          </span>
          <SelectMenu value={topic} options={topicOptions(topics, topic)} onChange={setTopic} ariaLabel="Select topic for producer query" className="producer-topic-select" />
        </label>
        <label className="producer-query-field producer-group-field">
          <span>
            <strong>*</strong> PRODUCER_GROUP
          </span>
          <input value={group} placeholder="please_rename_unique_group_name" onChange={(event) => setGroup(event.target.value)} />
        </label>
        <button type="button" className="button producer-search-button" onClick={queryConnections} disabled={querying}>
          {querying ? <RefreshCw className="spin" size={15} aria-hidden="true" /> : <Search size={15} aria-hidden="true" />}
          SEARCH
        </button>
      </section>

      <section className="producer-metric-grid">
        <ProducerMetric label="Producer groups" value={items.length} detail="discovered groups" icon={<Users size={18} />} />
        <ProducerMetric label="Connections" value={connection?.connections.length ?? 0} detail="current query result" icon={<Server size={18} />} />
        <ProducerMetric label="Topic" value={connection?.topic || topic || '-'} detail="selected query topic" icon={<DatabaseZap size={18} />} />
        <ProducerMetric label="Producer group" value={connection?.producerGroup || group || '-'} detail="selected query group" icon={<Search size={18} />} />
      </section>

      <div className="producer-content-grid">
        <section className="producer-results-panel">
          <div className="topic-list-toolbar">
            <div>
              <h2>Connection results</h2>
              <p>clientId / clientAddr / language / version</p>
            </div>
            <div className="topic-list-summary">
              <StatusBadge status={`${filteredConnections.length} online`} tone={filteredConnections.length > 0 ? 'success' : 'neutral'} />
              <button type="button" className="icon-button" title="Refresh producer query" onClick={queryConnections} disabled={querying || !topic || !group}>
                <RefreshCw size={15} aria-hidden="true" />
              </button>
            </div>
          </div>
          <div className="topic-list-search">
            <Search size={16} aria-hidden="true" />
            <input value={tableQuery} placeholder="Search client id, address, language, or version" onChange={(event) => setTableQuery(event.target.value)} />
          </div>
          <div className="producer-table-scroll">
            <table className="producer-connection-table">
              <thead>
                <tr>
                  <th>clientId</th>
                  <th>clientAddr</th>
                  <th>language</th>
                  <th>version</th>
                  <th>Operation</th>
                </tr>
              </thead>
              <tbody>
                {visibleConnections.map((row) => (
                  <tr key={row.clientId}>
                    <td>
                      <div className="producer-client-cell">
                        <code>{row.clientId}</code>
                        <span>active producer client</span>
                      </div>
                    </td>
                    <td>
                      <code>{row.clientAddr}</code>
                    </td>
                    <td>
                      <StatusBadge status={row.language || 'UNKNOWN'} />
                    </td>
                    <td>
                      <StatusBadge status={formatRocketMqVersion(row.version) || 'UNKNOWN'} tone="success" />
                    </td>
                    <td>
                      <div className="producer-action-grid">
                        <button type="button" className="button button-secondary producer-action-button" onClick={() => setSelectedConnection(row)}>
                          <Server size={14} aria-hidden="true" /> Details
                        </button>
                        <button type="button" className="button producer-action-button" onClick={() => copyConnection(row)}>
                          <Copy size={14} aria-hidden="true" /> Copy
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {visibleConnections.length === 0 ? (
            <EmptyState title="No producer connections" detail="Select a topic and producer group, then run SEARCH." />
          ) : null}
          <div className="topic-list-footer">
            <span>
              {filteredConnections.length} rows / page {currentPage} of {pageCount}
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

        <section className="producer-groups-panel">
          <div className="topic-list-toolbar">
            <div>
              <h2>Discovered producer groups</h2>
              <p>Use a group as a query shortcut.</p>
            </div>
            <StatusBadge status={`${filteredProducerGroups.length} groups`} />
          </div>
          <div className="topic-list-search">
            <Search size={16} aria-hidden="true" />
            <input value={groupQuery} placeholder="Search producer group" onChange={(event) => setGroupQuery(event.target.value)} />
          </div>
          <div className="producer-group-list">
            {filteredProducerGroups.map((item) => (
              <button type="button" className="producer-group-card" key={`${item.topic}-${item.producerGroup}`} onClick={() => useProducerGroup(item)}>
                <span>
                  <code>{item.producerGroup}</code>
                  <small>{item.topic || 'select topic in query form'}</small>
                </span>
                <StatusBadge status={`${item.connectionCount} conn`} tone={item.connectionCount > 0 ? 'success' : 'neutral'} />
              </button>
            ))}
          </div>
          {filteredProducerGroups.length === 0 ? <EmptyState title="No producer group summary" detail="Rust backend returned no producer discovery rows." /> : null}
        </section>
      </div>

      <ProducerConnectionDrawer
        connection={selectedConnection}
        topic={connection?.topic || topic}
        producerGroup={connection?.producerGroup || group}
        onOpenChange={(open) => {
          if (!open) setSelectedConnection(null);
        }}
      />
    </>
  );
}

interface ProducerMetricProps {
  label: string;
  value: string | number;
  detail: string;
  icon: ReactNode;
}

function ProducerMetric({ label, value, detail, icon }: ProducerMetricProps) {
  return (
    <div className="producer-metric-card">
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
      <div className="producer-metric-icon">{icon}</div>
    </div>
  );
}

interface ProducerConnectionDrawerProps {
  connection: ProducerConnectionInfo | null;
  topic: string;
  producerGroup: string;
  onOpenChange: (open: boolean) => void;
}

function ProducerConnectionDrawer({ connection, topic, producerGroup, onOpenChange }: ProducerConnectionDrawerProps) {
  return (
    <Dialog.Root open={connection !== null} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content producer-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Producer Client Detail</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={connection?.language ?? 'UNKNOWN'} />
                <StatusBadge status={formatRocketMqVersion(connection?.version) ?? 'UNKNOWN'} tone="success" />
              </div>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>
          <div className="producer-detail-list">
            <DetailRow label="clientId" value={connection?.clientId ?? ''} />
            <DetailRow label="clientAddr" value={connection?.clientAddr ?? ''} />
            <DetailRow label="language" value={connection?.language ?? ''} />
            <DetailRow label="version" value={formatRocketMqVersion(connection?.version) ?? ''} />
            <DetailRow label="topic" value={topic} />
            <DetailRow label="producerGroup" value={producerGroup} />
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="producer-detail-row">
      <span>{label}</span>
      <code>{value || '-'}</code>
    </div>
  );
}

function topicOptions(topics: TopicInfo[], currentTopic: string) {
  const options = topics.map((item) => ({ value: item.topic, label: item.topic }));
  if (currentTopic && !options.some((item) => item.value === currentTopic)) {
    options.unshift({ value: currentTopic, label: currentTopic });
  }
  if (options.length === 0) {
    return [{ value: '', label: 'Select a topic', disabled: true }];
  }
  return options;
}

function formatRocketMqVersion(version: string | undefined) {
  if (!version) return undefined;
  const trimmed = String(version).trim();
  if (!/^-?\d+$/.test(trimmed)) return trimmed;
  const knownVersions: Record<string, string> = {
    '513': 'V5_5_0'
  };
  return knownVersions[trimmed] ?? trimmed;
}
