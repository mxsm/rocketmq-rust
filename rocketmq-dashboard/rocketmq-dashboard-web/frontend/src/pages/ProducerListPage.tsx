import { Cable, DatabaseZap, RadioTower, Server, Users } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { producerApi } from '../api/producer_api';
import { topicApi } from '../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EmptyState from '../components/EmptyState';
import EntitySheet from '../components/EntitySheet';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import QueryToolbar from '../components/QueryToolbar';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import { Button } from '../components/ui/Button';
import type { ProducerConnectionInfo, ProducerConnectionView, ProducerInfo } from '../types/producer';
import type { TopicInfo } from '../types/topic';
import ProducerDetailContent from './producers/ProducerDetailContent';
import { filterProducers, getProducerMetrics } from './producers/producer-model';

const PAGE_SIZE = 10;

export default function ProducerListPage() {
  const [items, setItems] = useState<ProducerInfo[]>([]);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(1);
  const [selectedProducer, setSelectedProducer] = useState<ProducerInfo | null>(null);
  const [selectedTopic, setSelectedTopic] = useState('');
  const [connection, setConnection] = useState<ProducerConnectionView | null>(null);
  const [connectionLoading, setConnectionLoading] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [selectedClient, setSelectedClient] = useState<ProducerConnectionInfo | null>(null);
  const clientTriggerRef = useRef<HTMLElement | null>(null);
  const connectionRequestRef = useRef(0);

  const load = async () => {
    if (items.length > 0) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      const [nextItems, topicView] = await Promise.all([producerApi.list(), topicApi.list()]);
      setItems(nextItems);
      setTopics(topicView.items);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const metrics = useMemo(() => getProducerMetrics(items), [items]);
  const availableTopics = useMemo(
    () => Array.from(new Set(topics.map((topic) => topic.topic).filter(Boolean))).sort(),
    [topics]
  );
  const filteredProducers = useMemo(() => filterProducers(items, query), [items, query]);
  const pageCount = Math.max(1, Math.ceil(filteredProducers.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const visibleProducers = filteredProducers.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const selectProducer = (producer: ProducerInfo) => {
    connectionRequestRef.current += 1;
    setSelectedProducer(producer);
    setSelectedTopic('');
    setConnection(null);
    setConnectionError(null);
    setConnectionLoading(false);
    setSelectedClient(null);
  };

  const changeTopic = (topic: string) => {
    connectionRequestRef.current += 1;
    setSelectedTopic(topic);
    setConnection(null);
    setConnectionError(null);
    setConnectionLoading(false);
    setSelectedClient(null);
  };

  const queryConnections = async () => {
    if (!selectedProducer || !selectedTopic) return;
    const requestId = connectionRequestRef.current + 1;
    connectionRequestRef.current = requestId;
    setConnectionLoading(true);
    setConnection(null);
    setConnectionError(null);
    setSelectedClient(null);
    try {
      const nextConnection = await producerApi.connections(selectedTopic, selectedProducer.producerGroup);
      if (connectionRequestRef.current === requestId) setConnection(nextConnection);
    } catch (requestError) {
      if (connectionRequestRef.current === requestId) {
        setConnectionError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (connectionRequestRef.current === requestId) setConnectionLoading(false);
    }
  };

  const openClientDetails = (client: ProducerConnectionInfo, origin: HTMLElement) => {
    clientTriggerRef.current = origin;
    setSelectedClient(client);
  };

  const producerColumns: AppDataTableColumn<ProducerInfo>[] = [
    {
      id: 'group',
      header: 'Producer group',
      width: '180px',
      cell: (producer) => <strong className="mono">{producer.producerGroup}</strong>
    },
    {
      id: 'connections',
      header: 'Discovered connections',
      width: '150px',
      cell: (producer) => (
        <StatusBadge
          status={String(producer.connectionCount)}
          tone={producer.connectionCount > 0 ? 'success' : 'neutral'}
        />
      )
    },
    {
      id: 'status',
      header: 'Status',
      width: '110px',
      cell: (producer) => (
        <StatusBadge
          status={producer.connectionCount > 0 ? 'CONNECTED' : 'NO CLIENTS'}
          tone={producer.connectionCount > 0 ? 'success' : 'neutral'}
        />
      )
    },
    {
      id: 'actions',
      header: 'Actions',
      width: '56px',
      align: 'right',
      cell: (producer) => (
        <Button
          type="button"
          variant="ghost"
          size="icon"
          aria-label={`Select producer group ${producer.producerGroup}`}
          onClick={() => selectProducer(producer)}
        >
          <Cable size={16} aria-hidden="true" />
        </Button>
      )
    }
  ];

  const connectionColumns: AppDataTableColumn<ProducerConnectionInfo>[] = [
    { id: 'clientId', header: 'Client ID', width: '170px', cell: (client) => <strong className="mono">{client.clientId}</strong> },
    { id: 'address', header: 'Client address', width: '140px', cell: (client) => <span className="mono">{client.clientAddr}</span> },
    { id: 'language', header: 'Language', width: '80px', cell: (client) => <StatusBadge status={client.language || 'UNKNOWN'} tone="info" /> },
    { id: 'version', header: 'Version', width: '70px', cell: (client) => client.version || 'UNKNOWN' }
  ];

  if (loading) return <LoadingState label="Loading producers" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;

  const connections = connection?.connections ?? [];

  return (
    <div className="entity-workspace producer-workspace">
      <PageHeader
        title="Producers"
        description="Inspect discovered producer groups and load live client identity for an exact topic and group pair."
        actions={<RefreshButton refreshing={refreshing} onRefresh={() => void load()} />}
      />

      <div className="metric-grid entity-metrics">
        <MetricCard label="Producer groups" value={metrics.producerGroups} detail="Unique discovered groups" icon={<Users size={18} />} />
        <MetricCard label="Available topics" value={availableTopics.length} detail="Selectable query targets" icon={<DatabaseZap size={18} />} />
        <MetricCard label="Discovered connections" value={metrics.discoveredConnections} detail="Backend discovery count" icon={<RadioTower size={18} />} />
        <MetricCard label="Connected groups" value={metrics.connectedGroups} detail="Groups reporting clients" icon={<Server size={18} />} />
      </div>

      <div className="producer-workspace-grid">
        <section className="entity-table-card producer-inventory-card">
          <div className="entity-card-heading">
            <div>
              <h2>Producer inventory</h2>
              <p>Select a group, then choose the exact topic to query.</p>
            </div>
            <StatusBadge status={`${filteredProducers.length} groups`} tone="neutral" />
          </div>
          <QueryToolbar
            searchValue={query}
            searchPlaceholder="Filter producers"
            onSearchChange={(value) => {
              setQuery(value);
              setPage(1);
            }}
            onReset={() => {
              setQuery('');
              setPage(1);
            }}
          />
          <AppDataTable
            ariaLabel="Producer inventory"
            rows={visibleProducers}
            columns={producerColumns}
            getRowId={(producer) => producer.producerGroup}
            page={currentPage}
            pageSize={PAGE_SIZE}
            total={filteredProducers.length}
            onPageChange={setPage}
            onRowActivate={selectProducer}
            emptyTitle="No producers match"
            emptyDetail="Adjust the producer group filter."
          />
        </section>

        <section className="entity-table-card producer-connections-card" aria-labelledby="producer-connections-heading">
          <div className="entity-card-heading">
            <div>
              <h2 id="producer-connections-heading">Producer connections</h2>
              <p>
                {selectedProducer
                  ? `${selectedProducer.producerGroup}${selectedTopic ? ` · ${selectedTopic}` : ''}`
                  : 'Choose a producer group, then select a topic.'}
              </p>
            </div>
          </div>

          {!selectedProducer ? (
            <EmptyState title="Select a producer group" detail="Connection data is loaded only after you choose a group and an exact topic." />
          ) : availableTopics.length === 0 ? (
            <div className="producer-empty-topics">
              <EmptyState
                title="No topic targets available"
                detail="Reload the topic inventory before querying producer connections."
              />
              <Button type="button" variant="outline" size="sm" loading={refreshing} onClick={() => void load()}>
                Refresh topics
              </Button>
            </div>
          ) : (
            <>
              <div className="producer-query-controls">
                <label>
                  <span>Topic</span>
                  <select
                    className="ui-select-native"
                    aria-label="Producer topic"
                    value={selectedTopic}
                    onChange={(event) => changeTopic(event.target.value)}
                  >
                    <option value="">Select a topic</option>
                    {availableTopics.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
                  </select>
                </label>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  loading={connectionLoading}
                  disabled={!selectedTopic}
                  aria-label="Query producer connections"
                  onClick={() => void queryConnections()}
                >
                  {!connectionLoading ? <Cable size={15} aria-hidden="true" /> : null}
                  Query connections
                </Button>
              </div>
              {connectionLoading || connectionError || connection ? (
                <AppDataTable
                  ariaLabel="Producer connections"
                  rows={connections}
                  columns={connectionColumns}
                  getRowId={(client) => `${client.clientId}-${client.clientAddr}`}
                  page={1}
                  pageSize={Math.max(1, connections.length)}
                  total={connections.length}
                  onPageChange={() => undefined}
                  onRowActivate={openClientDetails}
                  loading={connectionLoading}
                  error={connectionError}
                  onRetry={() => void queryConnections()}
                  retryLabel="Retry connection query"
                  emptyTitle="No producer connections"
                  emptyDetail="The selected topic and producer group returned no active client connections."
                />
              ) : (
                <EmptyState title="Choose a topic to query" detail="Select the exact topic associated with this producer group, then run the connection query." />
              )}
            </>
          )}
        </section>
      </div>

      <EntitySheet
        open={selectedClient !== null}
        title={selectedClient?.clientId ?? 'Producer client details'}
        description={connection ? `${connection.producerGroup} · ${connection.topic}` : undefined}
        restoreFocusRef={clientTriggerRef}
        onOpenChange={(open) => { if (!open) setSelectedClient(null); }}
      >
        {selectedClient && connection ? (
          <ProducerDetailContent
            connection={selectedClient}
            topic={connection.topic}
            producerGroup={connection.producerGroup}
          />
        ) : null}
      </EntitySheet>
    </div>
  );
}
