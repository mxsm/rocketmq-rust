import { Activity, ListRestart, MoreHorizontal, RotateCcw, Users } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EntitySheet from '../components/EntitySheet';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import QueryToolbar from '../components/QueryToolbar';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import { Button } from '../components/ui/Button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '../components/ui/DropdownMenu';
import type { ConsumerGroupInfo, ConsumerListView } from '../types/consumer';
import ConsumerDetailContent from './consumers/ConsumerDetailContent';
import {
  filterConsumers,
  getConsumerMetrics,
  normalizeConsumerValue,
  type ConsumerLagFilter
} from './consumers/consumer-model';

const PAGE_SIZE = 10;

export default function ConsumerListPage() {
  const [data, setData] = useState<ConsumerListView | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const [consumeType, setConsumeType] = useState('all');
  const [messageModel, setMessageModel] = useState('all');
  const [lag, setLag] = useState<ConsumerLagFilter>('all');
  const [page, setPage] = useState(1);
  const [selectedConsumer, setSelectedConsumer] = useState<ConsumerGroupInfo | null>(null);
  const [detailTab, setDetailTab] = useState<'overview' | 'progress' | 'reset'>('overview');
  const detailTriggerRef = useRef<HTMLElement | null>(null);
  const actionTriggerRefs = useRef(new Map<string, HTMLButtonElement>());

  const load = async () => {
    if (data) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      setData(await consumerApi.list());
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

  const consumers = data?.items ?? [];
  const metrics = useMemo(() => getConsumerMetrics(consumers), [consumers]);
  const consumeTypes = useMemo(
    () => Array.from(new Set(consumers.map((consumer) => normalizeConsumerValue(consumer.consumeType)))).sort(),
    [consumers]
  );
  const messageModels = useMemo(
    () => Array.from(new Set(consumers.map((consumer) => normalizeConsumerValue(consumer.messageModel)))).sort(),
    [consumers]
  );
  const filteredConsumers = useMemo(
    () => filterConsumers(consumers, { query, consumeType, messageModel, lag }),
    [consumeType, consumers, lag, messageModel, query]
  );
  const pageCount = Math.max(1, Math.ceil(filteredConsumers.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const visibleConsumers = filteredConsumers.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const updateFilter = <T,>(setter: (value: T) => void) => (value: T) => {
    setter(value);
    setPage(1);
  };

  const openDetails = (
    consumer: ConsumerGroupInfo,
    origin?: HTMLElement,
    tab: 'overview' | 'progress' | 'reset' = 'overview'
  ) => {
    if (origin) detailTriggerRef.current = origin;
    setDetailTab(tab);
    setSelectedConsumer(consumer);
  };

  const columns: AppDataTableColumn<ConsumerGroupInfo>[] = [
    {
      id: 'group',
      header: 'Consumer group',
      width: '280px',
      cell: (consumer) => (
        <div className="entity-name-cell">
          <strong>{consumer.group}</strong>
          <Link to={`/consumers/${encodeURIComponent(consumer.group)}`}>Full page</Link>
        </div>
      )
    },
    {
      id: 'consumeType',
      header: 'Consume type',
      width: '150px',
      cell: (consumer) => <StatusBadge status={normalizeConsumerValue(consumer.consumeType)} tone="info" />
    },
    {
      id: 'messageModel',
      header: 'Message model',
      width: '160px',
      cell: (consumer) => <StatusBadge status={normalizeConsumerValue(consumer.messageModel)} tone="success" />
    },
    { id: 'clients', header: 'Clients', width: '90px', cell: (consumer) => consumer.clientCount },
    {
      id: 'lag',
      header: 'Total lag',
      width: '120px',
      cell: (consumer) => <StatusBadge status={String(consumer.diffTotal)} tone={consumer.diffTotal > 0 ? 'warning' : 'success'} />
    },
    {
      id: 'actions',
      header: 'Actions',
      width: '88px',
      align: 'right',
      cell: (consumer) => (
        <DropdownMenu modal={false}>
          <DropdownMenuTrigger asChild>
            <Button
              ref={(node) => {
                if (node) actionTriggerRefs.current.set(consumer.group, node);
                else actionTriggerRefs.current.delete(consumer.group);
              }}
              type="button"
              variant="ghost"
              size="icon"
              aria-label={`Actions for ${consumer.group}`}
            >
              <MoreHorizontal size={16} aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={() => openDetails(consumer, actionTriggerRefs.current.get(consumer.group), 'overview')}>
              <Activity size={15} aria-hidden="true" /> Inspect group
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => openDetails(consumer, actionTriggerRefs.current.get(consumer.group), 'progress')}>
              <ListRestart size={15} aria-hidden="true" /> View progress
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => openDetails(consumer, actionTriggerRefs.current.get(consumer.group), 'reset')}>
              <RotateCcw size={15} aria-hidden="true" /> Reset offset
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      )
    }
  ];

  if (loading) return <LoadingState label="Loading consumers" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;

  return (
    <div className="entity-workspace consumer-workspace">
      <PageHeader
        title="Consumer groups"
        description="Monitor group identity, connected clients, queue lag, and protected offset maintenance from current API data."
        actions={<RefreshButton refreshing={refreshing} onRefresh={() => void load()} />}
      />

      <div className="metric-grid entity-metrics">
        <MetricCard label="Consumer groups" value={metrics.groups} detail="Visible API inventory" icon={<Users size={18} />} />
        <MetricCard label="Connected clients" value={metrics.connectedClients} detail="Across all groups" icon={<Activity size={18} />} />
        <MetricCard label="Total lag" value={metrics.totalLag} detail="Aggregate diffTotal" icon={<RotateCcw size={18} />} />
        <MetricCard label="Lagging groups" value={metrics.laggingGroups} detail="Groups with lag above zero" icon={<ListRestart size={18} />} />
      </div>

      <section className="entity-table-card">
        <QueryToolbar
          searchValue={query}
          searchPlaceholder="Filter consumer groups"
          onSearchChange={updateFilter(setQuery)}
          onReset={() => {
            setQuery('');
            setConsumeType('all');
            setMessageModel('all');
            setLag('all');
            setPage(1);
          }}
        >
          <label className="native-filter-field">
            <span>Consume type</span>
            <select aria-label="Consume type filter" value={consumeType} onChange={(event) => updateFilter(setConsumeType)(event.target.value)}>
              <option value="all">All consume types</option>
              {consumeTypes.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Message model</span>
            <select aria-label="Message model filter" value={messageModel} onChange={(event) => updateFilter(setMessageModel)(event.target.value)}>
              <option value="all">All message models</option>
              {messageModels.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Lag</span>
            <select aria-label="Lag filter" value={lag} onChange={(event) => updateFilter(setLag)(event.target.value as ConsumerLagFilter)}>
              <option value="all">Any lag</option>
              <option value="lagging">Lagging</option>
              <option value="clear">No lag</option>
            </select>
          </label>
        </QueryToolbar>

        <AppDataTable
          ariaLabel="Consumer group inventory"
          rows={visibleConsumers}
          columns={columns}
          getRowId={(consumer) => consumer.group}
          page={currentPage}
          pageSize={PAGE_SIZE}
          total={filteredConsumers.length}
          onPageChange={setPage}
          onRowActivate={openDetails}
          emptyTitle="No consumer groups match"
          emptyDetail="Adjust the group, type, model, or lag filters."
        />
      </section>

      <EntitySheet
        open={selectedConsumer !== null}
        title={selectedConsumer?.group ?? 'Consumer details'}
        description={selectedConsumer
          ? `${normalizeConsumerValue(selectedConsumer.consumeType)} · ${normalizeConsumerValue(selectedConsumer.messageModel)}`
          : undefined}
        restoreFocusRef={detailTriggerRef}
        onOpenChange={(open) => { if (!open) setSelectedConsumer(null); }}
      >
        {selectedConsumer ? (
          <ConsumerDetailContent group={selectedConsumer.group} consumer={selectedConsumer} initialTab={detailTab} />
        ) : null}
      </EntitySheet>
    </div>
  );
}
