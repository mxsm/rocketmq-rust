import { Activity, ListRestart, MoreHorizontal, Pencil, Plus, RotateCcw, Trash2, Users } from 'lucide-react';
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import ConsumerDeleteDialog from '../components/ConsumerDeleteDialog';
import ConsumerMutationDialog from '../components/ConsumerMutationDialog';
import { useConsumerMutationScopeRevision } from '../components/consumerMutationLock';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
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
import type { ConsumerGroupListItem, ConsumerGroupListView } from '../types/consumer';
import type { ConsumerOperationResult } from '../types/consumer';
import { useConsumerQueryScope } from './consumers/ConsumerQueryScopeProvider';
import {
  clampConsumerPage,
  consumerCategoryOf,
  DEFAULT_CONSUMER_FILTERS,
  DEFAULT_CONSUMER_SORT,
  getConsumerMetrics,
  isSystemConsumerGroup,
  normalizeConsumerValue,
  selectConsumerRows,
  summarizeConsumerTargets,
  type ConsumerCategory,
  type ConsumerFilters,
  type ConsumerLagFilter,
  type ConsumerSort,
  type ConsumerSortKey
} from './consumers/consumer-model';

const PAGE_SIZE = 10;

function consumerScopeKey(scope: { mode: string; proxyAddress?: string }) {
  return `${scope.mode}:${scope.proxyAddress ?? ''}`;
}

const SORT_OPTIONS: Array<{ key: ConsumerSortKey; label: string }> = [
  { key: 'rawGroupName', label: 'Group' },
  { key: 'connectionCount', label: 'Connections' },
  { key: 'consumeTps', label: 'TPS' },
  { key: 'diffTotal', label: 'Total lag' },
  { key: 'version', label: 'Version' },
  { key: 'updateTimestamp', label: 'Updated' }
];

export default function ConsumerListPage() {
  const { scope, revision } = useConsumerQueryScope();
  const scopeKey = consumerScopeKey(scope);
  const terminalRevision = useConsumerMutationScopeRevision(scopeKey);
  const [data, setData] = useState<ConsumerGroupListView | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [initialError, setInitialError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [filters, setFilters] = useState<ConsumerFilters>(DEFAULT_CONSUMER_FILTERS);
  const [sort, setSort] = useState<ConsumerSort>(DEFAULT_CONSUMER_SORT);
  const [page, setPage] = useState(1);
  const [mutationMode, setMutationMode] = useState<'create' | 'edit' | null>(null);
  const [mutationConsumer, setMutationConsumer] = useState<ConsumerGroupListItem | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ConsumerGroupListItem | null>(null);
  const requestToken = useRef(0);
  const committedScopeRef = useRef({ key: '', generation: 0 });

  const isCurrentScope = (requestScope: { key: string; generation: number }) => {
    const current = committedScopeRef.current;
    return current.key === requestScope.key && current.generation === requestScope.generation;
  };

  // A candidate can be rendered and discarded in concurrent React. Only the
  // layout effect publishes the current scope used by asynchronous reads.
  useLayoutEffect(() => {
    const current = committedScopeRef.current;
    if (current.key === scopeKey) return;
    committedScopeRef.current = { key: scopeKey, generation: current.generation + 1 };
    requestToken.current += 1;
    setData(null);
    setInitialError(null);
    setRefreshError(null);
    setLoading(true);
  }, [scopeKey]);

  const load = async (
    isRefresh: boolean,
    requestScope = committedScopeRef.current,
    requestScopeValue = scope
  ) => {
    if (requestScope.key !== consumerScopeKey(requestScopeValue) || !isCurrentScope(requestScope)) return;
    const token = ++requestToken.current;
    if (isRefresh) {
      setRefreshing(true);
      setRefreshError(null);
    } else {
      setLoading(true);
      setInitialError(null);
    }
    try {
      const next = await consumerApi.list(requestScopeValue);
      if (token !== requestToken.current || !isCurrentScope(requestScope)) return;
      setData(next);
    } catch (error) {
      if (token !== requestToken.current || !isCurrentScope(requestScope)) return;
      const message = error instanceof Error ? error.message : String(error);
      if (isRefresh) setRefreshError(message);
      else setInitialError(message);
    } finally {
      if (token === requestToken.current && isCurrentScope(requestScope)) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  };

  useEffect(() => {
    const requestScope = committedScopeRef.current;
    if (requestScope.key !== scopeKey) return;
    setPage(1);
    void load(false, requestScope, scope);
  }, [scopeKey, revision, terminalRevision]);

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
  const brokers = useMemo(
    () => Array.from(new Set(consumers.flatMap((consumer) => consumer.brokerNames))).sort(),
    [consumers]
  );
  const versions = useMemo(
    () => Array.from(new Set(consumers.map((consumer) => consumer.versionDesc).filter(Boolean))).sort(),
    [consumers]
  );

  const sorted = useMemo(
    () => selectConsumerRows(consumers, filters, sort),
    [consumers, filters, sort]
  );
  const currentPage = clampConsumerPage(page, PAGE_SIZE, sorted.length);
  const visible = sorted.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const resetFilters = () => {
    setFilters(DEFAULT_CONSUMER_FILTERS);
    setSort(DEFAULT_CONSUMER_SORT);
    setPage(1);
  };

  const columns: AppDataTableColumn<ConsumerGroupListItem>[] = [
    {
      id: 'group',
      header: 'Consumer group',
      width: '260px',
      cell: (consumer) => (
        <div className="entity-name-cell">
          <strong>{consumer.displayGroupName}</strong>
          {consumer.rawGroupName !== consumer.displayGroupName ? (
            <span className="mono entity-raw-name">{consumer.rawGroupName}</span>
          ) : null}
          <Link to={`/consumers/${encodeURIComponent(consumer.rawGroupName)}`}>Open workspace</Link>
        </div>
      )
    },
    {
      id: 'category',
      header: 'Category',
      width: '110px',
      cell: (consumer) => (
        <StatusBadge status={consumerCategoryOf(consumer)} tone={isSystemConsumerGroup(consumer) ? 'neutral' : 'info'} />
      )
    },
    { id: 'connections', header: 'Connections', width: '110px', cell: (consumer) => consumer.connectionCount },
    {
      id: 'version',
      header: 'Version',
      width: '110px',
      cell: (consumer) => consumer.versionDesc || '-'
    },
    {
      id: 'consumeType',
      header: 'Consume type',
      width: '140px',
      cell: (consumer) => <StatusBadge status={normalizeConsumerValue(consumer.consumeType)} tone="info" />
    },
    {
      id: 'messageModel',
      header: 'Message model',
      width: '150px',
      cell: (consumer) => <StatusBadge status={normalizeConsumerValue(consumer.messageModel)} tone="success" />
    },
    { id: 'tps', header: 'TPS', width: '90px', align: 'right', cell: (consumer) => consumer.consumeTps },
    {
      id: 'lag',
      header: 'Total lag',
      width: '120px',
      cell: (consumer) => <StatusBadge status={String(consumer.diffTotal)} tone={consumer.diffTotal > 0 ? 'warning' : 'success'} />
    },
    {
      id: 'targets',
      header: 'Targets',
      width: '150px',
      cell: (consumer) => <span title={consumer.brokerNames.join(', ')}>{summarizeConsumerTargets(consumer)}</span>
    },
    {
      id: 'updated',
      header: 'Updated',
      width: '160px',
      cell: (consumer) => formatTimestamp(consumer.updateTimestamp)
    },
    {
      id: 'actions',
      header: 'Actions',
      width: '88px',
      align: 'right',
      cell: (consumer) => (
        <DropdownMenu modal={false}>
          <DropdownMenuTrigger asChild>
            <Button type="button" variant="ghost" size="icon" aria-label={`Actions for ${consumer.rawGroupName}`}>
              <MoreHorizontal size={16} aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem asChild>
              <Link to={`/consumers/${encodeURIComponent(consumer.rawGroupName)}`}>Open workspace</Link>
            </DropdownMenuItem>
            <DropdownMenuItem asChild>
              <Link to={`/consumers/${encodeURIComponent(consumer.rawGroupName)}?tab=clients`}>View clients</Link>
            </DropdownMenuItem>
            <DropdownMenuItem asChild>
              <Link to={`/consumers/${encodeURIComponent(consumer.rawGroupName)}?tab=progress`}>View progress</Link>
            </DropdownMenuItem>
            <DropdownMenuItem asChild>
              <Link to={`/consumers/${encodeURIComponent(consumer.rawGroupName)}?tab=config`}>View configuration</Link>
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => { setMutationMode('edit'); setMutationConsumer(consumer); }}>
              <Pencil size={15} aria-hidden="true" /> Edit configuration
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => setDeleteTarget(consumer)}>
              <Trash2 size={15} aria-hidden="true" /> Delete group
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      )
    }
  ];

  if (loading) return <LoadingState label="Loading consumers" />;
  if (initialError) return <ErrorState message={initialError} onRetry={() => void load(false)} />;

  return (
    <div className="entity-workspace consumer-workspace">
      <PageHeader
        title="Consumer groups"
        description="Monitor group identity, connected clients, queue lag, and protected offset maintenance."
        actions={<>
          <Button type="button" variant="outline" onClick={() => { setMutationMode('create'); setMutationConsumer(null); }}>
            <Plus size={15} aria-hidden="true" /> Create group
          </Button>
          <RefreshButton refreshing={refreshing} onRefresh={() => void load(true)} />
        </>}
      />

      <div className="metric-grid entity-metrics">
        <MetricCard label="Consumer groups" value={metrics.groups} detail="Visible API inventory" icon={<Users size={18} />} />
        <MetricCard label="Connected clients" value={metrics.connectedClients} detail="Across all groups" icon={<Activity size={18} />} />
        <MetricCard label="Total lag" value={metrics.totalLag} detail="Aggregate diffTotal" icon={<RotateCcw size={18} />} />
        <MetricCard label="Lagging groups" value={metrics.laggingGroups} detail="Groups with lag above zero" icon={<ListRestart size={18} />} />
      </div>

      <section className="entity-table-card">
        {refreshError ? (
          <div className="notice notice-danger" role="alert">
            <span>{refreshError}</span>
            <Button type="button" variant="outline" size="sm" onClick={() => void load(true)}>Retry refresh</Button>
          </div>
        ) : null}

        <QueryToolbar
          searchValue={filters.query}
          searchPlaceholder="Filter consumer groups"
          onSearchChange={(value) => { setFilters((current) => ({ ...current, query: value })); setPage(1); }}
          onReset={resetFilters}
        >
          <label className="native-filter-field">
            <span>Category</span>
            <select aria-label="Category filter" value={filters.categories[0] ?? 'all'} onChange={(event) => {
              const value = event.target.value;
              setFilters((current) => ({ ...current, categories: value === 'all' ? [] : [value as ConsumerCategory] }));
              setPage(1);
            }}>
              <option value="all">All categories</option>
              <option value="NORMAL">Normal</option>
              <option value="FIFO">FIFO</option>
              <option value="SYSTEM">System</option>
            </select>
          </label>
          <label className="native-filter-field">
            <span>Consume type</span>
            <select aria-label="Consume type filter" value={filters.consumeTypes[0] ?? 'all'} onChange={(event) => {
              const value = event.target.value;
              setFilters((current) => ({ ...current, consumeTypes: value === 'all' ? [] : [value] }));
              setPage(1);
            }}>
              <option value="all">All consume types</option>
              {consumeTypes.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Message model</span>
            <select aria-label="Message model filter" value={filters.messageModels[0] ?? 'all'} onChange={(event) => {
              const value = event.target.value;
              setFilters((current) => ({ ...current, messageModels: value === 'all' ? [] : [value] }));
              setPage(1);
            }}>
              <option value="all">All message models</option>
              {messageModels.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Lag</span>
            <select aria-label="Lag filter" value={filters.lag} onChange={(event) => {
              setFilters((current) => ({ ...current, lag: event.target.value as ConsumerLagFilter }));
              setPage(1);
            }}>
              <option value="all">Any lag</option>
              <option value="lagging">Lagging</option>
              <option value="clear">No lag</option>
            </select>
          </label>
          <label className="native-filter-field">
            <span>Broker</span>
            <select aria-label="Broker filter" value={filters.brokers[0] ?? 'all'} onChange={(event) => {
              const value = event.target.value;
              setFilters((current) => ({ ...current, brokers: value === 'all' ? [] : [value] }));
              setPage(1);
            }}>
              <option value="all">All brokers</option>
              {brokers.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Version</span>
            <select aria-label="Version filter" value={filters.versions[0] ?? 'all'} onChange={(event) => {
              const value = event.target.value;
              setFilters((current) => ({ ...current, versions: value === 'all' ? [] : [value] }));
              setPage(1);
            }}>
              <option value="all">All versions</option>
              {versions.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Sort</span>
            <select aria-label="Sort by" value={`${sort.key}:${sort.direction}`} onChange={(event) => {
              const [key, direction] = event.target.value.split(':') as [ConsumerSortKey, ConsumerSort['direction']];
              setSort({ key, direction });
              setPage(1);
            }}>
              {SORT_OPTIONS.flatMap((option) => [
                <option key={`${option.key}:asc`} value={`${option.key}:asc`}>{option.label} (asc)</option>,
                <option key={`${option.key}:desc`} value={`${option.key}:desc`}>{option.label} (desc)</option>
              ])}
            </select>
          </label>
        </QueryToolbar>

        <AppDataTable
          ariaLabel="Consumer group inventory"
          rows={visible}
          columns={columns}
          getRowId={(consumer) => consumer.rawGroupName}
          page={currentPage}
          pageSize={PAGE_SIZE}
          total={sorted.length}
          onPageChange={setPage}
          emptyTitle="No consumer groups match"
          emptyDetail="Adjust the group, category, type, model, broker, or lag filters."
        />
      </section>

      <ConsumerMutationDialog
        open={mutationMode !== null}
        mode={mutationMode ?? 'create'}
        consumer={mutationConsumer}
        onOpenChange={(open) => { if (!open) { setMutationMode(null); setMutationConsumer(null); } }}
        onSucceeded={() => {
          if (mutationMode !== 'create') void load(false);
        }}
      />

      <ConsumerDeleteDialog
        open={deleteTarget !== null}
        consumer={deleteTarget}
        onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}
        onSucceeded={() => void load(false)}
        onAppliedAuditFailure={() => load(false)}
      />
    </div>
  );
}

function formatTimestamp(value: number): string {
  if (!value) return '-';
  return new Date(value).toLocaleString();
}
