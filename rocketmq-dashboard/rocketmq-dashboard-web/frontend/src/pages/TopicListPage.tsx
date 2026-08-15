import { Database, Layers3, MoreHorizontal, Plus, RotateCcw, ShieldAlert, Trash2 } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EntitySheet from '../components/EntitySheet';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import QueryToolbar from '../components/QueryToolbar';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import TopicMaintenanceDialog from '../components/TopicMaintenanceDialog';
import TopicMutationDialog from '../components/TopicMutationDialog';
import { Button } from '../components/ui/Button';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from '../components/ui/AlertDialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '../components/ui/DropdownMenu';
import type { ConsumerGroupInfo } from '../types/consumer';
import type { TopicInfo, TopicListView, TopicMutationRequest } from '../types/topic';
import TopicDetailContent from './topics/TopicDetailContent';
import {
  filterTopics,
  getTopicCategory,
  getTopicMetrics,
  getTopicPermissionLabel,
  type TopicOperationalCategory
} from './topics/topic-model';

const PAGE_SIZE = 10;

export default function TopicListPage() {
  const [data, setData] = useState<TopicListView | null>(null);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroupInfo[]>([]);
  const [consumerGroupsLoading, setConsumerGroupsLoading] = useState(true);
  const [consumerGroupsError, setConsumerGroupsError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [brokerName, setBrokerName] = useState('all');
  const [category, setCategory] = useState<TopicOperationalCategory | 'all'>('all');
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [mutationOpen, setMutationOpen] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<TopicInfo | null>(null);
  const detailTriggerRef = useRef<HTMLElement | null>(null);
  const [maintenanceTopic, setMaintenanceTopic] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<TopicInfo | null>(null);
  const [deleting, setDeleting] = useState(false);
  const topicListRequestRef = useRef(0);
  const mountedRef = useRef(false);
  const consumerGroupsRequestRef = useRef(0);

  const load = async () => {
    const requestId = ++topicListRequestRef.current;
    const hasCatalog = data !== null;
    if (hasCatalog) setRefreshing(true);
    else setLoading(true);
    setError(null);
    setRefreshError(null);
    try {
      const nextData = await topicApi.list();
      if (mountedRef.current && topicListRequestRef.current === requestId) setData(nextData);
    } catch (requestError) {
      if (mountedRef.current && topicListRequestRef.current === requestId) {
        const message = requestError instanceof Error ? requestError.message : String(requestError);
        if (hasCatalog) setRefreshError(message);
        else setError(message);
      }
    } finally {
      if (mountedRef.current && topicListRequestRef.current === requestId) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  };

  const loadConsumerGroups = async () => {
    const requestId = ++consumerGroupsRequestRef.current;
    setConsumerGroupsLoading(true);
    setConsumerGroupsError(null);
    try {
      const consumerData = await consumerApi.list();
      if (consumerGroupsRequestRef.current === requestId) setConsumerGroups(consumerData.items);
    } catch (requestError) {
      if (consumerGroupsRequestRef.current === requestId) {
        setConsumerGroups([]);
        setConsumerGroupsError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (consumerGroupsRequestRef.current === requestId) setConsumerGroupsLoading(false);
    }
  };

  useEffect(() => {
    mountedRef.current = true;
    void load();
    void loadConsumerGroups();
    return () => {
      mountedRef.current = false;
      topicListRequestRef.current += 1;
      consumerGroupsRequestRef.current += 1;
    };
  }, []);

  const topics = data?.items ?? [];
  const metrics = useMemo(() => getTopicMetrics(topics), [topics]);
  const brokers = useMemo(
    () => Array.from(new Set(topics.map((topic) => topic.brokerName).filter((name): name is string => Boolean(name)))).sort(),
    [topics]
  );
  const filteredTopics = useMemo(
    () => filterTopics(topics, { query: search, brokerName, category }),
    [brokerName, category, search, topics]
  );
  const pageCount = Math.max(1, Math.ceil(filteredTopics.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const visibleTopics = filteredTopics.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const updateFilter = (setter: (value: string) => void) => (value: string) => {
    setter(value);
    setPage(1);
  };

  const saveTopic = async (request: TopicMutationRequest) => {
    if (topics.some((topic) => topic.topic === request.topic)) {
      throw new Error(`Topic \`${request.topic}\` already exists. Choose a new name.`);
    }
    const result = await topicApi.create(request);
    setNotice(result.success ? `Topic ${request.topic} created.` : null);
    void load();
    return result;
  };

  const deleteTopic = async (topic: string) => {
    setDeleting(true);
    try {
      await topicApi.delete(topic);
      setDeleteTarget(null);
      setNotice(`Topic ${topic} deleted.`);
      await load();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      setDeleting(false);
    }
  };

  const openCreate = () => {
    setMutationOpen(true);
  };

  const openDetails = (topic: TopicInfo, origin?: HTMLElement) => {
    if (origin) detailTriggerRef.current = origin;
    setSelectedTopic(topic);
  };

  const columns: AppDataTableColumn<TopicInfo>[] = [
    {
      id: 'topic',
      header: 'Topic',
      width: '260px',
      cell: (topic) => (
        <div className="entity-name-cell">
          <strong>{topic.topic}</strong>
          <Link to={`/topics/${encodeURIComponent(topic.topic)}`}>Full page</Link>
        </div>
      )
    },
    { id: 'category', header: 'Category', width: '130px', cell: (topic) => <StatusBadge status={topic.category.toUpperCase()} tone={categoryTone(getTopicCategory(topic))} /> },
    { id: 'broker', header: 'Broker', width: '150px', cell: (topic) => topic.brokerName || 'All brokers' },
    { id: 'queues', header: 'Queues R / W', width: '130px', cell: (topic) => `${topic.readQueueCount} / ${topic.writeQueueCount}` },
    { id: 'permission', header: 'Permission', width: '110px', cell: (topic) => <code>{getTopicPermissionLabel(topic.perm)}</code> },
    {
      id: 'actions',
      header: 'Actions',
      width: '88px',
      align: 'right',
      cell: (topic) => (
        <div className="entity-row-actions">
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <Button type="button" variant="ghost" size="icon" aria-label={`Actions for ${topic.topic}`}>
                <MoreHorizontal size={16} aria-hidden="true" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {getTopicCategory(topic) !== 'system' ? (
                <>
                  <DropdownMenuItem
                    disabled={consumerGroupsLoading || Boolean(consumerGroupsError) || consumerGroups.length === 0}
                    onSelect={() => setMaintenanceTopic(topic.topic)}
                  >
                    <RotateCcw size={15} aria-hidden="true" /> Reset offsets
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem className="ui-menu-item-danger" onSelect={() => setDeleteTarget(topic)}>
                    <Trash2 size={15} aria-hidden="true" /> Delete topic
                  </DropdownMenuItem>
                </>
              ) : null}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      )
    }
  ];

  if (loading) return <LoadingState label="Loading topics" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;

  return (
    <div className="entity-workspace topic-workspace">
      <PageHeader
        title="Topics"
        description="Manage topic inventory, queue permissions, routes, and API-backed maintenance from one workspace."
        actions={
          <>
            <Button type="button" onClick={openCreate}><Plus size={15} aria-hidden="true" /> Create topic</Button>
            <RefreshButton
              refreshing={refreshing || consumerGroupsLoading}
              onRefresh={() => {
                void load();
                void loadConsumerGroups();
              }}
            />
          </>
        }
      />

      {notice ? <div className="notice notice-success" role="status">{notice}</div> : null}
      {refreshError ? (
        <div className="notice notice-danger entity-auxiliary-error" role="alert">
          <span>{refreshError}</span>
          <Button type="button" variant="outline" size="sm" onClick={() => void load()}>
            Retry topic catalog
          </Button>
        </div>
      ) : null}
      {consumerGroupsError ? (
        <div className="notice notice-danger entity-auxiliary-error" role="alert">
          <span>{consumerGroupsError}</span>
          <Button type="button" variant="outline" size="sm" onClick={() => void loadConsumerGroups()}>
            Retry consumer groups
          </Button>
        </div>
      ) : null}
      {!consumerGroupsLoading && !consumerGroupsError && consumerGroups.length === 0 ? (
        <div className="notice notice-warning entity-auxiliary-error" role="status">
          <span>No consumer groups are available for offset reset.</span>
          <Button type="button" variant="outline" size="sm" onClick={() => void loadConsumerGroups()}>
            Reload consumer groups
          </Button>
        </div>
      ) : null}

      <div className="metric-grid entity-metrics">
        <MetricCard label="Total topics" value={metrics.total} detail="Visible API inventory" icon={<Database size={18} />} />
        <MetricCard label="Application" value={metrics.application} detail="Non-system workloads" icon={<Layers3 size={18} />} />
        <MetricCard label="Retry" value={metrics.retry} detail="Retry delivery topics" icon={<RotateCcw size={18} />} />
        <MetricCard label="DLQ" value={metrics.dlq} detail="Dead-letter topics" icon={<ShieldAlert size={18} />} />
        <MetricCard label="System" value={metrics.system} detail="RocketMQ internal topics" icon={<Database size={18} />} />
      </div>

      <section className="entity-table-card">
        <QueryToolbar
          searchValue={search}
          searchPlaceholder="Filter topics"
          onSearchChange={updateFilter(setSearch)}
          onReset={() => { setSearch(''); setBrokerName('all'); setCategory('all'); setPage(1); }}
        >
          <label className="native-filter-field">
            <span>Category</span>
            <select
              aria-label="Category filter"
              value={category}
              onChange={(event) => { setCategory(event.target.value as TopicOperationalCategory | 'all'); setPage(1); }}
            >
              <option value="all">All categories</option>
              <option value="application">Application</option>
              <option value="retry">Retry</option>
              <option value="dlq">DLQ</option>
              <option value="system">System</option>
            </select>
          </label>
          <label className="native-filter-field">
            <span>Broker</span>
            <select aria-label="Broker filter" value={brokerName} onChange={(event) => updateFilter(setBrokerName)(event.target.value)}>
              <option value="all">All brokers</option>
              {brokers.map((broker) => <option key={broker} value={broker}>{broker}</option>)}
            </select>
          </label>
        </QueryToolbar>
        <AppDataTable
          ariaLabel="Topic inventory"
          rows={visibleTopics}
          columns={columns}
          getRowId={(topic) => topic.topic}
          page={currentPage}
          pageSize={PAGE_SIZE}
          total={filteredTopics.length}
          onPageChange={setPage}
          onRowActivate={openDetails}
          emptyTitle="No topics match"
          emptyDetail="Adjust the search, category, or broker filters."
        />
      </section>

      <TopicMutationDialog
        open={mutationOpen}
        mode="create"
        targets={data?.targets ?? []}
        onOpenChange={setMutationOpen}
        onSubmit={saveTopic}
      />
      <EntitySheet
        open={selectedTopic !== null}
        title={selectedTopic?.topic ?? 'Topic details'}
        description={selectedTopic ? `${selectedTopic.category} · ${selectedTopic.brokerName || 'All brokers'}` : undefined}
        restoreFocusRef={detailTriggerRef}
        onOpenChange={(open) => { if (!open) setSelectedTopic(null); }}
      >
        {selectedTopic ? (
          <TopicDetailContent topicName={selectedTopic.topic} topic={selectedTopic} />
        ) : null}
      </EntitySheet>
      <TopicMaintenanceDialog
        open={maintenanceTopic !== null}
        topic={maintenanceTopic}
        consumerGroups={consumerGroups}
        onOpenChange={(open) => { if (!open) setMaintenanceTopic(null); }}
        onMutationFinished={() => void load()}
      />
      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => { if (!open && !deleting) setDeleteTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogTitle>Delete topic?</AlertDialogTitle>
          <AlertDialogDescription>
            Delete {deleteTarget?.topic}? This changes cluster metadata and cannot be undone from the dashboard.
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={deleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={deleting}
              onClick={(event) => {
                event.preventDefault();
                if (deleteTarget) void deleteTopic(deleteTarget.topic);
              }}
            >
              {deleting ? 'Deleting' : 'Delete topic'}
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function categoryTone(category: TopicOperationalCategory) {
  if (category === 'retry') return 'warning';
  if (category === 'dlq') return 'danger';
  if (category === 'system') return 'info';
  return 'success';
}
