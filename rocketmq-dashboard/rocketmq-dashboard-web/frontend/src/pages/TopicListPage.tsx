import {
  Database,
  Eye,
  Layers3,
  MoreHorizontal,
  Pencil,
  Plus,
  RotateCcw,
  Send,
  ShieldAlert,
  SkipForward,
  Trash2
} from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { topicApi } from '../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EntitySheet from '../components/EntitySheet';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import TopicDeleteDialog from '../components/TopicDeleteDialog';
import TopicMutationDialog from '../components/TopicMutationDialog';
import TopicResetOffsetDialog from '../components/TopicResetOffsetDialog';
import TopicSendMessageDialog from '../components/TopicSendMessageDialog';
import TopicSkipBacklogDialog from '../components/TopicSkipBacklogDialog';
import { Button } from '../components/ui/Button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '../components/ui/DropdownMenu';
import type {
  TopicConfigView,
  TopicConsumerView,
  TopicInfo,
  TopicListView,
  TopicMutationRequest,
  TopicOperationResult
} from '../types/topic';
import TopicConsumerActionDialog, { type TopicConsumerActionKind } from './topics/TopicConsumerActionDialog';
import TopicDetailContent from './topics/TopicDetailContent';
import TopicFilterToolbar from './topics/TopicFilterToolbar';
import TopicTargetSummary from './topics/TopicTargetSummary';
import {
  filterTopics,
  getTopicActionAvailability,
  getTopicCategory,
  getTopicMetrics,
  getTopicPermissionLabel,
  type TopicFilters,
  type TopicOperationalCategory
} from './topics/topic-model';

const PAGE_SIZE = 10;
const INITIAL_FILTERS: TopicFilters = {
  query: '',
  brokerName: 'all',
  clusterName: 'all',
  messageTypes: [],
  categories: []
};

type TopicAction =
  | { kind: 'edit'; topic: TopicInfo }
  | { kind: 'send'; topic: TopicInfo }
  | { kind: 'reset'; topic: TopicInfo; consumerGroup?: string }
  | { kind: 'skip'; topic: TopicInfo; consumerGroup?: string }
  | { kind: 'delete-broker'; topic: TopicInfo; brokerName?: string }
  | { kind: 'delete-topic'; topic: TopicInfo };

type MenuAction = 'view' | 'edit' | 'send' | 'reset' | 'skip' | 'delete-broker' | 'delete-topic';

interface ConfigDiscovery {
  topicName: string;
  data: TopicConfigView | null;
  loading: boolean;
  error: string | null;
}

interface ConsumerDiscovery {
  topicName: string;
  kind: TopicConsumerActionKind;
  items: TopicConsumerView[];
  loading: boolean;
  error: string | null;
}

const emptyConfig = (topicName = ''): ConfigDiscovery => ({ topicName, data: null, loading: false, error: null });
const emptyConsumers = (topicName = '', kind: TopicConsumerActionKind = 'reset'): ConsumerDiscovery => ({
  topicName,
  kind,
  items: [],
  loading: false,
  error: null
});
const errorMessage = (error: unknown) => error instanceof Error ? error.message : String(error);

export default function TopicListPage() {
  const [data, setData] = useState<TopicListView | null>(null);
  const dataRef = useRef<TopicListView | null>(null);
  const [filters, setFilters] = useState<TopicFilters>(INITIAL_FILTERS);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<TopicInfo | null>(null);
  const [detailRevisions, setDetailRevisions] = useState({ stats: 0, route: 0, consumers: 0, config: 0 });
  const [action, setAction] = useState<TopicAction | null>(null);
  const [editConfig, setEditConfig] = useState<ConfigDiscovery>(() => emptyConfig());
  const [consumerDiscovery, setConsumerDiscovery] = useState<ConsumerDiscovery>(() => emptyConsumers());

  const mountedRef = useRef(false);
  const topicListRequestRef = useRef(0);
  const detailTriggerRef = useRef<HTMLElement | null>(null);
  const selectedTopicRef = useRef<TopicInfo | null>(null);
  const actionRef = useRef<TopicAction | null>(null);
  const actionGenerationRef = useRef(0);
  const configRequestRef = useRef(0);
  const consumersRequestRef = useRef(0);
  const createOpenRef = useRef(false);
  const createGenerationRef = useRef(0);

  dataRef.current = data;
  selectedTopicRef.current = selectedTopic;

  const load = useCallback(async () => {
    const requestId = ++topicListRequestRef.current;
    const hasCatalog = dataRef.current !== null;
    if (hasCatalog) setRefreshing(true);
    else setLoading(true);
    setError(null);
    setRefreshError(null);
    try {
      const nextData = await topicApi.list();
      if (mountedRef.current && topicListRequestRef.current === requestId) {
        dataRef.current = nextData;
        setData(nextData);
        const openTopic = selectedTopicRef.current;
        if (openTopic) {
          const refreshedTopic = nextData.items.find((topic) => topic.topic === openTopic.topic) ?? null;
          selectedTopicRef.current = refreshedTopic;
          setSelectedTopic(refreshedTopic);
        }
      }
    } catch (requestError) {
      if (mountedRef.current && topicListRequestRef.current === requestId) {
        if (hasCatalog) setRefreshError(errorMessage(requestError));
        else setError(errorMessage(requestError));
      }
    } finally {
      if (mountedRef.current && topicListRequestRef.current === requestId) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void load();
    return () => {
      mountedRef.current = false;
      topicListRequestRef.current += 1;
      configRequestRef.current += 1;
      consumersRequestRef.current += 1;
      actionGenerationRef.current += 1;
      createGenerationRef.current += 1;
    };
  }, [load]);

  const topics = data?.items ?? [];
  const metrics = useMemo(() => getTopicMetrics(topics), [topics]);
  const clusterOptions = useMemo(
    () => Array.from(new Set((data?.targets ?? []).map((target) => target.clusterName))).sort(),
    [data?.targets]
  );
  const brokerOptions = useMemo(
    () => Array.from(new Set((data?.targets ?? []).flatMap((target) => target.brokerNames))).sort(),
    [data?.targets]
  );
  const filteredTopics = useMemo(() => filterTopics(topics, filters), [filters, topics]);
  const pageCount = Math.max(1, Math.ceil(filteredTopics.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const visibleTopics = filteredTopics.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  useEffect(() => {
    if (page !== currentPage) setPage(currentPage);
  }, [currentPage, page]);

  const activateAction = (nextAction: TopicAction) => {
    const generation = actionGenerationRef.current + 1;
    actionGenerationRef.current = generation;
    actionRef.current = nextAction;
    setAction(nextAction);
    return generation;
  };

  const closeAction = () => {
    actionGenerationRef.current += 1;
    actionRef.current = null;
    setAction(null);
  };

  const refreshDetailResources = (...resources: Array<keyof typeof detailRevisions>) => {
    setDetailRevisions((current) => {
      const next = { ...current };
      for (const resource of resources) next[resource] += 1;
      return next;
    });
  };

  const openDetails = (topic: TopicInfo, origin?: HTMLElement) => {
    if (origin) detailTriggerRef.current = origin;
    selectedTopicRef.current = topic;
    setSelectedTopic(topic);
  };

  const loadEditConfig = async (topic: TopicInfo, generation: number) => {
    const requestId = ++configRequestRef.current;
    setEditConfig({ topicName: topic.topic, data: null, loading: true, error: null });
    try {
      const nextConfig = await topicApi.config(topic.topic);
      if (isCurrentAction(actionRef.current, generation, 'edit', topic.topic, actionGenerationRef.current)
        && configRequestRef.current === requestId && mountedRef.current) {
        setEditConfig({ topicName: topic.topic, data: nextConfig, loading: false, error: null });
      }
    } catch (requestError) {
      if (isCurrentAction(actionRef.current, generation, 'edit', topic.topic, actionGenerationRef.current)
        && configRequestRef.current === requestId && mountedRef.current) {
        setEditConfig({ topicName: topic.topic, data: null, loading: false, error: errorMessage(requestError) });
      }
    }
  };

  const openEdit = (topic: TopicInfo, providedConfig?: TopicConfigView) => {
    const generation = activateAction({ kind: 'edit', topic });
    if (providedConfig?.topicName === topic.topic) {
      configRequestRef.current += 1;
      setEditConfig({ topicName: topic.topic, data: providedConfig, loading: false, error: null });
    } else {
      void loadEditConfig(topic, generation);
    }
  };

  const retryEditConfig = () => {
    const currentAction = actionRef.current;
    if (currentAction?.kind === 'edit') void loadEditConfig(currentAction.topic, actionGenerationRef.current);
  };

  const loadConsumers = async (topic: TopicInfo, kind: TopicConsumerActionKind, generation: number) => {
    const requestId = ++consumersRequestRef.current;
    setConsumerDiscovery({ topicName: topic.topic, kind, items: [], loading: true, error: null });
    try {
      const nextConsumers = await topicApi.consumers(topic.topic);
      if (isCurrentAction(actionRef.current, generation, kind, topic.topic, actionGenerationRef.current)
        && consumersRequestRef.current === requestId && mountedRef.current) {
        setConsumerDiscovery({ topicName: topic.topic, kind, items: nextConsumers.items, loading: false, error: null });
      }
    } catch (requestError) {
      if (isCurrentAction(actionRef.current, generation, kind, topic.topic, actionGenerationRef.current)
        && consumersRequestRef.current === requestId && mountedRef.current) {
        setConsumerDiscovery({ topicName: topic.topic, kind, items: [], loading: false, error: errorMessage(requestError) });
      }
    }
  };

  const openConsumerAction = (kind: TopicConsumerActionKind, topic: TopicInfo, consumerGroup?: string) => {
    const generation = activateAction({ kind, topic, consumerGroup });
    if (consumerGroup) {
      consumersRequestRef.current += 1;
      setConsumerDiscovery(emptyConsumers(topic.topic, kind));
    } else {
      void loadConsumers(topic, kind, generation);
    }
  };

  const retryConsumers = () => {
    const currentAction = actionRef.current;
    if (currentAction?.kind === 'reset' || currentAction?.kind === 'skip') {
      void loadConsumers(currentAction.topic, currentAction.kind, actionGenerationRef.current);
    }
  };

  const chooseConsumer = (consumerGroup: string) => {
    const currentAction = actionRef.current;
    if (currentAction?.kind === 'reset' || currentAction?.kind === 'skip') {
      activateAction({ ...currentAction, consumerGroup });
    }
  };

  const handleMenuAction = (menuAction: MenuAction, topic: TopicInfo, origin?: HTMLElement) => {
    if (menuAction === 'view') openDetails(topic, origin);
    else if (menuAction === 'edit') openEdit(topic);
    else if (menuAction === 'send') activateAction({ kind: 'send', topic });
    else if (menuAction === 'reset' || menuAction === 'skip') openConsumerAction(menuAction, topic);
    else if (menuAction === 'delete-broker') {
      activateAction({
        kind: 'delete-broker',
        topic,
        brokerName: topic.brokers.length === 1 ? topic.brokers[0] : undefined
      });
    } else {
      activateAction({ kind: 'delete-topic', topic });
    }
  };

  const openCreate = () => {
    createGenerationRef.current += 1;
    createOpenRef.current = true;
    setNotice(null);
    setCreateOpen(true);
  };

  const changeCreateOpen = (open: boolean) => {
    if (!open) createGenerationRef.current += 1;
    createOpenRef.current = open;
    setCreateOpen(open);
  };

  const saveCreate = async (request: TopicMutationRequest) => {
    if ((dataRef.current?.items ?? []).some((topic) => topic.topic === request.topic)) {
      throw new Error(`Topic \`${request.topic}\` already exists. Choose a new name.`);
    }
    const generation = createGenerationRef.current;
    const result = await topicApi.create(request);
    if (mountedRef.current && createOpenRef.current && createGenerationRef.current === generation && result.success) {
      setNotice(result.message || `Topic ${request.topic} created.`);
    }
    void load();
    return result;
  };

  const saveEdit = async (request: TopicMutationRequest) => {
    const currentAction = actionRef.current;
    const generation = actionGenerationRef.current;
    if (currentAction?.kind !== 'edit' || currentAction.topic.topic !== request.topic) {
      throw new Error('The selected topic changed before the update started.');
    }
    const topicName = currentAction.topic.topic;
    const result = await topicApi.update(topicName, request);
    if (isCurrentAction(actionRef.current, generation, 'edit', topicName, actionGenerationRef.current) && result.success) {
      setNotice(result.message || `Topic ${topicName} updated.`);
    }
    if (result.success && selectedTopicRef.current?.topic === topicName) refreshDetailResources('config');
    void load();
    return result;
  };

  const handleDeleteSucceeded = (result: TopicOperationResult) => {
    const currentAction = actionRef.current;
    if (!currentAction || (currentAction.kind !== 'delete-topic' && currentAction.kind !== 'delete-broker')) return;
    const deletedTopic = currentAction.topic.topic;
    if (currentAction.kind === 'delete-topic' && selectedTopicRef.current?.topic === deletedTopic) {
      selectedTopicRef.current = null;
      setSelectedTopic(null);
    }
    setNotice(result.message || `Topic ${deletedTopic} deleted.`);
    closeAction();
  };

  const handleDeleteResult = (result: TopicOperationResult) => {
    const currentAction = actionRef.current;
    if (!currentAction || (currentAction.kind !== 'delete-topic' && currentAction.kind !== 'delete-broker')) return;
    if (selectedTopicRef.current?.topic === result.topic) refreshDetailResources('route', 'stats', 'config');
    void load();
  };

  const refreshOffsetResources = (topicName: string) => {
    if (selectedTopicRef.current?.topic === topicName) refreshDetailResources('stats', 'consumers');
  };

  const columns: AppDataTableColumn<TopicInfo>[] = [
    {
      id: 'topic',
      header: 'Topic',
      width: '250px',
      cell: (topic) => (
        <div className="entity-name-cell">
          <strong>{topic.topic}</strong>
          <Link to={`/topics/${encodeURIComponent(topic.topic)}`}>Full page</Link>
        </div>
      )
    },
    {
      id: 'category',
      header: 'Category',
      width: '120px',
      cell: (topic) => (
        <StatusBadge status={topic.category || 'UNKNOWN'} tone={categoryTone(getTopicCategory(topic))} />
      )
    },
    { id: 'messageType', header: 'Message type', width: '120px', cell: (topic) => topic.messageType || 'UNSPECIFIED' },
    {
      id: 'targets',
      header: 'Targets',
      width: '190px',
      cell: (topic) => <TopicTargetSummary clusters={topic.clusters} brokers={topic.brokers} />
    },
    { id: 'queues', header: 'Queues R / W', width: '120px', cell: (topic) => `${topic.readQueueCount} / ${topic.writeQueueCount}` },
    { id: 'ordered', header: 'Ordered', width: '88px', cell: (topic) => topic.order ? 'Yes' : 'No' },
    { id: 'permission', header: 'Permission', width: '100px', cell: (topic) => <code>{getTopicPermissionLabel(topic.perm)}</code> },
    {
      id: 'actions',
      header: 'Actions',
      width: '78px',
      align: 'right',
      cell: (topic) => <TopicActionMenu topic={topic} onAction={handleMenuAction} />
    }
  ];

  if (loading) return <LoadingState label="Loading topics" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;

  const consumerAction = action?.kind === 'reset' || action?.kind === 'skip' ? action : null;
  const editAction = action?.kind === 'edit' ? action : null;
  const sendAction = action?.kind === 'send' ? action : null;
  const deleteAction = action?.kind === 'delete-topic' || action?.kind === 'delete-broker' ? action : null;
  const discoveryMatches = consumerAction
    && consumerDiscovery.topicName === consumerAction.topic.topic
    && consumerDiscovery.kind === consumerAction.kind;

  return (
    <div className="entity-workspace topic-workspace">
      <PageHeader
        title="Topics"
        description="Manage topic inventory, queue permissions, routes, and API-backed operations from one workspace."
        actions={
          <>
            <Button type="button" onClick={openCreate}><Plus size={15} aria-hidden="true" /> Create topic</Button>
            <RefreshButton refreshing={refreshing} onRefresh={() => void load()} />
          </>
        }
      />

      {notice ? <div className="notice notice-success" role="status">{notice}</div> : null}
      {refreshError ? (
        <div className="notice notice-danger entity-auxiliary-error" role="alert">
          <span>{refreshError}</span>
          <Button type="button" variant="outline" size="sm" onClick={() => void load()}>Retry topic catalog</Button>
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
        <TopicFilterToolbar
          filters={filters}
          clusterOptions={clusterOptions}
          brokerOptions={brokerOptions}
          onFiltersChange={(nextFilters) => { setFilters(nextFilters); setPage(1); }}
        />
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
          emptyDetail="Adjust the search, target, message type, or category filters."
        />
      </section>

      <TopicMutationDialog
        open={createOpen}
        mode="create"
        targets={data?.targets ?? []}
        onOpenChange={changeCreateOpen}
        onSubmit={saveCreate}
      />
      <TopicMutationDialog
        open={Boolean(editAction)}
        mode="edit"
        targets={data?.targets ?? []}
        config={editAction && editConfig.topicName === editAction.topic.topic ? editConfig.data : null}
        loadingConfig={Boolean(editAction && editConfig.topicName === editAction.topic.topic && editConfig.loading)}
        configError={editAction && editConfig.topicName === editAction.topic.topic ? editConfig.error : null}
        onRetryConfig={retryEditConfig}
        onOpenChange={(open) => { if (!open) closeAction(); }}
        onSubmit={saveEdit}
      />
      <EntitySheet
        open={selectedTopic !== null}
        title={selectedTopic?.topic ?? 'Topic details'}
        description={selectedTopic ? `${selectedTopic.category} · ${selectedTopic.brokerName || 'All brokers'}` : undefined}
        restoreFocusRef={detailTriggerRef}
        onOpenChange={(open) => {
          if (!open) {
            selectedTopicRef.current = null;
            setSelectedTopic(null);
          }
        }}
      >
        {selectedTopic ? (
          <>
            {getTopicActionAvailability(selectedTopic).deleteTopic ? (
              <div className="entity-row-actions">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => handleMenuAction('delete-broker', selectedTopic)}
                >
                  <Trash2 size={15} aria-hidden="true" /> Delete from broker
                </Button>
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  onClick={() => handleMenuAction('delete-topic', selectedTopic)}
                >
                  <Trash2 size={15} aria-hidden="true" /> Delete topic
                </Button>
              </div>
            ) : null}
            <TopicDetailContent
              topicName={selectedTopic.topic}
              topic={selectedTopic}
              resourceRevisions={detailRevisions}
              onEdit={getTopicActionAvailability(selectedTopic).edit ? (config) => openEdit(selectedTopic, config) : undefined}
              onReset={getTopicActionAvailability(selectedTopic).reset ? (group) => openConsumerAction('reset', selectedTopic, group) : undefined}
              onSkip={getTopicActionAvailability(selectedTopic).skip ? (group) => openConsumerAction('skip', selectedTopic, group) : undefined}
            />
          </>
        ) : null}
      </EntitySheet>
      <TopicConsumerActionDialog
        open={Boolean(consumerAction && !consumerAction.consumerGroup)}
        kind={consumerAction?.kind ?? 'reset'}
        topicName={consumerAction?.topic.topic ?? ''}
        consumers={discoveryMatches ? consumerDiscovery.items : []}
        loading={Boolean(discoveryMatches && consumerDiscovery.loading)}
        error={discoveryMatches ? consumerDiscovery.error : null}
        onRetry={retryConsumers}
        onSelect={chooseConsumer}
        onOpenChange={(open) => { if (!open) closeAction(); }}
      />
      <TopicSendMessageDialog
        open={Boolean(sendAction)}
        topic={sendAction?.topic.topic ?? ''}
        onOpenChange={(open) => { if (!open) closeAction(); }}
        onSucceeded={() => undefined}
      />
      <TopicResetOffsetDialog
        open={Boolean(consumerAction?.kind === 'reset' && consumerAction.consumerGroup)}
        topic={consumerAction?.topic.topic ?? ''}
        consumerGroup={consumerAction?.consumerGroup ?? ''}
        onOpenChange={(open) => { if (!open) closeAction(); }}
        onSucceeded={(result) => refreshOffsetResources(result.topic)}
      />
      <TopicSkipBacklogDialog
        open={Boolean(consumerAction?.kind === 'skip' && consumerAction.consumerGroup)}
        topic={consumerAction?.topic.topic ?? ''}
        consumerGroup={consumerAction?.consumerGroup ?? ''}
        onOpenChange={(open) => { if (!open) closeAction(); }}
        onSucceeded={(result) => refreshOffsetResources(result.topic)}
      />
      <TopicDeleteDialog
        open={Boolean(deleteAction)}
        topic={deleteAction?.topic ?? null}
        mode={deleteAction?.kind === 'delete-broker' ? 'broker' : 'topic'}
        brokerName={deleteAction?.kind === 'delete-broker' ? deleteAction.brokerName : undefined}
        onOpenChange={(open) => { if (!open) closeAction(); }}
        onResult={handleDeleteResult}
        onSucceeded={handleDeleteSucceeded}
      />
    </div>
  );
}

interface TopicActionMenuProps {
  topic: TopicInfo;
  onAction: (action: MenuAction, topic: TopicInfo, origin?: HTMLElement) => void;
}

function TopicActionMenu({ topic, onAction }: TopicActionMenuProps) {
  const availability = getTopicActionAvailability(topic);
  const triggerRef = useRef<HTMLButtonElement>(null);
  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <Button ref={triggerRef} type="button" variant="ghost" size="icon" aria-label={`Actions for ${topic.topic}`}>
          <MoreHorizontal size={16} aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => onAction('view', topic, triggerRef.current ?? undefined)}><Eye size={15} aria-hidden="true" /> View details</DropdownMenuItem>
        {availability.edit ? <DropdownMenuItem onSelect={() => onAction('edit', topic)}><Pencil size={15} aria-hidden="true" /> Edit configuration</DropdownMenuItem> : null}
        {availability.send ? <DropdownMenuItem onSelect={() => onAction('send', topic)}><Send size={15} aria-hidden="true" /> Send test message</DropdownMenuItem> : null}
        {availability.reset ? <DropdownMenuItem onSelect={() => onAction('reset', topic)}><RotateCcw size={15} aria-hidden="true" /> Reset consumer offset</DropdownMenuItem> : null}
        {availability.skip ? <DropdownMenuItem onSelect={() => onAction('skip', topic)}><SkipForward size={15} aria-hidden="true" /> Skip accumulated messages</DropdownMenuItem> : null}
        {availability.deleteBroker || availability.deleteTopic ? <DropdownMenuSeparator /> : null}
        {availability.deleteBroker ? <DropdownMenuItem className="ui-menu-item-danger" onSelect={() => onAction('delete-broker', topic)}><Trash2 size={15} aria-hidden="true" /> Delete from broker</DropdownMenuItem> : null}
        {availability.deleteTopic ? <DropdownMenuItem className="ui-menu-item-danger" onSelect={() => onAction('delete-topic', topic)}><Trash2 size={15} aria-hidden="true" /> Delete topic</DropdownMenuItem> : null}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function isCurrentAction(
  action: TopicAction | null,
  expectedGeneration: number,
  kind: 'edit' | TopicConsumerActionKind,
  topicName: string,
  currentGeneration: number
) {
  return currentGeneration === expectedGeneration && action?.kind === kind && action.topic.topic === topicName;
}

function categoryTone(category: TopicOperationalCategory) {
  if (category === 'retry') return 'warning' as const;
  if (category === 'dlq') return 'danger' as const;
  if (category === 'system') return 'neutral' as const;
  return 'info' as const;
}
