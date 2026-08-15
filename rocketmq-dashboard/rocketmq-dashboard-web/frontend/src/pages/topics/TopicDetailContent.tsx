import { Database, SlidersHorizontal } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { topicApi } from '../../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../../components/AppDataTable';
import ErrorState from '../../components/ErrorState';
import LoadingState from '../../components/LoadingState';
import MetricCard from '../../components/MetricCard';
import StatusBadge from '../../components/StatusBadge';
import { Button } from '../../components/ui/Button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../../components/ui/Select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/Tabs';
import type {
  TopicConfigView,
  TopicConsumersView,
  TopicInfo,
  TopicRouteInfo,
  TopicStatsInfo
} from '../../types/topic';
import { getTopicPermissionLabel } from './topic-model';

export type TopicDetailTab = 'overview' | 'routes' | 'consumers' | 'configuration';

interface TopicDetailContentProps {
  topicName: string;
  topic?: TopicInfo;
  initialTab?: TopicDetailTab;
  resourceRevisions?: TopicDetailResourceRevisions;
  onEdit?: (config: TopicConfigView) => void;
  onReset?: (consumerGroup: string) => void;
  onSkip?: (consumerGroup: string) => void;
}

export interface TopicDetailResourceRevisions {
  stats?: number;
  route?: number;
  consumers?: number;
  config?: number;
}

interface ResourceState<T> {
  topicName: string;
  data: T | null;
  loading: boolean;
  error: string | null;
}

interface SelectedBrokerState {
  topicName: string;
  brokerName?: string;
}

const emptyResource = <T,>(topicName: string, data: T | null = null): ResourceState<T> => ({
  topicName,
  data,
  loading: false,
  error: null
});

const errorMessage = (error: unknown) => error instanceof Error ? error.message : String(error);
const EMPTY_REVISIONS: TopicDetailResourceRevisions = {};

export default function TopicDetailContent({
  topicName,
  topic,
  initialTab = 'overview',
  resourceRevisions = EMPTY_REVISIONS,
  onEdit,
  onReset,
  onSkip
}: TopicDetailContentProps) {
  const providedTopic = topic?.topic === topicName ? topic : null;
  const [tabState, setTabState] = useState<{ topicName: string; tab: TopicDetailTab }>({
    topicName,
    tab: initialTab
  });
  const [identityState, setIdentityState] = useState<ResourceState<TopicInfo>>(
    emptyResource(topicName, providedTopic)
  );
  const [statsState, setStatsState] = useState<ResourceState<TopicStatsInfo>>(
    emptyResource(topicName)
  );
  const [routeState, setRouteState] = useState<ResourceState<TopicRouteInfo>>(
    emptyResource(topicName)
  );
  const [consumersState, setConsumersState] = useState<ResourceState<TopicConsumersView>>(
    emptyResource(topicName)
  );
  const [configState, setConfigState] = useState<ResourceState<TopicConfigView>>(
    emptyResource(topicName)
  );
  const [selectedBrokerState, setSelectedBrokerState] = useState<SelectedBrokerState>({ topicName });

  const topicNameRef = useRef(topicName);
  const stateTopicRef = useRef(topicName);
  const identityRequestRef = useRef(0);
  const statsRequestRef = useRef(0);
  const routeRequestRef = useRef(0);
  const consumersRequestRef = useRef(0);
  const configRequestRef = useRef(0);
  const identityPendingRef = useRef(new Map<string, number>());
  const statsPendingRef = useRef(new Map<string, number>());
  const routePendingRef = useRef(new Map<string, number>());
  const consumersPendingRef = useRef(new Map<string, number>());
  const configPendingRef = useRef(new Map<string, number>());
  const resourceRevisionsRef = useRef({ topicName, revisions: resourceRevisions });
  topicNameRef.current = topicName;

  const activeTab = tabState.topicName === topicName ? tabState.tab : initialTab;
  const identity = identityState.topicName === topicName ? identityState : emptyResource<TopicInfo>(topicName);
  const stats = statsState.topicName === topicName ? statsState : emptyResource<TopicStatsInfo>(topicName);
  const route = routeState.topicName === topicName ? routeState : emptyResource<TopicRouteInfo>(topicName);
  const consumers = consumersState.topicName === topicName
    ? consumersState
    : emptyResource<TopicConsumersView>(topicName);
  const config = configState.topicName === topicName ? configState : emptyResource<TopicConfigView>(topicName);
  const selectedBroker = selectedBrokerState.topicName === topicName
    ? selectedBrokerState.brokerName
    : undefined;
  const topicInfo = providedTopic ?? identity.data;

  useEffect(() => {
    if (stateTopicRef.current === topicName) return;
    stateTopicRef.current = topicName;
    identityRequestRef.current += 1;
    statsRequestRef.current += 1;
    routeRequestRef.current += 1;
    consumersRequestRef.current += 1;
    configRequestRef.current += 1;
    identityPendingRef.current.clear();
    statsPendingRef.current.clear();
    routePendingRef.current.clear();
    consumersPendingRef.current.clear();
    configPendingRef.current.clear();
    setTabState({ topicName, tab: initialTab });
    setIdentityState(emptyResource(topicName, providedTopic));
    setStatsState(emptyResource(topicName));
    setRouteState(emptyResource(topicName));
    setConsumersState(emptyResource(topicName));
    setConfigState(emptyResource(topicName));
    setSelectedBrokerState({ topicName });
  }, [initialTab, providedTopic, topicName]);

  useEffect(() => {
    if (!providedTopic) return;
    setIdentityState((current) => current.topicName === topicName && current.data === providedTopic
      ? current
      : emptyResource(topicName, providedTopic));
  }, [providedTopic, topicName]);

  const loadIdentity = useCallback(async () => {
    const requestTopic = topicName;
    const requestKey = requestTopic;
    if (identityPendingRef.current.has(requestKey)) return;
    const requestId = ++identityRequestRef.current;
    identityPendingRef.current.set(requestKey, requestId);
    setIdentityState({ topicName: requestTopic, data: null, loading: true, error: null });
    try {
      const nextTopic = await topicApi.get(requestTopic);
      if (requestId !== identityRequestRef.current || topicNameRef.current !== requestTopic) return;
      setIdentityState({ topicName: requestTopic, data: nextTopic, loading: false, error: null });
    } catch (requestError) {
      if (requestId !== identityRequestRef.current || topicNameRef.current !== requestTopic) return;
      setIdentityState({ topicName: requestTopic, data: null, loading: false, error: errorMessage(requestError) });
    } finally {
      if (identityPendingRef.current.get(requestKey) === requestId) {
        identityPendingRef.current.delete(requestKey);
      }
    }
  }, [topicName]);

  const loadStats = useCallback(async (force = false) => {
    const requestTopic = topicName;
    const requestKey = requestTopic;
    if (!force && statsPendingRef.current.has(requestKey)) return;
    const requestId = ++statsRequestRef.current;
    statsPendingRef.current.set(requestKey, requestId);
    setStatsState((current) => ({
      topicName: requestTopic,
      data: current.topicName === requestTopic ? current.data : null,
      loading: true,
      error: null
    }));
    try {
      const nextStats = await topicApi.stats(requestTopic);
      if (requestId !== statsRequestRef.current || topicNameRef.current !== requestTopic) return;
      setStatsState({ topicName: requestTopic, data: nextStats, loading: false, error: null });
    } catch (requestError) {
      if (requestId !== statsRequestRef.current || topicNameRef.current !== requestTopic) return;
      setStatsState((current) => ({
        topicName: requestTopic,
        data: current.topicName === requestTopic ? current.data : null,
        loading: false,
        error: errorMessage(requestError)
      }));
    } finally {
      if (statsPendingRef.current.get(requestKey) === requestId) statsPendingRef.current.delete(requestKey);
    }
  }, [topicName]);

  const loadRoute = useCallback(async (force = false) => {
    const requestTopic = topicName;
    const requestKey = requestTopic;
    if (!force && routePendingRef.current.has(requestKey)) return;
    const requestId = ++routeRequestRef.current;
    routePendingRef.current.set(requestKey, requestId);
    setRouteState((current) => ({
      topicName: requestTopic,
      data: current.topicName === requestTopic ? current.data : null,
      loading: true,
      error: null
    }));
    try {
      const nextRoute = await topicApi.route(requestTopic);
      if (requestId !== routeRequestRef.current || topicNameRef.current !== requestTopic) return;
      setRouteState({ topicName: requestTopic, data: nextRoute, loading: false, error: null });
    } catch (requestError) {
      if (requestId !== routeRequestRef.current || topicNameRef.current !== requestTopic) return;
      setRouteState((current) => ({
        topicName: requestTopic,
        data: current.topicName === requestTopic ? current.data : null,
        loading: false,
        error: errorMessage(requestError)
      }));
    } finally {
      if (routePendingRef.current.get(requestKey) === requestId) routePendingRef.current.delete(requestKey);
    }
  }, [topicName]);

  const loadConsumers = useCallback(async (force = false) => {
    const requestTopic = topicName;
    const requestKey = requestTopic;
    if (!force && consumersPendingRef.current.has(requestKey)) return;
    const requestId = ++consumersRequestRef.current;
    consumersPendingRef.current.set(requestKey, requestId);
    setConsumersState((current) => ({
      topicName: requestTopic,
      data: current.topicName === requestTopic ? current.data : null,
      loading: true,
      error: null
    }));
    try {
      const nextConsumers = await topicApi.consumers(requestTopic);
      if (requestId !== consumersRequestRef.current || topicNameRef.current !== requestTopic) return;
      setConsumersState({ topicName: requestTopic, data: nextConsumers, loading: false, error: null });
    } catch (requestError) {
      if (requestId !== consumersRequestRef.current || topicNameRef.current !== requestTopic) return;
      setConsumersState((current) => ({
        topicName: requestTopic,
        data: current.topicName === requestTopic ? current.data : null,
        loading: false,
        error: errorMessage(requestError)
      }));
    } finally {
      if (consumersPendingRef.current.get(requestKey) === requestId) consumersPendingRef.current.delete(requestKey);
    }
  }, [topicName]);

  const loadConfig = useCallback(async (brokerName?: string, force = false) => {
    const requestTopic = topicName;
    const requestKey = `${requestTopic}:${brokerName ?? ''}`;
    if (!force && configPendingRef.current.get(requestKey) === configRequestRef.current) return;
    const requestId = ++configRequestRef.current;
    configPendingRef.current.set(requestKey, requestId);
    if (brokerName) setSelectedBrokerState({ topicName: requestTopic, brokerName });
    setConfigState((current) => ({
      topicName: requestTopic,
      data: current.topicName === requestTopic ? current.data : null,
      loading: true,
      error: null
    }));
    try {
      const nextConfig = brokerName
        ? await topicApi.config(requestTopic, brokerName)
        : await topicApi.config(requestTopic);
      if (requestId !== configRequestRef.current || topicNameRef.current !== requestTopic) return;
      setConfigState({ topicName: requestTopic, data: nextConfig, loading: false, error: null });
      setSelectedBrokerState({ topicName: requestTopic, brokerName: nextConfig.brokerName });
    } catch (requestError) {
      if (requestId !== configRequestRef.current || topicNameRef.current !== requestTopic) return;
      setConfigState((current) => ({
        topicName: requestTopic,
        data: current.topicName === requestTopic ? current.data : null,
        loading: false,
        error: errorMessage(requestError)
      }));
    } finally {
      if (configPendingRef.current.get(requestKey) === requestId) configPendingRef.current.delete(requestKey);
    }
  }, [topicName]);

  useEffect(() => {
    const previous = resourceRevisionsRef.current;
    resourceRevisionsRef.current = { topicName, revisions: resourceRevisions };
    if (previous.topicName !== topicName) return;

    if (previous.revisions.stats !== resourceRevisions.stats) void loadStats(true);
    if (previous.revisions.route !== resourceRevisions.route) void loadRoute(true);
    if (previous.revisions.consumers !== resourceRevisions.consumers) void loadConsumers(true);
    if (previous.revisions.config !== resourceRevisions.config) void loadConfig(selectedBroker, true);
  }, [loadConfig, loadConsumers, loadRoute, loadStats, resourceRevisions, selectedBroker, topicName]);

  useEffect(() => {
    if (!providedTopic && !identity.data && !identity.loading && !identity.error) void loadIdentity();
  }, [identity.data, identity.error, identity.loading, loadIdentity, providedTopic]);

  useEffect(() => {
    if ((activeTab === 'overview' || activeTab === 'routes')
      && !stats.data && !stats.loading && !stats.error) void loadStats();
    if (activeTab === 'routes' && !route.data && !route.loading && !route.error) void loadRoute();
    if (activeTab === 'consumers'
      && !consumers.data && !consumers.loading && !consumers.error) void loadConsumers();
    if (activeTab === 'configuration'
      && !config.data && !config.loading && !config.error) void loadConfig();
  }, [
    activeTab,
    config.data,
    config.error,
    config.loading,
    consumers.data,
    consumers.error,
    consumers.loading,
    loadConfig,
    loadConsumers,
    loadRoute,
    loadStats,
    route.data,
    route.error,
    route.loading,
    stats.data,
    stats.error,
    stats.loading
  ]);

  const routeColumns: AppDataTableColumn<TopicRouteInfo['queues'][number]>[] = [
    { id: 'broker', header: 'Broker', cell: (queue) => queue.brokerName },
    { id: 'read', header: 'Read queues', align: 'right', cell: (queue) => queue.readQueueNums },
    { id: 'write', header: 'Write queues', align: 'right', cell: (queue) => queue.writeQueueNums },
    { id: 'permission', header: 'Permission', cell: (queue) => <code>{getTopicPermissionLabel(queue.perm)}</code> }
  ];

  const offsetColumns: AppDataTableColumn<TopicStatsInfo['offsets'][number]>[] = [
    { id: 'broker', header: 'Broker', cell: (offset) => offset.brokerName },
    { id: 'queue', header: 'Queue', align: 'right', cell: (offset) => offset.queueId },
    { id: 'minimum', header: 'Minimum offset', align: 'right', cell: (offset) => offset.minOffset },
    { id: 'maximum', header: 'Maximum offset', align: 'right', cell: (offset) => offset.maxOffset },
    {
      id: 'messages',
      header: 'Messages',
      align: 'right',
      cell: (offset) => Math.max(0, offset.maxOffset - offset.minOffset)
    },
    {
      id: 'updated',
      header: 'Last update',
      cell: (offset) => new Date(offset.lastUpdateTimestamp).toLocaleString()
    }
  ];

  const consumersColumns: AppDataTableColumn<TopicConsumersView['items'][number]>[] = [
    { id: 'group', header: 'Consumer group', cell: (consumer) => <code>{consumer.consumerGroup}</code> },
    { id: 'total', header: 'Total backlog', align: 'right', cell: (consumer) => consumer.totalDiff },
    { id: 'inflight', header: 'Inflight', align: 'right', cell: (consumer) => consumer.inflightDiff },
    { id: 'tps', header: 'Consume TPS', align: 'right', cell: (consumer) => consumer.consumeTps },
    ...(!topicInfo?.systemTopic && (onReset || onSkip) ? [{
      id: 'actions',
      header: 'Actions',
      cell: (consumer: TopicConsumersView['items'][number]) => (
        <div className="query-actions">
          {onReset ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              aria-label={`Reset ${consumer.consumerGroup}`}
              onClick={() => onReset(consumer.consumerGroup)}
            >
              Reset
            </Button>
          ) : null}
          {onSkip ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              aria-label={`Skip ${consumer.consumerGroup}`}
              onClick={() => onSkip(consumer.consumerGroup)}
            >
              Skip
            </Button>
          ) : null}
        </div>
      )
    }] satisfies AppDataTableColumn<TopicConsumersView['items'][number]>[] : [])
  ];

  if (!topicInfo && (identity.loading || !identity.error)) {
    return <LoadingState label="Loading topic identity" />;
  }
  if (!topicInfo && identity.error) {
    return <ErrorState message={identity.error} onRetry={() => void loadIdentity()} retryLabel="Retry topic identity" />;
  }

  const configBrokerOptions = config.data
    ? Array.from(new Set([config.data.brokerName, ...config.data.brokerNameList].filter(Boolean)))
    : [];
  const mutationAllowed = !topicInfo?.systemTopic;

  return (
    <div className="entity-detail-content topic-detail-content">
      <Tabs
        value={activeTab}
        onValueChange={(value) => setTabState({ topicName, tab: value as TopicDetailTab })}
      >
        <TabsList aria-label="Topic detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="routes">Routes and status</TabsTrigger>
          <TabsTrigger value="consumers">Consumers</TabsTrigger>
          <TabsTrigger value="configuration">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="entity-identity-grid">
            <IdentityItem label="Topic" value={topicName} mono />
            <IdentityItem label="Broker scope" value={topicInfo?.brokerName || 'All brokers'} />
            <IdentityItem label="Category" value={topicInfo?.category || 'Unknown'} status />
            <IdentityItem label="Message type" value={topicInfo?.messageType || 'Unknown'} />
            <IdentityItem label="Permission" value={getTopicPermissionLabel(topicInfo?.perm ?? 0)} mono />
            <IdentityItem label="Ordered" value={topicInfo?.order ? 'Yes' : 'No'} />
          </div>
          {stats.loading ? <LoadingState label="Loading topic overview" /> : null}
          {!stats.loading && stats.error ? (
            <ErrorState message={stats.error} onRetry={() => void loadStats()} retryLabel="Retry stats" />
          ) : null}
          {!stats.loading && !stats.error && stats.data ? (
            <div className="metric-grid entity-detail-metrics">
              <MetricCard label="Queue entries" value={stats.data.queueCount} icon={<Database size={17} />} />
              <MetricCard label="Messages" value={stats.data.totalMessageCount} icon={<Database size={17} />} />
              <MetricCard label="Minimum offset" value={stats.data.totalMinOffset} icon={<SlidersHorizontal size={17} />} />
              <MetricCard label="Maximum offset" value={stats.data.totalMaxOffset} icon={<SlidersHorizontal size={17} />} />
            </div>
          ) : null}
        </TabsContent>

        <TabsContent value="routes">
          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Route queues</h3><p>Broker queue allocation reported by the route API.</p></div>
              {route.data ? <StatusBadge status={`${route.data.brokers.length} brokers`} tone="info" /> : null}
            </div>
            <AppDataTable
              ariaLabel="Topic routes"
              rows={route.data?.queues ?? []}
              columns={routeColumns}
              getRowId={(queue) => queue.brokerName}
              page={1}
              pageSize={Math.max(route.data?.queues.length ?? 0, 1)}
              total={route.data?.queues.length ?? 0}
              onPageChange={() => undefined}
              loading={route.loading}
              error={route.error}
              onRetry={() => void loadRoute()}
              retryLabel="Retry routes"
              emptyTitle="No route queues"
            />
          </section>

          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Queue offsets</h3><p>Per-queue status and message counts reported by the stats API.</p></div>
              {stats.data ? <StatusBadge status={`${stats.data.offsets.length} queues`} tone="info" /> : null}
            </div>
            <AppDataTable
              ariaLabel="Topic queue offsets"
              rows={stats.data?.offsets ?? []}
              columns={offsetColumns}
              getRowId={(offset) => `${offset.brokerName}:${offset.queueId}`}
              page={1}
              pageSize={Math.max(stats.data?.offsets.length ?? 0, 1)}
              total={stats.data?.offsets.length ?? 0}
              onPageChange={() => undefined}
              loading={stats.loading}
              error={stats.error}
              onRetry={() => void loadStats()}
              retryLabel="Retry status"
              emptyTitle="No queue offsets"
            />
          </section>
        </TabsContent>

        <TabsContent value="consumers">
          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Topic consumers</h3><p>Backlog and throughput for each subscribed consumer group.</p></div>
              {consumers.data ? <StatusBadge status={`${consumers.data.items.length} groups`} tone="info" /> : null}
            </div>
            <AppDataTable
              ariaLabel="Topic consumers"
              rows={consumers.data?.items ?? []}
              columns={consumersColumns}
              getRowId={(consumer) => consumer.consumerGroup}
              page={1}
              pageSize={Math.max(consumers.data?.items.length ?? 0, 1)}
              total={consumers.data?.items.length ?? 0}
              onPageChange={() => undefined}
              loading={consumers.loading}
              error={consumers.error}
              onRetry={() => void loadConsumers()}
              retryLabel="Retry consumers"
              emptyTitle="No consumers"
              emptyDetail="No consumer groups currently subscribe to this topic."
            />
          </section>
        </TabsContent>

        <TabsContent value="configuration">
          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Topic configuration</h3><p>Effective broker-backed values and target metadata.</p></div>
              {config.data ? (
                <div className="query-actions">
                  {configBrokerOptions.length > 0 ? (
                    <div className="native-filter-field">
                      <span>Broker</span>
                      <Select
                        value={selectedBroker ?? config.data.brokerName}
                        onValueChange={(brokerName) => void loadConfig(brokerName)}
                      >
                        <SelectTrigger aria-label="Configuration broker"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          {configBrokerOptions.map((brokerName) => (
                            <SelectItem key={brokerName} value={brokerName}>{brokerName}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  ) : null}
                  {mutationAllowed && onEdit && !config.loading && !config.error ? (
                    <Button type="button" size="sm" onClick={() => onEdit(config.data!)}>Edit topic</Button>
                  ) : null}
                </div>
              ) : null}
            </div>

            {config.loading ? <LoadingState label="Loading topic configuration" /> : null}
            {!config.loading && config.error ? (
              <ErrorState
                message={config.error}
                onRetry={() => void loadConfig(selectedBroker)}
                retryLabel="Retry configuration"
              />
            ) : null}
            {!config.loading && !config.error && config.data ? (
              <>
                {config.data.inconsistentFields.length > 0 ? (
                  <div className="state-block state-block-error" role="alert">
                    <span>
                      Brokers disagree on: <strong>{config.data.inconsistentFields.join(', ')}</strong>
                    </span>
                  </div>
                ) : null}
                <div className="entity-identity-grid">
                  <IdentityItem label="Topic" value={config.data.topicName} mono />
                  <IdentityItem label="Effective broker" value={config.data.brokerName} />
                  <IdentityItem label="Cluster" value={config.data.clusterName || 'Unknown'} />
                  <IdentityItem label="Read queues" value={String(config.data.readQueueNums)} />
                  <IdentityItem label="Write queues" value={String(config.data.writeQueueNums)} />
                  <IdentityItem label="Permission" value={getTopicPermissionLabel(config.data.perm)} mono />
                  <IdentityItem label="Message type" value={config.data.messageType} />
                  <IdentityItem label="Ordered" value={config.data.order ? 'Yes' : 'No'} />
                  <IdentityItem label="Broker targets" value={config.data.brokerNameList.join(', ') || 'None'} />
                  <IdentityItem label="Cluster targets" value={config.data.clusterNameList.join(', ') || 'None'} />
                </div>
                <section className="entity-detail-section">
                  <div className="entity-detail-heading">
                    <div><h3>Attributes</h3><p>Broker-reported topic attributes.</p></div>
                  </div>
                  {Object.keys(config.data.attributes).length > 0 ? (
                    <div className="entity-identity-grid">
                      {Object.entries(config.data.attributes).map(([name, value]) => (
                        <IdentityItem key={name} label={name} value={value} mono />
                      ))}
                    </div>
                  ) : (
                    <div className="state-block"><span>No topic attributes</span></div>
                  )}
                </section>
              </>
            ) : null}
          </section>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function IdentityItem({
  label,
  value,
  mono = false,
  status = false
}: {
  label: string;
  value: string;
  mono?: boolean;
  status?: boolean;
}) {
  return (
    <div className="entity-identity-item">
      <span>{label}</span>
      {status
        ? <StatusBadge status={value} tone="info" />
        : <strong className={mono ? 'mono' : undefined}>{value}</strong>}
    </div>
  );
}
