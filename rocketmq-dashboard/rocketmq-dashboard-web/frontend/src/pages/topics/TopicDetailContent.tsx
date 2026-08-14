import { Database, SlidersHorizontal } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { topicApi } from '../../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../../components/AppDataTable';
import ErrorState from '../../components/ErrorState';
import LoadingState from '../../components/LoadingState';
import MetricCard from '../../components/MetricCard';
import StatusBadge from '../../components/StatusBadge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/Tabs';
import type { TopicInfo, TopicRouteInfo, TopicStatsInfo } from '../../types/topic';
import { getTopicCategory, getTopicPermissionLabel } from './topic-model';

export type TopicDetailTab = 'overview' | 'routes' | 'configuration';

interface TopicDetailContentProps {
  topicName: string;
  topic?: TopicInfo;
  initialTab?: TopicDetailTab;
}

export default function TopicDetailContent({
  topicName,
  topic,
  initialTab = 'overview'
}: TopicDetailContentProps) {
  const [activeTab, setActiveTab] = useState<TopicDetailTab>(initialTab);
  const [topicInfo, setTopicInfo] = useState<TopicInfo | null>(topic ?? null);
  const [stats, setStats] = useState<TopicStatsInfo | null>(null);
  const [route, setRoute] = useState<TopicRouteInfo | null>(null);
  const [identityLoading, setIdentityLoading] = useState(!topic);
  const [statsLoading, setStatsLoading] = useState(false);
  const [routeLoading, setRouteLoading] = useState(false);
  const [identityError, setIdentityError] = useState<string | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);
  const [routeError, setRouteError] = useState<string | null>(null);
  const topicNameRef = useRef(topicName);
  const identityRequestRef = useRef(0);
  const statsRequestRef = useRef(0);
  const routeRequestRef = useRef(0);
  topicNameRef.current = topicName;

  useEffect(() => {
    identityRequestRef.current += 1;
    statsRequestRef.current += 1;
    routeRequestRef.current += 1;
    setActiveTab(initialTab);
    setTopicInfo(topic ?? null);
    setStats(null);
    setRoute(null);
    setIdentityLoading(!topic);
    setStatsLoading(false);
    setRouteLoading(false);
    setIdentityError(null);
    setStatsError(null);
    setRouteError(null);
  }, [initialTab, topic, topicName]);

  const loadIdentity = useCallback(async () => {
    const requestTopic = topicName;
    const requestId = ++identityRequestRef.current;
    setIdentityLoading(true);
    setIdentityError(null);
    try {
      const nextTopic = await topicApi.get(requestTopic);
      if (requestId !== identityRequestRef.current || topicNameRef.current !== requestTopic) return;
      setTopicInfo(nextTopic);
    } catch (requestError) {
      if (requestId !== identityRequestRef.current || topicNameRef.current !== requestTopic) return;
      setIdentityError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === identityRequestRef.current && topicNameRef.current === requestTopic) setIdentityLoading(false);
    }
  }, [topicName]);

  const loadStats = useCallback(async () => {
    const requestTopic = topicName;
    const requestId = ++statsRequestRef.current;
    setStatsLoading(true);
    setStatsError(null);
    try {
      const nextStats = await topicApi.stats(requestTopic);
      if (requestId !== statsRequestRef.current || topicNameRef.current !== requestTopic) return;
      setStats(nextStats);
    } catch (requestError) {
      if (requestId !== statsRequestRef.current || topicNameRef.current !== requestTopic) return;
      setStatsError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === statsRequestRef.current && topicNameRef.current === requestTopic) setStatsLoading(false);
    }
  }, [topicName]);

  const loadRoute = useCallback(async () => {
    const requestTopic = topicName;
    const requestId = ++routeRequestRef.current;
    setRouteLoading(true);
    setRouteError(null);
    try {
      const nextRoute = await topicApi.route(requestTopic);
      if (requestId !== routeRequestRef.current || topicNameRef.current !== requestTopic) return;
      setRoute(nextRoute);
    } catch (requestError) {
      if (requestId !== routeRequestRef.current || topicNameRef.current !== requestTopic) return;
      setRouteError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === routeRequestRef.current && topicNameRef.current === requestTopic) setRouteLoading(false);
    }
  }, [topicName]);

  useEffect(() => {
    if (!topicInfo && !identityError) void loadIdentity();
  }, [identityError, loadIdentity, topicInfo]);

  useEffect(() => {
    if (activeTab === 'overview' && !stats && !statsLoading && !statsError) void loadStats();
    if (activeTab === 'routes' && !route && !routeLoading && !routeError) void loadRoute();
  }, [activeTab, loadRoute, loadStats, route, routeError, routeLoading, stats, statsError, statsLoading]);

  const routeColumns: AppDataTableColumn<TopicRouteInfo['queues'][number]>[] = [
    { id: 'broker', header: 'Broker', cell: (queue) => queue.brokerName },
    { id: 'read', header: 'Read queues', align: 'right', cell: (queue) => queue.readQueueNums },
    { id: 'write', header: 'Write queues', align: 'right', cell: (queue) => queue.writeQueueNums },
    { id: 'permission', header: 'Permission', cell: (queue) => <code>{getTopicPermissionLabel(queue.perm)}</code> }
  ];

  if (identityLoading) return <LoadingState label="Loading topic identity" />;
  if (identityError) return <ErrorState message={identityError} onRetry={() => void loadIdentity()} />;

  return (
    <div className="entity-detail-content topic-detail-content">
      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as TopicDetailTab)}>
        <TabsList aria-label="Topic detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="routes">Routes</TabsTrigger>
          <TabsTrigger value="configuration">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="entity-identity-grid">
            <IdentityItem label="Topic" value={topicName} mono />
            <IdentityItem label="Broker scope" value={topicInfo?.brokerName || 'All brokers'} />
            <IdentityItem label="Category" value={topicInfo?.category || 'Unknown'} status />
            <IdentityItem label="Permission" value={getTopicPermissionLabel(topicInfo?.perm ?? 0)} mono />
          </div>
          {statsLoading ? <LoadingState label="Loading topic overview" /> : null}
          {!statsLoading && statsError ? <ErrorState message={statsError} onRetry={() => void loadStats()} /> : null}
          {!statsLoading && !statsError && stats ? (
            <div className="metric-grid entity-detail-metrics">
              <MetricCard label="Queue entries" value={stats.queueCount} icon={<Database size={17} />} />
              <MetricCard label="Minimum offset" value={stats.totalMinOffset} icon={<SlidersHorizontal size={17} />} />
              <MetricCard label="Maximum offset" value={stats.totalMaxOffset} icon={<SlidersHorizontal size={17} />} />
            </div>
          ) : null}
        </TabsContent>

        <TabsContent value="routes">
          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Route queues</h3><p>Broker queue allocation reported by the route API.</p></div>
              {route ? <StatusBadge status={`${route.brokers.length} brokers`} tone="info" /> : null}
            </div>
            <AppDataTable
              ariaLabel="Topic routes"
              rows={route?.queues ?? []}
              columns={routeColumns}
              getRowId={(queue) => `${queue.brokerName}-${queue.readQueueNums}-${queue.writeQueueNums}`}
              page={1}
              pageSize={Math.max(route?.queues.length ?? 0, 1)}
              total={route?.queues.length ?? 0}
              onPageChange={() => undefined}
              loading={routeLoading}
              error={routeError}
              onRetry={() => void loadRoute()}
              emptyTitle="No route queues"
            />
          </section>
        </TabsContent>

        <TabsContent value="configuration">
          <section className="entity-detail-section">
            <div className="entity-detail-heading">
              <div><h3>Topic configuration</h3><p>Read-only queue and permission values returned by the topic API.</p></div>
            </div>
            <div className="entity-identity-grid">
              <IdentityItem label="Read queues" value={String(topicInfo?.readQueueCount ?? 0)} />
              <IdentityItem label="Write queues" value={String(topicInfo?.writeQueueCount ?? 0)} />
              <IdentityItem label="Permission" value={getTopicPermissionLabel(topicInfo?.perm ?? 0)} mono />
              <IdentityItem label="Operational class" value={topicInfo ? getTopicCategory(topicInfo) : 'unknown'} />
            </div>
          </section>
        </TabsContent>
      </Tabs>

    </div>
  );
}

function IdentityItem({ label, value, mono = false, status = false }: { label: string; value: string; mono?: boolean; status?: boolean }) {
  return (
    <div className="entity-identity-item">
      <span>{label}</span>
      {status ? <StatusBadge status={value} tone="info" /> : <strong className={mono ? 'mono' : undefined}>{value}</strong>}
    </div>
  );
}
