import { Layers, RadioTower, RefreshCw, Send, Server, Users, Inbox } from 'lucide-react';
import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import ErrorState from '../components/ErrorState';
import HealthSignal from '../components/HealthSignal';
import PageHeader from '../components/PageHeader';
import RefreshButton, { useSpinUntilTurnEnds } from '../components/RefreshButton';
import { Button, buttonVariants } from '../components/ui/Button';
import { Card, CardDescription, CardTitle } from '../components/ui/Card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/Select';
import { Skeleton } from '../components/ui/Skeleton';
import { StatusDot } from '../components/ui/StatusDot';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/Tabs';
import { usePrefersReducedMotion } from '../lib/usePrefersReducedMotion';
import type { TopologySnapshot, TopologySourceId, TopologySourceStatus } from '../types/topology';
import TopologyMap, { GROUP_PREVIEW, type GroupSide } from './topology/TopologyMap';
import { nodeId } from './topology/topology-layout';
import {
  ALL_CLUSTERS,
  buildSignals,
  formatCount,
  MAX_GROUPS_PER_SIDE,
  newWarnings,
  selectTopologyView,
  TOPOLOGY_SOURCE_LABELS,
  type TopologyView
} from './topology/topology-model';
import { ChangesPanel, SourcesPanel, TrafficPanel } from './topology/TopologyPanels';
import { supportsWebGL } from './topology/sphere/webgl';
import { useTopologySnapshot } from './topology/useTopologySnapshot';
import './topology/topology.css';

// three.js only loads with the sphere view.
const TopologySphere = lazy(() => import('./topology/sphere/TopologySphere'));

type MapStyle = 'sphere' | 'map';

const MAP_STYLE_KEY = 'rocketmq-dashboard.topology.map-style';

/** The style chosen last time, or the sphere when the browser can draw it. */
function readMapStyle(webgl: boolean): MapStyle {
  if (!webgl) return 'map';
  try {
    return window.localStorage.getItem(MAP_STYLE_KEY) === 'map' ? 'map' : 'sphere';
  } catch {
    return 'sphere';
  }
}

function storeMapStyle(style: MapStyle) {
  try {
    window.localStorage.setItem(MAP_STYLE_KEY, style);
  } catch {
    // Storage can be blocked; the choice then lasts until the page is left.
  }
}

const INTERVALS = [
  { value: '5', label: 'Every 5 s', ms: 5_000 },
  { value: '10', label: 'Every 10 s', ms: 10_000 },
  { value: '30', label: 'Every 30 s', ms: 30_000 },
  { value: 'manual', label: 'Manual', ms: null }
] as const;

type IntervalValue = (typeof INTERVALS)[number]['value'];

const DEFAULT_INTERVAL: IntervalValue = '10';

const formatTime = (timestamp: number) =>
  new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

function usePageHidden() {
  const [hidden, setHidden] = useState(() => document.visibilityState === 'hidden');
  useEffect(() => {
    const update = () => setHidden(document.visibilityState === 'hidden');
    document.addEventListener('visibilitychange', update);
    return () => document.removeEventListener('visibilitychange', update);
  }, []);
  return hidden;
}

function joinLabels(labels: string[]) {
  if (labels.length <= 1) return labels.join('');
  return `${labels.slice(0, -1).join(', ')} and ${labels[labels.length - 1]}`;
}

function staleMessage(failed: TopologySourceStatus[], loadedAt: Partial<Record<TopologySourceId, number>>) {
  const list = joinLabels(failed.map((source) => TOPOLOGY_SOURCE_LABELS[source.source]));
  const times = failed.map((source) => loadedAt[source.source]).filter((time): time is number => time !== undefined);
  const one = failed.length === 1;
  if (times.length === failed.length) {
    return `${list} could not be refreshed. The map shows ${one ? 'its' : 'their'} data from ${formatTime(Math.min(...times))}.`;
  }
  if (times.length === 0) {
    return `${list} could not be loaded, so ${one ? 'that layer is' : 'those layers are'} missing from the map.`;
  }
  return `${list} could not be refreshed. Layers that never loaded are missing from the map; the others show their last data.`;
}

function SnapshotStatus({ querying, generatedAt, stale }: { querying: boolean; generatedAt: number | null; stale: boolean }) {
  const { spinning, finishTurn } = useSpinUntilTurnEnds(querying);
  let tone: 'primary' | 'warning' | 'success' = 'success';
  if (querying) tone = 'primary';
  else if (stale) tone = 'warning';
  return (
    <div className="topology-snapshot-status" data-querying={querying ? 'true' : undefined}>
      <StatusDot tone={tone} />
      <span>{querying || generatedAt === null ? 'Querying snapshot…' : `Updated ${formatTime(generatedAt)}`}</span>
      <RefreshCw
        className={spinning ? 'refresh-icon is-spinning' : 'refresh-icon'}
        size={14}
        aria-hidden="true"
        onAnimationIteration={finishTurn}
        onAnimationEnd={finishTurn}
      />
    </div>
  );
}

function mapSummary(view: TopologyView) {
  const groups = view.clusters.reduce((total, cluster) => total + cluster.brokers.length, 0);
  return [
    `${formatCount(view.clusters.length)} ${view.clusters.length === 1 ? 'cluster' : 'clusters'}`,
    `${formatCount(groups)} broker ${groups === 1 ? 'group' : 'groups'}`,
    `${formatCount(view.producers.length)} of ${formatCount(view.producersInScope)} producer groups and `
      + `${formatCount(view.consumers.length)} of ${formatCount(view.consumersInScope)} consumer groups, ranked by connections`
  ].join(' · ');
}

function TopologySkeleton() {
  return (
    <div className="topology-skeleton" role="status" aria-label="Loading topology">
      <Skeleton className="topology-skeleton-rail" />
      <div className="topology-skeleton-map" data-surface="panel">
        <div className="topology-skeleton-column">
          {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} />)}
        </div>
        <div className="topology-skeleton-centre">
          <Skeleton />
          <Skeleton className="topology-skeleton-tier" />
          <Skeleton />
        </div>
        <div className="topology-skeleton-column">
          {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} />)}
        </div>
      </div>
    </div>
  );
}

function SignalRail({ snapshot }: { snapshot: TopologySnapshot }) {
  const signals = buildSignals(snapshot);
  return (
    <Card className="dashboard-health-rail topology-signal-rail" aria-label="Topology summary">
      <HealthSignal icon={<Layers size={16} />} label="Clusters" {...signals.clusters} />
      <HealthSignal icon={<RadioTower size={16} />} label="NameServers" {...signals.nameServers} />
      <HealthSignal icon={<Server size={16} />} label="Brokers" {...signals.brokers} />
      <HealthSignal icon={<Send size={16} />} label="Producers" {...signals.producers} />
      <HealthSignal icon={<Users size={16} />} label="Consumers" {...signals.consumers} />
      <HealthSignal icon={<Inbox size={16} />} label="Backlog" {...signals.backlog} />
    </Card>
  );
}

const LEGEND = {
  map: [
    { kind: 'produce', label: 'Produce' },
    { kind: 'consume', label: 'Consume' },
    { kind: 'heartbeat', label: 'Heartbeat' },
    { kind: 'route', label: 'Route lookup' }
  ],
  sphere: [
    { kind: 'produce', label: 'Produce' },
    { kind: 'consume', label: 'Consume' },
    { kind: 'replication', label: 'Replication' },
    { kind: 'heartbeat', label: 'Heartbeat' },
    { kind: 'route', label: 'Route lookup' }
  ]
} as const;

function SphereFallback() {
  return <Skeleton className="topology-sphere-loading" role="status" aria-label="Loading the sphere view" />;
}

export default function TopologyPage() {
  const [intervalValue, setIntervalValue] = useState<IntervalValue>(DEFAULT_INTERVAL);
  const intervalMs = INTERVALS.find((option) => option.value === intervalValue)?.ms ?? null;
  const topology = useTopologySnapshot({ intervalMs });
  const { snapshot, previous, refresh, querying } = topology;
  const [scope, setScope] = useState<string>(ALL_CLUSTERS);
  const [expanded, setExpanded] = useState<Record<GroupSide, boolean>>({ producers: false, consumers: false });
  const reducedMotion = usePrefersReducedMotion();
  const hidden = usePageHidden();
  const [webgl, setWebgl] = useState(supportsWebGL);
  const [mapStyle, setMapStyle] = useState<MapStyle>(() => readMapStyle(webgl));
  const chooseMapStyle = (style: MapStyle) => {
    setMapStyle(style);
    storeMapStyle(style);
  };
  const sphereUnavailable = () => {
    setWebgl(false);
    setMapStyle('map');
  };
  const toggleGroups = (side: GroupSide) => setExpanded((current) => ({ ...current, [side]: !current[side] }));

  const clusterNames = useMemo(() => snapshot?.clusters.map((cluster) => cluster.clusterName) ?? [], [snapshot]);
  // A cluster that is no longer reported drops the scope back to every cluster.
  const activeScope = scope === ALL_CLUSTERS || clusterNames.includes(scope) ? scope : ALL_CLUSTERS;
  const view = useMemo(() => {
    if (!snapshot) return null;
    return selectTopologyView(snapshot, {
      scope: activeScope,
      producerLimit: expanded.producers ? MAX_GROUPS_PER_SIDE : GROUP_PREVIEW,
      consumerLimit: expanded.consumers ? MAX_GROUPS_PER_SIDE : GROUP_PREVIEW
    });
  }, [snapshot, activeScope, expanded]);
  const newAlerts = useMemo(() => {
    const { brokers, consumers } = newWarnings(previous, snapshot);
    return new Set([...[...brokers].map(nodeId.broker), ...[...consumers].map(nodeId.consumer)]);
  }, [previous, snapshot]);

  const failed = snapshot?.sources.filter((source) => source.status === 'failed') ?? [];
  const brokersLoaded = topology.loadedAt.brokers !== undefined;

  const actions = (
    <>
      <SnapshotStatus querying={querying} generatedAt={snapshot?.generatedAt ?? null} stale={failed.length > 0} />
      <Select value={activeScope} onValueChange={setScope}>
        <SelectTrigger className="topology-select" aria-label="Clusters shown">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_CLUSTERS}>All clusters</SelectItem>
          {clusterNames.map((name) => <SelectItem key={name} value={name}>{name}</SelectItem>)}
        </SelectContent>
      </Select>
      <Select value={intervalValue} onValueChange={(value) => setIntervalValue(value as IntervalValue)}>
        <SelectTrigger className="topology-select" aria-label="Refresh interval">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {INTERVALS.map((option) => <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>)}
        </SelectContent>
      </Select>
      <RefreshButton refreshing={querying} onRefresh={refresh} />
    </>
  );

  let body;
  if (!snapshot && topology.error) {
    body = <ErrorState message={topology.error} onRetry={refresh} />;
  } else if (!snapshot || !view) {
    body = <TopologySkeleton />;
  } else {
    let emptyState = null;
    if (snapshot.clusters.length === 0 && !brokersLoaded) {
      emptyState = <ErrorState message="The broker list could not be loaded, so the map cannot be drawn." onRetry={refresh} />;
    } else if (snapshot.clusters.length === 0) {
      emptyState = (
        <div className="state-block topology-empty">
          <Server size={24} aria-hidden="true" />
          <strong>No brokers are registered</strong>
          <span>The NameServers report no broker. Check the NameServer addresses on the OPS page.</span>
          <Link className={buttonVariants({ variant: 'secondary' })} to="/config">Open OPS</Link>
        </div>
      );
    }
    const activeStyle: MapStyle = webgl ? mapStyle : 'map';

    body = (
      <>
        <SignalRail snapshot={snapshot} />
        <Card className="topology-map-panel">
          <Tabs className="topology-map-tabs" value={activeStyle} onValueChange={(value) => chooseMapStyle(value as MapStyle)}>
            <div className="topology-map-header">
              <div>
                <CardTitle>Cluster map</CardTitle>
                <CardDescription>{mapSummary(view)}</CardDescription>
              </div>
              <div className="topology-map-tools">
                <TabsList aria-label="Map style">
                  <TabsTrigger value="sphere" disabled={!webgl} title={webgl ? undefined : 'The sphere needs WebGL, which this browser does not provide'}>
                    Sphere
                  </TabsTrigger>
                  <TabsTrigger value="map">Map</TabsTrigger>
                </TabsList>
                <ul className="topology-legend" aria-label="Legend">
                  {LEGEND[activeStyle].map((item) => <li key={item.kind} data-kind={item.kind}>{item.label}</li>)}
                </ul>
              </div>
            </div>
            <TabsContent value="sphere" className="topology-map-view">
              {emptyState ?? (
                <Suspense fallback={<SphereFallback />}>
                  <TopologySphere
                    view={view}
                    refreshCount={topology.refreshCount}
                    reducedMotion={reducedMotion}
                    hidden={hidden}
                    expanded={expanded}
                    onToggleGroups={toggleGroups}
                    onUnavailable={sphereUnavailable}
                  />
                </Suspense>
              )}
            </TabsContent>
            <TabsContent value="map" className="topology-map-view">
              {emptyState ?? (
                <TopologyMap
                  view={view}
                  refreshCount={topology.refreshCount}
                  newAlerts={newAlerts}
                  reducedMotion={reducedMotion}
                  paused={hidden}
                  expanded={expanded}
                  onToggleGroups={toggleGroups}
                />
              )}
            </TabsContent>
          </Tabs>
        </Card>
        <div className="topology-panels">
          <TrafficPanel clusters={view.clusters} />
          <ChangesPanel changes={topology.changes} latestChangeCount={topology.latestChangeCount} />
          <SourcesPanel
            sources={snapshot.sources}
            querying={querying}
            intervalMs={intervalMs}
            nextRefreshAt={topology.nextRefreshAt}
            refreshCount={topology.refreshCount}
          />
        </div>
      </>
    );
  }

  return (
    <div className="topology-page">
      <PageHeader
        title="Live topology"
        description="Producers, NameServers, brokers and consumers drawn from the latest backend snapshot. Particle speed follows reported TPS."
        actions={actions}
      />
      {snapshot && failed.length > 0 ? (
        <div className="notice notice-warning topology-stale" role="status">
          <span>{staleMessage(failed, topology.loadedAt)}</span>
          <Button type="button" size="sm" variant="outline" onClick={refresh} disabled={querying}>Retry</Button>
        </div>
      ) : null}
      {body}
    </div>
  );
}
