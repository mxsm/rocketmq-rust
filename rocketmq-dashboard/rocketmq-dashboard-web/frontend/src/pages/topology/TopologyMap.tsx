import { ChevronsDown, ChevronsUp, Layers, RadioTower, Send, Server, Users } from 'lucide-react';
import { useId, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { Link } from 'react-router-dom';
import StatusBadge from '../../components/StatusBadge';
import { StatusDot, type StatusDotTone } from '../../components/ui/StatusDot';
import type { TopologyBrokerGroup, TopologyConsumer, TopologyNameServer, TopologyProducer } from '../../types/topology';
import {
  layoutTopology,
  particlesFor,
  TOPOLOGY_LAYOUT,
  travelSeconds,
  type PlacedBroker,
  type PlacedChip,
  type PlacedClient,
  type PlacedCluster,
  type TopologyLayout,
  type TopologyLink
} from './topology-layout';
import {
  brokerTone,
  clusterIsHealthy,
  consumerTone,
  describeTopology,
  formatCount,
  formatRate,
  formatTps,
  producerTone,
  type TopologyView
} from './topology-model';

/** Groups drawn per side before the list is expanded. */
export const GROUP_PREVIEW = 6;

/** Heartbeats leave the clusters this far apart, so two clusters never pulse together. */
const HEARTBEAT_STAGGER_S = 1.5;

export type GroupSide = 'producers' | 'consumers';

interface TopologyMapProps {
  view: TopologyView;
  /** Completed refreshes; a change replays the per-refresh scan and route lookup. */
  refreshCount: number;
  /** Nodes that turned to a warning in the latest refresh. */
  newAlerts: ReadonlySet<string>;
  reducedMotion: boolean;
  /** Pauses every looping animation, for a hidden page. */
  paused: boolean;
  expanded: Record<GroupSide, boolean>;
  onToggleGroups: (side: GroupSide) => void;
}

function useContainerWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return undefined;
    const measure = () => setWidth(element.clientWidth);
    measure();
    if (typeof ResizeObserver === 'undefined') return undefined;
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}

const px = (value: number) => `${Math.round(value * 100) / 100}px`;

function boxStyle(box: { x: number; y: number; width: number; height: number }, origin = { x: 0, y: 0 }): CSSProperties {
  return { left: px(box.x - origin.x), top: px(box.y - origin.y), width: px(box.width), height: px(box.height) };
}

const plural = (count: number, one: string, many: string) => `${formatCount(count)} ${count === 1 ? one : many}`;

function producerMetric(producer: TopologyProducer) {
  return plural(producer.connectionCount, 'connection', 'connections');
}

function consumerMetric(consumer: TopologyConsumer) {
  const lag = `lag ${formatCount(consumer.diffTotal)}`;
  return consumer.connectionCount === 0 ? `no clients · ${lag}` : `${formatRate(consumer.consumeTps)} TPS · ${lag}`;
}

/** The shortest particle period among a client's links, which its port halo follows. */
function clientPeriods(links: readonly TopologyLink[]) {
  const periods = new Map<string, number>();
  for (const link of links) {
    if ((link.kind !== 'produce' && link.kind !== 'consume') || link.idle || link.tps <= 0) continue;
    const client = link.kind === 'produce' ? link.nodes[0] : link.nodes[1];
    const period = travelSeconds(link.tps);
    periods.set(client, Math.min(periods.get(client) ?? period, period));
  }
  return periods;
}

export default function TopologyMap({ view, refreshCount, newAlerts, reducedMotion, paused, expanded, onToggleGroups }: TopologyMapProps) {
  const [scrollRef, containerWidth] = useContainerWidth();
  const layout = useMemo(() => layoutTopology(view, containerWidth), [view, containerWidth]);
  const particles = useMemo(() => (reducedMotion ? [] : particlesFor(layout.links)), [layout, reducedMotion]);
  const periods = useMemo(() => clientPeriods(layout.links), [layout]);
  const [hovered, setHovered] = useState<string | null>(null);
  const summaryId = useId();

  const highlighted = useMemo(() => {
    if (!hovered) return null;
    return new Set(layout.links.filter((link) => link.nodes.includes(hovered)).map((link) => link.id));
  }, [hovered, layout]);

  const focusHandlers = (id: string) => ({
    onMouseEnter: () => setHovered(id),
    onMouseLeave: () => setHovered((current) => (current === id ? null : current)),
    onFocus: () => setHovered(id),
    onBlur: () => setHovered((current) => (current === id ? null : current))
  });

  return (
    <div className="topology-map-scroll" ref={scrollRef}>
      <div
        className="topology-map"
        role="group"
        aria-label="Cluster map"
        aria-describedby={summaryId}
        data-paused={paused ? 'true' : undefined}
        data-hovering={hovered ? 'true' : undefined}
        style={{ width: px(layout.width), height: px(layout.height) } as CSSProperties}
      >
        <p id={summaryId} className="sr-only">{describeTopology(view)}</p>
        <LinkLayer layout={layout} highlighted={highlighted} />

        <ColumnHeading
          side="producers"
          label="Producers"
          x={layout.producerColumn.x}
          shown={view.producers.length}
          total={view.producersInScope}
          expanded={expanded.producers}
          onToggle={onToggleGroups}
        />
        <ColumnHeading
          side="consumers"
          label="Consumers"
          x={layout.consumerColumn.x}
          shown={view.consumers.length}
          total={view.consumersInScope}
          expanded={expanded.consumers}
          onToggle={onToggleGroups}
        />

        <NameServerTier tier={layout.tier} nameServers={view.nameServers} />
        {layout.clusters.map((cluster, index) => (
          <ClusterPanel key={cluster.id} cluster={cluster} index={index} newAlerts={newAlerts} focusHandlers={focusHandlers} />
        ))}
        {layout.producers.map((placed, index) => (
          <ProducerNode key={placed.id} placed={placed} index={index} alert={newAlerts.has(placed.id)} {...focusHandlers(placed.id)} />
        ))}
        {layout.consumers.map((placed, index) => (
          <ConsumerNode key={placed.id} placed={placed} index={index} alert={newAlerts.has(placed.id)} {...focusHandlers(placed.id)} />
        ))}

        <svg className="topology-ports" width={layout.width} height={layout.height} aria-hidden="true" focusable="false">
          {layout.ports.map((port) => {
            const period = port.client ? periods.get(port.client) : undefined;
            return (
              <g key={port.id} className="topology-port" data-kind={port.kind}>
                {period && !reducedMotion ? (
                  <circle className="topology-port-halo" cx={port.x} cy={port.y} r={3.5} style={{ animationDuration: `${period}s` }} />
                ) : null}
                <circle className="topology-port-dot" cx={port.x} cy={port.y} r={3.5} />
              </g>
            );
          })}
        </svg>

        {!reducedMotion ? (
          <div className="topology-motion" aria-hidden="true">
            {particles.map((particle) => (
              <span
                key={particle.id}
                className="topology-particle"
                data-kind={particle.kind}
                style={{ offsetPath: `path("${particle.d}")`, animationDuration: `${particle.durationS}s`, animationDelay: `${particle.delayS}s` }}
              />
            ))}
            {layout.links.filter((link) => link.kind === 'heartbeat' && !link.unavailable).map((link) => (
              // Heartbeat timing is illustrative: it shows that brokers register, not when they did.
              <span
                key={link.id}
                className="topology-pulse"
                data-kind="heartbeat"
                style={{ offsetPath: `path("${link.d}")`, animationDelay: `${clusterIndexOf(layout, link) * HEARTBEAT_STAGGER_S}s` }}
              />
            ))}
            {refreshCount > 0
              ? layout.links.filter((link) => link.kind === 'route' && !link.id.startsWith('route-trunk')).map((link) => (
                // One illustrative lookup per refresh, not a measured route query.
                <span key={`${link.id}@${refreshCount}`} className="topology-pulse" data-kind="route" style={{ offsetPath: `path("${link.d}")` }} />
              ))
              : null}
          </div>
        ) : null}
        {!reducedMotion && refreshCount > 1 ? (
          <div key={refreshCount} className="topology-scan" aria-hidden="true" style={{ '--scan-distance': px(layout.width) } as CSSProperties} />
        ) : null}
      </div>
    </div>
  );
}

function clusterIndexOf(layout: TopologyLayout, link: TopologyLink) {
  return Math.max(0, layout.clusters.findIndex((cluster) => link.nodes[0] === cluster.id));
}

function LinkLayer({ layout, highlighted }: { layout: TopologyLayout; highlighted: Set<string> | null }) {
  return (
    <svg className="topology-links" width={layout.width} height={layout.height} aria-hidden="true" focusable="false">
      {layout.links.map((link) => {
        const solid = (link.kind === 'produce' || link.kind === 'consume') && !link.idle;
        let state: 'active' | 'dim' | undefined;
        if (highlighted) state = highlighted.has(link.id) ? 'active' : 'dim';
        return (
          <path
            key={link.id}
            className="topology-link"
            d={link.d}
            data-kind={link.kind}
            data-idle={link.idle ? 'true' : undefined}
            data-unavailable={link.unavailable ? 'true' : undefined}
            data-state={state}
            // A unit path length lets the entrance draw a solid line with one dash.
            pathLength={solid ? 1 : undefined}
            data-draw={solid ? 'true' : undefined}
          />
        );
      })}
    </svg>
  );
}

interface ColumnHeadingProps {
  side: GroupSide;
  label: string;
  x: number;
  shown: number;
  total: number;
  expanded: boolean;
  onToggle: (side: GroupSide) => void;
}

function ColumnHeading({ side, label, x, shown, total, expanded, onToggle }: ColumnHeadingProps) {
  const group = label === 'Producers' ? 'producer' : 'consumer';
  const action = expanded ? `Show the top ${GROUP_PREVIEW} ${group} groups` : `Show all ${group} groups`;
  const Icon = expanded ? ChevronsUp : ChevronsDown;
  return (
    <div className="topology-column-heading" style={{ left: px(x), width: px(TOPOLOGY_LAYOUT.clientWidth) }}>
      <span className="topology-overline">{label}</span>
      <span className="topology-heading-count">{`${formatCount(shown)} of ${plural(total, 'group', 'groups')}`}</span>
      {total > GROUP_PREVIEW ? (
        <button type="button" className="topology-expand" aria-label={action} title={action} aria-expanded={expanded} onClick={() => onToggle(side)}>
          <Icon size={14} aria-hidden="true" />
        </button>
      ) : null}
    </div>
  );
}

function nameServerTone(nameServer: TopologyNameServer): StatusDotTone {
  if (nameServer.status === 'unavailable') return 'danger';
  if (nameServer.status === 'unknown') return 'neutral';
  return nameServer.current ? 'primary' : 'success';
}

function NameServerTier({ tier, nameServers }: { tier: TopologyLayout['tier']; nameServers: TopologyNameServer[] }) {
  const checked = nameServers.filter((nameServer) => nameServer.status !== 'unknown');
  const available = checked.filter((nameServer) => nameServer.status === 'available').length;
  let count = `${formatCount(nameServers.length)} configured`;
  let tone = 'neutral';
  if (checked.length > 0) {
    count = `${formatCount(available)} of ${formatCount(nameServers.length)} available`;
    tone = available === nameServers.length ? 'success' : 'warning';
  }
  return (
    <section className="topology-tier" data-surface="panel" aria-label="NameServers" style={boxStyle(tier)}>
      <div className="topology-tier-header">
        <RadioTower size={13} aria-hidden="true" />
        <h3 className="topology-overline">NameServers</h3>
        <span className="topology-tier-count" data-tone={tone}>{count}</span>
      </div>
      {nameServers.length === 0 ? <p className="topology-tier-empty">No NameServer is configured.</p> : null}
      {tier.chips.map((chip) => <NameServerChip key={chip.id} chip={chip} tier={tier} />)}
    </section>
  );
}

function NameServerChip({ chip, tier }: { chip: PlacedChip; tier: TopologyLayout['tier'] }) {
  const { nameServer } = chip;
  const states = [nameServer.current ? 'current' : null, nameServer.status === 'unknown' ? 'status unknown' : nameServer.status].filter(Boolean);
  return (
    <div
      className="topology-chip"
      data-status={nameServer.status}
      data-current={nameServer.current ? 'true' : undefined}
      style={boxStyle(chip, tier)}
      title={`${nameServer.address}: ${states.join(', ')}`}
    >
      <StatusDot tone={nameServerTone(nameServer)} />
      <span className="topology-chip-address">{nameServer.address}</span>
      <span className="sr-only">{`, ${states.join(', ')}`}</span>
    </div>
  );
}

interface FocusHandlers {
  onMouseEnter: () => void;
  onMouseLeave: () => void;
  onFocus: () => void;
  onBlur: () => void;
}

interface ClusterPanelProps {
  cluster: PlacedCluster;
  index: number;
  newAlerts: ReadonlySet<string>;
  focusHandlers: (id: string) => FocusHandlers;
}

function ClusterPanel({ cluster, index, newAlerts, focusHandlers }: ClusterPanelProps) {
  const healthy = clusterIsHealthy(cluster.cluster);
  const { clusterName, produceTps, consumeTps } = cluster.cluster;
  const traffic = `in ${formatTps(produceTps)} · out ${formatTps(consumeTps)}`;
  return (
    <section
      className="topology-cluster"
      data-surface="panel"
      data-health={healthy ? 'healthy' : 'degraded'}
      aria-label={`Cluster ${clusterName}`}
      style={{ ...boxStyle(cluster), '--enter-index': index } as CSSProperties}
    >
      <header className="topology-cluster-header">
        <Layers size={16} aria-hidden="true" />
        <h3 title={clusterName}>{clusterName}</h3>
        <span className="topology-cluster-tps" title={traffic}>
          <span key={traffic} className="topology-value">{traffic}</span>
        </span>
        <StatusBadge status={healthy ? 'Healthy' : 'Degraded'} tone={healthy ? 'success' : 'warning'} />
      </header>
      {cluster.brokers.map((placed) => (
        <BrokerRow key={placed.id} placed={placed} cluster={cluster} alert={newAlerts.has(placed.id)} {...focusHandlers(placed.id)} />
      ))}
    </section>
  );
}

function brokerSubline(group: TopologyBrokerGroup) {
  const master = group.masterAddress ?? 'no master';
  if (group.slaveAddresses.length === 0) return `${master} · no slave`;
  return group.version ? `${master} · ${group.version}` : master;
}

function brokerLabel(group: TopologyBrokerGroup) {
  const master = group.masterAddress ? `master ${group.masterAddress}` : 'no master registered';
  const slaves = group.slaveAddresses.length === 0 ? 'no slave registered' : plural(group.slaveAddresses.length, 'slave', 'slaves');
  return `Broker ${group.brokerName}, ${master}, ${slaves}, in ${formatTps(group.produceTps)} TPS, out ${formatTps(group.consumeTps)} TPS`;
}

interface BrokerRowProps extends FocusHandlers {
  placed: PlacedBroker;
  cluster: PlacedCluster;
  alert: boolean;
}

function BrokerRow({ placed, cluster, alert, ...handlers }: BrokerRowProps) {
  const { group } = placed;
  const tone = brokerTone(group);
  const slaves = group.slaveAddresses.length;
  return (
    <Link
      to={`/brokers/${encodeURIComponent(group.brokerName)}`}
      className="topology-broker"
      data-tone={tone}
      data-alert={alert ? 'new' : undefined}
      aria-label={brokerLabel(group)}
      style={boxStyle(placed, cluster)}
      {...handlers}
    >
      <span className="topology-node-icon" aria-hidden="true"><Server size={16} /></span>
      <span className="topology-node-text">
        <strong>{group.brokerName}</strong>
        <small>{brokerSubline(group)}</small>
      </span>
      <span className="topology-replica" aria-hidden="true" data-synced={group.masterAddress && slaves > 0 ? 'true' : undefined}>
        <span className="topology-replica-mark" data-present={group.masterAddress ? 'true' : undefined}>M</span>
        <span className="topology-replica-line"><span className="topology-sync-dot" /></span>
        <span className="topology-replica-mark" data-present={slaves > 0 ? 'true' : undefined}>
          S{slaves > 1 ? <sub>{slaves}</sub> : null}
        </span>
      </span>
      <span className="topology-broker-tps">
        <span key={`in-${group.produceTps}`} className="topology-value"><small>in</small> {formatTps(group.produceTps)}</span>
        <span key={`out-${group.consumeTps}`} className="topology-value"><small>out</small> {formatTps(group.consumeTps)}</span>
      </span>
    </Link>
  );
}

interface ClientNodeProps<Client> extends FocusHandlers {
  placed: PlacedClient<Client>;
  index: number;
  alert: boolean;
}

function ProducerNode({ placed, index, alert, ...handlers }: ClientNodeProps<TopologyProducer>) {
  const producer = placed.client;
  const metric = producerMetric(producer);
  return (
    <Link
      to="/producers"
      className="topology-client"
      data-side="producer"
      data-tone={producerTone(producer)}
      data-alert={alert ? 'new' : undefined}
      aria-label={`Producer group ${producer.producerGroup}, ${metric}`}
      style={{ ...boxStyle(placed), '--enter-index': index } as CSSProperties}
      {...handlers}
    >
      <span className="topology-node-icon" aria-hidden="true"><Send size={14} /></span>
      <span className="topology-node-text">
        <strong title={producer.producerGroup}>{producer.producerGroup}</strong>
        <small key={metric} className="topology-value">{metric}</small>
      </span>
    </Link>
  );
}

function ConsumerNode({ placed, index, alert, ...handlers }: ClientNodeProps<TopologyConsumer>) {
  const consumer = placed.client;
  const metric = consumerMetric(consumer);
  return (
    <Link
      to={`/consumers/${encodeURIComponent(consumer.group)}`}
      className="topology-client"
      data-side="consumer"
      data-tone={consumerTone(consumer)}
      data-alert={alert ? 'new' : undefined}
      aria-label={`Consumer group ${consumer.displayName}, ${metric}`}
      style={{ ...boxStyle(placed), '--enter-index': index } as CSSProperties}
      {...handlers}
    >
      <span className="topology-node-icon" aria-hidden="true"><Users size={14} /></span>
      <span className="topology-node-text">
        <strong title={consumer.displayName}>{consumer.displayName}</strong>
        <small key={metric} className="topology-value">{metric}</small>
      </span>
    </Link>
  );
}
