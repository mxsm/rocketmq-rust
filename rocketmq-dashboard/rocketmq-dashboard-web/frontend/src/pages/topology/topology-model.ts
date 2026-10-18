import type { BrokerInfo } from '../../types/broker';
import type { DashboardConfigView, NameserverAvailabilityView } from '../../types/config';
import type { ConsumerGroupListItem } from '../../types/consumer';
import type { ProducerInfo } from '../../types/producer';
import type { TopicInfo } from '../../types/topic';
import type {
  TopologyBrokerGroup,
  TopologyCluster,
  TopologyConsumer,
  TopologyController,
  TopologyLayerId,
  TopologyNameServer,
  TopologyProducer,
  TopologySnapshot,
  TopologySourceId,
  TopologySourceStatus
} from '../../types/topology';
import { isSystemConsumerGroup } from '../consumers/consumer-model';
import { formatDashboardMetric } from '../dashboard/dashboard-model';

/** A consumer group whose backlog is above this many messages is drawn as a warning. */
export const BACKLOG_ALERT_THRESHOLD = 1_000;

/** Growth of an already high backlog that is worth listing as a change. */
const BACKLOG_CHANGE_RATIO = 0.05;

/** A source answering slower than this is labelled slow in the source list. */
export const SLOW_SOURCE_MS = 300;

/** Most groups drawn per side, matching the cap of the aggregated endpoint. */
export const MAX_GROUPS_PER_SIDE = 50;

/** The layers the list queries load; replication comes only from the aggregated endpoint. */
export const TOPOLOGY_SOURCES: readonly TopologyLayerId[] = [
  'config',
  'nameservers',
  'brokers',
  'topics',
  'producers',
  'consumers'
];

export const TOPOLOGY_SOURCE_LABELS: Record<TopologySourceId, string> = {
  config: 'NameServer config',
  nameservers: 'NameServer health',
  brokers: 'Broker list',
  topics: 'Topic routes',
  producers: 'Producer inventory',
  consumers: 'Consumer groups',
  replication: 'Replication and Controllers'
};

/** Responses of the per-layer queries; `null` marks a layer whose query failed. */
export interface TopologyLayers {
  config: DashboardConfigView | null;
  nameservers: NameserverAvailabilityView | null;
  brokers: BrokerInfo[] | null;
  topics: TopicInfo[] | null;
  producers: ProducerInfo[] | null;
  consumers: ConsumerGroupListItem[] | null;
}

const collator = new Intl.Collator('en', { numeric: true });
const byText = (left: string, right: string) => collator.compare(left, right);

function sum<T>(items: readonly T[], value: (item: T) => number) {
  return items.reduce((total, item) => {
    const next = value(item);
    return Number.isFinite(next) ? total + next : total;
  }, 0);
}

const roundTps = (value: number) => Math.round(value * 100) / 100;

function byConnections<T extends { connectionCount: number }>(name: (item: T) => string) {
  return (left: T, right: T) => right.connectionCount - left.connectionCount || byText(name(left), name(right));
}

const producerName = (producer: TopologyProducer) => producer.producerGroup;
const consumerName = (consumer: TopologyConsumer) => consumer.group;

/** Groups broker instances by cluster and broker name; broker id 0 is the master. */
export function groupBrokers(brokers: readonly BrokerInfo[]): TopologyCluster[] {
  const clusters = new Map<string, Map<string, BrokerInfo[]>>();
  for (const broker of brokers) {
    let groups = clusters.get(broker.clusterName);
    if (!groups) {
      groups = new Map();
      clusters.set(broker.clusterName, groups);
    }
    const members = groups.get(broker.brokerName);
    if (members) members.push(broker);
    else groups.set(broker.brokerName, [broker]);
  }

  return [...clusters.entries()]
    .sort(([left], [right]) => byText(left, right))
    .map(([clusterName, groups]) => {
      const brokerGroups = [...groups.entries()]
        .sort(([left], [right]) => byText(left, right))
        .map(([brokerName, members]) => toBrokerGroup(brokerName, members));
      return {
        clusterName,
        produceTps: roundTps(sum(brokerGroups, (group) => group.produceTps)),
        consumeTps: roundTps(sum(brokerGroups, (group) => group.consumeTps)),
        brokers: brokerGroups
      };
    });
}

function toBrokerGroup(brokerName: string, members: readonly BrokerInfo[]): TopologyBrokerGroup {
  const master = members.find((member) => member.brokerId === 0) ?? null;
  return {
    brokerName,
    masterAddress: master?.address ?? null,
    slaveAddresses: members.filter((member) => member.brokerId !== 0).map((member) => member.address).sort(byText),
    version: master?.version ?? members[0]?.version ?? '',
    // Same reading as the broker list: every instance of the group reports its own traffic.
    produceTps: roundTps(sum(members, (member) => member.produceTps)),
    consumeTps: roundTps(sum(members, (member) => member.consumeTps))
  };
}

/** Orders clusters and their broker names the way `groupBrokers` does, for data built elsewhere. */
export function sortClusters(clusters: readonly TopologyCluster[]): TopologyCluster[] {
  return [...clusters]
    .sort((left, right) => byText(left.clusterName, right.clusterName))
    .map((cluster) => ({
      ...cluster,
      brokers: [...cluster.brokers].sort((left, right) => byText(left.brokerName, right.brokerName))
    }));
}

/** Configured NameServers in configuration order, with the latest availability check of each. */
export function buildNameServers(
  config: DashboardConfigView | null,
  availability: NameserverAvailabilityView | null
): TopologyNameServer[] {
  const checks = new Map((availability?.endpoints ?? []).map((endpoint) => [endpoint.address, endpoint]));
  const addresses = [...new Set([...(config?.namesrvAddrList ?? []), ...checks.keys()])];
  const current = config?.currentNamesrv ?? null;
  return addresses.map((address) => {
    const check = checks.get(address);
    return {
      address,
      current: address === current,
      status: check?.status ?? 'unknown',
      checkedAt: check?.checkedAt ?? null
    };
  });
}

/**
 * Folds the producer rows into one entry per group. Rows of one group that name different topics
 * usually come from the same clients, so the largest row gives the connection count; adding the rows
 * up would count those clients once per topic. A group links to the brokers its rows report and to
 * the brokers of its topics.
 */
export function aggregateProducers(producers: readonly ProducerInfo[], topics: readonly TopicInfo[] | null): TopologyProducer[] {
  const topicBrokers = new Map((topics ?? []).map((topic) => [topic.topic, topic.brokers]));
  const groups = new Map<string, { connectionCount: number; topics: Set<string>; brokerNames: Set<string> }>();
  for (const row of producers) {
    if (!row.producerGroup) continue;
    const group = groups.get(row.producerGroup) ?? { connectionCount: 0, topics: new Set<string>(), brokerNames: new Set<string>() };
    group.connectionCount = Math.max(group.connectionCount, row.connectionCount);
    if (row.topic) group.topics.add(row.topic);
    for (const brokerName of row.brokerNames ?? []) group.brokerNames.add(brokerName);
    groups.set(row.producerGroup, group);
  }

  return [...groups.entries()]
    .map(([producerGroup, group]) => {
      const groupTopics = [...group.topics].sort(byText);
      const brokerNames = new Set([...group.brokerNames, ...groupTopics.flatMap((topic) => topicBrokers.get(topic) ?? [])]);
      return {
        producerGroup,
        connectionCount: group.connectionCount,
        topics: groupTopics,
        brokerNames: [...brokerNames].sort(byText)
      };
    })
    .sort(byConnections(producerName));
}

/** Application consumer groups ranked by connections; system groups stay off the map. */
export function mapConsumers(items: readonly ConsumerGroupListItem[]): TopologyConsumer[] {
  return items
    .filter((item) => !isSystemConsumerGroup(item))
    .map((item) => ({
      group: item.rawGroupName,
      displayName: item.displayGroupName || item.rawGroupName,
      connectionCount: item.connectionCount,
      consumeTps: item.consumeTps,
      diffTotal: item.diffTotal,
      brokerNames: [...new Set(item.brokerNames)].sort(byText)
    }))
    .sort(byConnections(consumerName));
}

export function buildTopologySnapshot(
  layers: TopologyLayers,
  sources: TopologySourceStatus[],
  generatedAt: number
): TopologySnapshot {
  const producers = layers.producers ? aggregateProducers(layers.producers, layers.topics) : [];
  const consumers = layers.consumers ? mapConsumers(layers.consumers) : [];
  return {
    generatedAt,
    sources,
    nameServers: buildNameServers(layers.config, layers.nameservers),
    clusters: layers.brokers ? groupBrokers(layers.brokers) : [],
    controllers: [],
    producers,
    consumers,
    totals: {
      producerGroups: producers.length,
      consumerGroups: consumers.length,
      messageBacklog: sum(consumers, (consumer) => consumer.diffTotal)
    }
  };
}

export function failedSources(snapshot: Pick<TopologySnapshot, 'sources'>): Set<TopologySourceId> {
  return new Set(snapshot.sources.filter((source) => source.status === 'failed').map((source) => source.source));
}

export function allSourcesFailed(snapshot: Pick<TopologySnapshot, 'sources'>): boolean {
  return snapshot.sources.length > 0 && snapshot.sources.every((source) => source.status === 'failed');
}

function mergeNameServers(
  next: TopologyNameServer[],
  previous: TopologyNameServer[],
  configFailed: boolean,
  availabilityFailed: boolean
): TopologyNameServer[] {
  if (configFailed && availabilityFailed) return previous;
  if (availabilityFailed) {
    const earlier = new Map(previous.map((nameServer) => [nameServer.address, nameServer]));
    return next.map((nameServer) => {
      const known = earlier.get(nameServer.address);
      return known ? { ...nameServer, status: known.status, checkedAt: known.checkedAt } : nameServer;
    });
  }
  if (configFailed && previous.length > 0) {
    const checks = new Map(next.map((nameServer) => [nameServer.address, nameServer]));
    return previous.map((nameServer) => {
      const check = checks.get(nameServer.address);
      return check ? { ...nameServer, status: check.status, checkedAt: check.checkedAt } : nameServer;
    });
  }
  return next;
}

/** The groups of `next` with the replication readings `previous` had for them. */
function withReplicationOf(next: readonly TopologyCluster[], previous: readonly TopologyCluster[]): TopologyCluster[] {
  const earlier = new Map(previous.flatMap((cluster) => cluster.brokers).map((group) => [group.brokerName, group]));
  return next.map((cluster) => ({
    ...cluster,
    brokers: cluster.brokers.map((group) => {
      const known = earlier.get(group.brokerName);
      if (!known) return group;
      const readings = new Map((known.replicas ?? []).map((replica) => [replica.brokerId, replica]));
      return {
        ...group,
        election: known.election,
        controllerAddresses: known.controllerAddresses,
        inSyncSlaveCount: known.inSyncSlaveCount,
        controllerState: known.controllerState,
        // A fresh replica has no readings, so the earlier ones show through.
        replicas: group.replicas?.map((replica) => ({ ...readings.get(replica.brokerId), ...replica }))
      };
    })
  }));
}

/**
 * Keeps the last good data of every layer whose query failed this time, so one slow or failing
 * query does not wipe its part of the map. The source list still reports the failure.
 */
export function carryForwardFailedLayers(next: TopologySnapshot, previous: TopologySnapshot | null): TopologySnapshot {
  if (!previous) return next;
  const failed = failedSources(next);
  if (failed.size === 0) return next;

  let producers = failed.has('producers') ? previous.producers : next.producers;
  if (!failed.has('producers') && failed.has('topics')) {
    // Brokers found through topics come from the last snapshot; the ones the rows report are current.
    const knownBrokers = new Map(previous.producers.map((producer) => [producer.producerGroup, producer.brokerNames]));
    producers = producers.map((producer) => ({
      ...producer,
      brokerNames: [...new Set([...producer.brokerNames, ...(knownBrokers.get(producer.producerGroup) ?? [])])].sort(byText)
    }));
  }
  const consumersFailed = failed.has('consumers');
  // Replication is read only after the broker list, so a failed broker list keeps both.
  const brokersFailed = failed.has('brokers');
  const replicationFailed = !brokersFailed && failed.has('replication');

  return {
    ...next,
    nameServers: mergeNameServers(next.nameServers, previous.nameServers, failed.has('config'), failed.has('nameservers')),
    clusters: brokersFailed ? previous.clusters : replicationFailed ? withReplicationOf(next.clusters, previous.clusters) : next.clusters,
    controllers: brokersFailed || replicationFailed ? previous.controllers : next.controllers,
    producers,
    consumers: consumersFailed ? previous.consumers : next.consumers,
    totals: {
      producerGroups: failed.has('producers') ? previous.totals.producerGroups : next.totals.producerGroups,
      consumerGroups: consumersFailed ? previous.totals.consumerGroups : next.totals.consumerGroups,
      messageBacklog: consumersFailed ? previous.totals.messageBacklog : next.totals.messageBacklog
    }
  };
}

export type BrokerTone = 'normal' | 'danger';
export type ClientTone = 'normal' | 'warning' | 'idle';

/**
 * A broker name without a master is down for writes. A master without a slave is a supported
 * single-replica deployment, and the backend does not say how many replicas were intended, so it
 * stays normal; a slave that goes away is reported by `newWarnings` and the change list instead.
 */
export function brokerTone(group: TopologyBrokerGroup): BrokerTone {
  return group.masterAddress ? 'normal' : 'danger';
}

export function clusterIsHealthy(cluster: TopologyCluster): boolean {
  return cluster.brokers.every((group) => brokerTone(group) === 'normal');
}

/** Backlog outranks idleness: a group without clients and with a growing backlog needs attention. */
export function consumerTone(consumer: TopologyConsumer): ClientTone {
  if (consumer.diffTotal > BACKLOG_ALERT_THRESHOLD) return 'warning';
  if (consumer.connectionCount === 0) return 'idle';
  return 'normal';
}

export function producerTone(producer: TopologyProducer): ClientTone {
  return producer.connectionCount === 0 ? 'idle' : 'normal';
}

/**
 * Broker names that lost their master or every slave, and consumer groups that turned to a
 * warning, since `previous`.
 */
export function newWarnings(previous: TopologySnapshot | null, next: TopologySnapshot | null) {
  const brokers = new Set<string>();
  const consumers = new Set<string>();
  if (!previous || !next) return { brokers, consumers };
  const before = new Map(previous.clusters.flatMap((cluster) => cluster.brokers).map((group) => [group.brokerName, group]));
  for (const group of next.clusters.flatMap((cluster) => cluster.brokers)) {
    const earlier = before.get(group.brokerName);
    if (!earlier) continue;
    const lostMaster = brokerTone(group) === 'danger' && brokerTone(earlier) === 'normal';
    const lostSlaves = earlier.slaveAddresses.length > 0 && group.slaveAddresses.length === 0;
    if (lostMaster || lostSlaves) brokers.add(group.brokerName);
  }
  const consumerTones = new Map(previous.consumers.map((consumer) => [consumer.group, consumerTone(consumer)]));
  for (const consumer of next.consumers) {
    if (consumerTone(consumer) === 'warning' && consumerTones.has(consumer.group) && consumerTones.get(consumer.group) !== 'warning') {
      consumers.add(consumer.group);
    }
  }
  return { brokers, consumers };
}

export function countBrokerInstances(clusters: readonly TopologyCluster[]): number {
  return sum(clusters, (cluster) => sum(cluster.brokers, (group) => (group.masterAddress ? 1 : 0) + group.slaveAddresses.length));
}

export type TopologyChangeTone = 'success' | 'warning' | 'danger' | 'info';

export interface TopologyChange {
  subject: string;
  detail: string;
  tone: TopologyChangeTone;
}

const integerFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const tpsFormat = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

const fineRateFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 });

export const formatCount = (value: number) => integerFormat.format(value);
export const formatTps = (value: number) => tpsFormat.format(value);
/** Group throughput in whole messages, keeping one decimal for very low rates. */
export const formatRate = (value: number) => (Math.abs(value) < 10 ? fineRateFormat : integerFormat).format(value);
export const formatBacklog = (value: number) => formatDashboardMetric(value);

const toneOrder: Record<TopologyChangeTone, number> = { danger: 0, warning: 1, success: 2, info: 3 };

function connectionChange(subject: string, before: number, after: number): TopologyChange {
  let tone: TopologyChangeTone = 'info';
  if (after === 0) tone = 'warning';
  else if (after > before) tone = 'success';
  return { subject, detail: `connections ${formatCount(before)} → ${formatCount(after)}`, tone };
}

function backlogChange(subject: string, before: number, after: number): TopologyChange | null {
  const wasHigh = before > BACKLOG_ALERT_THRESHOLD;
  const isHigh = after > BACKLOG_ALERT_THRESHOLD;
  const detail = `lag ${formatCount(before)} → ${formatCount(after)}`;
  if (wasHigh !== isHigh) return { subject, detail, tone: isHigh ? 'warning' : 'success' };
  if (isHigh && Math.abs(after - before) >= before * BACKLOG_CHANGE_RATIO) {
    return { subject, detail, tone: after > before ? 'warning' : 'info' };
  }
  return null;
}

function diffNameServers(previous: TopologyNameServer[], next: TopologyNameServer[], changes: TopologyChange[]) {
  const earlier = new Map(previous.map((nameServer) => [nameServer.address, nameServer]));
  for (const nameServer of next) {
    const before = earlier.get(nameServer.address);
    earlier.delete(nameServer.address);
    if (!before) {
      changes.push({ subject: nameServer.address, detail: 'added to the NameServer list', tone: 'info' });
      continue;
    }
    if (before.status !== nameServer.status && before.status !== 'unknown' && nameServer.status !== 'unknown') {
      changes.push(nameServer.status === 'unavailable'
        ? { subject: nameServer.address, detail: 'became unavailable', tone: 'danger' }
        : { subject: nameServer.address, detail: 'became available', tone: 'success' });
    }
    if (nameServer.current && !before.current) {
      changes.push({ subject: nameServer.address, detail: 'is now the current NameServer', tone: 'info' });
    }
  }
  for (const removed of earlier.values()) {
    changes.push({ subject: removed.address, detail: 'removed from the NameServer list', tone: 'info' });
  }
}

function brokerIndex(clusters: TopologyCluster[]) {
  const index = new Map<string, { clusterName: string; group: TopologyBrokerGroup }>();
  for (const cluster of clusters) {
    for (const group of cluster.brokers) index.set(group.brokerName, { clusterName: cluster.clusterName, group });
  }
  return index;
}

function diffBrokers(previous: TopologyCluster[], next: TopologyCluster[], changes: TopologyChange[]) {
  const earlier = brokerIndex(previous);
  for (const [brokerName, { clusterName, group }] of brokerIndex(next)) {
    const before = earlier.get(brokerName)?.group;
    earlier.delete(brokerName);
    if (!before) {
      changes.push({ subject: brokerName, detail: `joined ${clusterName}`, tone: 'success' });
      continue;
    }
    if (before.masterAddress && !group.masterAddress) {
      changes.push({ subject: brokerName, detail: 'master no longer registered', tone: 'danger' });
    } else if (!before.masterAddress && group.masterAddress) {
      changes.push({ subject: brokerName, detail: 'master registered', tone: 'success' });
    } else if (before.masterAddress && group.masterAddress && before.masterAddress !== group.masterAddress) {
      changes.push({ subject: brokerName, detail: `master ${before.masterAddress} → ${group.masterAddress}`, tone: 'warning' });
    }
    const slavesBefore = before.slaveAddresses.length;
    const slavesAfter = group.slaveAddresses.length;
    if (slavesBefore > 0 && slavesAfter === 0) {
      changes.push({ subject: brokerName, detail: 'slave no longer registered', tone: 'warning' });
    } else if (slavesBefore === 0 && slavesAfter > 0) {
      changes.push({ subject: brokerName, detail: 'slave registered', tone: 'success' });
    } else if (slavesBefore !== slavesAfter) {
      changes.push({
        subject: brokerName,
        detail: `slaves ${slavesBefore} → ${slavesAfter}`,
        tone: slavesAfter < slavesBefore ? 'warning' : 'success'
      });
    }
  }
  for (const [brokerName] of earlier) {
    changes.push({ subject: brokerName, detail: 'no longer registered', tone: 'danger' });
  }
}

function diffClients<T extends { connectionCount: number }>(
  previous: readonly T[],
  next: readonly T[],
  identity: (client: T) => string,
  label: (client: T) => string,
  changes: TopologyChange[],
  extra?: (before: T, after: T, subject: string) => TopologyChange | null
) {
  const earlier = new Map(previous.map((client) => [identity(client), client]));
  for (const client of next) {
    const before = earlier.get(identity(client));
    earlier.delete(identity(client));
    const subject = label(client);
    if (!before) {
      changes.push({ subject, detail: 'group appeared', tone: 'info' });
      continue;
    }
    if (before.connectionCount !== client.connectionCount) {
      changes.push(connectionChange(subject, before.connectionCount, client.connectionCount));
    }
    const additional = extra?.(before, client, subject);
    if (additional) changes.push(additional);
  }
  for (const removed of earlier.values()) {
    changes.push({ subject: label(removed), detail: 'no longer reported', tone: 'info' });
  }
}

/**
 * What changed between two snapshots, most severe first. Layers in `skipped` are not compared;
 * by default those are the layers that failed in either snapshot, so a failed query never reads
 * as groups disappearing.
 */
export function diffSnapshots(
  previous: TopologySnapshot,
  next: TopologySnapshot,
  skipped: ReadonlySet<TopologySourceId> = new Set([...failedSources(previous), ...failedSources(next)])
): TopologyChange[] {
  const changes: TopologyChange[] = [];
  if (!skipped.has('config') && !skipped.has('nameservers')) diffNameServers(previous.nameServers, next.nameServers, changes);
  if (!skipped.has('brokers')) diffBrokers(previous.clusters, next.clusters, changes);
  if (!skipped.has('consumers')) {
    diffClients(previous.consumers, next.consumers, consumerName, (consumer) => consumer.displayName, changes, (before, after, subject) =>
      backlogChange(subject, before.diffTotal, after.diffTotal)
    );
  }
  if (!skipped.has('producers')) diffClients(previous.producers, next.producers, producerName, producerName, changes);
  return changes
    .map((change, index) => ({ change, index }))
    .sort((left, right) => toneOrder[left.change.tone] - toneOrder[right.change.tone] || left.index - right.index)
    .map(({ change }) => change);
}

/** Scope value for every cluster; `*` cannot clash with a cluster name in practice. */
export const ALL_CLUSTERS = '*';

export interface TopologyViewOptions {
  /** `ALL_CLUSTERS` or one cluster name. */
  scope: string;
  producerLimit: number;
  consumerLimit: number;
}

/** The part of a snapshot drawn on the map for one scope. */
export interface TopologyView {
  clusters: TopologyCluster[];
  nameServers: TopologyNameServer[];
  /** The Controllers of the groups in scope. */
  controllers: TopologyController[];
  producers: TopologyProducer[];
  consumers: TopologyConsumer[];
  producersInScope: number;
  consumersInScope: number;
}

export function selectTopologyView(snapshot: TopologySnapshot, options: TopologyViewOptions): TopologyView {
  const allClusters = options.scope === ALL_CLUSTERS;
  const clusters = allClusters
    ? snapshot.clusters
    : snapshot.clusters.filter((cluster) => cluster.clusterName === options.scope);
  const brokerNames = new Set(clusters.flatMap((cluster) => cluster.brokers.map((group) => group.brokerName)));
  const inScope = (client: { brokerNames: string[] }) => allClusters || client.brokerNames.some((name) => brokerNames.has(name));
  const producers = snapshot.producers.filter(inScope);
  const consumers = snapshot.consumers.filter(inScope);
  const limit = (requested: number) => Math.min(Math.max(requested, 0), MAX_GROUPS_PER_SIDE);
  const named = new Set(clusters.flatMap((cluster) => cluster.brokers.flatMap((group) => group.controllerAddresses ?? [])));

  return {
    clusters,
    nameServers: snapshot.nameServers,
    controllers: allClusters ? snapshot.controllers : snapshot.controllers.filter((controller) => named.has(controller.address)),
    producers: producers.slice(0, limit(options.producerLimit)),
    consumers: consumers.slice(0, limit(options.consumerLimit)),
    // With every cluster in scope the totals also count the groups the aggregated endpoint left out.
    producersInScope: allClusters ? Math.max(snapshot.totals.producerGroups, producers.length) : producers.length,
    consumersInScope: allClusters ? Math.max(snapshot.totals.consumerGroups, consumers.length) : consumers.length
  };
}

export type SignalTone = 'success' | 'warning' | 'danger';

export interface TopologySignal {
  value: string;
  tone?: SignalTone;
}

export interface TopologySignals {
  clusters: TopologySignal;
  nameServers: TopologySignal;
  brokers: TopologySignal;
  producers: TopologySignal;
  consumers: TopologySignal;
  backlog: TopologySignal;
}

export function buildSignals(snapshot: TopologySnapshot): TopologySignals {
  const checked = snapshot.nameServers.filter((nameServer) => nameServer.status !== 'unknown');
  const available = checked.filter((nameServer) => nameServer.status === 'available').length;
  let nameServerTone: SignalTone | undefined;
  if (snapshot.nameServers.length === 0) nameServerTone = 'danger';
  else if (checked.length > 0) {
    if (available === 0) nameServerTone = 'danger';
    else if (available < snapshot.nameServers.length) nameServerTone = 'warning';
    else nameServerTone = 'success';
  }
  const instances = countBrokerInstances(snapshot.clusters);
  const lagging = snapshot.consumers.some((consumer) => consumer.diffTotal > BACKLOG_ALERT_THRESHOLD);

  return {
    clusters: { value: formatCount(snapshot.clusters.length), tone: snapshot.clusters.length > 0 ? 'success' : 'danger' },
    nameServers: {
      value: checked.length > 0 ? `${available} of ${snapshot.nameServers.length}` : formatCount(snapshot.nameServers.length),
      tone: nameServerTone
    },
    brokers: { value: `${formatCount(instances)} online`, tone: instances > 0 ? 'success' : 'danger' },
    producers: { value: `${formatCount(snapshot.totals.producerGroups)} groups` },
    consumers: { value: `${formatCount(snapshot.totals.consumerGroups)} groups` },
    backlog: { value: formatBacklog(snapshot.totals.messageBacklog), tone: lagging ? 'warning' : 'success' }
  };
}

/** Screen reader summary of the map: what is there and what needs attention. */
export function describeTopology(view: TopologyView): string {
  const groups = view.clusters.flatMap((cluster) => cluster.brokers);
  const parts = [
    `${view.clusters.length} ${view.clusters.length === 1 ? 'cluster' : 'clusters'}`,
    `${groups.length} broker ${groups.length === 1 ? 'group' : 'groups'}`
  ];
  const unavailable = view.nameServers.filter((nameServer) => nameServer.status === 'unavailable').map((nameServer) => nameServer.address);
  if (unavailable.length > 0) parts.push(`NameServer unavailable: ${unavailable.join(', ')}`);
  if (view.controllers.length > 0) {
    const leader = view.controllers.find((controller) => controller.leader === true);
    const count = `${view.controllers.length} ${view.controllers.length === 1 ? 'Controller' : 'Controllers'}`;
    parts.push(`${count}, ${leader ? `leader ${leader.address}` : 'no leader known'}`);
    const down = view.controllers.filter((controller) => controller.status === 'unavailable').map((controller) => controller.address);
    if (down.length > 0) parts.push(`Controller unavailable: ${down.join(', ')}`);
  }
  const withoutSlave = groups.filter((group) => group.masterAddress && group.slaveAddresses.length === 0).map((group) => group.brokerName);
  if (withoutSlave.length > 0) parts.push(`no slave registered: ${withoutSlave.join(', ')}`);
  const withoutMaster = groups.filter((group) => brokerTone(group) === 'danger').map((group) => group.brokerName);
  if (withoutMaster.length > 0) parts.push(`no master registered: ${withoutMaster.join(', ')}`);
  const lagging = view.consumers.filter((consumer) => consumerTone(consumer) === 'warning').map((consumer) => consumer.displayName);
  if (lagging.length > 0) parts.push(`consumer backlog above ${formatCount(BACKLOG_ALERT_THRESHOLD)}: ${lagging.join(', ')}`);
  return `${parts.join('; ')}.`;
}
