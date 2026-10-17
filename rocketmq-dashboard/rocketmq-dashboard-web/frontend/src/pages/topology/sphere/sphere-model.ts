import type {
  TopologyBrokerGroup,
  TopologyConsumer,
  TopologyNameServer,
  TopologyProducer
} from '../../../types/topology';
import { nodeId } from '../topology-layout';
import {
  brokerTone,
  consumerTone,
  formatCount,
  formatRate,
  formatTps,
  producerTone,
  type TopologyView
} from '../topology-model';

/** A point in the frame of its sphere, in units of the broker sphere's radius. */
export type Vec3 = readonly [number, number, number];

/** The broker sphere holds clusters and client groups; the NameServer sphere floats beside it. */
export type SphereKey = 'brokers' | 'nameservers';

/** Height of node centres on the broker sphere, so markers never sink into it. */
export const SURFACE = 1.012;
/** Radius of the NameServer sphere relative to the broker sphere. */
export const NAMESERVER_RADIUS = 0.34;
const NAMESERVER_LIFT = 1.04;
/** Widest angle between two broker groups of one cluster, in degrees. */
const GROUP_SPACING = 26;
/** Angle between a master and its slaves, in degrees. */
const SLAVE_OFFSET = 9;
/** Sideways step between the heartbeat lines of one bundle, in sphere units. */
const HEARTBEAT_SPREAD = 0.028;
/** Above this many groups per side the clients get a third ring. */
const TWO_RING_LIMIT = 16;

export type ClusterMode = 'master-slave' | 'single';

interface Placed {
  id: string;
  sphere: SphereKey;
  position: Vec3;
}

interface OnBrokerSphere extends Placed {
  sphere: 'brokers';
  /** Latitude and longitude in degrees. */
  lat: number;
  lon: number;
}

/** A broker name. Without a registered master it stays on the map as a broker group that cannot take writes. */
export interface MasterNode extends OnBrokerSphere {
  kind: 'master';
  clusterName: string;
  group: TopologyBrokerGroup;
}

export interface SlaveNode extends OnBrokerSphere {
  kind: 'slave';
  clusterName: string;
  group: TopologyBrokerGroup;
  address: string;
  masterId: string;
}

export interface ProducerNode extends OnBrokerSphere {
  kind: 'producer';
  producer: TopologyProducer;
}

export interface ConsumerNode extends OnBrokerSphere {
  kind: 'consumer';
  consumer: TopologyConsumer;
}

export interface NameServerNode extends Placed {
  kind: 'nameserver';
  sphere: 'nameservers';
  nameServer: TopologyNameServer;
}

export type SphereNode = MasterNode | SlaveNode | ProducerNode | ConsumerNode | NameServerNode;
export type SphereNodeKind = SphereNode['kind'];

/**
 * `heartbeat`: a master registering with a NameServer. `route`: a client group looking up its
 * routes on a NameServer, drawn from the NameServer toward the group.
 */
export type SphereLinkKind = 'produce' | 'consume' | 'replication' | 'heartbeat' | 'route';

export interface SphereLink {
  id: string;
  kind: SphereLinkKind;
  from: string;
  to: string;
  /** Throughput the link carries: the broker's for produce and replication links, the group's for consume links. */
  tps: number;
  /** A client group without clients: dashed, without particles. */
  idle: boolean;
  /** A heartbeat to a NameServer that failed its latest check. */
  unavailable: boolean;
  /** Sideways offset that fans the heartbeat lines of one bundle apart, in sphere units. */
  offset: number;
}

export interface SphereCluster {
  clusterName: string;
  mode: ClusterMode;
  /** Longitudes, in degrees, of the cluster's middle and of the ends of its equator band. */
  centre: number;
  from: number;
  to: number;
  groups: number;
  instances: number;
  healthy: boolean;
  produceTps: number;
  consumeTps: number;
}

export interface SphereModel {
  nodes: SphereNode[];
  byId: ReadonlyMap<string, SphereNode>;
  links: SphereLink[];
  clusters: SphereCluster[];
  /** Longitude to turn to the front: the middle of the busiest cluster. */
  frontLon: number;
  /** Client groups drawn, and how many the current scope holds. */
  groups: { producers: number; producersInScope: number; consumers: number; consumersInScope: number };
  /** Changes when nodes, links or their states change; throughput and lag updates keep it. */
  structure: string;
}

const DEG = Math.PI / 180;

export function surfacePoint(lat: number, lon: number, radius = SURFACE): Vec3 {
  const phi = lat * DEG;
  const lambda = lon * DEG;
  return [radius * Math.cos(phi) * Math.sin(lambda), radius * Math.sin(phi), radius * Math.cos(phi) * Math.cos(lambda)];
}

/** Angle, in radians, between the front of a small sphere and the circle its nodes sit on. */
export function capAngle(count: number): number {
  if (count <= 1) return 0;
  if (count === 2) return 40 * DEG;
  return (count <= 6 ? 44 : 56) * DEG;
}

/**
 * Node `index` of `count` on a small sphere: on a circle around the sphere's front (+z), the first
 * at the top, so two nodes stack vertically and their side labels never cover each other.
 */
export function capPoint(index: number, count: number, radius: number): Vec3 {
  const alpha = capAngle(count);
  const bearing = Math.PI / 2 + (index * 2 * Math.PI) / Math.max(count, 1);
  return [radius * Math.sin(alpha) * Math.cos(bearing), radius * Math.sin(alpha) * Math.sin(bearing), radius * Math.cos(alpha)];
}

/** Pushes neighbours apart around the circle until each pair is at least `gap` degrees apart. */
export function spreadAround(lons: readonly number[], gap: number): number[] {
  const spread = [...lons];
  const n = spread.length;
  for (let pass = 0; pass < 200 && n > 1; pass += 1) {
    let moved = false;
    for (let i = 0; i < n; i += 1) {
      const j = (i + 1) % n;
      const space = spread[j] + (j === 0 ? 360 : 0) - spread[i];
      if (space < gap - 0.01) {
        spread[i] -= (gap - space) / 2;
        spread[j] += (gap - space) / 2;
        moved = true;
      }
    }
    if (!moved) break;
  }
  return spread;
}

const wrap = (lon: number) => ((((lon + 180) % 360) + 360) % 360) - 180;

/**
 * Longitude a client group prefers: the mean direction of its brokers. A group whose brokers sit
 * on opposite sides has no clear direction and alternates between them.
 */
function preferredLongitudes(brokerLons: readonly (readonly number[])[]) {
  let alternate = 0;
  return brokerLons.map((lons) => {
    if (lons.length === 0) return 0;
    const x = lons.reduce((total, lon) => total + Math.sin(lon * DEG), 0);
    const z = lons.reduce((total, lon) => total + Math.cos(lon * DEG), 0);
    if (Math.hypot(x, z) / lons.length >= 0.25) return wrap(Math.atan2(x, z) / DEG);
    const mean = lons.reduce((total, lon) => total + lon, 0) / lons.length + (alternate % 2) * 180;
    alternate += 1;
    return wrap(mean);
  });
}

function clientRings(count: number, side: 'producer' | 'consumer') {
  if (side === 'producer') return count > TWO_RING_LIMIT ? [28, 42, 56] : [32, 48];
  // Consumer rings sit a little closer to the equator: the sphere is tilted toward its north.
  return count > TWO_RING_LIMIT ? [-22, -36, -50] : [-26, -42];
}

/** Spreads client groups over rings of latitude, each near the direction of its brokers. */
function placeClients<Client>(
  clients: readonly Client[],
  side: 'producer' | 'consumer',
  name: (client: Client) => string,
  brokers: (client: Client) => readonly string[],
  brokerLon: ReadonlyMap<string, number>
): { client: Client; lat: number; lon: number }[] {
  const ordered = [...clients].sort((left, right) => name(left).localeCompare(name(right)));
  const preferred = preferredLongitudes(
    ordered.map((client) => brokers(client).map((broker) => brokerLon.get(broker)).filter((lon): lon is number => lon !== undefined))
  );
  const items = ordered
    .map((client, index) => ({ client, preferred: preferred[index] }))
    .sort((left, right) => left.preferred - right.preferred || name(left.client).localeCompare(name(right.client)));
  const rings = clientRings(items.length, side);
  const placed: { client: Client; lat: number; lon: number }[] = [];
  rings.forEach((lat, ring) => {
    const row = items.filter((_, index) => index % rings.length === ring);
    if (row.length === 0) return;
    const even = 360 / row.length;
    const gap = Math.min(Math.max(30 / Math.cos(lat * DEG), even) * 0.7, even * 0.95);
    const lons = spreadAround(row.map((item) => item.preferred), gap);
    row.forEach((item, index) => placed.push({ client: item.client, lat, lon: lons[index] }));
  });
  return placed;
}

const slaveId = (brokerName: string, address: string) => `slave:${brokerName}:${address}`;

/**
 * One entry per NameServer. A configured entry may list several addresses joined by `;`, the way
 * clients take them; each address is a server of its own and shares the entry's check result.
 */
export function splitNameServers(nameServers: readonly TopologyNameServer[]): TopologyNameServer[] {
  const seen = new Set<string>();
  const servers: TopologyNameServer[] = [];
  for (const nameServer of nameServers) {
    for (const address of nameServer.address.split(';').map((part) => part.trim()).filter(Boolean)) {
      if (seen.has(address)) continue;
      seen.add(address);
      servers.push({ ...nameServer, address });
    }
  }
  return servers;
}

export function buildSphereModel(view: TopologyView): SphereModel {
  const nodes: SphereNode[] = [];
  const links: SphereLink[] = [];
  const clusters: SphereCluster[] = [];
  const brokerLon = new Map<string, number>();

  // Each cluster gets an arc of the equator in proportion to its broker groups, so a large
  // cluster never squeezes its groups together.
  const weights = view.clusters.map((cluster) => cluster.brokers.length + 1);
  const totalWeight = weights.reduce((total, weight) => total + weight, 0);
  let start = -180;
  view.clusters.forEach((cluster, index) => {
    const arc = (360 * weights[index]) / Math.max(totalWeight, 1);
    const centre = start + arc / 2;
    start += arc;
    const count = cluster.brokers.length;
    const spacing = count > 1 ? Math.min(GROUP_SPACING, (arc * 0.7) / (count - 1)) : 0;
    const lons = cluster.brokers.map((_, position) => centre + (position - (count - 1) / 2) * spacing);
    cluster.brokers.forEach((group, position) => {
      const lon = lons[position];
      const masterId = nodeId.broker(group.brokerName);
      brokerLon.set(group.brokerName, lon);
      nodes.push({ id: masterId, kind: 'master', sphere: 'brokers', lat: 0, lon, position: surfacePoint(0, lon), clusterName: cluster.clusterName, group });
      group.slaveAddresses.forEach((address, slave) => {
        const turn = (-90 + (slave * 360) / group.slaveAddresses.length) * DEG;
        const lat = SLAVE_OFFSET * Math.sin(turn);
        const slaveLon = lon + SLAVE_OFFSET * Math.cos(turn);
        nodes.push({
          id: slaveId(group.brokerName, address),
          kind: 'slave',
          sphere: 'brokers',
          lat,
          lon: slaveLon,
          position: surfacePoint(lat, slaveLon),
          clusterName: cluster.clusterName,
          group,
          address,
          masterId
        });
      });
    });
    const pad = Math.min(17, arc / 2 - 2);
    clusters.push({
      clusterName: cluster.clusterName,
      mode: cluster.brokers.some((group) => group.slaveAddresses.length > 0) ? 'master-slave' : 'single',
      centre,
      from: Math.min(...lons) - pad,
      to: Math.max(...lons) + pad,
      groups: count,
      instances: cluster.brokers.reduce((total, group) => total + (group.masterAddress ? 1 : 0) + group.slaveAddresses.length, 0),
      healthy: cluster.brokers.every((group) => brokerTone(group) === 'normal'),
      produceTps: cluster.produceTps,
      consumeTps: cluster.consumeTps
    });
  });

  for (const { client, lat, lon } of placeClients(view.producers, 'producer', (producer) => producer.producerGroup, (producer) => producer.brokerNames, brokerLon)) {
    nodes.push({ id: nodeId.producer(client.producerGroup), kind: 'producer', sphere: 'brokers', lat, lon, position: surfacePoint(lat, lon), producer: client });
  }
  for (const { client, lat, lon } of placeClients(view.consumers, 'consumer', (consumer) => consumer.group, (consumer) => consumer.brokerNames, brokerLon)) {
    nodes.push({ id: nodeId.consumer(client.group), kind: 'consumer', sphere: 'brokers', lat, lon, position: surfacePoint(lat, lon), consumer: client });
  }
  const nameServerRadius = NAMESERVER_RADIUS * NAMESERVER_LIFT;
  const nameServers = splitNameServers(view.nameServers);
  nameServers.forEach((nameServer, index) => {
    nodes.push({
      id: nodeId.nameServer(nameServer.address),
      kind: 'nameserver',
      sphere: 'nameservers',
      position: capPoint(index, nameServers.length, nameServerRadius),
      nameServer
    });
  });

  const byId = new Map(nodes.map((node) => [node.id, node]));
  const groups = new Map(view.clusters.flatMap((cluster) => cluster.brokers).map((group) => [group.brokerName, group]));
  for (const producer of view.producers) {
    for (const brokerName of producer.brokerNames) {
      const group = groups.get(brokerName);
      if (!group) continue;
      const from = nodeId.producer(producer.producerGroup);
      const to = nodeId.broker(brokerName);
      links.push({ id: `produce:${from}>${to}`, kind: 'produce', from, to, tps: group.produceTps, idle: producer.connectionCount === 0, unavailable: false, offset: 0 });
    }
  }
  for (const consumer of view.consumers) {
    for (const brokerName of consumer.brokerNames) {
      if (!groups.has(brokerName)) continue;
      const from = nodeId.broker(brokerName);
      const to = nodeId.consumer(consumer.group);
      links.push({ id: `consume:${from}>${to}`, kind: 'consume', from, to, tps: consumer.consumeTps, idle: consumer.connectionCount === 0, unavailable: false, offset: 0 });
    }
  }
  const masters = nodes.filter((node): node is MasterNode => node.kind === 'master');
  for (const node of nodes) {
    if (node.kind !== 'slave' || !node.group.masterAddress) continue;
    links.push({ id: `replication:${node.masterId}>${node.id}`, kind: 'replication', from: node.masterId, to: node.id, tps: node.group.produceTps, idle: false, unavailable: false, offset: 0 });
  }
  // Every broker registers with every NameServer; the masters stand for their groups.
  const registered = masters.filter((master) => master.group.masterAddress);
  for (const nameServer of nameServers) {
    const to = nodeId.nameServer(nameServer.address);
    registered.forEach((master, index) => {
      links.push({
        id: `heartbeat:${master.id}>${to}`,
        kind: 'heartbeat',
        from: master.id,
        to,
        tps: 0,
        idle: false,
        unavailable: nameServer.status === 'unavailable',
        offset: (index - (registered.length - 1) / 2) * HEARTBEAT_SPREAD
      });
    });
  }

  // Clients pick a NameServer for their route lookups; which one is not reported, so the connected
  // groups are spread over the reachable NameServers in turn. The lines are illustrative.
  const reachable = nameServers.filter((nameServer) => nameServer.status !== 'unavailable');
  const lookups = [
    ...view.producers.filter((producer) => producer.connectionCount > 0).map((producer) => nodeId.producer(producer.producerGroup)),
    ...view.consumers.filter((consumer) => consumer.connectionCount > 0).map((consumer) => nodeId.consumer(consumer.group))
  ];
  if (reachable.length > 0) {
    const share = new Map<string, number>();
    lookups.forEach((client, index) => {
      const from = nodeId.nameServer(reachable[index % reachable.length].address);
      const position = share.get(from) ?? 0;
      share.set(from, position + 1);
      links.push({ id: `route:${from}>${client}`, kind: 'route', from, to: client, tps: 0, idle: false, unavailable: false, offset: position });
    });
    // Fan each NameServer's bundle around its middle line.
    for (const link of links) {
      if (link.kind !== 'route') continue;
      link.offset = (link.offset - ((share.get(link.from) ?? 1) - 1) / 2) * HEARTBEAT_SPREAD * 0.8;
    }
  }

  const busiest = [...clusters].sort((left, right) => right.produceTps + right.consumeTps - (left.produceTps + left.consumeTps))[0];
  const structure = [
    ...nodes.map((node) => `${node.id}|${nodeTone(node)}`),
    ...links.map((link) => `${link.id}|${link.idle ? 'idle' : ''}|${link.unavailable ? 'down' : ''}`)
  ].join('\n');
  const groupCounts = {
    producers: view.producers.length,
    producersInScope: view.producersInScope,
    consumers: view.consumers.length,
    consumersInScope: view.consumersInScope
  };
  return { nodes, byId, links, clusters, frontLon: busiest?.centre ?? 0, groups: groupCounts, structure };
}

export type SphereNodeTone = 'normal' | 'warning' | 'danger' | 'idle' | 'current' | 'unknown';

export function nodeTone(node: SphereNode): SphereNodeTone {
  switch (node.kind) {
    case 'master':
      return brokerTone(node.group);
    case 'slave':
      return 'normal';
    case 'producer':
      return producerTone(node.producer);
    case 'consumer':
      return consumerTone(node.consumer);
    case 'nameserver':
      if (node.nameServer.status === 'unavailable') return 'danger';
      if (node.nameServer.status === 'unknown') return 'unknown';
      return node.nameServer.current ? 'current' : 'normal';
  }
}

const plural = (count: number, one: string, many: string) => `${formatCount(count)} ${count === 1 ? one : many}`;

/** Name line of a node's label; slaves carry a single line. */
export function nodeTitle(node: SphereNode): string {
  switch (node.kind) {
    case 'master':
      return node.group.brokerName;
    case 'slave':
      return '';
    case 'producer':
      return node.producer.producerGroup;
    case 'consumer':
      return node.consumer.displayName;
    case 'nameserver':
      return node.nameServer.address;
  }
}

/** Second line of a node's label, the reading that changes between refreshes. */
export function nodeMetric(node: SphereNode): string {
  switch (node.kind) {
    case 'master':
      return node.group.masterAddress
        ? `in ${formatTps(node.group.produceTps)} · out ${formatTps(node.group.consumeTps)}`
        : 'no master registered';
    case 'slave':
      return `slave · ${node.address}`;
    case 'producer':
      return plural(node.producer.connectionCount, 'connection', 'connections');
    case 'consumer': {
      const lag = `lag ${formatCount(node.consumer.diffTotal)}`;
      return node.consumer.connectionCount === 0 ? `no clients · ${lag}` : `${formatRate(node.consumer.consumeTps)} TPS · ${lag}`;
    }
    case 'nameserver':
      if (node.nameServer.status === 'unavailable') return 'unavailable';
      if (node.nameServer.status === 'unknown') return 'status unknown';
      return node.nameServer.current ? 'current' : 'available';
  }
}

const KIND_NAMES: Record<SphereNodeKind, string> = {
  master: 'Broker group',
  slave: 'Slave',
  producer: 'Producer group',
  consumer: 'Consumer group',
  nameserver: 'NameServer'
};

export function kindName(kind: SphereNodeKind): string {
  return KIND_NAMES[kind];
}

/** Full name of a node for lists and screen readers. */
export function nodeName(node: SphereNode): string {
  return node.kind === 'slave' ? `${node.group.brokerName} slave ${node.address}` : nodeTitle(node);
}

/** The nodes a node is linked to, with the kind of each link. */
export function linkedNodes(model: SphereModel, id: string): { node: SphereNode; kind: SphereLinkKind }[] {
  const linked: { node: SphereNode; kind: SphereLinkKind }[] = [];
  for (const link of model.links) {
    if (link.from !== id && link.to !== id) continue;
    const other = model.byId.get(link.from === id ? link.to : link.from);
    if (other) linked.push({ node: other, kind: link.kind });
  }
  return linked;
}
