import type {
  TopologyBrokerGroup,
  TopologyCluster,
  TopologyConsumer,
  TopologyNameServer,
  TopologyProducer
} from '../../types/topology';
import type { TopologyView } from './topology-model';

/**
 * Sizes of the Topology artboard, in CSS pixels. The artboard's map is 1,112 wide with 112-pixel
 * link gutters; a narrower container narrows the gutters down to `minWidth`, then scrolls.
 */
export const TOPOLOGY_LAYOUT = {
  minWidth: 1000,
  clientWidth: 216,
  centreWidth: 456,
  clientHeight: 52,
  clientGap: 8,
  /** Top of the first client node, below the column heading. */
  headerClearance: 36,
  /** Space between the client nodes of two clusters. */
  bandGap: 24,
  /** Space between the route lookup line and the nearest client node. */
  routeClearance: 12,
  /** Vertical space between the blocks of the centre column. */
  blockGap: 36,
  clusterPadding: 12,
  clusterHeader: 40,
  brokerHeight: 62,
  brokerGap: 8,
  tierPadding: 8,
  tierLabel: 18,
  tierLabelGap: 6,
  chipHeight: 40,
  chipGap: 8,
  chipsPerRow: 3
} as const;

const L = TOPOLOGY_LAYOUT;

export interface Point {
  x: number;
  y: number;
}

export interface Box {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface PlacedBroker extends Box {
  id: string;
  group: TopologyBrokerGroup;
}

export interface PlacedCluster extends Box {
  id: string;
  cluster: TopologyCluster;
  brokers: PlacedBroker[];
}

export interface PlacedClient<Client> extends Box {
  id: string;
  client: Client;
  /** Index of the cluster the node is drawn beside. */
  band: number;
}

export interface PlacedChip extends Box {
  id: string;
  nameServer: TopologyNameServer;
}

export type LinkKind = 'produce' | 'consume' | 'heartbeat' | 'route';

export interface TopologyLink {
  id: string;
  kind: LinkKind;
  /** SVG path data. */
  d: string;
  from: Point;
  to: Point;
  /** A group without clients: dashed and without particles. */
  idle: boolean;
  /** A heartbeat line to a NameServer that failed its latest check. */
  unavailable: boolean;
  /** Reported throughput carried by the link: the broker's for produce links, the group's for consume links. */
  tps: number;
  /** Nodes the link joins, for highlighting. */
  nodes: string[];
}

export interface TopologyPort extends Point {
  id: string;
  kind: 'produce' | 'consume' | 'route';
  /** The client node the port belongs to, when it is on one. */
  client: string | null;
}

export interface TopologyLayout {
  width: number;
  height: number;
  producerColumn: { x: number; width: number };
  centreColumn: { x: number; width: number };
  consumerColumn: { x: number; width: number };
  tier: Box & { chips: PlacedChip[] };
  /** Vertical centre of the NameServer tier, where the route lookup lines run. */
  routeY: number;
  clusters: PlacedCluster[];
  producers: PlacedClient<TopologyProducer>[];
  consumers: PlacedClient<TopologyConsumer>[];
  links: TopologyLink[];
  ports: TopologyPort[];
}

export const nodeId = {
  producer: (group: string) => `producer:${group}`,
  consumer: (group: string) => `consumer:${group}`,
  broker: (brokerName: string) => `broker:${brokerName}`,
  cluster: (clusterName: string) => `cluster:${clusterName}`,
  nameServer: (address: string) => `nameserver:${address}`
};

export function clusterHeight(brokerCount: number): number {
  const rows = Math.max(brokerCount, 1);
  return L.clusterPadding * 2 + L.clusterHeader + rows * L.brokerHeight + (rows - 1) * L.brokerGap;
}

function chipRows(count: number) {
  return Math.max(1, Math.ceil(count / L.chipsPerRow));
}

export function tierHeight(nameServerCount: number): number {
  const rows = chipRows(nameServerCount);
  return L.tierPadding * 2 + L.tierLabel + L.tierLabelGap + rows * L.chipHeight + (rows - 1) * L.chipGap;
}

const clientExtent = (count: number) => (count > 0 ? count * L.clientHeight + (count - 1) * L.clientGap : 0);

/** Cubic curve leaving and entering horizontally, the control points half the horizontal distance out. */
export function curvePath(from: Point, to: Point): string {
  const reach = (to.x - from.x) / 2;
  return `M ${from.x} ${from.y} C ${from.x + reach} ${from.y}, ${to.x - reach} ${to.y}, ${to.x} ${to.y}`;
}

const linePath = (from: Point, to: Point) => `M ${from.x} ${from.y} L ${to.x} ${to.y}`;

/**
 * Puts each client group beside the cluster it shares the most brokers with, so most links stay
 * short. Groups linked to no drawn broker go beside the cluster with the fewest groups.
 */
function assignBands<Client extends { brokerNames: string[] }>(clients: readonly Client[], brokerBand: Map<string, number>, bands: number) {
  const assigned = clients.map((client) => {
    const votes = new Array<number>(bands).fill(0);
    for (const name of client.brokerNames) {
      const band = brokerBand.get(name);
      if (band !== undefined) votes[band] += 1;
    }
    const best = Math.max(0, ...votes);
    return best > 0 ? votes.indexOf(best) : -1;
  });
  const sizes = new Array<number>(bands).fill(0);
  for (const band of assigned) if (band >= 0) sizes[band] += 1;
  return assigned.map((band) => {
    if (band >= 0) return band;
    const smallest = sizes.indexOf(Math.min(...sizes));
    sizes[smallest] += 1;
    return smallest;
  });
}

/**
 * Lays the map out in three columns: producer groups, the clusters with the NameServer tier, and
 * consumer groups. Two clusters sit above and below the tier as on the artboard; any other count
 * puts the tier on top. The layout grows downwards, and below `minWidth` the map scrolls sideways.
 */
export function layoutTopology(view: TopologyView, containerWidth: number): TopologyLayout {
  const width = Math.max(L.minWidth, Math.floor(containerWidth));
  const gutter = (width - L.clientWidth * 2 - L.centreWidth) / 2;
  const producerColumn = { x: 0, width: L.clientWidth };
  const centreColumn = { x: L.clientWidth + gutter, width: L.centreWidth };
  const consumerColumn = { x: width - L.clientWidth, width: L.clientWidth };
  const innerX = centreColumn.x + L.clusterPadding;
  const innerWidth = L.centreWidth - L.clusterPadding * 2;

  const clusterCount = view.clusters.length;
  const brokerBand = new Map<string, number>();
  view.clusters.forEach((cluster, index) => cluster.brokers.forEach((group) => brokerBand.set(group.brokerName, index)));
  const bands = Math.max(clusterCount, 1);
  const producerBands = assignBands(view.producers, brokerBand, bands);
  const consumerBands = assignBands(view.consumers, brokerBand, bands);

  const order: Array<'tier' | number> = clusterCount === 2 ? [0, 'tier', 1] : ['tier', ...view.clusters.map((_, index) => index)];
  const tierBox = { x: centreColumn.x, y: 0, width: L.centreWidth, height: tierHeight(view.nameServers.length) };
  const clusters: PlacedCluster[] = [];
  const producers: PlacedClient<TopologyProducer>[] = [];
  const consumers: PlacedClient<TopologyConsumer>[] = [];
  // Per client column: where the next cluster's groups may start, and the bottom of the last one.
  const minTop: Record<'producers' | 'consumers', number> = { producers: L.headerClearance, consumers: L.headerClearance };
  const lowest: Record<'producers' | 'consumers', number> = { producers: 0, consumers: 0 };
  let cursor = 0;

  for (const block of order) {
    if (block === 'tier') {
      // Keep the route lookup line clear of the client nodes above it.
      const above = Math.max(lowest.producers, lowest.consumers);
      tierBox.y = Math.max(cursor, above > 0 ? above + L.routeClearance - tierBox.height / 2 : 0);
      const routeY = tierBox.y + tierBox.height / 2;
      minTop.producers = Math.max(minTop.producers, routeY + L.routeClearance);
      minTop.consumers = Math.max(minTop.consumers, routeY + L.routeClearance);
      cursor = tierBox.y + tierBox.height + L.blockGap;
      continue;
    }

    const cluster = view.clusters[block];
    const height = clusterHeight(cluster.brokers.length);
    const bandProducers = view.producers.filter((_, index) => producerBands[index] === block);
    const bandConsumers = view.consumers.filter((_, index) => consumerBands[index] === block);
    const sides = [
      { key: 'producers' as const, extent: clientExtent(bandProducers.length) },
      { key: 'consumers' as const, extent: clientExtent(bandConsumers.length) }
    ];
    // Centre the cluster on its groups, moving both down when the groups need the room. Below the
    // column heading the groups may hang a little past the cluster without moving it, as drawn.
    let centre = cursor + height / 2;
    for (const side of sides) {
      if (side.extent === 0) continue;
      const lowestCentre = minTop[side.key] + side.extent / 2;
      if (minTop[side.key] > L.headerClearance) {
        centre = Math.max(centre, lowestCentre);
      } else {
        const groupsBottom = Math.max(centre - side.extent / 2, minTop[side.key]) + side.extent;
        if (groupsBottom - (centre + height / 2) > L.headerClearance) centre = Math.max(centre, lowestCentre);
      }
    }
    const top = centre - height / 2;
    const placedBrokers = cluster.brokers.map((group, index) => ({
      id: nodeId.broker(group.brokerName),
      group,
      x: innerX,
      y: top + L.clusterPadding + L.clusterHeader + index * (L.brokerHeight + L.brokerGap),
      width: innerWidth,
      height: L.brokerHeight
    }));
    clusters.push({ id: nodeId.cluster(cluster.clusterName), cluster, brokers: placedBrokers, x: centreColumn.x, y: top, width: L.centreWidth, height });

    const placeSide = <Client,>(items: Client[], key: 'producers' | 'consumers', x: number, id: (client: Client) => string, into: PlacedClient<Client>[]) => {
      if (items.length === 0) return;
      const extent = clientExtent(items.length);
      const first = Math.max(centre - extent / 2, minTop[key]);
      items.forEach((client, index) => {
        into.push({ id: id(client), client, band: block, x, y: first + index * (L.clientHeight + L.clientGap), width: L.clientWidth, height: L.clientHeight });
      });
      lowest[key] = first + extent;
      minTop[key] = lowest[key] + L.bandGap;
    };
    placeSide(bandProducers, 'producers', producerColumn.x, (producer) => nodeId.producer(producer.producerGroup), producers);
    placeSide(bandConsumers, 'consumers', consumerColumn.x, (consumer) => nodeId.consumer(consumer.group), consumers);
    cursor = top + height + L.blockGap;
  }

  const chips = placeChips(view.nameServers, tierBox, innerX, innerWidth);
  const routeY = tierBox.y + tierBox.height / 2;
  const links = [
    ...clientLinks(producers, consumers, clusters),
    ...heartbeatLinks(order, clusters, tierBox, chips),
    ...routeLinks(producers, consumers, producerColumn, consumerColumn, tierBox, routeY)
  ];
  const height = Math.max(cursor - L.blockGap, lowest.producers, lowest.consumers);

  return {
    width,
    height,
    producerColumn,
    centreColumn,
    consumerColumn,
    tier: { ...tierBox, chips },
    routeY,
    clusters,
    producers,
    consumers,
    links,
    ports: portsOf(links)
  };
}

function placeChips(nameServers: readonly TopologyNameServer[], tier: Box, innerX: number, innerWidth: number): PlacedChip[] {
  const chipWidth = (innerWidth - (L.chipsPerRow - 1) * L.chipGap) / L.chipsPerRow;
  const firstRowY = tier.y + L.tierPadding + L.tierLabel + L.tierLabelGap;
  return nameServers.map((nameServer, index) => {
    const row = Math.floor(index / L.chipsPerRow);
    const inRow = Math.min(L.chipsPerRow, nameServers.length - row * L.chipsPerRow);
    const rowWidth = inRow * chipWidth + (inRow - 1) * L.chipGap;
    const column = index % L.chipsPerRow;
    return {
      id: nodeId.nameServer(nameServer.address),
      nameServer,
      x: innerX + (innerWidth - rowWidth) / 2 + column * (chipWidth + L.chipGap),
      y: firstRowY + row * (L.chipHeight + L.chipGap),
      width: chipWidth,
      height: L.chipHeight
    };
  });
}

function clientLinks(
  producers: PlacedClient<TopologyProducer>[],
  consumers: PlacedClient<TopologyConsumer>[],
  clusters: PlacedCluster[]
): TopologyLink[] {
  const brokers = new Map(clusters.flatMap((cluster) => cluster.brokers).map((broker) => [broker.group.brokerName, broker]));
  const links: TopologyLink[] = [];
  for (const producer of producers) {
    for (const brokerName of producer.client.brokerNames) {
      const broker = brokers.get(brokerName);
      if (!broker) continue;
      const from = { x: producer.x + producer.width, y: producer.y + producer.height / 2 };
      const to = { x: broker.x, y: broker.y + broker.height / 2 };
      links.push({
        id: `produce:${producer.client.producerGroup}->${brokerName}`,
        kind: 'produce',
        d: curvePath(from, to),
        from,
        to,
        idle: producer.client.connectionCount === 0,
        unavailable: false,
        tps: broker.group.produceTps,
        nodes: [producer.id, broker.id]
      });
    }
  }
  for (const consumer of consumers) {
    for (const brokerName of consumer.client.brokerNames) {
      const broker = brokers.get(brokerName);
      if (!broker) continue;
      const from = { x: broker.x + broker.width, y: broker.y + broker.height / 2 };
      const to = { x: consumer.x, y: consumer.y + consumer.height / 2 };
      links.push({
        id: `consume:${brokerName}->${consumer.client.group}`,
        kind: 'consume',
        d: curvePath(from, to),
        from,
        to,
        idle: consumer.client.connectionCount === 0,
        unavailable: false,
        tps: consumer.client.consumeTps,
        nodes: [broker.id, consumer.id]
      });
    }
  }
  return links;
}

/**
 * Brokers register with every NameServer. One vertical line per chip column joins the tier to each
 * cluster next to it; with three or more clusters only the nearest one is adjacent, which keeps the
 * lines from crossing other cluster panels.
 */
function heartbeatLinks(order: Array<'tier' | number>, clusters: PlacedCluster[], tier: Box, chips: PlacedChip[]): TopologyLink[] {
  if (chips.length === 0) return [];
  const tierIndex = order.indexOf('tier');
  const adjacent = [order[tierIndex - 1], order[tierIndex + 1]].filter((block): block is number => typeof block === 'number');
  const columns = new Map<number, PlacedChip[]>();
  for (const chip of chips) {
    const x = Math.round((chip.x + chip.width / 2) * 100) / 100;
    columns.set(x, [...(columns.get(x) ?? []), chip]);
  }
  const links: TopologyLink[] = [];
  for (const block of adjacent) {
    const cluster = clusters[block];
    const above = cluster.y < tier.y;
    for (const [x, column] of columns) {
      const from = { x, y: above ? cluster.y + cluster.height : cluster.y };
      const to = { x, y: above ? tier.y : tier.y + tier.height };
      links.push({
        id: `heartbeat:${cluster.cluster.clusterName}:${x}`,
        kind: 'heartbeat',
        d: linePath(from, to),
        from,
        to,
        idle: false,
        unavailable: column.every((chip) => chip.nameServer.status === 'unavailable'),
        tps: 0,
        nodes: [cluster.id, ...column.map((chip) => chip.id)]
      });
    }
  }
  return links;
}

/** Clients look routes up from the NameServers: a horizontal line from the tier to a trunk in each client column. */
function routeLinks(
  producers: PlacedClient<TopologyProducer>[],
  consumers: PlacedClient<TopologyConsumer>[],
  producerColumn: { x: number; width: number },
  consumerColumn: { x: number; width: number },
  tier: Box,
  routeY: number
): TopologyLink[] {
  const links: TopologyLink[] = [];
  const add = (side: 'producers' | 'consumers', nodes: Box[], trunkX: number, tierX: number) => {
    if (nodes.length === 0) return;
    const above = nodes.filter((node) => node.y + node.height <= routeY).map((node) => node.y + node.height);
    const below = nodes.filter((node) => node.y >= routeY).map((node) => node.y);
    const top = above.length > 0 ? Math.max(...above) : routeY;
    const bottom = below.length > 0 ? Math.min(...below) : routeY;
    const lookupFrom = { x: tierX, y: routeY };
    const lookupTo = { x: trunkX, y: routeY };
    links.push({
      id: `route:${side}`,
      kind: 'route',
      d: linePath(lookupFrom, lookupTo),
      from: lookupFrom,
      to: lookupTo,
      idle: false,
      unavailable: false,
      tps: 0,
      nodes: []
    });
    if (bottom > top) {
      const trunkFrom = { x: trunkX, y: top };
      const trunkTo = { x: trunkX, y: bottom };
      links.push({
        id: `route-trunk:${side}`,
        kind: 'route',
        d: linePath(trunkFrom, trunkTo),
        from: trunkFrom,
        to: trunkTo,
        idle: false,
        unavailable: false,
        tps: 0,
        nodes: []
      });
    }
  };
  add('producers', producers, producerColumn.x + producerColumn.width / 2, tier.x);
  add('consumers', consumers, consumerColumn.x + consumerColumn.width / 2, tier.x + tier.width);
  return links;
}

function portsOf(links: TopologyLink[]): TopologyPort[] {
  const ports = new Map<string, TopologyPort>();
  const add = (point: Point, kind: TopologyPort['kind'], client: string | null) => {
    const id = `${kind}:${Math.round(point.x)}:${Math.round(point.y)}`;
    if (!ports.has(id)) ports.set(id, { id, kind, client, ...point });
  };
  for (const link of links) {
    if (link.kind === 'produce') {
      add(link.from, 'produce', link.nodes[0]);
      add(link.to, 'produce', null);
    } else if (link.kind === 'consume') {
      add(link.from, 'consume', null);
      add(link.to, 'consume', link.nodes[1]);
    } else if (link.kind === 'route' && !link.id.startsWith('route-trunk')) {
      add(link.to, 'route', null);
    }
  }
  return [...ports.values()];
}

/** Travel time of a particle: higher throughput is faster, between 1.4 and 2.6 seconds. */
export function travelSeconds(tps: number): number {
  const speed = Math.min(Math.max(Math.log10(Math.max(tps, 0) + 1) / 4, 0), 1);
  return Math.round((2.6 - 1.2 * speed) * 100) / 100;
}

export interface TopologyParticle {
  id: string;
  linkId: string;
  kind: 'produce' | 'consume';
  d: string;
  durationS: number;
  /** Negative, so particles start spread along the path instead of all at its start. */
  delayS: number;
}

/** Most particles drawn at once; the quietest links lose theirs first. */
export const MAX_PARTICLES = 40;

function phaseOf(id: string) {
  let hash = 0;
  for (const character of id) hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  return (hash % 1000) / 1000;
}

/** One particle per busy link, two above 500 TPS, none on idle or silent links. */
export function particlesFor(links: readonly TopologyLink[], max = MAX_PARTICLES): TopologyParticle[] {
  const moving = links
    .filter((link): link is TopologyLink & { kind: 'produce' | 'consume' } => (link.kind === 'produce' || link.kind === 'consume') && !link.idle && link.tps > 0)
    .sort((left, right) => right.tps - left.tps || left.id.localeCompare(right.id));
  const particles: TopologyParticle[] = [];
  for (const link of moving) {
    const count = link.tps >= 500 ? 2 : 1;
    const durationS = travelSeconds(link.tps);
    const phase = phaseOf(link.id);
    for (let index = 0; index < count && particles.length < max; index += 1) {
      particles.push({
        id: `${link.id}#${index}`,
        linkId: link.id,
        kind: link.kind,
        d: link.d,
        durationS,
        delayS: -Math.round(durationS * ((phase + index / count) % 1) * 100) / 100
      });
    }
    if (particles.length >= max) break;
  }
  return particles;
}
