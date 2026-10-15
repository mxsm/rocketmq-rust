import { describe, expect, it } from 'vitest';
import { broker, consumerGroup, fixtureBrokers, fixtureSnapshot } from './topology-fixtures';
import {
  layoutTopology,
  MAX_PARTICLES,
  particlesFor,
  TOPOLOGY_LAYOUT,
  travelSeconds,
  type Box,
  type TopologyLayout,
  type TopologyLink
} from './topology-layout';
import { ALL_CLUSTERS, selectTopologyView } from './topology-model';

function layoutFor(snapshot = fixtureSnapshot(), width = 1112, limit = 6) {
  return layoutTopology(selectTopologyView(snapshot, { scope: ALL_CLUSTERS, producerLimit: limit, consumerLimit: limit }), width);
}

const threeClusters = () => fixtureSnapshot({
  brokers: [
    ...fixtureBrokers,
    broker('cluster-west', 'broker-e', 0, '10.0.6.21:10911', 90, 80),
    broker('cluster-west', 'broker-e', 1, '10.0.6.121:10911')
  ]
});

const oneCluster = () => fixtureSnapshot({ brokers: fixtureBrokers.filter((item) => item.clusterName === 'DefaultCluster') });

function overlaps(left: Box, right: Box) {
  return left.x < right.x + right.width && right.x < left.x + left.width && left.y < right.y + right.height && right.y < left.y + left.height;
}

function boxes(layout: TopologyLayout): Array<Box & { id: string }> {
  return [
    { ...layout.tier, id: 'tier' },
    ...layout.clusters,
    ...layout.producers,
    ...layout.consumers
  ];
}

function expectNoOverlap(layout: TopologyLayout) {
  const all = boxes(layout);
  for (const [index, box] of all.entries()) {
    for (const other of all.slice(index + 1)) {
      expect(overlaps(box, other), `${box.id} overlaps ${other.id}`).toBe(false);
    }
  }
  for (const cluster of layout.clusters) {
    for (const placed of cluster.brokers) {
      expect(placed.y).toBeGreaterThanOrEqual(cluster.y);
      expect(placed.y + placed.height).toBeLessThanOrEqual(cluster.y + cluster.height);
    }
  }
  expect(Math.max(...all.map((box) => box.y + box.height))).toBeLessThanOrEqual(layout.height);
}

function expectLinksOnEdges(layout: TopologyLayout) {
  const nodes = new Map<string, Box>([
    ...layout.producers.map((node) => [node.id, node] as const),
    ...layout.consumers.map((node) => [node.id, node] as const),
    ...layout.clusters.flatMap((cluster) => cluster.brokers).map((node) => [node.id, node] as const)
  ]);
  const clientLinks = layout.links.filter((link) => link.kind === 'produce' || link.kind === 'consume');
  expect(clientLinks.length).toBeGreaterThan(0);
  for (const link of clientLinks) {
    const [start, end] = link.nodes.map((id) => nodes.get(id)!);
    expect(link.from).toEqual({ x: start.x + start.width, y: start.y + start.height / 2 });
    expect(link.to).toEqual({ x: end.x, y: end.y + end.height / 2 });
    expect(link.d.startsWith(`M ${link.from.x} ${link.from.y} C`)).toBe(true);
    expect(link.d.endsWith(`${link.to.x} ${link.to.y}`)).toBe(true);
  }
}

describe('layoutTopology', () => {
  it('puts two clusters above and below the NameServer tier as on the artboard', () => {
    const layout = layoutFor();
    const [east, defaultCluster] = layout.clusters;

    expect(layout.clusters.map((cluster) => cluster.cluster.clusterName)).toEqual(['cluster-east', 'DefaultCluster']);
    expect(east.y).toBe(0);
    expect(east.y + east.height).toBeLessThan(layout.tier.y);
    expect(layout.tier.y + layout.tier.height).toBeLessThan(defaultCluster.y);
    expect(layout.tier.chips).toHaveLength(3);
    expect(layout.width).toBe(1112);
    expect(layout.centreColumn).toEqual({ x: 328, width: 456 });
    expectNoOverlap(layout);
    expectLinksOnEdges(layout);
  });

  it('draws each group beside the cluster it uses most', () => {
    const layout = layoutFor();
    const band = (id: string) => [...layout.producers, ...layout.consumers].find((node) => node.id === id)?.band;

    expect(band('producer:analytics-ingest')).toBe(0);
    expect(band('producer:order-service-producer')).toBe(1);
    expect(band('consumer:audit-archiver')).toBe(0);
    expect(band('consumer:order-consumer')).toBe(1);
  });

  it('puts the tier on top of a single cluster', () => {
    const layout = layoutFor(oneCluster());

    expect(layout.tier.y).toBe(0);
    expect(layout.clusters[0].y).toBeGreaterThan(layout.tier.y + layout.tier.height);
    expectNoOverlap(layout);
    expectLinksOnEdges(layout);
  });

  it('stacks three clusters below the tier and joins the tier to the nearest one', () => {
    const layout = layoutFor(threeClusters());
    const heartbeats = layout.links.filter((link) => link.kind === 'heartbeat');

    expect(layout.tier.y).toBe(0);
    expect(layout.clusters.map((cluster) => cluster.y)).toEqual([...layout.clusters.map((cluster) => cluster.y)].sort((a, b) => a - b));
    expect(heartbeats).toHaveLength(3);
    expect(new Set(heartbeats.map((link) => link.nodes[0]))).toEqual(new Set([layout.clusters[0].id]));
    expectNoOverlap(layout);
    expectLinksOnEdges(layout);
  });

  it('marks heartbeat lines to an unavailable NameServer', () => {
    const layout = layoutFor();
    const unavailable = layout.links.filter((link) => link.kind === 'heartbeat' && link.unavailable);

    expect(unavailable).toHaveLength(2);
    expect(unavailable.every((link) => link.nodes.includes('nameserver:10.0.4.12:9876'))).toBe(true);
  });

  it('keeps the route lookup line clear of client nodes, with trunks between them', () => {
    for (const snapshot of [fixtureSnapshot(), oneCluster(), threeClusters()]) {
      const layout = layoutFor(snapshot, 1112, 12);
      for (const node of [...layout.producers, ...layout.consumers]) {
        const clear = node.y + node.height + TOPOLOGY_LAYOUT.routeClearance <= layout.routeY
          || node.y - TOPOLOGY_LAYOUT.routeClearance >= layout.routeY;
        expect(clear, node.id).toBe(true);
      }
      const trunks = layout.links.filter((link) => link.id.startsWith('route-trunk'));
      for (const trunk of trunks) {
        for (const node of [...layout.producers, ...layout.consumers]) {
          const crosses = node.x < trunk.from.x && trunk.from.x < node.x + node.width
            && node.y < trunk.to.y && trunk.from.y < node.y + node.height;
          expect(crosses, `${trunk.id} crosses ${node.id}`).toBe(false);
        }
      }
    }
  });

  it('dashes the links of groups without clients', () => {
    const layout = layoutFor();
    const idle = layout.links.filter((link) => link.idle).map((link) => link.id);

    expect(idle).toEqual(['produce:batch-export-producer->broker-d', 'consume:broker-d->audit-archiver']);
  });

  it('fits the gutters to the container down to the minimum width, then keeps that width', () => {
    expect(layoutFor(fixtureSnapshot(), 1400).centreColumn.x).toBe(472);
    expect(layoutFor(fixtureSnapshot(), 1080).centreColumn.x).toBe(312);
    expect(layoutFor(fixtureSnapshot(), 700).width).toBe(TOPOLOGY_LAYOUT.minWidth);
  });

  it('grows downwards for long group lists without overlapping', () => {
    const consumers = Array.from({ length: 30 }, (_, index) =>
      consumerGroup(`group-${index}`, 30 - index, 10, 0, [index % 2 === 0 ? 'broker-a' : 'broker-c'])
    );
    const layout = layoutFor(fixtureSnapshot({ consumers }), 1112, 50);

    expect(layout.consumers).toHaveLength(30);
    expectNoOverlap(layout);
    expectLinksOnEdges(layout);
  });
});

describe('particlesFor', () => {
  const link = (id: string, tps: number, idle = false): TopologyLink => ({
    id,
    kind: 'produce',
    d: 'M 0 0 L 1 1',
    from: { x: 0, y: 0 },
    to: { x: 1, y: 1 },
    idle,
    unavailable: false,
    tps,
    nodes: []
  });

  it('sends one particle on a quiet link, two on a busy one, none without clients or traffic', () => {
    const particles = particlesFor([link('busy', 1200), link('quiet', 40), link('idle', 900, true), link('silent', 0)]);

    expect(particles.map((particle) => particle.linkId)).toEqual(['busy', 'busy', 'quiet']);
    expect(particles.every((particle) => particle.delayS <= 0 && particle.delayS > -particle.durationS)).toBe(true);
  });

  it('drops the particles of the quietest links above the cap', () => {
    const links = Array.from({ length: 30 }, (_, index) => link(`link-${index}`, 1000 - index * 10));
    const particles = particlesFor(links);

    expect(particles).toHaveLength(MAX_PARTICLES);
    expect(particles.some((particle) => particle.linkId === 'link-29')).toBe(false);
    expect(particles.some((particle) => particle.linkId === 'link-0')).toBe(true);
  });

  it('moves faster with more throughput, within 1.4 to 2.6 seconds', () => {
    expect(travelSeconds(0)).toBe(2.6);
    expect(travelSeconds(100)).toBeLessThan(travelSeconds(10));
    expect(travelSeconds(1_000_000)).toBe(1.4);
  });
});
