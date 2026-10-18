import { describe, expect, it } from 'vitest';
import type { ConsumerGroupListItem } from '../../../types/consumer';
import { fixtureConfig, fixtureConsumers, fixtureControllerAddresses, fixtureSnapshot, withReplication } from '../topology-fixtures';
import { ALL_CLUSTERS, selectTopologyView } from '../topology-model';
import {
  buildSphereModel,
  capPoint,
  IN_SYNC_GAP_BYTES,
  linkedNodes,
  nodeDescription,
  nodeLabelMetric,
  nodeMetric,
  nodeName,
  nodeTone,
  replicaBehind,
  spreadAround,
  type SlaveNode,
  type SphereNode,
  type Vec3
} from './sphere-model';

function viewOf(consumers: ConsumerGroupListItem[] = fixtureConsumers) {
  return selectTopologyView(fixtureSnapshot({ consumers }), { scope: ALL_CLUSTERS, producerLimit: 50, consumerLimit: 50 });
}

const degreesBetween = (a: Vec3, b: Vec3) => {
  const dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
  return (Math.acos(dot / (Math.hypot(...a) * Math.hypot(...b))) * 180) / Math.PI;
};

function node(model: ReturnType<typeof buildSphereModel>, id: string): SphereNode {
  const found = model.byId.get(id);
  if (!found) throw new Error(`no node ${id}`);
  return found;
}

/** Smallest angle between neighbours around a ring, the seam at ±180° included. */
function smallestGap(lons: number[]) {
  const sorted = lons.map((lon) => ((lon % 360) + 360) % 360).sort((left, right) => left - right);
  return Math.min(...sorted.map((lon, index) => (index === 0 ? lon + 360 - sorted[sorted.length - 1] : lon - sorted[index - 1])));
}

describe('buildSphereModel', () => {
  it('puts masters on the equator, slaves beside them and each cluster on an arc of its own', () => {
    const model = buildSphereModel(viewOf());
    const masters = model.nodes.filter((item) => item.kind === 'master');

    expect(masters.map((item) => item.id)).toEqual(['broker:broker-c', 'broker:broker-d', 'broker:broker-a', 'broker:broker-b']);
    expect(masters.every((item) => item.position[1] === 0)).toBe(true);
    expect(degreesBetween(node(model, 'slave:broker-a:10.0.4.121:10911').position, node(model, 'broker:broker-a').position)).toBeCloseTo(9, 0);
    const [east, defaultCluster] = model.clusters;
    expect(east.to).toBeLessThan(defaultCluster.from);
    expect([east.instances, defaultCluster.instances]).toEqual([3, 4]);
    // DefaultCluster carries the most traffic, so it starts at the front.
    expect(model.frontLon).toBe(defaultCluster.centre);
  });

  it('spreads client groups over northern and southern rings without crowding them', () => {
    const model = buildSphereModel(viewOf());
    const producers = model.nodes.filter((item) => item.kind === 'producer');
    const consumers = model.nodes.filter((item) => item.kind === 'consumer');

    expect(producers).toHaveLength(6);
    expect(consumers).toHaveLength(6);
    expect(producers.every((item) => item.position[1] > 0)).toBe(true);
    expect(consumers.every((item) => item.position[1] < 0)).toBe(true);
    for (const group of [producers, consumers]) {
      const rings = new Map<number, number[]>();
      for (const item of group) {
        if (item.kind !== 'producer' && item.kind !== 'consumer') continue;
        rings.set(item.lat, [...(rings.get(item.lat) ?? []), item.lon]);
      }
      for (const lons of rings.values()) if (lons.length > 1) expect(smallestGap(lons)).toBeGreaterThan(80);
    }
  });

  it('links client groups to their brokers, masters to their slaves and every master to every NameServer', () => {
    const model = buildSphereModel(viewOf());
    const ids = (kind: string) => model.links.filter((link) => link.kind === kind).map((link) => link.id);

    expect(ids('produce')).toEqual(expect.arrayContaining([
      'produce:producer:order-service-producer>broker:broker-a',
      'produce:producer:order-service-producer>broker:broker-b'
    ]));
    expect(ids('consume')).toContain('consume:broker:broker-c>consumer:analytics-stream');
    expect(ids('replication')).toEqual([
      'replication:broker:broker-d>slave:broker-d:10.0.5.123:10911',
      'replication:broker:broker-a>slave:broker-a:10.0.4.121:10911',
      'replication:broker:broker-b>slave:broker-b:10.0.4.123:10911'
    ]);
    const heartbeats = model.links.filter((link) => link.kind === 'heartbeat');
    expect(heartbeats).toHaveLength(12);
    expect(heartbeats.filter((link) => link.unavailable).map((link) => link.to)).toEqual(Array(4).fill('nameserver:10.0.4.12:9876'));
    expect(model.links.filter((link) => link.idle).map((link) => link.from)).toEqual(['producer:batch-export-producer', 'broker:broker-d']);
  });

  it('spreads the route lookups of connected client groups over the reachable NameServers', () => {
    const model = buildSphereModel(viewOf());
    const routes = model.links.filter((link) => link.kind === 'route');
    const perServer = (address: string) => routes.filter((link) => link.from === `nameserver:${address}`).length;

    expect(routes).toHaveLength(10);
    expect([perServer('127.0.0.1:9876'), perServer('10.0.4.11:9876'), perServer('10.0.4.12:9876')]).toEqual([5, 5, 0]);
    expect(routes.map((link) => link.to)).not.toEqual(expect.arrayContaining(['producer:batch-export-producer']));
    expect(routes.map((link) => link.to)).not.toEqual(expect.arrayContaining(['consumer:audit-archiver']));
    expect(linkedNodes(model, 'consumer:order-consumer').map(({ node: other, kind }) => `${kind} ${other.id}`)).toEqual([
      'consume broker:broker-a',
      'consume broker:broker-b',
      'route nameserver:127.0.0.1:9876'
    ]);
  });

  it('draws each address of a NameServer entry that lists several as a server of its own', () => {
    const joined = '10.0.4.11:9876;10.0.4.12:9876';
    const snapshot = fixtureSnapshot({
      config: { ...fixtureConfig, currentNamesrv: joined, namesrvAddrList: [joined] },
      nameservers: { endpoints: [{ address: joined, status: 'available', checkedAt: 1_000 }] }
    });
    const model = buildSphereModel(selectTopologyView(snapshot, { scope: ALL_CLUSTERS, producerLimit: 6, consumerLimit: 6 }));
    const servers = model.nodes.filter((item) => item.kind === 'nameserver');

    expect(servers.map((item) => item.id)).toEqual(['nameserver:10.0.4.11:9876', 'nameserver:10.0.4.12:9876']);
    expect(servers.map((item) => nodeMetric(item))).toEqual(['current', 'current']);
    expect(model.links.filter((link) => link.kind === 'heartbeat')).toHaveLength(8);
  });

  it('keeps its structure key when only readings change', () => {
    const base = buildSphereModel(viewOf());
    const busier = buildSphereModel(viewOf(fixtureConsumers.map((item) => ({ ...item, consumeTps: item.consumeTps + 5, diffTotal: item.diffTotal + 1 }))));
    const lagging = buildSphereModel(viewOf(fixtureConsumers.map((item) => (item.rawGroupName === 'inventory-worker' ? { ...item, diffTotal: 5_000 } : item))));

    expect(busier.structure).toBe(base.structure);
    expect(lagging.structure).not.toBe(base.structure);
  });

  it('describes nodes the way the flat map does', () => {
    const model = buildSphereModel(viewOf());

    expect(nodeMetric(node(model, 'broker:broker-a'))).toBe('in 1,204.36 · out 1,198.02');
    expect(nodeMetric(node(model, 'slave:broker-a:10.0.4.121:10911'))).toBe('slave · 10.0.4.121:10911');
    expect(nodeMetric(node(model, 'consumer:audit-archiver'))).toBe('no clients · lag 0');
    expect(nodeMetric(node(model, 'consumer:order-consumer'))).toBe('1,012 TPS · lag 8,420');
    expect(nodeMetric(node(model, 'nameserver:127.0.0.1:9876'))).toBe('current');
    expect(nodeMetric(node(model, 'nameserver:10.0.4.12:9876'))).toBe('unavailable');
  });

  it('lists the nodes a node is linked to, with the kind of each link', () => {
    const model = buildSphereModel(viewOf());
    const linked = linkedNodes(model, 'broker:broker-c').map(({ node: other, kind }) => `${kind} ${other.id}`);

    expect(linked).toEqual([
      'produce producer:analytics-ingest',
      'produce producer:notification-producer',
      'consume consumer:analytics-stream',
      'consume consumer:notification-push',
      'heartbeat nameserver:127.0.0.1:9876',
      'heartbeat nameserver:10.0.4.11:9876',
      'heartbeat nameserver:10.0.4.12:9876'
    ]);
  });
});

describe('Controllers and replication', () => {
  const replicationModel = (leader = 0) =>
    buildSphereModel(selectTopologyView(withReplication(fixtureSnapshot(), leader), { scope: ALL_CLUSTERS, producerLimit: 50, consumerLimit: 50 }));
  const [leaderAddress, ...followerAddresses] = fixtureControllerAddresses;
  const leaderId = `controller:${leaderAddress}`;

  it('puts the Controllers on a sphere of their own, the leader replicating to the followers', () => {
    const model = replicationModel();
    const controllers = model.nodes.filter((item) => item.kind === 'controller');

    expect(controllers.map((item) => item.id)).toEqual(fixtureControllerAddresses.map((address) => `controller:${address}`));
    expect(controllers.every((item) => item.sphere === 'controllers')).toBe(true);
    expect(model.links.filter((link) => link.kind === 'raft').map((link) => `${link.from}>${link.to}`)).toEqual(
      followerAddresses.map((address) => `${leaderId}>controller:${address}`)
    );
    expect(model.clusters.find((cluster) => cluster.clusterName === 'cluster-east')?.mode).toBe('controller');
    expect(model.clusters.find((cluster) => cluster.clusterName === 'DefaultCluster')?.mode).toBe('master-slave');
    expect([nodeTone(node(model, leaderId)), nodeMetric(node(model, leaderId))]).toEqual(['current', 'leader']);
  });

  it('links every broker of a Controller-mode group to the leading Controller', () => {
    const model = replicationModel();
    const elections = model.links.filter((link) => link.kind === 'election');

    expect(elections.map((link) => `${link.from}>${link.to}`)).toEqual([
      `broker:broker-d>${leaderId}`,
      `slave:broker-d:10.0.5.123:10911>${leaderId}`
    ]);
    expect(linkedNodes(model, leaderId).map(({ node: other, kind }) => `${kind} ${other.id}`)).toEqual([
      ...followerAddresses.map((address) => `raft controller:${address}`),
      'election broker:broker-d',
      'election slave:broker-d:10.0.5.123:10911'
    ]);
  });

  it('draws no Raft or election links while no leader is known', () => {
    const model = replicationModel(-1);
    const follower = node(model, `controller:${followerAddresses[0]}`);

    expect(model.links.filter((link) => link.kind === 'raft' || link.kind === 'election')).toEqual([]);
    expect([nodeTone(follower), nodeMetric(follower)]).toEqual(['warning', 'no leader known']);
  });

  it('flags replicas outside the sync state set and unconnected slaves, not slaves a little behind', () => {
    const model = replicationModel();
    const replica = node(model, 'slave:broker-d:10.0.5.123:10911') as SlaveNode;
    const behind = node(model, 'slave:broker-a:10.0.4.121:10911') as SlaveNode;
    const disconnected = node(model, 'slave:broker-b:10.0.4.123:10911') as SlaveNode;
    const replicationTo = (id: string) => model.links.find((link) => link.kind === 'replication' && link.to === id);

    expect([replica, behind, disconnected].map(nodeTone)).toEqual(['warning', 'normal', 'warning']);
    expect([nodeName(replica), nodeName(disconnected)]).toEqual(['broker-d replica 1', 'broker-b slave 1']);
    expect(nodeDescription(replica)).toBe('broker-d replica 1, replica · 10.0.5.123:10911 · out of sync');
    expect(nodeDescription(node(model, 'controller:10.0.9.1:9878'))).toBe('Controller 10.0.9.1:9878, leader');
    // On the sphere the state takes a line of its own, so the label is no wider than the address.
    expect([replica, behind, disconnected].map(nodeLabelMetric)).toEqual([
      'replica · 10.0.5.123:10911\nout of sync',
      'slave · 10.0.4.121:10911',
      'slave · 10.0.4.123:10911\nout of sync'
    ]);
    expect([replica, behind, disconnected].map((item) => replicationTo(item.id)?.outOfSync)).toEqual([true, false, true]);
    // A slave as far behind as RocketMQ's own limit is out of sync too.
    expect(replicaBehind({ ...behind, replica: { ...behind.replica!, lagBytes: IN_SYNC_GAP_BYTES } })).toBe(true);
    // Without a replication reading nothing is flagged, and a slave goes by its address.
    const unread = node(buildSphereModel(viewOf()), 'slave:broker-b:10.0.4.123:10911');
    expect([nodeTone(unread), nodeName(unread)]).toEqual(['normal', 'broker-b slave 10.0.4.123:10911']);
  });
});

describe('capPoint', () => {
  it('puts a single node at the front of its sphere', () => {
    expect(capPoint(0, 1, 2)).toEqual([0, 0, 2]);
  });

  it('stacks two nodes vertically on the front of their sphere', () => {
    const top = capPoint(0, 2, 1);
    const bottom = capPoint(1, 2, 1);

    expect(top[0]).toBeCloseTo(0);
    expect(top[1]).toBeGreaterThan(0.5);
    expect(bottom[1]).toBeLessThan(-0.5);
    expect(Math.min(top[2], bottom[2])).toBeGreaterThan(0.7);
  });
});

describe('spreadAround', () => {
  it('pushes neighbours apart across the seam at ±180 degrees', () => {
    expect(smallestGap(spreadAround([-175, 0, 170], 60))).toBeGreaterThanOrEqual(59.9);
  });

  it('leaves well spread neighbours where they are', () => {
    expect(spreadAround([-120, 0, 120], 60)).toEqual([-120, 0, 120]);
  });
});
