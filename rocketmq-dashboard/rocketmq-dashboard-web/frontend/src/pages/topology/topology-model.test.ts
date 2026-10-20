import { describe, expect, it } from 'vitest';
import type { TopologySnapshot } from '../../types/topology';
import {
  broker,
  consumerGroup,
  fixtureAvailability,
  fixtureConfig,
  fixtureConsumers,
  fixtureProducers,
  fixtureSnapshot,
  fixtureTopics,
  withReplication
} from './topology-fixtures';
import {
  aggregateProducers,
  ALL_CLUSTERS,
  brokerTone,
  buildNameServers,
  buildSignals,
  carryForwardFailedLayers,
  clusterIsHealthy,
  consumerTone,
  describeTopology,
  diffSnapshots,
  groupBrokers,
  mapConsumers,
  MAX_GROUPS_PER_SIDE,
  newWarnings,
  selectTopologyView
} from './topology-model';

const subjects = (snapshot: TopologySnapshot) => snapshot.consumers.map((consumer) => consumer.group);

describe('groupBrokers', () => {
  it('groups instances by cluster and broker name with the master from broker id 0', () => {
    const clusters = groupBrokers([
      broker('east', 'broker-c', 1, '10.0.5.121:10911', 0, 30),
      broker('east', 'broker-c', 0, '10.0.5.21:10911', 420.3, 412.8),
      broker('east', 'broker-c', 2, '10.0.5.111:10911', 0, 1.15)
    ]);

    expect(clusters).toEqual([
      {
        clusterName: 'east',
        produceTps: 420.3,
        consumeTps: 443.95,
        brokers: [
          {
            brokerName: 'broker-c',
            masterAddress: '10.0.5.21:10911',
            slaveAddresses: ['10.0.5.111:10911', '10.0.5.121:10911'],
            version: 'V5_3_1',
            produceTps: 420.3,
            consumeTps: 443.95
          }
        ]
      }
    ]);
  });

  it('orders clusters and broker names naturally and sums cluster traffic', () => {
    const clusters = groupBrokers([
      broker('west', 'broker-10', 0, 'a:1', 1, 2),
      broker('west', 'broker-2', 0, 'b:1', 3, 4),
      broker('east', 'broker-1', 0, 'c:1', 5, 6)
    ]);

    expect(clusters.map((cluster) => cluster.clusterName)).toEqual(['east', 'west']);
    expect(clusters[1].brokers.map((group) => group.brokerName)).toEqual(['broker-2', 'broker-10']);
    expect(clusters[1]).toMatchObject({ produceTps: 4, consumeTps: 6 });
  });

  it('keeps a broker name whose master is missing', () => {
    const [cluster] = groupBrokers([broker('east', 'broker-c', 1, '10.0.5.121:10911')]);

    expect(cluster.brokers[0]).toMatchObject({ masterAddress: null, slaveAddresses: ['10.0.5.121:10911'] });
    expect(brokerTone(cluster.brokers[0])).toBe('danger');
  });
});

describe('buildNameServers', () => {
  it('lists configured NameServers in order with their latest check', () => {
    expect(buildNameServers(fixtureConfig, fixtureAvailability)).toEqual([
      { address: '127.0.0.1:9876', current: true, status: 'available', checkedAt: 1_000 },
      { address: '10.0.4.11:9876', current: false, status: 'available', checkedAt: 1_000 },
      { address: '10.0.4.12:9876', current: false, status: 'unavailable', checkedAt: 1_000 }
    ]);
  });

  it('marks the status unknown when the availability check is missing', () => {
    expect(buildNameServers(fixtureConfig, null).map((nameServer) => nameServer.status)).toEqual(['unknown', 'unknown', 'unknown']);
  });

  it('still lists checked NameServers when the configuration is missing', () => {
    expect(buildNameServers(null, fixtureAvailability).map((nameServer) => nameServer.address)).toHaveLength(3);
    expect(buildNameServers(null, fixtureAvailability).some((nameServer) => nameServer.current)).toBe(false);
  });
});

describe('aggregateProducers', () => {
  it('folds topic rows into groups and links them to the brokers of their topics', () => {
    const producers = aggregateProducers(fixtureProducers, fixtureTopics);
    const orderService = producers.find((producer) => producer.producerGroup === 'order-service-producer');

    expect(orderService).toEqual({
      producerGroup: 'order-service-producer',
      connectionCount: 6,
      topics: ['order-events', 'payment-events'],
      brokerNames: ['broker-a', 'broker-b']
    });
  });

  it('ranks groups by connections and then by name', () => {
    const producers = aggregateProducers(fixtureProducers, fixtureTopics);

    expect(producers.map((producer) => producer.producerGroup)).toEqual([
      'analytics-ingest',
      'order-service-producer',
      'payment-gateway-producer',
      'inventory-producer',
      'notification-producer',
      'batch-export-producer'
    ]);
  });

  it('leaves groups unlinked when their topics are unknown', () => {
    const [producer] = aggregateProducers([{ topic: 'missing', producerGroup: 'lonely', connectionCount: 1 }], fixtureTopics);

    expect(producer.brokerNames).toEqual([]);
  });

  it('links groups to the brokers their rows report, with or without the topic list', () => {
    const rows = [
      { topic: '', producerGroup: 'registered', connectionCount: 2, brokerNames: ['broker-d'] },
      { topic: 'payment-events', producerGroup: 'registered', connectionCount: 2 }
    ];

    expect(aggregateProducers(rows, fixtureTopics)[0]).toMatchObject({ connectionCount: 2, brokerNames: ['broker-a', 'broker-d'] });
    expect(aggregateProducers(rows, null)[0].brokerNames).toEqual(['broker-d']);
  });
});

describe('mapConsumers', () => {
  it('keeps application groups ranked by connections and drops system groups', () => {
    const consumers = mapConsumers(fixtureConsumers);

    expect(consumers.map((consumer) => consumer.group)).toEqual([
      'analytics-stream',
      'order-consumer',
      'payment-consumer',
      'inventory-worker',
      'notification-push',
      'audit-archiver'
    ]);
  });

  it('uses the raw group name as identity and the display name as label', () => {
    const [consumer] = mapConsumers([{ ...consumerGroup('%NS%orders', 1, 1, 0, []), displayGroupName: 'orders' }]);

    expect(consumer).toMatchObject({ group: '%NS%orders', displayName: 'orders' });
  });
});

describe('buildTopologySnapshot', () => {
  it('counts groups and the backlog of every application group', () => {
    expect(fixtureSnapshot().totals).toEqual({ producerGroups: 6, consumerGroups: 6, messageBacklog: 12_840 });
  });

  it('leaves the layer of a failed query empty', () => {
    const snapshot = fixtureSnapshot({}, ['brokers']);

    expect(snapshot.clusters).toEqual([]);
    expect(snapshot.sources.find((source) => source.source === 'brokers')?.status).toBe('failed');
  });
});

describe('carryForwardFailedLayers', () => {
  const previous = fixtureSnapshot();

  it('keeps the previous data of a failed layer and the fresh data of the others', () => {
    const next = fixtureSnapshot(
      { consumers: [consumerGroup('order-consumer', 20, 1, 0, ['broker-a'])] },
      ['brokers'],
      20_000
    );
    const merged = carryForwardFailedLayers(next, previous);

    expect(merged.clusters).toBe(previous.clusters);
    expect(subjects(merged)).toEqual(['order-consumer']);
    expect(merged.generatedAt).toBe(20_000);
    expect(merged.sources).toBe(next.sources);
  });

  it('keeps the previous consumers and their totals when the consumer query fails', () => {
    const merged = carryForwardFailedLayers(fixtureSnapshot({}, ['consumers']), previous);

    expect(merged.consumers).toBe(previous.consumers);
    expect(merged.totals.messageBacklog).toBe(12_840);
    expect(merged.totals.consumerGroups).toBe(6);
  });

  it('keeps the broker links of producers when only the topic query fails', () => {
    const merged = carryForwardFailedLayers(fixtureSnapshot({}, ['topics']), previous);

    expect(merged.producers.find((producer) => producer.producerGroup === 'analytics-ingest')?.brokerNames).toEqual([
      'broker-c',
      'broker-d'
    ]);
  });

  it('adds the brokers the fresh rows report to the links kept while the topic query fails', () => {
    const producers = fixtureProducers.map((row) =>
      row.producerGroup === 'analytics-ingest' ? { ...row, brokerNames: ['broker-e'] } : row
    );
    const merged = carryForwardFailedLayers(fixtureSnapshot({ producers }, ['topics']), previous);

    expect(merged.producers.find((producer) => producer.producerGroup === 'analytics-ingest')?.brokerNames).toEqual([
      'broker-c',
      'broker-d',
      'broker-e'
    ]);
  });

  it('keeps the previous availability when only the check fails', () => {
    const merged = carryForwardFailedLayers(fixtureSnapshot({}, ['nameservers']), previous);

    expect(merged.nameServers.map((nameServer) => nameServer.status)).toEqual(['available', 'available', 'unavailable']);
  });

  it('applies a fresh check to the previous list when only the configuration fails', () => {
    const next = fixtureSnapshot(
      { nameservers: { endpoints: [{ address: '10.0.4.12:9876', status: 'available', checkedAt: 2_000 }] } },
      ['config']
    );
    const merged = carryForwardFailedLayers(next, previous);

    expect(merged.nameServers.map((nameServer) => [nameServer.address, nameServer.current, nameServer.status])).toEqual([
      ['127.0.0.1:9876', true, 'available'],
      ['10.0.4.11:9876', false, 'available'],
      ['10.0.4.12:9876', false, 'available']
    ]);
  });

  it('returns the new snapshot untouched without a previous one', () => {
    const next = fixtureSnapshot({}, ['brokers']);

    expect(carryForwardFailedLayers(next, null)).toBe(next);
  });

  it('keeps the last replication readings and Controllers on the fresh broker list when only replication fails', () => {
    const before = withReplication(previous);
    const fresh = fixtureSnapshot({}, [], 20_000);
    // The aggregated endpoint lists the replicas of a group even when it could not read them.
    const next: TopologySnapshot = {
      ...fresh,
      sources: [...fresh.sources, { source: 'replication', status: 'failed', durationMs: 3_000, message: 'The query failed.' }],
      clusters: fresh.clusters.map((cluster) => ({
        ...cluster,
        brokers: cluster.brokers.map((group) =>
          group.brokerName === 'broker-d' ? { ...group, replicas: [{ brokerId: 1, address: '10.0.5.123:10911' }] } : group
        )
      }))
    };

    const merged = carryForwardFailedLayers(next, before);
    const brokerD = merged.clusters.flatMap((cluster) => cluster.brokers).find((group) => group.brokerName === 'broker-d');

    expect(merged.controllers).toBe(before.controllers);
    expect(brokerD).toMatchObject({
      election: 'controller',
      controllerState: { masterEpoch: 3, syncStateSetEpoch: 5 },
      replicas: [{ brokerId: 1, address: '10.0.5.123:10911', lagBytes: 0, transferBytesPerSecond: 2_048 }]
    });
    expect(merged.clusters).not.toBe(before.clusters);
  });
});

describe('derived states', () => {
  const snapshot = fixtureSnapshot();
  const group = (name: string) => snapshot.clusters.flatMap((cluster) => cluster.brokers).find((item) => item.brokerName === name)!;
  const consumer = (name: string) => snapshot.consumers.find((item) => item.group === name)!;

  it('keeps a single-replica broker name healthy and degrades a cluster only for a missing master', () => {
    expect(brokerTone(group('broker-a'))).toBe('normal');
    expect(brokerTone(group('broker-c'))).toBe('normal');
    expect(snapshot.clusters.map((cluster) => [cluster.clusterName, clusterIsHealthy(cluster)])).toEqual([
      ['cluster-east', true],
      ['DefaultCluster', true]
    ]);

    const withoutMaster = fixtureSnapshot({ brokers: [broker('cluster-east', 'broker-c', 1, '10.0.5.121:10911')] });
    expect(brokerTone(withoutMaster.clusters[0].brokers[0])).toBe('danger');
    expect(clusterIsHealthy(withoutMaster.clusters[0])).toBe(false);
  });

  it('flags a broker name that loses its last slave or its master since the previous snapshot', () => {
    const next = fixtureSnapshot({
      brokers: [
        broker('DefaultCluster', 'broker-a', 0, '10.0.4.21:10911'),
        broker('DefaultCluster', 'broker-b', 1, '10.0.4.123:10911'),
        broker('cluster-east', 'broker-c', 0, '10.0.5.21:10911')
      ]
    });

    expect([...newWarnings(snapshot, next).brokers].sort()).toEqual(['broker-a', 'broker-b']);
    expect(newWarnings(next, next).brokers.size).toBe(0);
  });

  it('warns above the backlog threshold and marks groups without clients idle', () => {
    expect(consumerTone(consumer('order-consumer'))).toBe('warning');
    expect(consumerTone(consumer('inventory-worker'))).toBe('normal');
    expect(consumerTone(consumer('audit-archiver'))).toBe('idle');
    expect(consumerTone({ ...consumer('audit-archiver'), diffTotal: 96_340 })).toBe('warning');
  });
});

describe('diffSnapshots', () => {
  const previous = fixtureSnapshot();

  it('reports a lost slave, an unavailable NameServer, a backlog change and new connections, most severe first', () => {
    const next = fixtureSnapshot({
      brokers: [
        broker('DefaultCluster', 'broker-a', 0, '10.0.4.21:10911'),
        broker('DefaultCluster', 'broker-b', 0, '10.0.4.23:10911'),
        broker('DefaultCluster', 'broker-b', 1, '10.0.4.123:10911'),
        broker('cluster-east', 'broker-c', 0, '10.0.5.21:10911'),
        broker('cluster-east', 'broker-d', 0, '10.0.5.23:10911'),
        broker('cluster-east', 'broker-d', 1, '10.0.5.123:10911')
      ],
      nameservers: {
        endpoints: fixtureAvailability.endpoints.map((endpoint) =>
          endpoint.address === '10.0.4.11:9876' ? { ...endpoint, status: 'unavailable' as const } : endpoint
        )
      },
      consumers: fixtureConsumers.map((item) => {
        if (item.rawGroupName === 'order-consumer') return { ...item, diffTotal: 9_100 };
        if (item.rawGroupName === 'analytics-stream') return { ...item, connectionCount: 18 };
        return item;
      })
    });

    expect(diffSnapshots(previous, next)).toEqual([
      { subject: '10.0.4.11:9876', detail: 'became unavailable', tone: 'danger' },
      { subject: 'broker-a', detail: 'slave no longer registered', tone: 'warning' },
      { subject: 'order-consumer', detail: 'lag 8,420 → 9,100', tone: 'warning' },
      { subject: 'analytics-stream', detail: 'connections 16 → 18', tone: 'success' }
    ]);
  });

  it('reports brokers that join or leave and a backlog that clears', () => {
    const next = fixtureSnapshot({
      brokers: [
        broker('DefaultCluster', 'broker-a', 0, '10.0.4.21:10911'),
        broker('DefaultCluster', 'broker-a', 1, '10.0.4.121:10911'),
        broker('DefaultCluster', 'broker-b', 0, '10.0.4.23:10911'),
        broker('DefaultCluster', 'broker-b', 1, '10.0.4.123:10911'),
        broker('cluster-east', 'broker-c', 0, '10.0.5.21:10911'),
        broker('cluster-east', 'broker-c', 1, '10.0.5.121:10911'),
        broker('cluster-east', 'broker-e', 0, '10.0.5.25:10911')
      ],
      consumers: fixtureConsumers.map((item) => (item.rawGroupName === 'payment-consumer' ? { ...item, diffTotal: 40 } : item))
    });

    expect(diffSnapshots(previous, next)).toEqual([
      { subject: 'broker-d', detail: 'no longer registered', tone: 'danger' },
      { subject: 'broker-c', detail: 'slave registered', tone: 'success' },
      { subject: 'broker-e', detail: 'joined cluster-east', tone: 'success' },
      { subject: 'payment-consumer', detail: 'lag 4,108 → 40', tone: 'success' }
    ]);
  });

  it('ignores small backlog movement above the threshold and changes below it', () => {
    const next = fixtureSnapshot({
      consumers: fixtureConsumers.map((item) => {
        if (item.rawGroupName === 'order-consumer') return { ...item, diffTotal: 8_500 };
        if (item.rawGroupName === 'inventory-worker') return { ...item, diffTotal: 900 };
        return item;
      })
    });

    expect(diffSnapshots(previous, next)).toEqual([]);
  });

  it('does not read a failed layer as groups disappearing', () => {
    expect(diffSnapshots(previous, fixtureSnapshot({}, ['consumers', 'brokers']))).toEqual([]);
  });

  it('compares only the layers it is asked to', () => {
    const next = fixtureSnapshot({ producers: [] });

    expect(diffSnapshots(previous, next, new Set(['producers']))).toEqual([]);
    expect(diffSnapshots(previous, next)).toHaveLength(6);
  });

  it('warns when a group loses its last client', () => {
    const next = fixtureSnapshot({
      producers: fixtureProducers.map((row) => (row.producerGroup === 'notification-producer' ? { ...row, connectionCount: 0 } : row))
    });

    expect(diffSnapshots(previous, next)).toEqual([
      { subject: 'notification-producer', detail: 'connections 2 → 0', tone: 'warning' }
    ]);
  });
});

describe('selectTopologyView', () => {
  const snapshot = fixtureSnapshot();

  it('keeps the busiest groups up to the limit and counts all groups in scope', () => {
    const view = selectTopologyView(snapshot, { scope: ALL_CLUSTERS, producerLimit: 3, consumerLimit: 2 });

    expect(view.producers.map((producer) => producer.producerGroup)).toEqual([
      'analytics-ingest',
      'order-service-producer',
      'payment-gateway-producer'
    ]);
    expect(view.consumers).toHaveLength(2);
    expect(view.producersInScope).toBe(6);
    expect(view.consumersInScope).toBe(6);
  });

  it('narrows the map to one cluster and the groups linked to it', () => {
    const view = selectTopologyView(snapshot, { scope: 'cluster-east', producerLimit: 6, consumerLimit: 6 });

    expect(view.clusters.map((cluster) => cluster.clusterName)).toEqual(['cluster-east']);
    expect(view.producers.map((producer) => producer.producerGroup)).toEqual([
      'analytics-ingest',
      'notification-producer',
      'batch-export-producer'
    ]);
    expect(view.consumers.map((consumer) => consumer.group)).toEqual(['analytics-stream', 'notification-push', 'audit-archiver']);
    expect(view.nameServers).toHaveLength(3);
  });

  it('counts groups the aggregated endpoint left out and never draws more than the cap', () => {
    const view = selectTopologyView(
      { ...snapshot, totals: { ...snapshot.totals, producerGroups: 120 } },
      { scope: ALL_CLUSTERS, producerLimit: 500, consumerLimit: 6 }
    );

    expect(view.producersInScope).toBe(120);
    expect(view.producers.length).toBeLessThanOrEqual(MAX_GROUPS_PER_SIDE);
  });

  it('keeps only the Controllers that elect the masters of the groups in scope', () => {
    const replicated = withReplication(snapshot);
    const controllersOf = (scope: string) =>
      selectTopologyView(replicated, { scope, producerLimit: 6, consumerLimit: 6 }).controllers.map((controller) => controller.address);

    expect(controllersOf(ALL_CLUSTERS)).toHaveLength(3);
    expect(controllersOf('cluster-east')).toEqual(['10.0.9.1:9878', '10.0.9.2:9878', '10.0.9.3:9878']);
    expect(controllersOf('DefaultCluster')).toEqual([]);
  });
});

describe('buildSignals', () => {
  it('summarises clusters, NameServers, broker instances, groups and backlog', () => {
    expect(buildSignals(fixtureSnapshot())).toEqual({
      clusters: { value: '2', tone: 'success' },
      nameServers: { value: '2 of 3', tone: 'warning' },
      brokers: { value: '7 online', tone: 'success' },
      producers: { value: '6 groups' },
      consumers: { value: '6 groups' },
      backlog: { value: '12.84K', tone: 'warning' }
    });
  });

  it('shows only the count when no availability check could be read', () => {
    expect(buildSignals(fixtureSnapshot({}, ['nameservers'])).nameServers).toEqual({ value: '3', tone: undefined });
  });
});

describe('describeTopology', () => {
  it('names what needs attention', () => {
    const view = selectTopologyView(fixtureSnapshot(), { scope: ALL_CLUSTERS, producerLimit: 6, consumerLimit: 6 });

    expect(describeTopology(view)).toBe(
      '2 clusters; 4 broker groups; NameServer unavailable: 10.0.4.12:9876; no slave registered: broker-c; '
        + 'consumer backlog above 1,000: order-consumer, payment-consumer.'
    );
  });

  it('names the Controller leader, or says that none is known', () => {
    const summary = (leader: number) =>
      describeTopology(selectTopologyView(withReplication(fixtureSnapshot(), leader), { scope: ALL_CLUSTERS, producerLimit: 6, consumerLimit: 6 }));

    expect(summary(0)).toContain('; 3 Controllers, leader 10.0.9.1:9878; ');
    expect(summary(-1)).toContain('; 3 Controllers, no leader known; ');
  });
});
