import type { BrokerInfo } from '../../types/broker';
import type { DashboardConfigView, NameserverAvailabilityView } from '../../types/config';
import type { ConsumerGroupListItem } from '../../types/consumer';
import type { ProducerInfo } from '../../types/producer';
import type { TopicInfo } from '../../types/topic';
import type { TopologyBrokerGroup, TopologyLayerId, TopologySnapshot } from '../../types/topology';
import { buildTopologySnapshot, TOPOLOGY_SOURCES, type TopologyLayers } from './topology-model';

/** Test data shaped like the Topology artboard: two clusters, one broker without a slave. */

export function broker(
  clusterName: string,
  brokerName: string,
  brokerId: number,
  address: string,
  produceTps = 0,
  consumeTps = 0
): BrokerInfo {
  return {
    clusterName,
    brokerName,
    brokerId,
    address,
    role: brokerId === 0 ? 'MASTER' : 'SLAVE',
    version: 'V5_3_1',
    produceTps,
    consumeTps
  };
}

export const fixtureBrokers: BrokerInfo[] = [
  broker('DefaultCluster', 'broker-a', 0, '10.0.4.21:10911', 1204.36, 1198.02),
  broker('DefaultCluster', 'broker-a', 1, '10.0.4.121:10911'),
  broker('DefaultCluster', 'broker-b', 0, '10.0.4.23:10911', 612.48, 596.66),
  broker('DefaultCluster', 'broker-b', 1, '10.0.4.123:10911'),
  broker('cluster-east', 'broker-c', 0, '10.0.5.21:10911', 420.3, 412.8),
  broker('cluster-east', 'broker-d', 0, '10.0.5.23:10911', 203.8, 197.2),
  broker('cluster-east', 'broker-d', 1, '10.0.5.123:10911')
];

export function topic(name: string, brokers: string[]): TopicInfo {
  return {
    topic: name,
    brokerName: brokers[0] ?? null,
    brokers,
    clusters: [],
    readQueueCount: 8,
    writeQueueCount: 8,
    perm: 6,
    category: 'APPLICATION',
    messageType: 'NORMAL',
    order: false,
    systemTopic: false
  };
}

export const fixtureTopics: TopicInfo[] = [
  topic('order-events', ['broker-a', 'broker-b']),
  topic('payment-events', ['broker-a']),
  topic('inventory-sync', ['broker-b']),
  topic('notification-push', ['broker-c']),
  topic('analytics-events', ['broker-c', 'broker-d']),
  topic('batch-export', ['broker-d'])
];

export const fixtureProducers: ProducerInfo[] = [
  { topic: 'order-events', producerGroup: 'order-service-producer', connectionCount: 6 },
  { topic: 'payment-events', producerGroup: 'order-service-producer', connectionCount: 4 },
  { topic: 'payment-events', producerGroup: 'payment-gateway-producer', connectionCount: 4 },
  { topic: 'inventory-sync', producerGroup: 'inventory-producer', connectionCount: 3 },
  { topic: 'notification-push', producerGroup: 'notification-producer', connectionCount: 2 },
  { topic: 'analytics-events', producerGroup: 'analytics-ingest', connectionCount: 8 },
  { topic: 'batch-export', producerGroup: 'batch-export-producer', connectionCount: 0 }
];

export function consumerGroup(
  rawGroupName: string,
  connectionCount: number,
  consumeTps: number,
  diffTotal: number,
  brokerNames: string[],
  category = 'NORMAL'
): ConsumerGroupListItem {
  return {
    displayGroupName: rawGroupName,
    rawGroupName,
    category,
    connectionCount,
    consumeTps,
    diffTotal,
    messageModel: 'CLUSTERING',
    consumeType: 'CONSUME_PASSIVELY',
    version: null,
    versionDesc: '',
    brokerNames,
    brokerAddresses: [],
    updateTimestamp: 0
  };
}

export const fixtureConsumers: ConsumerGroupListItem[] = [
  consumerGroup('order-consumer', 12, 1012, 8420, ['broker-a', 'broker-b']),
  consumerGroup('payment-consumer', 6, 486, 4108, ['broker-a']),
  consumerGroup('inventory-worker', 4, 296, 312, ['broker-b']),
  consumerGroup('analytics-stream', 16, 412, 0, ['broker-c', 'broker-d']),
  consumerGroup('notification-push', 3, 198, 0, ['broker-c']),
  consumerGroup('audit-archiver', 0, 0, 0, ['broker-d']),
  consumerGroup('TOOLS_CONSUMER', 0, 0, 0, ['broker-a'], 'SYSTEM')
];

export const fixtureConfig: DashboardConfigView = {
  environmentId: 'env-prod',
  environmentName: 'production',
  revision: 3,
  endpoints: [],
  currentNamesrv: '127.0.0.1:9876',
  namesrvAddrList: ['127.0.0.1:9876', '10.0.4.11:9876', '10.0.4.12:9876'],
  useVIPChannel: false,
  useTLS: false,
  currentProxyAddr: null,
  proxyAddrList: [],
  storageBackend: 'file',
  storageMode: 'singleNode'
};

export const fixtureAvailability: NameserverAvailabilityView = {
  endpoints: [
    { address: '127.0.0.1:9876', status: 'available', checkedAt: 1_000 },
    { address: '10.0.4.11:9876', status: 'available', checkedAt: 1_000 },
    { address: '10.0.4.12:9876', status: 'unavailable', checkedAt: 1_000 }
  ]
};

export const fixtureLayers: TopologyLayers = {
  config: fixtureConfig,
  nameservers: fixtureAvailability,
  brokers: fixtureBrokers,
  topics: fixtureTopics,
  producers: fixtureProducers,
  consumers: fixtureConsumers
};

export function okSources(durationMs = 40) {
  return TOPOLOGY_SOURCES.map((source) => ({ source, status: 'ok' as const, durationMs }));
}

/** A snapshot built from the fixture layers; `failed` layers are reported as failed and left empty. */
export function fixtureSnapshot(
  overrides: Partial<TopologyLayers> = {},
  failed: TopologyLayerId[] = [],
  generatedAt = 10_000
): TopologySnapshot {
  const layers: TopologyLayers = { ...fixtureLayers, ...overrides };
  for (const source of failed) layers[source] = null;
  const sources = okSources().map((status) =>
    failed.includes(status.source) ? { ...status, status: 'failed' as const, message: 'The query failed.' } : status
  );
  return buildTopologySnapshot(layers, sources, generatedAt);
}

export const fixtureControllerAddresses = ['10.0.9.1:9878', '10.0.9.2:9878', '10.0.9.3:9878'];

/**
 * `snapshot` with the replication layer: broker-d in Controller mode with its replica out of the
 * sync state set, broker-a's slave a little behind its master, broker-b's slave not connected, and
 * three Controllers of which the one at `leader` leads (`-1`: no leader known).
 */
export function withReplication(snapshot: TopologySnapshot, leader = 0): TopologySnapshot {
  const decorate = (group: TopologyBrokerGroup): TopologyBrokerGroup => {
    switch (group.brokerName) {
      case 'broker-d':
        return {
          ...group,
          election: 'controller',
          controllerAddresses: fixtureControllerAddresses,
          replicas: [{ brokerId: 1, address: '10.0.5.123:10911', lagBytes: 0, inSync: false, transferBytesPerSecond: 2_048 }],
          inSyncSlaveCount: 0,
          controllerState: { masterEpoch: 3, syncStateSetEpoch: 5, inSyncBrokerIds: [0], outOfSyncBrokerIds: [1] }
        };
      case 'broker-a':
        return { ...group, election: 'configured', replicas: [{ brokerId: 1, address: '10.0.4.121:10911', lagBytes: 10_433, inSync: false }], inSyncSlaveCount: 0 };
      case 'broker-b':
        return { ...group, election: 'configured', replicas: [{ brokerId: 1, address: '10.0.4.123:10911', inSync: false }], inSyncSlaveCount: 0 };
      default:
        return { ...group, election: 'configured' };
    }
  };
  return {
    ...snapshot,
    sources: [...snapshot.sources, { source: 'replication', status: 'ok', durationMs: 40 }],
    clusters: snapshot.clusters.map((cluster) => ({ ...cluster, brokers: cluster.brokers.map(decorate) })),
    controllers: fixtureControllerAddresses.map((address, index) => ({
      address,
      status: 'available',
      leader: leader < 0 ? undefined : index === leader,
      leaderId: leader < 0 ? undefined : String(leader + 1),
      peerCount: 3,
      appliedLogIndex: 42,
      committedLogIndex: 42
    }))
  };
}
