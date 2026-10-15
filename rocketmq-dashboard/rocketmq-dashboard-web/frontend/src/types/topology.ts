/** Layers a topology snapshot is assembled from. Each one is a separate backend query. */
export type TopologySourceId = 'config' | 'nameservers' | 'brokers' | 'topics' | 'producers' | 'consumers';

export interface TopologySourceStatus {
  source: TopologySourceId;
  status: 'ok' | 'failed';
  durationMs: number;
  /** Public error code of a failed query, as the aggregated endpoint reports it. */
  code?: string | null;
  /** User-facing text for a failed query. */
  message?: string | null;
}

export interface TopologyNameServer {
  address: string;
  current: boolean;
  /** `unknown` when the availability check itself could not be read. */
  status: 'available' | 'unavailable' | 'unknown';
  checkedAt: number | null;
}

/** One broker name: its master and the slaves registered under the same name. */
export interface TopologyBrokerGroup {
  brokerName: string;
  masterAddress: string | null;
  slaveAddresses: string[];
  version: string;
  produceTps: number;
  consumeTps: number;
}

export interface TopologyCluster {
  clusterName: string;
  produceTps: number;
  consumeTps: number;
  brokers: TopologyBrokerGroup[];
}

export interface TopologyProducer {
  producerGroup: string;
  connectionCount: number;
  topics: string[];
  /** Brokers serving at least one of the group's topics. */
  brokerNames: string[];
}

export interface TopologyConsumer {
  /** Raw group name, the identity used by the consumer routes. */
  group: string;
  displayName: string;
  connectionCount: number;
  consumeTps: number;
  diffTotal: number;
  brokerNames: string[];
}

export interface TopologyTotals {
  /** Groups known to the backend; `producers` may hold only the busiest of them. */
  producerGroups: number;
  consumerGroups: number;
  /** Sum of `diffTotal` over every consumer group, not only the ones listed. */
  messageBacklog: number;
}

/**
 * Everything the topology page draws. The fields mirror what the backend reports today;
 * values it does not report, such as producer TPS, disk use or replication delay, are absent on purpose.
 */
export interface TopologySnapshot {
  generatedAt: number;
  sources: TopologySourceStatus[];
  nameServers: TopologyNameServer[];
  clusters: TopologyCluster[];
  producers: TopologyProducer[];
  consumers: TopologyConsumer[];
  totals: TopologyTotals;
}
