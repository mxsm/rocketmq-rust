/** Layers the list queries can also load when the backend has no aggregated endpoint. */
export type TopologyLayerId = 'config' | 'nameservers' | 'brokers' | 'topics' | 'producers' | 'consumers';

/**
 * Layers a topology snapshot is assembled from. Each one is a separate backend query; replication
 * (master election, HA state and Controllers) comes only from the aggregated endpoint.
 */
export type TopologySourceId = TopologyLayerId | 'replication';

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

/** What decides the master of a broker group; `unknown` when its broker config could not be read. */
export type TopologyMasterElection = 'configured' | 'controller' | 'unknown';

/** A slave of a broker group, with the replication state its master reports. */
export interface TopologyReplica {
  brokerId: number;
  address: string;
  /** Commit log bytes not yet acknowledged; absent while the master reports no connection from it. */
  lagBytes?: number;
  /** Whether the master counts the replica as in sync; absent when the master was not read. */
  inSync?: boolean;
  transferBytesPerSecond?: number;
}

/** What the Controllers keep for a group in Controller mode. Broker ids are the NameServer's: the master is 0. */
export interface TopologyControllerGroupState {
  masterEpoch: number;
  syncStateSetEpoch: number;
  /** The sync state set, the master included. */
  inSyncBrokerIds: number[];
  outOfSyncBrokerIds: number[];
}

/** One broker name: its master and the slaves registered under the same name. */
export interface TopologyBrokerGroup {
  brokerName: string;
  masterAddress: string | null;
  slaveAddresses: string[];
  version: string;
  produceTps: number;
  consumeTps: number;
  /** The replication fields are absent when the backend did not read them. */
  election?: TopologyMasterElection;
  /** Controllers the master names, in Controller mode. */
  controllerAddresses?: string[];
  /** The slaves ordered by broker id. */
  replicas?: TopologyReplica[];
  inSyncSlaveCount?: number;
  controllerState?: TopologyControllerGroupState;
}

/** A Controller that Controller-mode brokers name, with the Raft state it reports. */
export interface TopologyController {
  address: string;
  /** `unavailable` when its metadata could not be read. */
  status: 'available' | 'unavailable';
  /** Whether it leads its Raft group; absent while no leader is known. */
  leader?: boolean;
  /** Raft node id of the group's leader. */
  leaderId?: string;
  group?: string;
  peerCount?: number;
  lastLogIndex?: number;
  committedLogIndex?: number;
  appliedLogIndex?: number;
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
 * values it does not report, such as producer TPS or disk use, are absent on purpose.
 */
export interface TopologySnapshot {
  generatedAt: number;
  sources: TopologySourceStatus[];
  nameServers: TopologyNameServer[];
  clusters: TopologyCluster[];
  /** Ordered by address; empty without Controller-mode brokers. */
  controllers: TopologyController[];
  producers: TopologyProducer[];
  consumers: TopologyConsumer[];
  totals: TopologyTotals;
}
