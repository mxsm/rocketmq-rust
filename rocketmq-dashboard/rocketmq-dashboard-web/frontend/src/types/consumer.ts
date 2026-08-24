export type ConsumerQueryMode = 'nameServer' | 'proxy';

export interface ConsumerQueryScope {
  mode: ConsumerQueryMode;
  proxyAddress?: string;
}

/**
 * Identifies the consumer workspace that owned a mutation when it started.
 * The generation is supplied by the route owner so a delayed completion
 * cannot refresh or close a newer group or query scope.
 */
export interface ConsumerOperationIdentity {
  group: string;
  scopeKey: string;
  generation: number;
}

export interface ConsumerQuery {
  mode: ConsumerQueryMode;
  proxyAddress?: string;
  skipSystem?: boolean;
}

export interface ConsumerCapabilities {
  connections: boolean;
  progress: boolean;
  configuration: boolean;
  runningInfo: boolean;
  jstack: boolean;
}

export interface ConsumerClientCapabilities {
  runningInfo: boolean;
  jstack: boolean;
  runningInfoReason?: string | null;
  jstackReason?: string | null;
}

export interface ConsumerGroupListItem {
  displayGroupName: string;
  rawGroupName: string;
  category: string;
  connectionCount: number;
  consumeTps: number;
  diffTotal: number;
  messageModel: string;
  consumeType: string;
  version: number | null;
  versionDesc: string;
  brokerNames: string[];
  brokerAddresses: string[];
  updateTimestamp: number;
}

export interface ConsumerGroupListView {
  items: ConsumerGroupListItem[];
  total: number;
  queryScope: ConsumerQueryScope;
  capabilities: ConsumerCapabilities;
}

export interface ConsumerSummaryView {
  group: string;
  displayGroupName: string;
  category: string;
  connectionCount: number;
  consumeTps: number;
  diffTotal: number;
  messageModel: string;
  consumeType: string;
  version: number | null;
  versionDesc: string;
  brokerNames: string[];
  brokerAddresses: string[];
  updateTimestamp: number;
  queryScope: ConsumerQueryScope;
}

export interface ConsumerConnectionItem {
  clientId: string;
  clientAddr: string;
  language: string;
  version: number;
  versionDesc: string;
  capabilities: ConsumerClientCapabilities;
}

export interface ConsumerSubscriptionItem {
  topic: string;
  subString: string;
  expressionType: string;
  tagsSet: string[];
  codeSet: number[];
  subVersion: number;
}

export interface ConsumerConnectionView {
  group: string;
  connectionCount: number;
  consumeType: string;
  messageModel: string;
  consumeFromWhere: string;
  connections: ConsumerConnectionItem[];
  subscriptions: ConsumerSubscriptionItem[];
  queryScope: ConsumerQueryScope;
}

export interface ConsumerProgressQueue {
  brokerName: string;
  queueId: number;
  brokerOffset: number;
  consumerOffset: number;
  diffTotal: number;
  clientInfo: string;
  lastTimestamp: number;
}

export interface ConsumerProgressTopic {
  topic: string;
  diffTotal: number;
  lastTimestamp: number;
  queues: ConsumerProgressQueue[];
}

export interface ConsumerProgressView {
  group: string;
  topicCount: number;
  totalDiff: number;
  topics: ConsumerProgressTopic[];
  queryScope: ConsumerQueryScope;
}

export interface ConsumerConfigValue {
  consumeEnable: boolean;
  consumeFromMinEnable: boolean;
  consumeBroadcastEnable: boolean;
  consumeMessageOrderly: boolean;
  retryQueueNums: number;
  retryMaxTimes: number;
  brokerId: number;
  whichBrokerWhenConsumeSlowly: number;
  notifyConsumerIdsChangedEnable: boolean;
  groupSysFlag: number;
  consumeTimeoutMinute: number;
  groupRetryPolicyJson: string;
}

export interface ConsumerConfigAttribute {
  key: string;
  value: string;
}

export interface ConsumerConfigTarget {
  brokerName: string;
  brokerAddress: string;
  config: ConsumerConfigValue | null;
  subscriptionTopics: string[];
  attributes: ConsumerConfigAttribute[];
  error?: string | null;
}

export interface ConsumerConfigView {
  group: string;
  effective: ConsumerConfigValue | null;
  inconsistentFields: string[];
  targets: ConsumerConfigTarget[];
  queryScope: ConsumerQueryScope;
}

export interface ConsumerProcessQueue {
  topic: string;
  brokerName: string;
  queueId: number;
  cachedMessageCount: number;
  cachedMessageSizeInMib: number;
  commitOffset: number;
  dropped: boolean;
  lastConsumeTimestamp: number;
}

export interface ConsumerRunningInfoView {
  consumerGroup: string;
  clientId: string;
  properties: ConsumerConfigAttribute[];
  subscriptions: ConsumerSubscriptionItem[];
  processQueues: ConsumerProcessQueue[];
  jstack: string | null;
  truncated: boolean;
}

export interface ConsumerJStackView {
  consumerGroup: string;
  clientId: string;
  jstack: string | null;
  truncated: boolean;
}

export interface ConsumerBrokerInfo {
  brokerName: string;
  brokerAddress: string;
}

export interface ConsumerBrokerListView {
  items: ConsumerBrokerInfo[];
}

export interface ConsumerTargetResult {
  target: string;
  kind: string;
  success: boolean;
  message: string;
}

export interface ConsumerOperationResult {
  operation: string;
  consumerGroup: string;
  success: boolean;
  targetCount: number;
  message: string;
  targets: ConsumerTargetResult[];
}

export interface ConsumerUpsertRequest {
  consumerGroup?: string;
  clusterNameList: string[];
  brokerNameList: string[];
  consumeEnable: boolean;
  consumeFromMinEnable: boolean;
  consumeBroadcastEnable: boolean;
  consumeMessageOrderly: boolean;
  retryQueueNums: number;
  retryMaxTimes: number;
  brokerId: number;
  whichBrokerWhenConsumeSlowly: number;
  notifyConsumerIdsChangedEnable: boolean;
  groupSysFlag: number;
  consumeTimeoutMinute: number;
}

export interface ConsumerDeleteRequest {
  brokerNames: string[];
}

export interface ConsumerResetOffsetRequest {
  topic: string;
  resetTimestamp: number;
  force: boolean;
}
