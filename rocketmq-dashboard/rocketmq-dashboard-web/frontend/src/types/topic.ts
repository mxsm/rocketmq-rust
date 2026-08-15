export interface TopicInfo {
  topic: string;
  brokerName: string | null;
  brokers: string[];
  clusters: string[];
  readQueueCount: number;
  writeQueueCount: number;
  perm: number;
  category: string;
  messageType: string;
  order: boolean;
  systemTopic: boolean;
}

export interface TopicTargetOptionView {
  clusterName: string;
  brokerNames: string[];
}

export interface TopicListView {
  items: TopicInfo[];
  total: number;
  targets: TopicTargetOptionView[];
}

export interface TopicConfigView {
  topicName: string;
  brokerName: string;
  clusterName: string | null;
  brokerNameList: string[];
  clusterNameList: string[];
  readQueueNums: number;
  writeQueueNums: number;
  perm: number;
  order: boolean;
  messageType: string;
  attributes: Record<string, string>;
  inconsistentFields: string[];
}

export interface TopicConsumerView {
  consumerGroup: string;
  totalDiff: number;
  inflightDiff: number;
  consumeTps: number;
}

export interface TopicConsumersView {
  items: TopicConsumerView[];
}

export interface TopicRouteInfo {
  topic: string;
  brokers: TopicRouteBroker[];
  queues: TopicRouteQueue[];
}

export interface TopicRouteBroker {
  brokerName: string;
  brokerAddrs: string[];
}

export interface TopicRouteQueue {
  brokerName: string;
  readQueueNums: number;
  writeQueueNums: number;
  perm: number;
}

export interface TopicStatsInfo {
  topic: string;
  queueCount: number;
  totalMessageCount: number;
  totalMinOffset: number;
  totalMaxOffset: number;
  offsets: TopicQueueOffsetView[];
}

export interface TopicQueueOffsetView {
  brokerName: string;
  queueId: number;
  minOffset: number;
  maxOffset: number;
  lastUpdateTimestamp: number;
}

export interface TopicMutationRequest {
  topic: string;
  readQueueCount: number;
  writeQueueCount: number;
  perm: number;
  brokerNameList: string[];
  clusterNameList: string[];
  order?: boolean;
  messageType?: string;
}

export interface TopicTestMessageRequest {
  key: string;
  tag: string;
  messageBody: string;
  traceEnabled: boolean;
}

export interface TopicSendResultView {
  topic: string;
  success: boolean;
  sendStatus: string;
  messageId: string | null;
  brokerName: string | null;
  queueId: number | null;
  queueOffset: number;
  transactionId: string | null;
  regionId: string | null;
  localTransactionState: string | null;
}

export interface TopicResetOffsetRequest {
  consumerGroup: string;
  resetTimestamp: number;
  force: boolean;
}

export interface TopicSkipOffsetRequest {
  consumerGroup: string;
}

export interface TopicOffsetResult {
  operation: string;
  topic: string;
  consumerGroup: string;
  success: boolean;
  affectedQueueCount: number;
  appliedTimestamp: number;
  message: string;
}

export interface TopicTargetResult {
  target: string;
  success: boolean;
  message: string;
}

export interface TopicOperationResult {
  operation: string;
  topic: string;
  success: boolean;
  targetCount: number;
  message: string;
  targets: TopicTargetResult[];
}

export interface MutationResult {
  message: string;
}
