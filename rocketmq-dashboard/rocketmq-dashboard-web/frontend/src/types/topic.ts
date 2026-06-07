export interface TopicInfo {
  topic: string;
  brokerName?: string | null;
  readQueueCount: number;
  writeQueueCount: number;
  perm: number;
  category: string;
}

export interface TopicListView {
  items: TopicInfo[];
  total: number;
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
  totalMinOffset: number;
  totalMaxOffset: number;
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

export interface MutationResult {
  message: string;
}
