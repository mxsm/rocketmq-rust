export interface DashboardOverview {
  currentNamesrv?: string | null;
  brokerCount: number;
  topicCount: number;
  consumerGroupCount: number;
  producerCount: number;
  messageBacklog: number;
  systemStatus: string;
}

export interface DashboardTopicCurrent {
  totalTopics: number;
  topTopics: TopicCurrentMetric[];
}

export interface TopicCurrentMetric {
  topic: string;
  totalMsg: number;
  inTps: number;
  outTps: number;
}

export interface DashboardHistoryQuery {
  date: string;
  topicName?: string;
}

export interface DashboardHistorySeries {
  date: string;
  metric: string;
  topicName?: string | null;
  collected: boolean;
  points: DashboardHistoryPoint[];
}

export interface DashboardHistoryPoint {
  timestamp: number;
  value: number;
}
