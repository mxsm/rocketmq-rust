export interface ConsumerGroupInfo {
  group: string;
  consumeType: string;
  messageModel: string;
  clientCount: number;
  diffTotal: number;
}

export interface ConsumerListView {
  items: ConsumerGroupInfo[];
  total: number;
}

export interface ConsumerProgress {
  group: string;
  topicCount: number;
  diffTotal: number;
  queues: ConsumerQueueProgress[];
}

export interface ConsumerQueueProgress {
  topic: string;
  brokerName: string;
  queueId: number;
  brokerOffset: number;
  consumerOffset: number;
  diff: number;
}

export interface ConsumerResetOffsetRequest {
  topic: string;
  resetTimestamp: number;
  force: boolean;
}
