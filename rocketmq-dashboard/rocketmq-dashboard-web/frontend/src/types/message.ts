export interface MessageView {
  topic: string;
  messageId: string;
  keys?: string | null;
  tags?: string | null;
  bornTimestamp: number;
  storeTimestamp: number;
  queueId: number;
  queueOffset: number;
  body: string;
  properties: Record<string, string>;
}

export interface MessageListView {
  items: MessageView[];
  total: number;
}

export interface MessageQueryParams {
  topic?: string;
  key?: string;
  messageId?: string;
  begin?: number;
  end?: number;
}

export interface MessageResendRequest {
  topic: string;
  consumerGroup: string;
  clientId?: string;
}

export interface MessageTraceView {
  messageId: string;
  traceTopic: string;
  nodes: MessageTraceNode[];
}

export interface MessageTraceNode {
  nodeType: string;
  name: string;
  status: string;
  timestamp: number;
}

export interface DlqMessageQueryParams {
  consumerGroup: string;
  key?: string;
  messageId?: string;
  begin?: number;
  end?: number;
  pageNum?: number;
  pageSize?: number;
}

export interface DlqMessageRef {
  topicName?: string;
  consumerGroup: string;
  msgId: string;
  clientId?: string;
}

export interface DlqBatchResendRequest {
  messages: DlqMessageRef[];
}

export interface DlqMessageResendResult {
  msgId: string;
  consumeResult: string;
  remark?: string;
}

export interface DlqExportView {
  fileName: string;
  rows: MessageView[];
  csv: string;
}
