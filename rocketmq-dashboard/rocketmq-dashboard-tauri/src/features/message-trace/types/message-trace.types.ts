export interface MessageTraceQueryRequest {
    traceTopic: string;
    messageId: string;
}

export interface MessageTraceNode {
    traceType: string;
    role: string;
    groupName: string;
    clientHost: string;
    storeHost: string;
    timestamp: number;
    costTime: number;
    status: string;
    retryTimes: number;
    fromTransactionCheck: boolean;
}

export interface MessageTraceConsumerGroup {
    consumerGroup: string;
    nodes: MessageTraceNode[];
}

export interface MessageTraceDetail {
    msgId: string;
    traceTopic: string;
    topic?: string | null;
    tags?: string | null;
    keys?: string | null;
    storeHost?: string | null;
    producerGroup?: string | null;
    producerClientHost?: string | null;
    producerStoreHost?: string | null;
    producerTimestamp?: number | null;
    producerCostTime?: number | null;
    producerStatus?: string | null;
    producerTraceType?: string | null;
    minTimestamp?: number | null;
    maxTimestamp?: number | null;
    totalSpanMs?: number | null;
    timeline: MessageTraceNode[];
    consumerGroups: MessageTraceConsumerGroup[];
    transactionChecks: MessageTraceNode[];
}
