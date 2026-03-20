export interface ConsumerGroupListRequest {
    skipSysGroup?: boolean;
    address?: string;
}

export interface ConsumerGroupRefreshRequest {
    consumerGroup: string;
    address?: string;
}

export interface ConsumerConnectionQueryRequest {
    consumerGroup: string;
    address?: string;
}

export interface ConsumerTopicDetailQueryRequest {
    consumerGroup: string;
    address?: string;
}

export interface ConsumerGroupListSummary {
    totalGroups: number;
    normalGroups: number;
    fifoGroups: number;
    systemGroups: number;
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
    version?: number | null;
    versionDesc: string;
    brokerAddresses: string[];
    updateTimestamp: number;
}

export interface ConsumerGroupListResponse {
    items: ConsumerGroupListItem[];
    summary: ConsumerGroupListSummary;
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
}

export interface ConsumerConnectionItem {
    clientId: string;
    clientAddr: string;
    language: string;
    version: number;
    versionDesc: string;
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
    consumerGroup: string;
    connectionCount: number;
    consumeType: string;
    messageModel: string;
    consumeFromWhere: string;
    connections: ConsumerConnectionItem[];
    subscriptions: ConsumerSubscriptionItem[];
}

export interface ConsumerTopicDetailQueueItem {
    brokerName: string;
    queueId: number;
    brokerOffset: number;
    consumerOffset: number;
    diffTotal: number;
    clientInfo: string;
    lastTimestamp: number;
}

export interface ConsumerTopicDetailItem {
    topic: string;
    diffTotal: number;
    lastTimestamp: number;
    queueStatInfoList: ConsumerTopicDetailQueueItem[];
}

export interface ConsumerTopicDetailView {
    consumerGroup: string;
    topicCount: number;
    totalDiff: number;
    topics: ConsumerTopicDetailItem[];
}
