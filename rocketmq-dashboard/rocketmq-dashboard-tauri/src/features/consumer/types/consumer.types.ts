export type ConsumerQueryScope = { mode: 'name_server' } | { mode: 'proxy'; endpointId: string };

export interface ConsumerGroupListRequest {
    skipSysGroup?: boolean;
    scope: ConsumerQueryScope;
}

export interface ConsumerGroupRefreshRequest {
    consumerGroup: string;
    scope: ConsumerQueryScope;
}

export interface ConsumerConnectionQueryRequest {
    consumerGroup: string;
    scope: ConsumerQueryScope;
}

export interface ConsumerTopicDetailQueryRequest {
    consumerGroup: string;
    scope: ConsumerQueryScope;
}

export interface ConsumerConfigQueryRequest {
    consumerGroup: string;
    address?: string;
}

export interface ConsumerCreateOrUpdateRequest {
    clusterNameList: string[];
    brokerNameList: string[];
    consumerGroup: string;
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
    consumerGroup: string;
    brokerNameList: string[];
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
    brokerNames: string[];
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

export interface ConsumerConfigAttributeItem {
    key: string;
    value: string;
}

export interface ConsumerConfigView {
    consumerGroup: string;
    brokerName: string;
    brokerAddress: string;
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
    subscriptionTopicCount: number;
    subscriptionTopics: string[];
    attributes: ConsumerConfigAttributeItem[];
}

export interface ConsumerMutationResult {
    consumerGroup: string;
    operation: 'upsert' | 'delete';
    targets: ConsumerTargetResult[];
    targetCount: number;
    success: boolean;
}

export interface ConsumerTargetResult {
    target: string;
    kind: string;
    success: boolean;
    errorCode: string | null;
    message: string;
}
