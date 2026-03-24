export type TopicCategory =
    | 'SYSTEM'
    | 'RETRY'
    | 'DLQ'
    | 'NORMAL'
    | 'UNSPECIFIED'
    | 'FIFO'
    | 'DELAY'
    | 'TRANSACTION';

export type TopicMessageType = Exclude<TopicCategory, 'SYSTEM' | 'RETRY' | 'DLQ'>;

export interface TopicTargetOption {
    clusterName: string;
    brokerNames: string[];
}

export interface TopicListItem {
    topic: string;
    category: TopicCategory;
    messageType: string;
    clusters: string[];
    brokers: string[];
    readQueueCount: number;
    writeQueueCount: number;
    perm: number;
    order: boolean;
    systemTopic: boolean;
}

export interface TopicListResponse {
    items: TopicListItem[];
    total: number;
    targets: TopicTargetOption[];
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
}

export interface TopicBrokerAddressView {
    brokerId: number;
    address: string;
}

export interface TopicRouteBrokerView {
    clusterName: string;
    brokerName: string;
    addresses: TopicBrokerAddressView[];
}

export interface TopicRouteQueueView {
    brokerName: string;
    readQueueNums: number;
    writeQueueNums: number;
    perm: number;
}

export interface TopicRouteView {
    topic: string;
    brokers: TopicRouteBrokerView[];
    queues: TopicRouteQueueView[];
}

export interface TopicStatusOffsetView {
    brokerName: string;
    queueId: number;
    minOffset: number;
    maxOffset: number;
    lastUpdateTimestamp: number;
}

export interface TopicStatusView {
    topic: string;
    totalMessageCount: number;
    queueCount: number;
    offsets: TopicStatusOffsetView[];
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

export interface TopicMutationResult {
    success: boolean;
    message: string;
    topicName?: string | null;
    affectedQueues?: number | null;
}

export interface TopicConsumerGroupListResponse {
    topic: string;
    consumerGroups: string[];
}

export interface TopicConsumerInfoView {
    consumerGroup: string;
    totalDiff: number;
    inflightDiff: number;
    consumeTps: number;
}

export interface TopicConsumerInfoResponse {
    topic: string;
    items: TopicConsumerInfoView[];
}

export interface TopicSendMessageResult {
    topic: string;
    sendStatus: string;
    messageId?: string | null;
    brokerName?: string | null;
    queueId?: number | null;
    queueOffset: number;
    transactionId?: string | null;
    regionId?: string | null;
    localTransactionState?: string | null;
}

export interface TopicListRequest {
    skipSysProcess: boolean;
    skipRetryAndDlq: boolean;
}

export interface TopicQueryRequest {
    topic: string;
}

export interface TopicConfigQueryRequest {
    topic: string;
    brokerName?: string | null;
}

export interface TopicConfigRequest {
    clusterNameList: string[];
    brokerNameList: string[];
    topicName: string;
    writeQueueNums: number;
    readQueueNums: number;
    perm: number;
    order: boolean;
    messageType?: string | null;
}

export interface DeleteTopicRequest {
    topic: string;
    clusterName?: string | null;
}

export interface DeleteTopicByBrokerRequest {
    brokerName: string;
    topic: string;
}

export interface ResetOffsetRequest {
    consumerGroupList: string[];
    topic: string;
    resetTime: number;
    force: boolean;
}

export interface SendTopicMessageRequest {
    topic: string;
    key: string;
    tag: string;
    messageBody: string;
    traceEnabled: boolean;
}

export interface TopicEditorSeed {
    topicName: string;
    clusterNameList: string[];
    brokerNameList: string[];
    writeQueueNums: number;
    readQueueNums: number;
    perm: number;
    order: boolean;
    messageType: string;
}

export const TOPIC_CATEGORY_OPTIONS: TopicCategory[] = [
    'NORMAL',
    'FIFO',
    'DELAY',
    'TRANSACTION',
    'UNSPECIFIED',
    'RETRY',
    'DLQ',
    'SYSTEM',
];

export const TOPIC_MESSAGE_TYPE_OPTIONS: TopicMessageType[] = [
    'NORMAL',
    'FIFO',
    'DELAY',
    'TRANSACTION',
    'UNSPECIFIED',
];
