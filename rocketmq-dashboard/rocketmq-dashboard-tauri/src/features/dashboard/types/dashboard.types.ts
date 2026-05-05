export interface DashboardBrokerOverviewRequest {
    forceRefresh: boolean;
}

export interface DashboardBrokerSummary {
    totalClusters: number;
    totalBrokers: number;
    totalMasters: number;
    totalSlaves: number;
    activeBrokers: number;
    inactiveBrokers: number;
    brokersWithStatusErrors: number;
}

export interface DashboardBrokerTopItem {
    clusterName: string;
    brokerName: string;
    brokerId: number;
    address: string;
    receivedTotal: number;
}

export interface DashboardBrokerTpsItem {
    clusterName: string;
    brokerName: string;
    brokerId: number;
    address: string;
    produceTps: number;
    consumeTps: number;
    totalTps: number;
}

export interface DashboardBrokerOverviewResponse {
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
    summary: DashboardBrokerSummary;
    brokerTop: DashboardBrokerTopItem[];
    brokerTps: DashboardBrokerTpsItem[];
}

export interface DashboardTopicQueueItem {
    topic: string;
    category: string;
    readQueueCount: number;
    writeQueueCount: number;
    totalQueueCount: number;
}

export interface DashboardTopicTopItem {
    topic: string;
    totalMsg: number;
    producedMsgCount24h: number;
    consumedMsgCount24h: number;
    inTps: number;
    outTps: number;
    consumerGroupCount: number;
}

export interface DashboardTopicCategoryItem {
    category: string;
    count: number;
}

export interface DashboardTopicCurrentResponse {
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
    totalTopics: number;
    topicTop: DashboardTopicTopItem[];
    topicQueueTop: DashboardTopicQueueItem[];
    topicCategoryDistribution: DashboardTopicCategoryItem[];
}
