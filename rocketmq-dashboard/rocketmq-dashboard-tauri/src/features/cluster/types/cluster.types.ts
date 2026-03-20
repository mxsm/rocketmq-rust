export interface ClusterHomePageRequest {
    forceRefresh: boolean;
}

export interface ClusterBrokerConfigRequest {
    brokerAddr: string;
}

export interface ClusterBrokerStatusRequest {
    brokerAddr: string;
}

export interface ClusterOverviewSummary {
    totalClusters: number;
    totalBrokers: number;
    totalMasters: number;
    totalSlaves: number;
    activeBrokers: number;
    inactiveBrokers: number;
    brokersWithStatusErrors: number;
}

export interface ClusterBrokerCardItem {
    clusterName: string;
    brokerName: string;
    brokerId: number;
    role: string;
    address: string;
    version: string;
    produceTps: number;
    consumeTps: number;
    todayReceivedTotal: number;
    yesterdayProduce: number;
    yesterdayConsume: number;
    todayProduce: number;
    todayConsume: number;
    isActive: boolean;
    statusLoadError?: string | null;
    rawStatus: Record<string, string>;
}

export interface ClusterHomePageResponse {
    clusters: string[];
    items: ClusterBrokerCardItem[];
    summary: ClusterOverviewSummary;
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
}

export interface ClusterBrokerConfigView {
    brokerAddr: string;
    entries: Record<string, string>;
}

export interface ClusterBrokerStatusView {
    brokerAddr: string;
    entries: Record<string, string>;
}
