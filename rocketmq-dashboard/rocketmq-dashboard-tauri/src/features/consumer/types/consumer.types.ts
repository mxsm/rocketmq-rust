export interface ConsumerGroupListRequest {
    skipSysGroup?: boolean;
    address?: string;
}

export interface ConsumerGroupRefreshRequest {
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
