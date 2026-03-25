export interface NameServerConfigSnapshot {
    currentNamesrv: string | null;
    namesrvAddrList: string[];
    useVIPChannel: boolean;
    useTLS: boolean;
}

export interface NameServerStatusItem {
    address: string;
    isCurrent: boolean;
    isAlive: boolean;
}

export interface NameServerHomePageInfo extends NameServerConfigSnapshot {
    servers: NameServerStatusItem[];
}

export interface NameServerMutationResult {
    message: string;
    snapshot: NameServerConfigSnapshot;
}
