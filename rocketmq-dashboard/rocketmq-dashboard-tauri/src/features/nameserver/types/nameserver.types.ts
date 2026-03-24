export interface NameServerConfigSnapshot {
    currentNamesrv: string | null;
    namesrvAddrList: string[];
    useVIPChannel: boolean;
    useTLS: boolean;
}

export interface NameServerHomePageInfo extends NameServerConfigSnapshot {}

export interface NameServerMutationResult {
    message: string;
    snapshot: NameServerConfigSnapshot;
}
