export interface NameServerHomePageResponse {
    success: boolean;
    message: string;
    namesrvAddrList: string[];
    currentNamesrv: string | null;
    useVIPChannel: boolean;
    useTLS: boolean;
}

export interface NameServerMutationResponse {
    success: boolean;
    message: string;
}
