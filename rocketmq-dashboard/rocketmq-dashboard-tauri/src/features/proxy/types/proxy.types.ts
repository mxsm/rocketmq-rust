export interface ProxyConfigSnapshot {
    currentProxyAddr: string | null;
    proxyAddrList: string[];
}

export interface ProxyMutationResult {
    message: string;
    snapshot: ProxyConfigSnapshot;
}
