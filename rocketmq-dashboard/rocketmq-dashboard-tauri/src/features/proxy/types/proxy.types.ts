import type { ConnectionSettingsView } from '../../../services/connection.store';
export interface ProxyConfigSnapshot {
    currentProxyAddr: string | null;
    proxyAddrList: string[];
}

export interface ProxyMutationResult {
    message: string;
    settings: ConnectionSettingsView;
}

export interface ProxyHomePageInfo extends ProxyConfigSnapshot { settings: ConnectionSettingsView }
