import type { ConnectionSettingsView } from '../../../services/connection.store';
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
    settings: ConnectionSettingsView;
}

export interface NameServerMutationResult {
    message: string;
    settings: ConnectionSettingsView;
}
