export type StorageBackend = 'file' | 'sqlite' | 'File' | 'Sqlite';

export interface DashboardConfigView {
  currentNamesrv?: string | null;
  namesrvAddrList: string[];
  useVIPChannel: boolean;
  useTLS: boolean;
  currentProxyAddr?: string | null;
  proxyAddrList: string[];
  storageBackend: StorageBackend;
}

export interface AddressRequest {
  address: string;
}

export interface NameserverListRequest {
  namesrvAddrList: string[];
  currentNamesrv?: string | null;
}

export interface BoolSettingRequest {
  enabled: boolean;
}

export interface ConfigMutationResult {
  message: string;
  config: DashboardConfigView;
}
