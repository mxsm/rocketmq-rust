export type StorageBackend = 'file' | 'sqlite' | 'mysql' | 'postgres';
export type StorageMode = 'singleNode' | 'multiNode';

export type EndpointType = 'nameserver' | 'proxy';
export type EndpointRole = 'primary' | 'secondary';

export interface EndpointView {
  endpointId: string;
  endpointType: EndpointType;
  address: string;
  role: EndpointRole;
  isEnabled: boolean;
  isActive: boolean;
  sortOrder: number;
}

export interface DashboardConfigView {
  environmentId: string;
  environmentName: string;
  revision: number;
  endpoints: EndpointView[];
  currentNamesrv?: string | null;
  namesrvAddrList: string[];
  useVIPChannel: boolean;
  useTLS: boolean;
  currentProxyAddr?: string | null;
  proxyAddrList: string[];
  storageBackend: StorageBackend;
  storageMode: StorageMode;
}

export type NameserverAvailabilityStatus = 'available' | 'unavailable';

export interface NameserverEndpointAvailability {
  address: string;
  status: NameserverAvailabilityStatus;
  checkedAt: number;
}

export interface NameserverAvailabilityView {
  endpoints: NameserverEndpointAvailability[];
}

export interface AddressRequest {
  address: string;
  expectedRevision: number;
}

export interface EndpointRequest {
  endpointId: string;
  expectedRevision: number;
}

export interface NameserverListRequest {
  namesrvAddrList: string[];
  currentNamesrv?: string | null;
  expectedRevision: number;
}

export interface BoolSettingRequest {
  enabled: boolean;
  expectedRevision: number;
}

export interface ConfigMutationResult {
  message: string;
  config: DashboardConfigView;
}
