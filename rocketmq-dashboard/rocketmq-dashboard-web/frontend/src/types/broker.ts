export interface BrokerInfo {
  clusterName: string;
  brokerName: string;
  brokerId: number;
  address: string;
  role: string;
  version: string;
  produceTps: number;
  consumeTps: number;
}

export interface BrokerListView {
  items: BrokerInfo[];
  total: number;
}

export interface BrokerRuntimeStats {
  brokerName: string;
  address: string;
  entries: Record<string, string>;
}

export interface BrokerConfigView {
  brokerName: string;
  address: string;
  entries: Record<string, string>;
}

export interface BrokerConfigUpdateRequest {
  entries: Record<string, string>;
}
