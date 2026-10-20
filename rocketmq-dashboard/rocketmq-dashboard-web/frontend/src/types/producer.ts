export interface ProducerInfo {
  topic: string;
  producerGroup: string;
  connectionCount: number;
  /** Brokers the group's clients are registered with; older backends leave it out. */
  brokerNames?: string[];
}

export interface ProducerConnectionInfo {
  clientId: string;
  clientAddr: string;
  language: string;
  version: string;
}

export interface ProducerConnectionView {
  topic: string;
  producerGroup: string;
  connections: ProducerConnectionInfo[];
}
