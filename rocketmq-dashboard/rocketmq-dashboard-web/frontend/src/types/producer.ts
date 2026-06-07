export interface ProducerInfo {
  topic: string;
  producerGroup: string;
  connectionCount: number;
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
