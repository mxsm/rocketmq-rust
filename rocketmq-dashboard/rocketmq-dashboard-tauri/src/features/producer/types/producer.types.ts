export interface ProducerTopicOptionsRequest {}

export interface ProducerTopicOptionsView {
    topics: string[];
    currentNamesrv: string;
    useVipChannel: boolean;
    useTls: boolean;
}

export interface ProducerConnectionItem {
    clientId: string;
    clientAddr: string;
    language: string;
    version: number;
    versionDesc: string;
}

export interface ProducerConnectionView {
    topic: string;
    producerGroup: string;
    connectionCount: number;
    connections: ProducerConnectionItem[];
}

export interface ProducerConnectionQueryRequest {
    topic: string;
    producerGroup: string;
}
