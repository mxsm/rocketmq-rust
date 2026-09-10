export interface FilterableTopic {
    name: string;
    type: string;
    messageType: string;
    clusters: string[];
    brokers: string[];
}

export interface TopicFilters {
    search: string;
    categories: Partial<Record<string, boolean>>;
    brokerName: string;
    clusterName: string;
    messageType: string;
}

export function filterTopics<T extends FilterableTopic>(topics: T[], filters: TopicFilters): T[] {
    const search = filters.search.trim().toLowerCase();
    return topics.filter(topic => {
        if (filters.categories[topic.type] === false) return false;
        if (filters.brokerName && !topic.brokers.includes(filters.brokerName)) return false;
        if (filters.clusterName && !topic.clusters.includes(filters.clusterName)) return false;
        if (filters.messageType && topic.messageType !== filters.messageType) return false;
        return !search || [topic.name, topic.type, topic.messageType, ...topic.clusters, ...topic.brokers].join(' ').toLowerCase().includes(search);
    });
}
