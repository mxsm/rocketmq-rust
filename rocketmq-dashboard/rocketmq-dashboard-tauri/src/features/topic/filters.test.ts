import { expect, it } from 'vitest';
import { filterTopics, type TopicFilters } from './filters';

const topics = [
    { name: 'orders', type: 'NORMAL', messageType: 'NORMAL', brokers: ['broker-a'], clusters: ['cluster-a'] },
    { name: 'orders-archive', type: 'NORMAL', messageType: 'NORMAL', brokers: ['broker-a-extra'], clusters: ['cluster-a-extra'] },
    { name: 'orders-fifo', type: 'FIFO', messageType: 'FIFO', brokers: ['broker-a'], clusters: ['cluster-a'] },
    { name: 'other', type: 'NORMAL', messageType: 'NORMAL', brokers: ['broker-a'], clusters: ['cluster-b'] },
];
const filters: TopicFilters = { search: '', categories: {}, brokerName: '', clusterName: '', messageType: '' };

it('combines search, category, exact Broker/Cluster membership and message type', () => {
    const selected = filterTopics(topics, { ...filters, search: ' ORDERS ', brokerName: 'broker-a', clusterName: 'cluster-a', messageType: 'NORMAL' });
    expect(selected.map(topic => topic.name)).toEqual(['orders']);
    expect(filterTopics(topics, { ...filters, categories: { NORMAL: false }, brokerName: 'broker-a', clusterName: 'cluster-a' }).map(topic => topic.name)).toEqual(['orders-fifo']);
    expect(filterTopics(topics, { ...filters, brokerName: 'broker' })).toEqual([]);
});

it('preserves broad text search while exact route filters never match substrings', () => {
    expect(filterTopics(topics, { ...filters, search: 'cluster-a' })).toHaveLength(3);
    expect(filterTopics(topics, { ...filters, clusterName: 'cluster-a' })).toHaveLength(2);
    expect(filterTopics(topics, { ...filters, brokerName: 'BROKER-A' })).toEqual([]);
    expect(filterTopics(topics, filters)).toEqual(topics);
});
