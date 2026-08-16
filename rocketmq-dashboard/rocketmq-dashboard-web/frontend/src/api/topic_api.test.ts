import { beforeEach, describe, expect, it, vi } from 'vitest';
import { apiClient } from './client';
import { topicApi } from './topic_api';

vi.mock('./client', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn()
  }
}));

describe('topicApi', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('URL-encodes every topic, broker, and query value', () => {
    const topic = 'orders/paid?region=cn north';
    const broker = 'broker/a?primary=true';
    const encodedTopic = 'orders%2Fpaid%3Fregion%3Dcn%20north';
    const encodedBroker = 'broker%2Fa%3Fprimary%3Dtrue';

    topicApi.get(topic);
    topicApi.update(topic, {
      topic,
      readQueueCount: 8,
      writeQueueCount: 8,
      perm: 6,
      brokerNameList: [broker],
      clusterNameList: [],
      order: false,
      messageType: 'NORMAL'
    });
    topicApi.delete(topic);
    topicApi.route(topic);
    topicApi.stats(topic);
    topicApi.config(topic, broker);
    topicApi.consumers(topic);
    topicApi.sendTestMessage(topic, { key: '', tag: '', messageBody: 'test', traceEnabled: false });
    topicApi.resetOffset(topic, { consumerGroup: 'orders-service', resetTimestamp: 1_700_000_000_000, force: true });
    topicApi.skipBacklog(topic, { consumerGroup: 'orders-service' });
    topicApi.deleteFromBroker(topic, broker);

    expect(apiClient.get).toHaveBeenCalledWith(`/api/topics/${encodedTopic}`);
    expect(apiClient.get).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/route`);
    expect(apiClient.get).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/stats`);
    expect(apiClient.get).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/config?brokerName=${encodedBroker}`);
    expect(apiClient.get).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/consumers`);
    expect(apiClient.post).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/test-message`, expect.any(Object));
    expect(apiClient.post).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/consumer-offset/reset`, expect.any(Object));
    expect(apiClient.post).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/consumer-offset/skip`, expect.any(Object));
    expect(apiClient.put).toHaveBeenCalledWith(`/api/topics/${encodedTopic}`, expect.any(Object));
    expect(apiClient.delete).toHaveBeenCalledWith(`/api/topics/${encodedTopic}`);
    expect(apiClient.delete).toHaveBeenCalledWith(`/api/topics/${encodedTopic}/brokers/${encodedBroker}`);
  });

  it('omits the config query string when no broker is selected', () => {
    topicApi.config('orders');

    expect(apiClient.get).toHaveBeenCalledWith('/api/topics/orders/config');
  });
});
