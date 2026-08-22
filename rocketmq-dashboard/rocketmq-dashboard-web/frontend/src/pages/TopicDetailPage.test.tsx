import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Route, Routes } from 'react-router-dom';
import { vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import TopicDetailPage from './TopicDetailPage';

vi.mock('../api/topic_api', () => ({
  topicApi: {
    get: vi.fn(),
    route: vi.fn(),
    stats: vi.fn(),
    update: vi.fn()
  }
}));

describe('TopicDetailPage', () => {
  it('reuses lazy topic detail content on the direct route', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.get).mockResolvedValue({
      topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'],
      readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL',
      order: false, systemTopic: false
    });
    vi.mocked(topicApi.stats).mockResolvedValue({
      topic: 'orders', queueCount: 2, totalMessageCount: 8_280,
      totalMinOffset: 120, totalMaxOffset: 8_400, offsets: []
    });
    vi.mocked(topicApi.route).mockResolvedValue({
      topic: 'orders',
      brokers: [{ brokerName: 'broker-a', brokerAddrs: ['127.0.0.1:10911'] }],
      queues: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6 }]
    });
    vi.mocked(topicApi.update).mockResolvedValue({
      operation: 'UPDATE', topic: 'orders', success: true, targetCount: 1,
      message: 'updated', targets: [{ target: 'broker-a', success: true, message: 'updated' }]
    });

    renderAtRoute(
      <Routes><Route path="/topics/:topic" element={<TopicDetailPage />} /></Routes>,
      '/topics/orders'
    );

    expect(await screen.findByRole('heading', { name: 'orders' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'orders' }).closest('[data-surface="frosted"]')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Back to topics' })).toHaveAttribute('href', '/topics');
    expect(await screen.findByRole('group', { name: 'Queue entries: 2' })).toBeInTheDocument();
    expect(topicApi.route).not.toHaveBeenCalled();

    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
    expect(await screen.findByRole('row', { name: /broker-a.*8.*8.*RW/ })).toBeInTheDocument();
  });
});
