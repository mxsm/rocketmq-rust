import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { topicApi } from '../../api/topic_api';
import { renderAtRoute } from '../../test/render';
import type { TopicInfo } from '../../types/topic';
import TopicDetailContent from './TopicDetailContent';

vi.mock('../../api/topic_api', () => ({
  topicApi: {
    get: vi.fn(),
    route: vi.fn(),
    stats: vi.fn()
  }
}));

const topic: TopicInfo = {
  topic: 'orders',
  brokerName: 'broker-a',
  readQueueCount: 8,
  writeQueueCount: 8,
  perm: 6,
  category: 'NORMAL'
};

describe('TopicDetailContent', () => {
  beforeEach(() => {
    vi.mocked(topicApi.get).mockResolvedValue(topic);
    vi.mocked(topicApi.stats).mockResolvedValue({
      topic: 'orders',
      queueCount: 2,
      totalMinOffset: 120,
      totalMaxOffset: 8_400
    });
    vi.mocked(topicApi.route).mockResolvedValue({
      topic: 'orders',
      brokers: [{ brokerName: 'broker-a', brokerAddrs: ['127.0.0.1:10911'] }],
      queues: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6 }]
    });
  });

  it('loads API-backed sections lazily and caches them for the selected topic', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicDetailContent topicName="orders" topic={topic} />, '/topics');

    expect(topicApi.get).not.toHaveBeenCalled();
    expect(await screen.findByRole('group', { name: 'Queue entries: 2' })).toBeInTheDocument();
    expect(topicApi.route).not.toHaveBeenCalled();

    await user.click(screen.getByRole('tab', { name: 'Routes' }));
    expect(await screen.findByRole('row', { name: /broker-a.*8.*8.*RW/ })).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'Overview' }));
    await user.click(screen.getByRole('tab', { name: 'Routes' }));

    expect(topicApi.stats).toHaveBeenCalledTimes(1);
    expect(topicApi.route).toHaveBeenCalledTimes(1);
  });

  it('loads topic identity for a direct route when list context is unavailable', async () => {
    renderAtRoute(<TopicDetailContent topicName="orders" />, '/topics/orders');

    expect(await screen.findByText('broker-a')).toBeInTheDocument();
    expect(topicApi.get).toHaveBeenCalledWith('orders');
  });

  it('keeps incomplete topic configuration read-only', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicDetailContent topicName="orders" topic={topic} />, '/topics');
    await screen.findByRole('group', { name: 'Queue entries: 2' });

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(screen.getByText('Read queues')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit topic' })).not.toBeInTheDocument();
  });
});
