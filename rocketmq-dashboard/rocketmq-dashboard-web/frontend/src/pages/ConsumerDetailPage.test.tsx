import { screen } from '@testing-library/react';
import { Route, Routes } from 'react-router-dom';
import { vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { renderAtRoute } from '../test/render';
import ConsumerDetailPage from './ConsumerDetailPage';

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    list: vi.fn(),
    progress: vi.fn(),
    resetOffset: vi.fn()
  }
}));

describe('ConsumerDetailPage', () => {
  it('resolves group identity and renders the reusable progress content on a direct route', async () => {
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [{ group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 3, diffTotal: 12 }],
      total: 1
    });
    vi.mocked(consumerApi.progress).mockResolvedValue({ group: 'order-service', topicCount: 1, diffTotal: 12, queues: [] });

    renderAtRoute(
      <Routes>
        <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
      </Routes>,
      '/consumers/order-service'
    );

    expect(await screen.findByRole('heading', { name: 'order-service' })).toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Connected clients: 3' })).toBeInTheDocument();
    expect(consumerApi.list).toHaveBeenCalledTimes(1);
    expect(consumerApi.progress).toHaveBeenCalledWith('order-service');
  });
});
