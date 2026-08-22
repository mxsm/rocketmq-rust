import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { vi } from 'vitest';
import { brokerApi } from '../api/broker_api';
import BrokerDetailPage from './BrokerDetailPage';

vi.mock('../api/broker_api', () => ({
  brokerApi: { list: vi.fn() }
}));

function renderDetailPage() {
  return render(
    <MemoryRouter initialEntries={['/brokers/broker-a']} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <Routes>
        <Route path="/brokers/:brokerName" element={<BrokerDetailPage />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('BrokerDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [
        {
          clusterName: 'DefaultCluster',
          brokerName: 'broker-a',
          brokerId: 0,
          address: '127.0.0.1:10911',
          role: 'MASTER',
          version: 'V5_3_1',
          produceTps: 12.5,
          consumeTps: 7.5
        }
      ],
      total: 1
    });
  });

  it('hydrates the full-page overview from the cluster inventory', async () => {
    renderDetailPage();

    expect(await screen.findByText('DefaultCluster')).toBeInTheDocument();
    expect(screen.getByText('127.0.0.1:10911')).toBeInTheDocument();
    expect(screen.getByText('V5_3_1')).toBeInTheDocument();
    expect(screen.getByText('MASTER')).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Produce TPS: 12.5' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'broker-a' }).closest('[data-surface="frosted"]')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Back to cluster' })).toHaveAttribute('href', '/brokers');
    expect(brokerApi.list).toHaveBeenCalledTimes(1);
    expect(screen.queryByText('Available from cluster inventory')).not.toBeInTheDocument();
  });
});
