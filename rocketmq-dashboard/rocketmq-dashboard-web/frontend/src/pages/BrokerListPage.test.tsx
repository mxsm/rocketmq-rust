import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { brokerApi } from '../api/broker_api';
import { renderAtRoute } from '../test/render';
import type { BrokerInfo } from '../types/broker';
import BrokerListPage from './BrokerListPage';

vi.mock('../api/broker_api', () => ({
  brokerApi: {
    list: vi.fn(),
    runtime: vi.fn(),
    config: vi.fn(),
    updateConfig: vi.fn()
  }
}));

const brokers: BrokerInfo[] = [
  {
    clusterName: 'east', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER',
    version: '5.3.2', produceTps: 120, consumeTps: 80
  },
  {
    clusterName: 'east', brokerName: 'broker-b', brokerId: 1, address: '10.0.0.2:10911', role: 'SLAVE',
    version: '5.3.2', produceTps: 0, consumeTps: 60
  },
  {
    clusterName: 'west', brokerName: 'broker-c', brokerId: 0, address: '10.1.0.1:10911', role: 'MASTER',
    version: '5.2.0', produceTps: 35.5, consumeTps: 11.1
  }
];

describe('BrokerListPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(brokerApi.list).mockResolvedValue({ items: brokers, total: brokers.length });
  });

  it('renders cluster evidence metrics and the operational table', async () => {
    renderAtRoute(<BrokerListPage />, '/brokers');

    expect(screen.getByRole('status', { name: 'Loading brokers' })).toBeInTheDocument();
    expect(await screen.findByRole('heading', { name: 'Cluster inventory' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Clusters: 2' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Brokers: 3' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Roles: 2' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Combined TPS: 306.6' })).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Broker inventory' })).toBeInTheDocument();
  });

  it('combines search, cluster, and role filters and resets them', async () => {
    const user = userEvent.setup();
    renderAtRoute(<BrokerListPage />, '/brokers');
    await screen.findByText('broker-a');

    await user.selectOptions(screen.getByRole('combobox', { name: 'Cluster filter' }), 'east');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Role filter' }), 'MASTER');
    await user.type(screen.getByRole('searchbox', { name: 'Filter brokers' }), 'broker-a');
    expect(screen.getByText('broker-a')).toBeInTheDocument();
    expect(screen.queryByText('broker-b')).not.toBeInTheDocument();
    expect(screen.queryByText('broker-c')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(screen.getByText('broker-b')).toBeInTheDocument();
    expect(screen.getByText('broker-c')).toBeInTheDocument();
  });

  it('opens a broker sheet from the row and preserves a direct detail link', async () => {
    const user = userEvent.setup();
    renderAtRoute(<BrokerListPage />, '/brokers');
    await screen.findByText('broker-a');

    const row = screen.getByText('broker-a').closest('tr');
    expect(row).not.toBeNull();
    expect(within(row as HTMLElement).getByRole('link', { name: 'View broker-a' })).toHaveAttribute('href', '/brokers/broker-a');
    await user.click(within(row as HTMLElement).getByRole('button', { name: 'Inspect broker-a' }));

    expect(await screen.findByRole('dialog', { name: 'broker-a' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Runtime' })).toBeInTheDocument();
  });

  it('shows a load error and retries the inventory request', async () => {
    const user = userEvent.setup();
    vi.mocked(brokerApi.list).mockRejectedValueOnce(new Error('inventory unavailable'));
    renderAtRoute(<BrokerListPage />, '/brokers');

    expect(await screen.findByRole('alert')).toHaveTextContent('inventory unavailable');
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('broker-a')).toBeInTheDocument();
    await waitFor(() => expect(brokerApi.list).toHaveBeenCalledTimes(2));
  });
});
