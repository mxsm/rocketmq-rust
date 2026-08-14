import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { producerApi } from '../api/producer_api';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import type { ProducerConnectionView, ProducerInfo } from '../types/producer';
import ProducerListPage from './ProducerListPage';

vi.mock('../api/producer_api', () => ({
  producerApi: {
    list: vi.fn(),
    connections: vi.fn()
  }
}));

vi.mock('../api/topic_api', () => ({
  topicApi: {
    list: vi.fn()
  }
}));

const producers: ProducerInfo[] = [
  { topic: '', producerGroup: 'order-producer', connectionCount: 3 },
  { topic: '', producerGroup: 'payment-producer', connectionCount: 4 },
  { topic: '', producerGroup: 'audit-producer', connectionCount: 0 }
];

const topics = [
  { topic: 'orders', brokerName: 'broker-a', readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL' },
  { topic: 'payment-events', brokerName: 'broker-a', readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL' },
  { topic: 'refund-events', brokerName: 'broker-b', readQueueCount: 4, writeQueueCount: 4, perm: 6, category: 'NORMAL' },
  { topic: 'audit-log', brokerName: 'broker-b', readQueueCount: 4, writeQueueCount: 4, perm: 6, category: 'NORMAL' }
];

const connectionView: ProducerConnectionView = {
  topic: 'payment-events',
  producerGroup: 'payment-producer',
  connections: [
    { clientId: 'payment-producer-1', clientAddr: '10.0.0.12:10911', language: 'JAVA', version: '5.2.0' },
    { clientId: 'payment-producer-2', clientAddr: '10.0.0.13:10911', language: 'RUST', version: '5.0.0' }
  ]
};

describe('ProducerListPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(producerApi.list).mockResolvedValue(producers);
    vi.mocked(topicApi.list).mockResolvedValue({ items: topics, total: topics.length });
    vi.mocked(producerApi.connections).mockResolvedValue(connectionView);
  });

  it('renders real discovery metrics and filters production rows with blank topics', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProducerListPage />, '/producers');

    expect(screen.getByRole('status', { name: 'Loading producers' })).toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Producer groups: 3' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Available topics: 4' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Discovered connections: 7' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connected groups: 2' })).toBeInTheDocument();
    expect(screen.queryByText(/publish TPS|success rate|latency/i)).not.toBeInTheDocument();

    await user.type(screen.getByRole('searchbox', { name: 'Filter producers' }), 'audit-producer');
    expect(screen.getByRole('row', { name: /audit-producer/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /order-producer/ })).not.toBeInTheDocument();
  });

  it('requires a real topic selection before querying a producer group and opens client detail', async () => {
    const user = userEvent.setup();
    let resolveConnections: (value: ProducerConnectionView) => void = () => undefined;
    vi.mocked(producerApi.connections).mockReturnValueOnce(new Promise((resolve) => { resolveConnections = resolve; }));
    renderAtRoute(<ProducerListPage />, '/producers');
    await screen.findByRole('heading', { name: 'Producers' });

    const producerRow = screen.getByRole('row', { name: /payment-producer/ });
    await user.click(producerRow);
    expect(producerApi.connections).not.toHaveBeenCalled();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Producer topic' }), 'payment-events');
    await user.click(screen.getByRole('button', { name: 'Query producer connections' }));
    expect(producerApi.connections).toHaveBeenCalledWith('payment-events', 'payment-producer');
    expect(screen.getByRole('status', { name: 'Loading producer connections' })).toBeInTheDocument();
    resolveConnections(connectionView);

    const clientRow = await screen.findByRole('row', { name: /payment-producer-1 10.0.0.12:10911 JAVA 5.2.0/ });
    await user.click(clientRow);
    const dialog = await screen.findByRole('dialog', { name: 'payment-producer-1' });
    expect(within(dialog).getByText('10.0.0.12:10911')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(clientRow).toHaveFocus());
  });

  it('supports retry and empty states for connection queries', async () => {
    const user = userEvent.setup();
    vi.mocked(producerApi.connections)
      .mockRejectedValueOnce(new Error('connection lookup unavailable'))
      .mockResolvedValue({ topic: 'orders', producerGroup: 'order-producer', connections: [] });
    renderAtRoute(<ProducerListPage />, '/producers');
    await screen.findByRole('heading', { name: 'Producers' });

    await user.click(screen.getByRole('row', { name: /order-producer/ }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Producer topic' }), 'orders');
    await user.click(screen.getByRole('button', { name: 'Query producer connections' }));
    expect(await screen.findByText('connection lookup unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry connection query' }));
    expect(await screen.findByText('No producer connections')).toBeInTheDocument();
    expect(producerApi.connections).toHaveBeenCalledTimes(2);
  });

  it('ignores stale connection responses after the selected producer changes', async () => {
    const user = userEvent.setup();
    let resolveOrders: (value: ProducerConnectionView) => void = () => undefined;
    let resolvePayments: (value: ProducerConnectionView) => void = () => undefined;
    vi.mocked(producerApi.connections)
      .mockReturnValueOnce(new Promise((resolve) => { resolveOrders = resolve; }))
      .mockReturnValueOnce(new Promise((resolve) => { resolvePayments = resolve; }));
    renderAtRoute(<ProducerListPage />, '/producers');
    await screen.findByRole('heading', { name: 'Producers' });

    await user.click(screen.getByRole('row', { name: /order-producer/ }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Producer topic' }), 'orders');
    await user.click(screen.getByRole('button', { name: 'Query producer connections' }));

    await user.click(screen.getByRole('row', { name: /payment-producer/ }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Producer topic' }), 'payment-events');
    await user.click(screen.getByRole('button', { name: 'Query producer connections' }));

    resolvePayments(connectionView);
    expect(await screen.findByText('payment-producer-1')).toBeInTheDocument();
    resolveOrders({
      topic: 'orders',
      producerGroup: 'order-producer',
      connections: [{ clientId: 'stale-order-client', clientAddr: '10.0.0.99:10911', language: 'JAVA', version: '5.2.0' }]
    });
    await waitFor(() => expect(screen.queryByText('stale-order-client')).not.toBeInTheDocument());
    expect(screen.getByText('payment-producer-1')).toBeInTheDocument();
    expect(screen.getByText(/payment-producer · payment-events/)).toBeInTheDocument();
  });

  it('explains an empty topic inventory and offers a reload action', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce({ items: [], total: 0 })
      .mockResolvedValueOnce({ items: topics, total: topics.length });
    renderAtRoute(<ProducerListPage />, '/producers');
    await screen.findByRole('heading', { name: 'Producers' });

    await user.click(screen.getByRole('row', { name: /order-producer/ }));
    expect(screen.getByText('No topic targets available')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Refresh topics' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    expect(await screen.findByRole('option', { name: 'orders' })).toBeInTheDocument();
  });
});
