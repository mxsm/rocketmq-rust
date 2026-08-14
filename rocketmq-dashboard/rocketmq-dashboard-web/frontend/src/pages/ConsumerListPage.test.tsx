import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { renderAtRoute } from '../test/render';
import type { ConsumerGroupInfo } from '../types/consumer';
import ConsumerListPage from './ConsumerListPage';

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    list: vi.fn(),
    progress: vi.fn(),
    resetOffset: vi.fn()
  }
}));

const consumers: ConsumerGroupInfo[] = [
  { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 6, diffTotal: 8_700 },
  { group: 'payment-broadcast', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_BROADCASTING', clientCount: 2, diffTotal: 0 },
  { group: 'audit-puller', consumeType: 'CONSUME_ACTIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 0, diffTotal: 25 },
  ...Array.from({ length: 8 }, (_, index) => ({
    group: `worker-${index}`,
    consumeType: 'CONSUME_PASSIVELY',
    messageModel: 'MESSAGE_MODEL_CLUSTERING',
    clientCount: 1,
    diffTotal: 0
  }))
];

describe('ConsumerListPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(consumerApi.list).mockResolvedValue({ items: consumers, total: consumers.length });
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service',
      topicCount: 1,
      diffTotal: 8_700,
      queues: [{ topic: 'orders', brokerName: 'broker-a', queueId: 0, brokerOffset: 10_000, consumerOffset: 1_300, diff: 8_700 }]
    });
    vi.mocked(consumerApi.resetOffset).mockResolvedValue({ message: 'reset' });
  });

  it('renders API-backed totals, combines filters, and paginates the inventory', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConsumerListPage />, '/consumers');

    expect(screen.getByRole('status', { name: 'Loading consumers' })).toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Consumer groups: 11' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connected clients: 16' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Total lag: 8725' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Lagging groups: 2' })).toBeInTheDocument();

    await user.selectOptions(screen.getByRole('combobox', { name: 'Consume type filter' }), 'ACTIVELY');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Message model filter' }), 'CLUSTERING');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Lag filter' }), 'lagging');
    await user.type(screen.getByRole('searchbox', { name: 'Filter consumer groups' }), 'audit');
    expect(screen.getByRole('row', { name: /audit-puller/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /order-service/ })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled();
    await user.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getByRole('row', { name: /worker-7/ })).toBeInTheDocument();
  });

  it('opens reusable progress details from a row and exposes no unsupported create or delete action', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConsumerListPage />, '/consumers');
    await screen.findByRole('heading', { name: 'Consumer groups' });

    expect(screen.queryByRole('button', { name: /create/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /delete/i })).not.toBeInTheDocument();

    const row = screen.getByRole('row', { name: /order-service/ });
    await user.click(screen.getByText('order-service'));
    expect(await screen.findByRole('dialog', { name: 'order-service' })).toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Total lag: 8700' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(row).toHaveFocus());
  });

  it('restores focus to the exact menu trigger that opened a detail sheet', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConsumerListPage />, '/consumers');
    await screen.findByRole('heading', { name: 'Consumer groups' });

    const orderRow = screen.getByRole('row', { name: /order-service/ });
    await user.click(screen.getByText('order-service'));
    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(orderRow).toHaveFocus());

    const paymentTrigger = screen.getByRole('button', { name: 'Actions for payment-broadcast' });
    await user.click(paymentTrigger);
    await user.click(screen.getByRole('menuitem', { name: 'View progress' }));
    expect(await screen.findByRole('dialog', { name: 'payment-broadcast' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(paymentTrigger).toHaveFocus());
  });

  it('shows a retryable list error and refreshes a successful inventory', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.list)
      .mockRejectedValueOnce(new Error('consumer service unavailable'))
      .mockResolvedValue({ items: consumers, total: consumers.length });
    renderAtRoute(<ConsumerListPage />, '/consumers');

    expect(await screen.findByText('consumer service unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('heading', { name: 'Consumer groups' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() => expect(consumerApi.list).toHaveBeenCalledTimes(3));
  });
});
