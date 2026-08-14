import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { consumerApi } from '../../api/consumer_api';
import type { ConsumerGroupInfo, ConsumerProgress } from '../../types/consumer';
import ConsumerDetailContent from './ConsumerDetailContent';

vi.mock('../../api/consumer_api', () => ({
  consumerApi: {
    list: vi.fn(),
    progress: vi.fn(),
    resetOffset: vi.fn()
  }
}));

const consumer: ConsumerGroupInfo = {
  group: 'order-service',
  consumeType: 'CONSUME_PASSIVELY',
  messageModel: 'MESSAGE_MODEL_CLUSTERING',
  clientCount: 3,
  diffTotal: 120
};

const progress: ConsumerProgress = {
  group: 'order-service',
  topicCount: 1,
  diffTotal: 120,
  queues: [
    { topic: 'orders', brokerName: 'broker-a', queueId: 0, brokerOffset: 500, consumerOffset: 380, diff: 120 }
  ]
};

describe('ConsumerDetailContent', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(consumerApi.list).mockResolvedValue({ items: [consumer], total: 1 });
    vi.mocked(consumerApi.progress).mockResolvedValue(progress);
    vi.mocked(consumerApi.resetOffset).mockResolvedValue({ message: 'reset' });
  });

  it('reuses supplied group identity and renders API-backed progress', async () => {
    const user = userEvent.setup();
    render(<ConsumerDetailContent group="order-service" consumer={consumer} />);

    expect(await screen.findByRole('group', { name: 'Total lag: 120' })).toBeInTheDocument();
    expect(consumerApi.list).not.toHaveBeenCalled();
    expect(consumerApi.progress).toHaveBeenCalledWith('order-service');

    await user.click(screen.getByRole('tab', { name: 'Progress' }));
    expect(screen.getByRole('row', { name: /orders broker-a 0 500 380 120/ })).toBeInTheDocument();
  });

  it('validates and confirms the exact reset-offset request before calling the API', async () => {
    const user = userEvent.setup();
    render(<ConsumerDetailContent group="order-service" consumer={consumer} />);
    await screen.findByRole('group', { name: 'Total lag: 120' });

    await user.click(screen.getByRole('tab', { name: 'Reset offset' }));
    const timestamp = screen.getByRole('spinbutton', { name: 'Reset timestamp' });
    await user.clear(timestamp);
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    expect(screen.getByRole('status')).toHaveTextContent('Reset timestamp must be a millisecond timestamp.');
    expect(consumerApi.resetOffset).not.toHaveBeenCalled();

    for (const invalidTimestamp of ['-1', '1.5', '9007199254740992']) {
      await user.clear(timestamp);
      await user.type(timestamp, invalidTimestamp);
      await user.click(screen.getByRole('button', { name: 'Review reset' }));
      expect(screen.getByRole('status')).toHaveTextContent('Reset timestamp must be a non-negative safe integer.');
      expect(consumerApi.resetOffset).not.toHaveBeenCalled();
    }

    await user.clear(timestamp);
    await user.type(timestamp, '1723651200000');
    await user.click(screen.getByRole('checkbox', { name: 'Force reset' }));
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    const confirmation = screen.getByRole('alertdialog', { name: 'Reset consumer offset?' });
    expect(consumerApi.resetOffset).not.toHaveBeenCalled();
    await user.click(within(confirmation).getByRole('button', { name: 'Confirm reset' }));

    await waitFor(() => expect(consumerApi.resetOffset).toHaveBeenCalledWith('order-service', {
      topic: 'orders',
      resetTimestamp: 1_723_651_200_000,
      force: true
    }));
    expect(await screen.findByText('Offsets reset for order-service on orders.')).toBeInTheDocument();
  });

  it('surfaces and retries a progress refresh failure after a successful reset', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.progress)
      .mockResolvedValueOnce(progress)
      .mockRejectedValueOnce(new Error('progress refresh unavailable'))
      .mockResolvedValueOnce({ ...progress, diffTotal: 0, queues: [{ ...progress.queues[0], consumerOffset: 500, diff: 0 }] });
    render(<ConsumerDetailContent group="order-service" consumer={consumer} />);
    await screen.findByRole('group', { name: 'Total lag: 120' });

    await user.click(screen.getByRole('tab', { name: 'Reset offset' }));
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Reset consumer offset?' })).getByRole('button', { name: 'Confirm reset' }));

    expect(await screen.findByText('Offsets reset for order-service on orders.')).toBeInTheDocument();
    expect(await screen.findByText('progress refresh unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry progress refresh' }));
    await waitFor(() => expect(consumerApi.progress).toHaveBeenCalledTimes(3));
    expect(screen.queryByText('progress refresh unavailable')).not.toBeInTheDocument();
  });

  it('ignores an old reset completion after the selected consumer group changes', async () => {
    const user = userEvent.setup();
    let resolveReset: (value: { message: string }) => void = () => undefined;
    const paymentConsumer: ConsumerGroupInfo = { ...consumer, group: 'payment-service', diffTotal: 8 };
    const paymentProgress: ConsumerProgress = {
      ...progress,
      group: 'payment-service',
      diffTotal: 8,
      queues: [{ ...progress.queues[0], topic: 'payments', diff: 8 }]
    };
    vi.mocked(consumerApi.progress)
      .mockResolvedValueOnce(progress)
      .mockResolvedValueOnce(paymentProgress);
    vi.mocked(consumerApi.resetOffset).mockReturnValueOnce(new Promise((resolve) => { resolveReset = resolve; }));

    const { rerender } = render(<ConsumerDetailContent group="order-service" consumer={consumer} />);
    await screen.findByRole('group', { name: 'Total lag: 120' });
    await user.click(screen.getByRole('tab', { name: 'Reset offset' }));
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Reset consumer offset?' })).getByRole('button', { name: 'Confirm reset' }));
    await waitFor(() => expect(consumerApi.resetOffset).toHaveBeenCalledWith('order-service', expect.any(Object)));

    rerender(<ConsumerDetailContent group="payment-service" consumer={paymentConsumer} />);
    expect(await screen.findByRole('group', { name: 'Total lag: 8' })).toBeInTheDocument();
    await act(async () => resolveReset({ message: 'reset' }));

    await waitFor(() => expect(screen.queryByText(/Offsets reset for order-service/)).not.toBeInTheDocument());
    expect(consumerApi.progress).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('group', { name: 'Total lag: 8' })).toBeInTheDocument();
  });
});
