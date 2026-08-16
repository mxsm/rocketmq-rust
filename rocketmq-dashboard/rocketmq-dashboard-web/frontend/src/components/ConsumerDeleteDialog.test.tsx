import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupListItem, ConsumerOperationResult } from '../types/consumer';
import ConsumerDeleteDialog from './ConsumerDeleteDialog';

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    brokers: vi.fn(),
    delete: vi.fn()
  }
}));

const consumer: ConsumerGroupListItem = {
  displayGroupName: 'orders-consumer',
  rawGroupName: 'orders-consumer',
  category: 'NORMAL',
  connectionCount: 1,
  consumeTps: 0,
  diffTotal: 0,
  messageModel: 'CLUSTERING',
  consumeType: 'CONSUME_PASSIVELY',
  version: 1,
  versionDesc: 'V5_3_0',
  brokerNames: ['broker-a', 'broker-b'],
  brokerAddresses: ['127.0.0.1:10911', '127.0.0.2:10911'],
  updateTimestamp: 0
};

const successResult: ConsumerOperationResult = {
  operation: 'DELETE',
  consumerGroup: 'orders-consumer',
  success: true,
  targetCount: 2,
  message: 'deleted',
  targets: [
    { target: 'broker-a', kind: 'BROKER', success: true, message: 'deleted' },
    { target: 'broker-b', kind: 'BROKER', success: true, message: 'deleted' }
  ]
};

describe('ConsumerDeleteDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(consumerApi.brokers).mockResolvedValue({
      items: [
        { brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911' },
        { brokerName: 'broker-b', brokerAddress: '127.0.0.2:10911' }
      ]
    });
  });

  it('requires exact group confirmation and full success before closing', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    vi.mocked(consumerApi.delete).mockResolvedValue(successResult);
    render(
      <ConsumerDeleteDialog
        open
        consumer={consumer}
        onOpenChange={onOpenChange}
        onSucceeded={onSucceeded}
      />
    );

    const dialog = screen.getByRole('dialog', { name: 'Delete consumer group' });
    const confirm = within(dialog).getByRole('button', { name: 'Delete consumer group' });
    expect(confirm).toBeDisabled();
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'orders-consumer');
    expect(confirm).toBeEnabled();
    await user.click(confirm);

    expect(consumerApi.delete).toHaveBeenCalledWith('orders-consumer', { brokerNames: ['broker-a'] });
    await waitFor(() => expect(onSucceeded).toHaveBeenCalledWith(successResult));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('keeps partial outcomes open and renders every target result', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    vi.mocked(consumerApi.delete).mockResolvedValue({
      ...successResult,
      success: false,
      targets: [
        { target: 'broker-a', kind: 'BROKER', success: true, message: 'deleted' },
        { target: 'broker-b', kind: 'BROKER', success: false, message: 'unavailable' }
      ]
    });
    render(
      <ConsumerDeleteDialog
        open
        consumer={consumer}
        onOpenChange={onOpenChange}
        onSucceeded={onSucceeded}
      />
    );

    const dialog = screen.getByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'orders-consumer');
    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));

    expect(await within(dialog).findByText((content) => content.includes('unavailable'))).toBeInTheDocument();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
    expect(onSucceeded).not.toHaveBeenCalled();
  });
});
