import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { brokerApi } from '../api/broker_api';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerGroupListItem, ConsumerOperationResult } from '../types/consumer';
import { ConsumerQueryScopeProvider } from '../pages/consumers/ConsumerQueryScopeProvider';
import ConsumerMutationDialog from './ConsumerMutationDialog';

vi.mock('../api/broker_api', () => ({
  brokerApi: { list: vi.fn() }
}));

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    create: vi.fn(),
    update: vi.fn(),
    config: vi.fn()
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
  brokerNames: ['broker-a'],
  brokerAddresses: ['127.0.0.1:10911'],
  updateTimestamp: 0
};

const successResult: ConsumerOperationResult = {
  operation: 'CREATE',
  consumerGroup: 'orders-consumer',
  success: true,
  targetCount: 1,
  message: 'created',
  targets: [{ target: 'broker-a', kind: 'BROKER', success: true, message: 'saved' }]
};

describe('ConsumerMutationDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 0, address: '127.0.0.1:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
    });
  });

  it('requires a group and target for create', async () => {
    const user = userEvent.setup();
    const onSucceeded = vi.fn();
    render(<ConsumerQueryScopeProvider><ConsumerMutationDialog open mode="create" onOpenChange={vi.fn()} onSucceeded={onSucceeded} /></ConsumerQueryScopeProvider>);

    const dialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    const save = within(dialog).getByRole('button', { name: 'Create group' });
    expect(save).toBeEnabled();
    await user.click(save);
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('Consumer group is required.');
    expect(consumerApi.create).not.toHaveBeenCalled();

    await user.type(within(dialog).getByLabelText('Consumer group'), 'orders-consumer');
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.click(save);
    expect(consumerApi.create).toHaveBeenCalled();
  });

  it('prefills edit targets and configuration from the current scope', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.config).mockResolvedValue({
      group: 'orders-consumer',
      effective: {
        consumeEnable: true,
        consumeFromMinEnable: true,
        consumeBroadcastEnable: false,
        consumeMessageOrderly: false,
        retryQueueNums: 4,
        retryMaxTimes: 8,
        brokerId: 0,
        whichBrokerWhenConsumeSlowly: 1,
        notifyConsumerIdsChangedEnable: true,
        groupSysFlag: 0,
        consumeTimeoutMinute: 20,
        groupRetryPolicyJson: '{}'
      },
      inconsistentFields: [],
      targets: [{
        brokerName: 'broker-a',
        brokerAddress: '127.0.0.1:10911',
        config: {
          consumeEnable: true,
          consumeFromMinEnable: true,
          consumeBroadcastEnable: false,
          consumeMessageOrderly: false,
          retryQueueNums: 4,
          retryMaxTimes: 8,
          brokerId: 0,
          whichBrokerWhenConsumeSlowly: 1,
          notifyConsumerIdsChangedEnable: true,
          groupSysFlag: 0,
          consumeTimeoutMinute: 20,
          groupRetryPolicyJson: '{}'
        },
        subscriptionTopics: [],
        attributes: []
      }],
      queryScope: { mode: 'nameServer' }
    });
    render(<ConsumerQueryScopeProvider><ConsumerMutationDialog open mode="edit" consumer={consumer} onOpenChange={vi.fn()} onSucceeded={vi.fn()} /></ConsumerQueryScopeProvider>);

    const dialog = screen.getByRole('dialog', { name: 'Edit orders-consumer' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    expect(within(dialog).getByLabelText('Retry queue nums')).toHaveValue(4);
    expect(within(dialog).getByLabelText('Consume timeout minutes')).toHaveValue(20);
  });
});
