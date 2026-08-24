import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { brokerApi } from '../api/broker_api';
import { consumerApi } from '../api/consumer_api';
import { ApiClientError } from '../api/client';
import type { ConsumerGroupListItem, ConsumerOperationResult } from '../types/consumer';
import { ConsumerQueryScopeProvider } from '../pages/consumers/ConsumerQueryScopeProvider';
import ConsumerMutationDialog from './ConsumerMutationDialog';
import { resetConsumerMutationLocksForTests } from './consumerMutationLock';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

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
    resetConsumerMutationLocksForTests();
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
    await waitFor(() => expect(save).toBeEnabled());
    await user.click(save);
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('Consumer group is required.');
    expect(consumerApi.create).not.toHaveBeenCalled();

    await user.type(within(dialog).getByLabelText('Consumer group'), 'orders-consumer');
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await waitFor(() => expect(save).toBeEnabled());
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

  afterEach(() => {
    resetConsumerMutationLocksForTests();
  });

  it('closes and refreshes once instead of retrying after an applied audit failure', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onAppliedAuditFailure = vi.fn().mockResolvedValue(undefined);
    vi.mocked(consumerApi.create).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer mutation applied.', { mutationApplied: true })
    );
    render(<ConsumerQueryScopeProvider><ConsumerMutationDialog open mode="create" onOpenChange={onOpenChange} onSucceeded={vi.fn()} onAppliedAuditFailure={onAppliedAuditFailure} /></ConsumerQueryScopeProvider>);

    const dialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    await user.type(within(dialog).getByLabelText('Consumer group'), 'orders-consumer');
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(dialog).getByRole('button', { name: 'Create group' }));

    await waitFor(() => expect(consumerApi.create).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(onOpenChange).toHaveBeenCalledWith(false));
    expect(onAppliedAuditFailure).toHaveBeenCalledTimes(1);
  });

  it('keeps the immutable create target locked across remount and discards its stale applied callback', async () => {
    const user = userEvent.setup();
    const pendingCreate = deferred<ConsumerOperationResult>();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    const onAppliedAuditFailure = vi.fn();
    const auditWarning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    vi.mocked(consumerApi.create).mockImplementationOnce(() => pendingCreate.promise);
    const renderDialog = (mounted: boolean) => (
      <ConsumerQueryScopeProvider>
        {mounted ? (
          <ConsumerMutationDialog
            open
            mode="create"
            onOpenChange={onOpenChange}
            onSucceeded={onSucceeded}
            onAppliedAuditFailure={onAppliedAuditFailure}
          />
        ) : null}
      </ConsumerQueryScopeProvider>
    );
    const { rerender } = render(renderDialog(true));

    let dialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    await user.type(within(dialog).getByLabelText('Consumer group'), 'orders-consumer');
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(dialog).getByRole('button', { name: 'Create group' }));
    await waitFor(() => expect(consumerApi.create).toHaveBeenCalledTimes(1));

    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Close dialog' })).toBeDisabled();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    fireEvent.pointerDown(document.querySelector('.ui-overlay')!);
    expect(onOpenChange).not.toHaveBeenCalled();

    rerender(renderDialog(false));
    rerender(renderDialog(true));
    dialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    const remountedGroup = within(dialog).getByLabelText('Consumer group');
    expect(remountedGroup).toBeDisabled();
    await user.type(remountedGroup, 'inventory-consumer');
    expect(remountedGroup).toHaveValue('');
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Close dialog' })).toBeDisabled();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    fireEvent.pointerDown(document.querySelector('.ui-overlay')!);
    expect(onOpenChange).not.toHaveBeenCalled();
    expect(within(dialog).getByRole('button', { name: 'Create group' })).toBeDisabled();

    await act(async () => {
      window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer create was applied.' }));
      pendingCreate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer create was applied.', { mutationApplied: true }));
    });
    await waitFor(() => expect(within(screen.getByRole('dialog', { name: 'Create consumer group' })).getByRole('button', { name: 'Create group' })).toBeEnabled());
    expect(onAppliedAuditFailure).not.toHaveBeenCalled();
    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(consumerApi.create).toHaveBeenCalledTimes(1);
    expect(onSucceeded).not.toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: /retry/i })).not.toBeInTheDocument();
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('releases a remounted create target only after the original successful request finally settles', async () => {
    const user = userEvent.setup();
    const pendingCreate = deferred<ConsumerOperationResult>();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    vi.mocked(consumerApi.create).mockImplementationOnce(() => pendingCreate.promise);
    const renderDialog = (mounted: boolean) => (
      <ConsumerQueryScopeProvider>
        {mounted ? <ConsumerMutationDialog open mode="create" onOpenChange={onOpenChange} onSucceeded={onSucceeded} /> : null}
      </ConsumerQueryScopeProvider>
    );
    const { rerender } = render(renderDialog(true));

    const originalDialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    await user.type(within(originalDialog).getByLabelText('Consumer group'), 'orders-consumer');
    await user.click(await within(originalDialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(originalDialog).getByRole('button', { name: 'Create group' }));
    await waitFor(() => expect(consumerApi.create).toHaveBeenCalledTimes(1));
    expect(consumerApi.create).toHaveBeenCalledWith(expect.objectContaining({ consumerGroup: 'orders-consumer' }));

    rerender(renderDialog(false));
    rerender(renderDialog(true));
    const remountedDialog = screen.getByRole('dialog', { name: 'Create consumer group' });
    const groupInput = within(remountedDialog).getByLabelText('Consumer group');
    expect(groupInput).toBeDisabled();
    await user.type(groupInput, 'inventory-consumer');
    expect(groupInput).toHaveValue('');
    fireEvent.keyDown(remountedDialog, { key: 'Escape' });
    fireEvent.pointerDown(document.querySelector('.ui-overlay')!);
    expect(within(remountedDialog).getByRole('button', { name: 'Cancel' })).toBeDisabled();
    expect(onOpenChange).not.toHaveBeenCalled();

    await act(async () => pendingCreate.resolve(successResult));
    await waitFor(() => expect(within(screen.getByRole('dialog', { name: 'Create consumer group' })).getByLabelText('Consumer group')).toBeEnabled());
    expect(consumerApi.create).toHaveBeenCalledTimes(1);
    expect(onSucceeded).not.toHaveBeenCalled();
    expect(onOpenChange).not.toHaveBeenCalled();
  });

  it('retains an edit lock across close and reopen while its applied refresh is still authoritative', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<ConsumerOperationResult>();
    const pendingRefresh = deferred<void>();
    const onAppliedAuditFailure = vi.fn(() => pendingRefresh.promise);
    vi.mocked(consumerApi.config).mockResolvedValue({
      group: consumer.rawGroupName,
      effective: {
        consumeEnable: true, consumeFromMinEnable: true, consumeBroadcastEnable: false, consumeMessageOrderly: false,
        retryQueueNums: 1, retryMaxTimes: 16, brokerId: 0, whichBrokerWhenConsumeSlowly: 1,
        notifyConsumerIdsChangedEnable: true, groupSysFlag: 0, consumeTimeoutMinute: 15, groupRetryPolicyJson: '{}'
      },
      inconsistentFields: [],
      targets: [{ brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911', config: null, subscriptionTopics: [], attributes: [] }],
      queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.update).mockImplementationOnce(() => pendingUpdate.promise);
    const renderDialog = (mounted: boolean) => (
      <ConsumerQueryScopeProvider>
        {mounted ? <ConsumerMutationDialog open mode="edit" consumer={consumer} onOpenChange={vi.fn()} onSucceeded={vi.fn()} onAppliedAuditFailure={onAppliedAuditFailure} /> : null}
      </ConsumerQueryScopeProvider>
    );
    const { rerender } = render(renderDialog(true));
    let dialog = screen.getByRole('dialog', { name: 'Edit orders-consumer' });
    await waitFor(() => expect(within(dialog).getByRole('button', { name: 'Update group' })).toBeEnabled());
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));

    rerender(renderDialog(false));
    rerender(renderDialog(true));
    dialog = screen.getByRole('dialog', { name: 'Edit orders-consumer' });
    await waitFor(() => expect(within(dialog).getByRole('button', { name: 'Update group' })).toBeDisabled());
    await act(async () => pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true })));
    await waitFor(() => expect(onAppliedAuditFailure).toHaveBeenCalledTimes(1));
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole('dialog', { name: 'Edit orders-consumer' })).getByRole('button', { name: 'Update group' })).toBeDisabled();

    await act(async () => pendingRefresh.resolve());
    await waitFor(() => expect(within(screen.getByRole('dialog', { name: 'Edit orders-consumer' })).getByRole('button', { name: 'Update group' })).toBeEnabled());
  });
});
