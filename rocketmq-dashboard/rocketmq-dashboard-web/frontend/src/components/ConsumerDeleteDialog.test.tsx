import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { ApiClientError } from '../api/client';
import type { ConsumerGroupListItem, ConsumerOperationResult } from '../types/consumer';
import { ConsumerQueryScopeProvider } from '../pages/consumers/ConsumerQueryScopeProvider';
import ConsumerDeleteDialog from './ConsumerDeleteDialog';
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
    resetConsumerMutationLocksForTests();
    vi.clearAllMocks();
    vi.mocked(consumerApi.brokers).mockResolvedValue({
      items: [
        { brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911' },
        { brokerName: 'broker-b', brokerAddress: '127.0.0.2:10911' }
      ]
    });
  });

  afterEach(() => {
    resetConsumerMutationLocksForTests();
  });

  it('requires exact group confirmation and full success before closing', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    vi.mocked(consumerApi.delete).mockResolvedValue(successResult);
    render(
      <ConsumerQueryScopeProvider>
        <ConsumerDeleteDialog
          open
          consumer={consumer}
          onOpenChange={onOpenChange}
          onSucceeded={onSucceeded}
        />
      </ConsumerQueryScopeProvider>
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
      <ConsumerQueryScopeProvider>
        <ConsumerDeleteDialog
          open
          consumer={consumer}
          onOpenChange={onOpenChange}
          onSucceeded={onSucceeded}
        />
      </ConsumerQueryScopeProvider>
    );

    const dialog = screen.getByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'orders-consumer');
    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));

    expect(await within(dialog).findByText((content) => content.includes('unavailable'))).toBeInTheDocument();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
    expect(onSucceeded).not.toHaveBeenCalled();
  });

  it('retains a delete lock across close and reopen until the original applied request settles', async () => {
    const user = userEvent.setup();
    const pendingDelete = deferred<ConsumerOperationResult>();
    const pendingRefresh = deferred<void>();
    const onOpenChange = vi.fn();
    const onAppliedAuditFailure = vi.fn(() => pendingRefresh.promise);
    const auditWarning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    vi.mocked(consumerApi.delete).mockImplementationOnce(() => pendingDelete.promise);
    const renderDialog = (mounted: boolean) => (
      <ConsumerQueryScopeProvider>
        {mounted ? <ConsumerDeleteDialog open consumer={consumer} onOpenChange={onOpenChange} onSucceeded={vi.fn()} onAppliedAuditFailure={onAppliedAuditFailure} /> : null}
      </ConsumerQueryScopeProvider>
    );
    const { rerender } = render(renderDialog(true));

    let dialog = screen.getByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'orders-consumer');
    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));

    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Close dialog' })).toBeDisabled();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    fireEvent.pointerDown(document.querySelector('.ui-overlay')!);
    expect(onOpenChange).not.toHaveBeenCalled();

    rerender(renderDialog(false));
    rerender(renderDialog(true));
    dialog = screen.getByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'orders-consumer');
    expect(within(dialog).getByRole('button', { name: 'Delete consumer group' })).toBeDisabled();

    await act(async () => {
      window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer deletion was applied.' }));
      pendingDelete.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true }));
    });
    await waitFor(() => expect(onAppliedAuditFailure).toHaveBeenCalledTimes(1));
    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole('dialog', { name: 'Delete consumer group' })).getByRole('button', { name: 'Delete consumer group' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: /retry/i })).not.toBeInTheDocument();

    await act(async () => pendingRefresh.resolve());
    await waitFor(() => expect(within(screen.getByRole('dialog', { name: 'Delete consumer group' })).getByRole('button', { name: 'Delete consumer group' })).toBeEnabled());
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });
});
