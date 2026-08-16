import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
import type { TopicInfo, TopicOperationResult } from '../types/topic';
import TopicDeleteDialog from './TopicDeleteDialog';

vi.mock('../api/topic_api', () => ({
  topicApi: {
    delete: vi.fn(),
    deleteFromBroker: vi.fn()
  }
}));

const topic = (name: string, brokers = ['broker-a', 'broker-b']): TopicInfo => ({
  topic: name,
  brokerName: brokers[0] ?? null,
  brokers,
  clusters: ['DefaultCluster'],
  readQueueCount: 8,
  writeQueueCount: 8,
  perm: 6,
  category: 'NORMAL',
  messageType: 'NORMAL',
  order: false,
  systemTopic: false
});

const successResult = (name: string, operation = 'DELETE_TOPIC'): TopicOperationResult => ({
  operation,
  topic: name,
  success: true,
  targetCount: 1,
  message: `${name} deleted`,
  targets: [{ target: 'broker-a', success: true, message: 'deleted from broker-a' }]
});

const defaultProps = {
  open: true,
  topic: topic('orders'),
  mode: 'topic' as const,
  onOpenChange: vi.fn(),
  onSucceeded: vi.fn()
};

beforeAll(() => {
  Object.defineProperty(Element.prototype, 'scrollIntoView', { configurable: true, value: vi.fn() });
});

afterAll(() => {
  Reflect.deleteProperty(Element.prototype, 'scrollIntoView');
});

describe('TopicDeleteDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.delete).mockResolvedValue(successResult('orders'));
    vi.mocked(topicApi.deleteFromBroker).mockResolvedValue(successResult('orders', 'DELETE_BROKER'));
  });

  it('requires the exact topic name and closes only after a full topic delete succeeds', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    render(<TopicDeleteDialog {...defaultProps} onOpenChange={onOpenChange} onSucceeded={onSucceeded} />);

    const dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    expect(dialog).toHaveTextContent('DefaultCluster');
    expect(dialog).toHaveTextContent('broker-a');
    const confirm = within(dialog).getByRole('button', { name: 'Delete topic' });
    expect(confirm).toBeDisabled();

    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'Orders');
    expect(confirm).toBeDisabled();
    await user.clear(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }));
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    expect(confirm).toBeEnabled();
    await user.click(confirm);

    expect(topicApi.delete).toHaveBeenCalledWith('orders');
    expect(topicApi.deleteFromBroker).not.toHaveBeenCalled();
    await waitFor(() => expect(onSucceeded).toHaveBeenCalledWith(successResult('orders')));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('selects only an actual topic broker and submits the captured broker identity', async () => {
    const user = userEvent.setup();
    const selectedTopic = topic('orders', ['broker-a', 'broker-b']);
    render(
      <TopicDeleteDialog
        {...defaultProps}
        topic={selectedTopic}
        mode="broker"
        brokerName="not-on-route"
      />
    );

    const dialog = screen.getByRole('alertdialog', { name: 'Delete topic from broker' });
    const broker = within(dialog).getByRole('combobox', { name: 'Broker' });
    expect(broker).toHaveTextContent('Select a broker');
    const nativeSelect = dialog.querySelector('select');
    expect(nativeSelect).not.toBeNull();
    expect(Array.from(nativeSelect!.options).map((option) => option.value)).toEqual(['', 'broker-a', 'broker-b']);
    fireEvent.change(nativeSelect!, { target: { value: 'broker-b' } });
    expect(broker).toHaveTextContent('broker-b');
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(dialog).getByRole('button', { name: 'Delete from broker' }));

    expect(topicApi.deleteFromBroker).toHaveBeenCalledWith('orders', 'broker-b');
    expect(topicApi.delete).not.toHaveBeenCalled();
  });

  it('fails closed when broker route metadata is missing', async () => {
    const user = userEvent.setup();
    render(<TopicDeleteDialog {...defaultProps} topic={topic('orders', [])} mode="broker" />);

    const dialog = screen.getByRole('alertdialog', { name: 'Delete topic from broker' });
    expect(within(dialog).getByRole('alert')).toHaveTextContent('No broker targets are available');
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    expect(within(dialog).getByRole('button', { name: 'Delete from broker' })).toBeDisabled();
    expect(topicApi.deleteFromBroker).not.toHaveBeenCalled();
  });

  it('fails closed when a whole-topic delete has no authoritative cluster targets', async () => {
    const user = userEvent.setup();
    const missingClusters = { ...topic('orders'), clusters: [' ', '\t'] };
    render(<TopicDeleteDialog {...defaultProps} topic={missingClusters} mode="topic" />);

    const dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    expect(within(dialog).getByRole('alert')).toHaveTextContent('No authoritative cluster targets are available');
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    expect(within(dialog).getByRole('button', { name: 'Delete topic' })).toBeDisabled();
    fireEvent.submit(dialog.querySelector('form')!);
    expect(topicApi.delete).not.toHaveBeenCalled();
    expect(topicApi.deleteFromBroker).not.toHaveBeenCalled();
  });

  it('renders every partial target outcome and keeps the dialog open', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    const onResult = vi.fn();
    vi.mocked(topicApi.delete).mockResolvedValue({
      operation: 'DELETE_TOPIC',
      topic: 'orders',
      success: false,
      targetCount: 2,
      message: '1 of 2 targets failed',
      targets: [
        { target: 'broker-a', success: true, message: 'deleted from broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
    render(
      <TopicDeleteDialog
        {...defaultProps}
        onOpenChange={onOpenChange}
        onResult={onResult}
        onSucceeded={onSucceeded}
      />
    );

    const dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(dialog).getByRole('button', { name: 'Delete topic' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(within(dialog).getByText('deleted from broker-a')).toBeInTheDocument();
    expect(within(dialog).getByText('broker-b unavailable')).toBeInTheDocument();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
    expect(onSucceeded).not.toHaveBeenCalled();
    expect(onResult).toHaveBeenCalledWith({
      operation: 'DELETE_TOPIC',
      topic: 'orders',
      success: false,
      targetCount: 2,
      message: '1 of 2 targets failed',
      targets: [
        { target: 'broker-a', success: true, message: 'deleted from broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
  });

  it('keeps the real delete locked across close and topic changes and drops stale outcomes', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    const onResult = vi.fn();
    vi.mocked(topicApi.delete).mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce(successResult('payments'));
    const { rerender } = render(
      <TopicDeleteDialog {...defaultProps} onOpenChange={onOpenChange} onResult={onResult} onSucceeded={onSucceeded} />
    );

    let dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(dialog).getByRole('button', { name: 'Delete topic' }));
    rerender(<TopicDeleteDialog {...defaultProps} open={false} onOpenChange={onOpenChange} onResult={onResult} onSucceeded={onSucceeded} />);
    rerender(
      <TopicDeleteDialog
        {...defaultProps}
        topic={topic('payments')}
        onOpenChange={onOpenChange}
        onResult={onResult}
        onSucceeded={onSucceeded}
      />
    );
    dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    expect(within(dialog).getByRole('textbox', { name: 'Confirm topic name' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Deleting' })).toBeDisabled();
    expect(topicApi.delete).toHaveBeenCalledTimes(1);

    await act(async () => pending.reject(new Error('stale orders delete failed')));
    expect(screen.queryByText('stale orders delete failed')).not.toBeInTheDocument();
    expect(onResult).not.toHaveBeenCalled();
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'payments');
    await waitFor(() => expect(within(dialog).getByRole('button', { name: 'Delete topic' })).toBeEnabled());
    await user.click(within(dialog).getByRole('button', { name: 'Delete topic' }));
    expect(topicApi.delete).toHaveBeenLastCalledWith('payments');
    expect(screen.queryByText('payments deleted')).not.toBeInTheDocument();
    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult).toHaveBeenCalledWith(successResult('payments'));
    expect(onSucceeded).toHaveBeenCalledWith(successResult('payments'));
  });

  it('uses a synchronous submit lock under StrictMode and ignores settlement after unmount', async () => {
    const pending = deferred<TopicOperationResult>();
    const onOpenChange = vi.fn();
    const onSucceeded = vi.fn();
    vi.mocked(topicApi.delete).mockReturnValue(pending.promise);
    const { unmount } = render(
      <StrictMode>
        <TopicDeleteDialog {...defaultProps} onOpenChange={onOpenChange} onSucceeded={onSucceeded} />
      </StrictMode>
    );

    const input = screen.getByRole('textbox', { name: 'Confirm topic name' });
    fireEvent.change(input, { target: { value: 'orders' } });
    const confirm = screen.getByRole('button', { name: 'Delete topic' });
    act(() => {
      confirm.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      confirm.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    });
    expect(topicApi.delete).toHaveBeenCalledTimes(1);

    unmount();
    await act(async () => pending.resolve(successResult('orders')));
    expect(onSucceeded).not.toHaveBeenCalled();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
  });
});
