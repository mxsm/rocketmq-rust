import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
import type { TopicSendResultView } from '../types/topic';
import TopicSendMessageDialog from './TopicSendMessageDialog';

vi.mock('../api/topic_api', () => ({
  topicApi: { sendTestMessage: vi.fn() }
}));

const sendOkFixture: TopicSendResultView = {
  topic: 'orders',
  success: true,
  sendStatus: 'SEND_OK',
  messageId: 'msg-old',
  brokerName: 'broker-a',
  queueId: 1,
  queueOffset: 42,
  transactionId: 'tx-1',
  regionId: 'region-a',
  localTransactionState: 'COMMIT_MESSAGE'
};

const defaultProps = {
  open: true,
  topic: 'orders',
  onOpenChange: vi.fn(),
  onSucceeded: vi.fn()
};

async function submitSend(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Review send' }));
  const confirmation = await screen.findByRole('alertdialog');
  await user.click(within(confirmation).getByRole('button', { name: 'Send test message' }));
}

describe('TopicSendMessageDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.sendTestMessage).mockResolvedValue(sendOkFixture);
  });

  it('sends the exact message and renders broker non-success as a full structured alert', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.sendTestMessage).mockResolvedValue({
      topic: 'orders',
      success: false,
      sendStatus: 'FLUSH_DISK_TIMEOUT',
      messageId: 'msg-1',
      brokerName: 'broker-a',
      queueId: 1,
      queueOffset: 42,
      transactionId: 'tx-1',
      regionId: 'region-a',
      localTransactionState: 'UNKNOW'
    });
    render(<TopicSendMessageDialog {...defaultProps} />);

    expect(screen.getByRole('textbox', { name: 'Topic' })).toBeDisabled();
    await user.type(screen.getByRole('textbox', { name: 'Key' }), 'order-42');
    await user.type(screen.getByRole('textbox', { name: 'Tag' }), 'created');
    fireEvent.change(screen.getByRole('textbox', { name: 'Message body' }), {
      target: { value: '{"id":42}' }
    });
    await user.click(screen.getByRole('checkbox', { name: 'Enable trace' }));
    await user.click(screen.getByRole('button', { name: 'Review send' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('orders');
    const confirm = within(confirmation).getByRole('button', { name: 'Send test message' });
    expect(confirm).not.toHaveClass('ui-button-destructive');
    await user.click(confirm);

    expect(topicApi.sendTestMessage).toHaveBeenCalledWith('orders', {
      key: 'order-42',
      tag: 'created',
      messageBody: '{"id":42}',
      traceEnabled: true
    });
    const alert = await screen.findByRole('alert');
    for (const value of ['FLUSH_DISK_TIMEOUT', 'msg-1', 'broker-a', '1', '42', 'tx-1', 'region-a', 'UNKNOW']) {
      expect(alert).toHaveTextContent(value);
    }
    expect(defaultProps.onSucceeded).not.toHaveBeenCalled();
  });

  it('requires a non-blank body before opening confirmation', async () => {
    const user = userEvent.setup();
    render(<TopicSendMessageDialog {...defaultProps} />);

    await user.type(screen.getByRole('textbox', { name: 'Message body' }), '   ');
    await user.click(screen.getByRole('button', { name: 'Review send' }));

    expect(screen.getByRole('alert')).toHaveTextContent('Message body is required.');
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(topicApi.sendTestMessage).not.toHaveBeenCalled();
  });

  it('preserves the exact form and restores Review focus when the request rejects', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.sendTestMessage).mockRejectedValue(new Error('broker unavailable'));
    render(<TopicSendMessageDialog {...defaultProps} />);

    await user.type(screen.getByRole('textbox', { name: 'Key' }), ' key with spaces ');
    await user.type(screen.getByRole('textbox', { name: 'Tag' }), 'created');
    await user.type(screen.getByRole('textbox', { name: 'Message body' }), 'payload');
    await user.click(screen.getByRole('checkbox', { name: 'Enable trace' }));
    await submitSend(user);

    expect(await screen.findByRole('alert')).toHaveTextContent('broker unavailable');
    expect(screen.getByRole('textbox', { name: 'Key' })).toHaveValue(' key with spaces ');
    expect(screen.getByRole('textbox', { name: 'Tag' })).toHaveValue('created');
    expect(screen.getByRole('textbox', { name: 'Message body' })).toHaveValue('payload');
    expect(screen.getByRole('checkbox', { name: 'Enable trace' })).toBeChecked();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review send' })).toHaveFocus());
    expect(topicApi.sendTestMessage).toHaveBeenCalledTimes(1);
  });

  it('keeps send locked across close and reopen and drops the old result', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicSendResultView>();
    vi.mocked(topicApi.sendTestMessage).mockReturnValue(pending.promise);
    const onSucceeded = vi.fn();
    const { rerender } = render(
      <TopicSendMessageDialog {...defaultProps} onSucceeded={onSucceeded} />
    );
    await user.type(screen.getByRole('textbox', { name: 'Message body' }), 'test');
    await submitSend(user);

    rerender(<TopicSendMessageDialog {...defaultProps} open={false} onSucceeded={onSucceeded} />);
    rerender(<TopicSendMessageDialog {...defaultProps} topic="payments" onSucceeded={onSucceeded} />);
    expect(screen.getByRole('textbox', { name: 'Message body' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Review send' })).toBeDisabled();
    expect(topicApi.sendTestMessage).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve(sendOkFixture));
    expect(screen.queryByText('msg-old')).not.toBeInTheDocument();
    expect(onSucceeded).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review send' })).toBeEnabled());
  });

  it('drops a stale rejection after the topic changes', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicSendResultView>();
    vi.mocked(topicApi.sendTestMessage).mockReturnValue(pending.promise);
    const { rerender } = render(<TopicSendMessageDialog {...defaultProps} />);
    await user.type(screen.getByRole('textbox', { name: 'Message body' }), 'test');
    await submitSend(user);

    rerender(<TopicSendMessageDialog {...defaultProps} topic="payments" />);
    await act(async () => pending.reject(new Error('old topic failed')));

    expect(screen.queryByText('old topic failed')).not.toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Topic' })).toHaveValue('payments');
  });

  it('uses a synchronous lock and unlocks after completion under StrictMode', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicSendResultView>();
    vi.mocked(topicApi.sendTestMessage)
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce({ ...sendOkFixture, messageId: 'msg-2' });
    const onSucceeded = vi.fn();
    render(
      <StrictMode>
        <TopicSendMessageDialog {...defaultProps} onSucceeded={onSucceeded} />
      </StrictMode>
    );
    await user.type(screen.getByRole('textbox', { name: 'Message body' }), 'test');
    await user.click(screen.getByRole('button', { name: 'Review send' }));
    const confirmation = screen.getByRole('alertdialog');
    const confirm = within(confirmation).getByRole('button', { name: 'Send test message' });
    fireEvent.click(confirm);
    fireEvent.click(confirm);
    expect(topicApi.sendTestMessage).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve(sendOkFixture));
    expect(await screen.findByRole('status')).toHaveTextContent('msg-old');
    expect(onSucceeded).toHaveBeenCalledWith(sendOkFixture);
    await submitSend(user);
    await waitFor(() => expect(topicApi.sendTestMessage).toHaveBeenCalledTimes(2));
  });
});
