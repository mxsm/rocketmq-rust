import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
import type { TopicOffsetResult } from '../types/topic';
import TopicSkipBacklogDialog from './TopicSkipBacklogDialog';

vi.mock('../api/topic_api', () => ({
  topicApi: { skipBacklog: vi.fn() }
}));

const skipOldGroupFixture: TopicOffsetResult = {
  operation: 'SKIP_BACKLOG',
  topic: 'orders',
  consumerGroup: 'order-service',
  success: true,
  affectedQueueCount: 8,
  appliedTimestamp: 1_786_762_800_000,
  message: '8 queues skipped'
};

const defaultProps = {
  open: true,
  topic: 'orders',
  consumerGroup: 'order-service',
  onOpenChange: vi.fn(),
  onSucceeded: vi.fn()
};

describe('TopicSkipBacklogDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.skipBacklog).mockResolvedValue(skipOldGroupFixture);
  });

  it('requires the exact consumer group and uses a destructive backlog-discard action', async () => {
    const user = userEvent.setup();
    render(<TopicSkipBacklogDialog {...defaultProps} />);

    expect(screen.getByText(/unread messages currently in the backlog will be skipped/i)).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Topic' })).toBeDisabled();
    expect(screen.getByRole('textbox', { name: 'Consumer group' })).toBeDisabled();
    const confirm = screen.getByRole('button', { name: 'Skip accumulated messages' });
    expect(confirm).toHaveClass('ui-button-destructive');
    expect(confirm).toBeDisabled();
    await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-servic');
    expect(confirm).toBeDisabled();
    await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'e');
    expect(confirm).toBeEnabled();
    await user.click(confirm);

    expect(topicApi.skipBacklog).toHaveBeenCalledWith('orders', { consumerGroup: 'order-service' });
  });

  it('renders the backend timestamp and affected queue count after success', async () => {
    const user = userEvent.setup();
    render(<TopicSkipBacklogDialog {...defaultProps} />);
    await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-service');
    await user.click(screen.getByRole('button', { name: 'Skip accumulated messages' }));

    const status = await screen.findByRole('status');
    expect(status).toHaveTextContent('8');
    expect(status).toHaveTextContent(new Date(skipOldGroupFixture.appliedTimestamp).toLocaleString());
    expect(defaultProps.onSucceeded).toHaveBeenCalledWith(skipOldGroupFixture);
    expect(screen.getByRole('textbox', { name: 'Confirm consumer group' })).toHaveValue('');
  });

  it('preserves confirmation text and restores destructive-action focus when the request rejects', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.skipBacklog).mockRejectedValue(new Error('skip failed'));
    render(<TopicSkipBacklogDialog {...defaultProps} />);
    const confirmation = screen.getByRole('textbox', { name: 'Confirm consumer group' });
    await user.type(confirmation, 'order-service');

    await user.click(screen.getByRole('button', { name: 'Skip accumulated messages' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('skip failed');
    expect(confirmation).toHaveValue('order-service');
    await waitFor(() => expect(screen.getByRole('button', { name: 'Skip accumulated messages' })).toHaveFocus());
    expect(topicApi.skipBacklog).toHaveBeenCalledTimes(1);
  });

  it('keeps skip locked across group changes and drops the old result', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.skipBacklog).mockReturnValue(pending.promise);
    const onSucceeded = vi.fn();
    const { rerender } = render(<TopicSkipBacklogDialog {...defaultProps} onSucceeded={onSucceeded} />);
    await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-service');
    await user.click(screen.getByRole('button', { name: 'Skip accumulated messages' }));

    rerender(<TopicSkipBacklogDialog {...defaultProps} consumerGroup="payment-service" onSucceeded={onSucceeded} />);
    expect(screen.getByRole('textbox', { name: 'Confirm consumer group' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Skip accumulated messages' })).toBeDisabled();
    expect(topicApi.skipBacklog).toHaveBeenCalledTimes(1);
    await act(async () => pending.resolve(skipOldGroupFixture));

    expect(screen.queryByText(/8 queues skipped/)).not.toBeInTheDocument();
    expect(onSucceeded).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Confirm consumer group' })).toBeEnabled());
  });

  it('drops a stale skip rejection after the group changes', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.skipBacklog).mockReturnValue(pending.promise);
    const { rerender } = render(<TopicSkipBacklogDialog {...defaultProps} />);
    await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-service');
    await user.click(screen.getByRole('button', { name: 'Skip accumulated messages' }));
    rerender(<TopicSkipBacklogDialog {...defaultProps} consumerGroup="payment-service" />);

    await act(async () => pending.reject(new Error('old skip failed')));

    expect(screen.queryByText('old skip failed')).not.toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Consumer group' })).toHaveValue('payment-service');
  });

  it('uses a synchronous lock and unlocks after completion under StrictMode', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.skipBacklog)
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce({ ...skipOldGroupFixture, message: 'second skip' });
    render(
      <StrictMode>
        <TopicSkipBacklogDialog {...defaultProps} />
      </StrictMode>
    );
    const groupConfirmation = screen.getByRole('textbox', { name: 'Confirm consumer group' });
    await user.type(groupConfirmation, 'order-service');
    const action = screen.getByRole('button', { name: 'Skip accumulated messages' });
    fireEvent.click(action);
    fireEvent.click(action);
    expect(topicApi.skipBacklog).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve(skipOldGroupFixture));
    await user.type(groupConfirmation, 'order-service');
    await user.click(action);
    await waitFor(() => expect(topicApi.skipBacklog).toHaveBeenCalledTimes(2));
  });
});
