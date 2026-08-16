import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
import type { TopicOffsetResult } from '../types/topic';
import TopicResetOffsetDialog, {
  hasExactLocalDateTimeFields,
  parseLocalDateTime
} from './TopicResetOffsetDialog';

vi.mock('../api/topic_api', () => ({
  topicApi: { resetOffset: vi.fn() }
}));

const resetOldGroupFixture: TopicOffsetResult = {
  operation: 'RESET_OFFSET',
  topic: 'orders',
  consumerGroup: 'order-service',
  success: true,
  affectedQueueCount: 8,
  appliedTimestamp: 1_786_762_800_000,
  message: '8 queues reset'
};

const defaultProps = {
  open: true,
  topic: 'orders',
  consumerGroup: 'order-service',
  onOpenChange: vi.fn(),
  onSucceeded: vi.fn()
};

async function submitReset(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Review reset' }));
  const confirmation = await screen.findByRole('alertdialog');
  await user.click(within(confirmation).getByRole('button', { name: 'Reset offset' }));
}

describe('TopicResetOffsetDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.resetOffset).mockResolvedValue(resetOldGroupFixture);
  });

  it('parses exact local datetime components including optional seconds', () => {
    expect(parseLocalDateTime('2026-08-15T10:30')).toBe(new Date(2026, 7, 15, 10, 30, 0, 0).getTime());
    expect(parseLocalDateTime('2026-08-15T10:30:45')).toBe(new Date(2026, 7, 15, 10, 30, 45, 0).getTime());
  });

  it('rejects normalized calendar values, local gaps, and negative timestamps', () => {
    expect(parseLocalDateTime('2026-02-30T10:30')).toBeNull();
    expect(parseLocalDateTime('1900-01-01T00:00')).toBeNull();

    const localEpochStart = new Date(1970, 0, 1, 0, 0, 0, 0).getTime();
    expect(parseLocalDateTime('1970-01-01T00:00')).toBe(localEpochStart < 0 ? null : localEpochStart);

    expect(hasExactLocalDateTimeFields(
      { year: 2026, month: 3, day: 8, hour: 2, minute: 30, second: 0 },
      { year: 2026, month: 3, day: 8, hour: 3, minute: 30, second: 0 }
    )).toBe(false);
  });

  it('resets the captured group to the selected local time after explicit primary confirmation', async () => {
    const user = userEvent.setup();
    const resetTimestamp = new Date('2026-08-15T10:30').getTime();
    const result = { ...resetOldGroupFixture, appliedTimestamp: resetTimestamp, message: 'reset' };
    vi.mocked(topicApi.resetOffset).mockResolvedValue(result);
    render(<TopicResetOffsetDialog {...defaultProps} />);

    expect(screen.getByRole('textbox', { name: 'Topic' })).toBeDisabled();
    expect(screen.getByRole('textbox', { name: 'Consumer group' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('orders');
    expect(confirmation).toHaveTextContent('order-service');
    expect(confirmation).toHaveTextContent(new Date(resetTimestamp).toLocaleString());
    const confirm = within(confirmation).getByRole('button', { name: 'Reset offset' });
    expect(confirm).not.toHaveClass('ui-button-destructive');
    await user.click(confirm);

    expect(topicApi.resetOffset).toHaveBeenCalledWith('orders', {
      consumerGroup: 'order-service',
      resetTimestamp,
      force: true
    });
    const status = await screen.findByRole('status');
    expect(status).toHaveTextContent('8');
    expect(status).toHaveTextContent(new Date(resetTimestamp).toLocaleString());
    expect(defaultProps.onSucceeded).toHaveBeenCalledWith(result);
  });

  it('requires a valid explicit reset time', async () => {
    const user = userEvent.setup();
    render(<TopicResetOffsetDialog {...defaultProps} />);

    await user.click(screen.getByRole('button', { name: 'Review reset' }));

    expect(screen.getByRole('alert')).toHaveTextContent('Select a valid reset time.');
    expect(topicApi.resetOffset).not.toHaveBeenCalled();
  });

  it('preserves the selected time and restores Review focus when the request rejects', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.resetOffset).mockRejectedValue(new Error('reset failed'));
    render(<TopicResetOffsetDialog {...defaultProps} />);
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });

    await submitReset(user);

    expect(await screen.findByRole('alert')).toHaveTextContent('reset failed');
    expect(screen.getByLabelText('Reset time')).toHaveValue('2026-08-15T10:30');
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review reset' })).toHaveFocus());
    expect(topicApi.resetOffset).toHaveBeenCalledTimes(1);
  });

  it('keeps reset locked across group changes and drops the old result', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.resetOffset).mockReturnValue(pending.promise);
    const onSucceeded = vi.fn();
    const { rerender } = render(<TopicResetOffsetDialog {...defaultProps} onSucceeded={onSucceeded} />);
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await submitReset(user);

    rerender(
      <TopicResetOffsetDialog {...defaultProps} consumerGroup="payment-service" onSucceeded={onSucceeded} />
    );
    expect(screen.getByLabelText('Reset time')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Review reset' })).toBeDisabled();
    expect(topicApi.resetOffset).toHaveBeenCalledTimes(1);
    await act(async () => pending.resolve(resetOldGroupFixture));

    expect(screen.queryByText(/8 queues reset/)).not.toBeInTheDocument();
    expect(onSucceeded).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review reset' })).toBeEnabled());
  });

  it('drops a stale reset rejection after the group changes', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.resetOffset).mockReturnValue(pending.promise);
    const { rerender } = render(<TopicResetOffsetDialog {...defaultProps} />);
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await submitReset(user);
    rerender(<TopicResetOffsetDialog {...defaultProps} consumerGroup="payment-service" />);

    await act(async () => pending.reject(new Error('old reset failed')));

    expect(screen.queryByText('old reset failed')).not.toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Consumer group' })).toHaveValue('payment-service');
  });

  it('drops a pending rejection after the dialog truly unmounts', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    const onSucceeded = vi.fn();
    vi.mocked(topicApi.resetOffset).mockReturnValue(pending.promise);
    const { unmount } = render(<TopicResetOffsetDialog {...defaultProps} onSucceeded={onSucceeded} />);
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await submitReset(user);

    unmount();
    await act(async () => pending.reject(new Error('unmounted reset failed')));

    expect(onSucceeded).not.toHaveBeenCalled();
    expect(screen.queryByText('unmounted reset failed')).not.toBeInTheDocument();
  });

  it('uses a synchronous lock and unlocks after completion under StrictMode', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOffsetResult>();
    vi.mocked(topicApi.resetOffset)
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce({ ...resetOldGroupFixture, message: 'second reset' });
    render(
      <StrictMode>
        <TopicResetOffsetDialog {...defaultProps} />
      </StrictMode>
    );
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    const confirm = within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Reset offset' });
    act(() => {
      confirm.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      confirm.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    });
    expect(topicApi.resetOffset).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve(resetOldGroupFixture));
    await submitReset(user);
    await waitFor(() => expect(topicApi.resetOffset).toHaveBeenCalledTimes(2));
  });
});
