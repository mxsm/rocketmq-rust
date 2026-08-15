import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import MonitorDialog from './MonitorDialog';

describe('MonitorDialog', () => {
  it('validates threshold values and preserves the draft when a save fails', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockRejectedValue(new Error('monitor service unavailable'));
    render(<MonitorDialog open onOpenChange={vi.fn()} onSubmit={onSubmit} />);

    await user.type(screen.getByRole('textbox', { name: 'Group' }), 'order-service');
    const minCount = screen.getByRole('spinbutton', { name: 'Min Count' });
    await user.clear(minCount);
    await user.type(minCount, '-1');
    await user.click(screen.getByRole('button', { name: 'Save rule' }));

    expect(screen.getByRole('status')).toHaveTextContent('Min Count must be a non-negative integer.');
    expect(onSubmit).not.toHaveBeenCalled();

    await user.clear(minCount);
    await user.type(minCount, '4');
    await user.click(screen.getByRole('button', { name: 'Save rule' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('monitor service unavailable');
    expect(screen.getByRole('textbox', { name: 'Group' })).toHaveValue('order-service');
    expect(screen.getByRole('spinbutton', { name: 'Min Count' })).toHaveValue(4);
    expect(screen.getByRole('button', { name: 'Retry save' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Retry save' }));
    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith({
      consumerGroup: 'order-service',
      minCount: 4,
      maxDiffTotal: 1000
    }));
  });

  it('ignores a stale save completion after the selected rule changes', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    let resolveSave: () => void = () => undefined;
    const onSubmit = vi.fn().mockReturnValue(new Promise<void>((resolve) => { resolveSave = resolve; }));
    const { rerender } = render(
      <MonitorDialog
        open
        rule={{ consumerGroup: 'order-service', minCount: 4, maxDiffTotal: 1200 }}
        onOpenChange={onOpenChange}
        onSubmit={onSubmit}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Save rule' }));
    rerender(
      <MonitorDialog
        open
        rule={{ consumerGroup: 'payment-worker', minCount: 2, maxDiffTotal: 800 }}
        onOpenChange={onOpenChange}
        onSubmit={onSubmit}
      />
    );
    resolveSave();

    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Group' })).toHaveValue('payment-worker'));
    expect(onOpenChange).not.toHaveBeenCalled();
  });
});
