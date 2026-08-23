import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { vi } from 'vitest';
import { ApiClientError } from '../../api/client';
import MonitorDialog from './MonitorDialog';

describe('MonitorDialog', () => {
  it('validates threshold values and preserves the draft when a save fails', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockRejectedValue(new Error('monitor service unavailable'));
    render(<MonitorDialog open environmentId="environment-default" onOpenChange={vi.fn()} onSubmit={onSubmit} />);

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

    await user.click(screen.getByRole('button', { name: 'Save rule' }));
    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith({
      environmentId: 'environment-default',
      consumerGroup: 'order-service',
      minCount: 4,
      maxDiffTotal: 1000,
      expectedRevision: 0
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
        environmentId="environment-default"
        rule={{ environmentId: 'environment-default', consumerGroup: 'order-service', minCount: 4, maxDiffTotal: 1200, revision: 3 }}
        onOpenChange={onOpenChange}
        onSubmit={onSubmit}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Save rule' }));
    rerender(
      <MonitorDialog
        open
        environmentId="environment-default"
        rule={{ environmentId: 'environment-default', consumerGroup: 'payment-worker', minCount: 2, maxDiffTotal: 800, revision: 4 }}
        onOpenChange={onOpenChange}
        onSubmit={onSubmit}
      />
    );
    resolveSave();

    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Group' })).toHaveValue('payment-worker'));
    expect(onOpenChange).not.toHaveBeenCalled();
  });

  it('loads the current rule revision on a conflict and reuses it only after an explicit retry', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn()
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Rule revision is stale.'))
      .mockResolvedValueOnce(undefined);
    const onConflict = vi.fn().mockResolvedValue({
      environmentId: 'environment-default', consumerGroup: 'order-service', minCount: 8, maxDiffTotal: 1200, revision: 9
    });
    render(
      <MonitorDialog
        open
        environmentId="environment-default"
        rule={{ environmentId: 'environment-default', consumerGroup: 'order-service', minCount: 4, maxDiffTotal: 1200, revision: 3 }}
        onOpenChange={vi.fn()}
        onSubmit={onSubmit}
        onConflict={onConflict}
      />
    );

    const minCount = screen.getByRole('spinbutton', { name: 'Min Count' });
    await user.clear(minCount);
    await user.type(minCount, '6');
    await user.click(screen.getByRole('button', { name: 'Save rule' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('current revision is loaded');
    expect(screen.getByRole('spinbutton', { name: 'Min Count' })).toHaveValue(6);
    expect(onConflict).toHaveBeenCalledWith('order-service');
    await user.clear(minCount);
    await user.type(minCount, '7');
    await user.click(screen.getByRole('button', { name: 'Save rule' }));

    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith({
      environmentId: 'environment-default', consumerGroup: 'order-service', minCount: 7, maxDiffTotal: 1200, expectedRevision: 9
    }));
  });

  it('keeps a concurrent-create draft and the refreshed revision when the parent selects the authoritative rule', async () => {
    const user = userEvent.setup();
    const authoritative = {
      environmentId: 'environment-default', consumerGroup: 'inventory-worker', minCount: 1, maxDiffTotal: 1000, revision: 6
    };
    const onSubmit = vi.fn()
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Rule revision is stale.'))
      .mockResolvedValueOnce(undefined);
    function Harness() {
      const [rule, setRule] = useState<typeof authoritative | null>(null);
      return (
        <MonitorDialog
          open
          environmentId="environment-default"
          rule={rule}
          onOpenChange={vi.fn()}
          onSubmit={onSubmit}
          onConflict={async () => {
            setRule(authoritative);
            return authoritative;
          }}
        />
      );
    }

    render(<Harness />);
    await user.type(screen.getByRole('textbox', { name: 'Group' }), 'inventory-worker');
    const minCount = screen.getByRole('spinbutton', { name: 'Min Count' });
    await user.clear(minCount);
    await user.type(minCount, '5');
    await user.click(screen.getByRole('button', { name: 'Save rule' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('current revision is loaded');
    expect(screen.getByRole('textbox', { name: 'Group' })).toHaveValue('inventory-worker');
    expect(screen.getByRole('spinbutton', { name: 'Min Count' })).toHaveValue(5);

    await user.click(screen.getByRole('button', { name: 'Retry save' }));
    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith({
      environmentId: 'environment-default', consumerGroup: 'inventory-worker', minCount: 5, maxDiffTotal: 1000, expectedRevision: 6
    }));
  });
});
