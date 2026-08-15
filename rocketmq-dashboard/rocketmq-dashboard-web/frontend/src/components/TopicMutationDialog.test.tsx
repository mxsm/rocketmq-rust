import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { deferred } from '../test/deferred';
import type {
  TopicConfigView,
  TopicOperationResult,
  TopicTargetOptionView
} from '../types/topic';
import TopicMutationDialog from './TopicMutationDialog';

const targets: TopicTargetOptionView[] = [
  { clusterName: 'DefaultCluster', brokerNames: ['broker-a', 'broker-b'] },
  { clusterName: 'BackupCluster', brokerNames: ['broker-c'] }
];

const config: TopicConfigView = {
  topicName: 'orders',
  brokerName: 'broker-a',
  clusterName: 'DefaultCluster',
  brokerNameList: ['broker-a'],
  clusterNameList: ['DefaultCluster'],
  readQueueNums: 8,
  writeQueueNums: 8,
  perm: 6,
  order: false,
  messageType: 'NORMAL',
  attributes: {},
  inconsistentFields: ['writeQueueNums']
};

const successResult: TopicOperationResult = {
  operation: 'UPDATE',
  topic: 'orders',
  success: true,
  targetCount: 2,
  message: '2 targets saved',
  targets: [
    { target: 'broker-a', success: true, message: 'saved' },
    { target: 'broker-b', success: true, message: 'saved' }
  ]
};

const partialResult: TopicOperationResult = {
  operation: 'UPDATE',
  topic: 'orders',
  success: false,
  targetCount: 2,
  message: '1 of 2 targets failed',
  targets: [
    { target: 'broker-a', success: true, message: 'saved on broker-a' },
    { target: 'broker-b', success: false, message: 'broker-b unavailable' }
  ]
};

const defaultProps = {
  open: true,
  mode: 'create' as const,
  targets,
  onOpenChange: vi.fn(),
  onSubmit: vi.fn().mockResolvedValue({ ...successResult, operation: 'CREATE', topic: 'new-topic' })
};

async function openConfirmation(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Save topic' }));
  return screen.findByRole('alertdialog');
}

describe('TopicMutationDialog', () => {
  it('uses discovered targets, validates queue bounds, and submits exact Create values', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockResolvedValue({
      ...successResult,
      operation: 'CREATE',
      topic: 'inventory-events'
    });
    const onOpenChange = vi.fn();
    render(<TopicMutationDialog {...defaultProps} onOpenChange={onOpenChange} onSubmit={onSubmit} />);

    expect(screen.queryByRole('textbox', { name: 'Cluster names' })).not.toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'DefaultCluster' })).not.toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'broker-a' })).not.toBeChecked();

    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('alert')).toHaveTextContent('Topic name cannot be empty.');

    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), ' inventory-events ');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('alert')).toHaveTextContent('Choose at least one cluster or broker target.');

    await user.click(screen.getByRole('checkbox', { name: 'DefaultCluster' }));
    expect(screen.getByRole('checkbox', { name: 'DefaultCluster' })).toBeChecked();
    const readQueues = screen.getByRole('spinbutton', { name: 'Read queue count' });
    await user.clear(readQueues);
    await user.type(readQueues, '129');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('alert')).toHaveTextContent('Queue counts must be whole numbers from 1 through 128.');

    await user.clear(readQueues);
    await user.type(readQueues, '4');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Message type' }), 'FIFO');
    await user.click(screen.getByRole('checkbox', { name: 'Ordered topic' }));
    const confirmation = await openConfirmation(user);
    expect(confirmation).toHaveTextContent('2 broker targets');
    expect(confirmation).toHaveTextContent('DefaultCluster');
    expect(onSubmit).not.toHaveBeenCalled();

    await user.click(within(confirmation).getByRole('button', { name: 'Create topic' }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith({
      topic: 'inventory-events',
      readQueueCount: 4,
      writeQueueCount: 8,
      perm: 6,
      brokerNameList: [],
      clusterNameList: ['DefaultCluster'],
      order: true,
      messageType: 'FIFO'
    }));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('builds permission bits from Read, Write, and Inherit and requires Read or Write', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockResolvedValue({
      ...successResult,
      operation: 'CREATE',
      topic: 'permission-topic'
    });
    const { unmount } = render(<TopicMutationDialog {...defaultProps} onSubmit={onSubmit} />);

    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), 'permission-topic');
    await user.click(screen.getByRole('checkbox', { name: 'broker-a' }));
    await user.click(screen.getByRole('checkbox', { name: 'Write' }));
    await user.click(screen.getByRole('checkbox', { name: 'Inherit' }));

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Create topic' }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ perm: 5 })));
    unmount();

    render(<TopicMutationDialog {...defaultProps} />);
    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), 'inherit-only');
    await user.click(screen.getByRole('checkbox', { name: 'broker-a' }));
    await user.click(screen.getByRole('checkbox', { name: 'Read' }));
    await user.click(screen.getByRole('checkbox', { name: 'Write' }));
    await user.click(screen.getByRole('checkbox', { name: 'Inherit' }));
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('alert')).toHaveTextContent('Enable Read or Write permission.');
  });

  it('loads edit config, locks the topic name, and submits selected canonical brokers', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockResolvedValue(successResult);
    render(
      <TopicMutationDialog
        open
        mode="edit"
        config={config}
        targets={targets}
        onOpenChange={vi.fn()}
        onSubmit={onSubmit}
      />
    );

    expect(screen.getByRole('textbox', { name: 'Topic name' })).toBeDisabled();
    expect(screen.getByRole('textbox', { name: 'Topic name' })).toHaveValue('orders');
    expect(screen.getByText(/Broker configurations disagree: writeQueueNums/)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'DefaultCluster' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'broker-a' })).toBeChecked();

    await user.click(screen.getByRole('checkbox', { name: 'broker-b' }));
    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      topic: 'orders',
      brokerNameList: ['broker-a', 'broker-b'],
      clusterNameList: ['DefaultCluster'],
      perm: 6
    }));
  });

  it('keeps confirmation target counts and payloads stable when discovered targets rerender', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockResolvedValue({
      ...successResult,
      operation: 'CREATE',
      topic: 'snapshot-topic'
    });
    const props = {
      open: true,
      mode: 'create' as const,
      onOpenChange: vi.fn(),
      onSubmit
    };
    const { rerender } = render(<TopicMutationDialog {...props} targets={targets} />);

    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), 'snapshot-topic');
    await user.click(screen.getByRole('checkbox', { name: 'DefaultCluster' }));
    await user.click(screen.getByRole('checkbox', { name: 'broker-c' }));
    const confirmation = await openConfirmation(user);
    expect(confirmation).toHaveTextContent('3 broker targets');
    expect(confirmation).toHaveTextContent('selected brokers broker-c');

    rerender(
      <TopicMutationDialog
        {...props}
        targets={[
          { clusterName: 'DefaultCluster', brokerNames: ['broker-a', 'broker-b', 'broker-d', 'broker-e'] },
          { clusterName: 'BackupCluster', brokerNames: ['broker-z'] }
        ]}
      />
    );

    expect(screen.getByRole('alertdialog')).toHaveTextContent('3 broker targets');
    expect(screen.getByRole('alertdialog')).toHaveTextContent('selected brokers broker-c');
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Create topic' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      topic: 'snapshot-topic',
      clusterNameList: ['DefaultCluster'],
      brokerNameList: ['broker-c']
    }));
  });

  it('uses a primary action for the non-destructive Create confirmation', async () => {
    const user = userEvent.setup();
    render(<TopicMutationDialog {...defaultProps} />);

    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), 'primary-action-topic');
    await user.click(screen.getByRole('checkbox', { name: 'broker-a' }));
    const confirmation = await openConfirmation(user);
    const createAction = within(confirmation).getByRole('button', { name: 'Create topic' });

    expect(createAction).toHaveClass('ui-button-default');
    expect(createAction).not.toHaveClass('ui-button-destructive');
  });

  it('shows config loading and retryable errors without presenting an editable form', async () => {
    const user = userEvent.setup();
    const onRetryConfig = vi.fn();
    const props = {
      open: true,
      mode: 'edit' as const,
      targets,
      config: null,
      onOpenChange: vi.fn(),
      onSubmit: vi.fn().mockResolvedValue(successResult)
    };
    const { rerender } = render(<TopicMutationDialog {...props} loadingConfig />);

    expect(screen.getByRole('status', { name: 'Loading topic configuration' })).toBeInTheDocument();
    expect(screen.queryByRole('textbox', { name: 'Topic name' })).not.toBeInTheDocument();

    rerender(
      <TopicMutationDialog
        {...props}
        configError="Configuration discovery failed"
        onRetryConfig={onRetryConfig}
      />
    );
    expect(screen.getByRole('alert')).toHaveTextContent('Configuration discovery failed');
    await user.click(screen.getByRole('button', { name: 'Retry configuration' }));
    expect(onRetryConfig).toHaveBeenCalledTimes(1);
  });

  it('preserves form values and returns focus to Save after submit rejects', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockRejectedValue(new Error('NameServer unavailable'));
    render(<TopicMutationDialog {...defaultProps} onSubmit={onSubmit} />);

    const topic = screen.getByRole('textbox', { name: 'Topic name' });
    await user.type(topic, 'kept-topic');
    await user.click(screen.getByRole('checkbox', { name: 'broker-c' }));
    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Create topic' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('NameServer unavailable');
    expect(topic).toHaveValue('kept-topic');
    expect(screen.getByRole('checkbox', { name: 'broker-c' })).toBeChecked();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Save topic' })).toHaveFocus());
  });

  it('renders every partial target result and keeps the Edit dialog open', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    render(
      <TopicMutationDialog
        open
        mode="edit"
        config={config}
        targets={targets}
        onOpenChange={onOpenChange}
        onSubmit={vi.fn().mockResolvedValue(partialResult)}
      />
    );

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(screen.getByText('saved on broker-a')).toBeInTheDocument();
    expect(screen.getByText('broker-b unavailable')).toBeInTheDocument();
    expect(screen.getByRole('dialog', { name: 'Edit topic' })).toBeInTheDocument();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
  });

  it('keeps the real mutation locked after close and reopen until the promise settles', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const onSubmit = vi.fn().mockReturnValue(pending.promise);
    const base = { mode: 'edit' as const, config, targets, onOpenChange: vi.fn(), onSubmit };
    const { rerender } = render(
      <StrictMode><TopicMutationDialog {...base} open /></StrictMode>
    );

    let confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    rerender(<StrictMode><TopicMutationDialog {...base} open={false} /></StrictMode>);
    rerender(<StrictMode><TopicMutationDialog {...base} open /></StrictMode>);
    confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    expect(onSubmit).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve(successResult));
  });

  it('drops a stale result when the captured Edit topic identity changes', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const onOpenChange = vi.fn();
    const base = { open: true, mode: 'edit' as const, targets, onOpenChange, onSubmit: vi.fn().mockReturnValue(pending.promise) };
    const { rerender } = render(<TopicMutationDialog {...base} config={config} />);

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    rerender(
      <TopicMutationDialog
        {...base}
        config={{ ...config, topicName: 'payments', inconsistentFields: [] }}
      />
    );
    await act(async () => pending.resolve(successResult));

    expect(screen.getByRole('textbox', { name: 'Topic name' })).toHaveValue('payments');
    expect(screen.queryByText('2 targets saved')).not.toBeInTheDocument();
    expect(onOpenChange).not.toHaveBeenCalledWith(false);
  });

  it('preserves dirty Edit values across same-topic config refreshes and rehydrates after reopen', async () => {
    const user = userEvent.setup();
    const props = {
      mode: 'edit' as const,
      targets,
      onOpenChange: vi.fn(),
      onSubmit: vi.fn().mockResolvedValue(successResult)
    };
    const { rerender } = render(<TopicMutationDialog {...props} open config={config} />);

    const readQueues = screen.getByRole('spinbutton', { name: 'Read queue count' });
    await user.clear(readQueues);
    await user.type(readQueues, '12');
    await user.click(screen.getByRole('checkbox', { name: 'broker-b' }));

    rerender(
      <TopicMutationDialog
        {...props}
        open
        config={{ ...config, inconsistentFields: ['perm'] }}
      />
    );
    expect(readQueues).toHaveValue(12);
    expect(screen.getByRole('checkbox', { name: 'broker-b' })).toBeChecked();
    expect(screen.getByText('Broker configurations disagree: perm')).toBeInTheDocument();

    rerender(<TopicMutationDialog {...props} open={false} config={{ ...config, inconsistentFields: ['perm'] }} />);
    rerender(<TopicMutationDialog {...props} open config={{ ...config, inconsistentFields: ['perm'] }} />);
    expect(screen.getByRole('spinbutton', { name: 'Read queue count' })).toHaveValue(8);
    expect(screen.getByRole('checkbox', { name: 'broker-b' })).not.toBeChecked();
  });

  it('accepts a pending partial result after a same-topic config refresh', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const props = {
      open: true,
      mode: 'edit' as const,
      targets,
      onOpenChange: vi.fn(),
      onSubmit: vi.fn().mockReturnValue(pending.promise)
    };
    const { rerender } = render(<TopicMutationDialog {...props} config={config} />);

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    rerender(<TopicMutationDialog {...props} config={{ ...config, inconsistentFields: ['perm'] }} />);
    await act(async () => pending.resolve(partialResult));

    expect(await screen.findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(props.onOpenChange).not.toHaveBeenCalledWith(false);
  });

  it('accepts a pending success after a same-topic config refresh', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const onOpenChange = vi.fn();
    const props = {
      open: true,
      mode: 'edit' as const,
      targets,
      onOpenChange,
      onSubmit: vi.fn().mockReturnValue(pending.promise)
    };
    const { rerender } = render(<TopicMutationDialog {...props} config={config} />);

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    rerender(<TopicMutationDialog {...props} config={{ ...config, inconsistentFields: ['perm'] }} />);
    await act(async () => pending.resolve(successResult));

    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('accepts a pending rejection after a same-topic config refresh', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicOperationResult>();
    const props = {
      open: true,
      mode: 'edit' as const,
      targets,
      onOpenChange: vi.fn(),
      onSubmit: vi.fn().mockReturnValue(pending.promise)
    };
    const { rerender } = render(<TopicMutationDialog {...props} config={config} />);

    const confirmation = await openConfirmation(user);
    await user.click(within(confirmation).getByRole('button', { name: 'Save changes' }));
    rerender(<TopicMutationDialog {...props} config={{ ...config, inconsistentFields: ['perm'] }} />);
    await act(async () => pending.reject(new Error('same-topic request failed')));

    expect(await screen.findByRole('alert')).toHaveTextContent('same-topic request failed');
    expect(screen.getByRole('textbox', { name: 'Topic name' })).toHaveValue('orders');
  });
});
