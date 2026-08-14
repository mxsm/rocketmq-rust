import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import TopicMutationDialog from './TopicMutationDialog';

describe('TopicMutationDialog', () => {
  it('validates before confirmation and submits the exact topic payload only after approval', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    const onOpenChange = vi.fn();
    render(<TopicMutationDialog open onOpenChange={onOpenChange} onSubmit={onSubmit} />);

    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('status')).toHaveTextContent('Topic name cannot be empty.');
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(onSubmit).not.toHaveBeenCalled();

    await user.type(screen.getByRole('textbox', { name: 'Topic name' }), 'orders');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('status')).toHaveTextContent('Choose at least one cluster or broker target.');

    const readQueues = screen.getByRole('spinbutton', { name: 'Read queue count' });
    await user.clear(readQueues);
    await user.type(readQueues, '1.5');
    await user.type(screen.getByRole('textbox', { name: 'Cluster names' }), 'DefaultCluster, BackupCluster');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('status')).toHaveTextContent('Queue counts must be positive 32-bit integers.');

    await user.clear(readQueues);
    await user.type(readQueues, '8');
    const permission = screen.getByRole('spinbutton', { name: 'Permission' });
    await user.clear(permission);
    await user.type(permission, '6.5');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    expect(screen.getByRole('status')).toHaveTextContent('Permission must be an integer between 0 and 7.');

    await user.clear(permission);
    await user.type(permission, '6');
    await user.type(screen.getByRole('textbox', { name: 'Broker names' }), 'broker-a');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Message type' }), 'FIFO');
    await user.click(screen.getByRole('button', { name: 'Save topic' }));

    expect(await screen.findByRole('alertdialog', { name: 'Create topic?' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onSubmit).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Save topic' }));
    await user.click(await screen.findByRole('button', { name: 'Create topic' }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith({
      topic: 'orders',
      readQueueCount: 8,
      writeQueueCount: 8,
      perm: 6,
      brokerNameList: ['broker-a'],
      clusterNameList: ['DefaultCluster', 'BackupCluster'],
      order: false,
      messageType: 'FIFO'
    }));
  });
});
