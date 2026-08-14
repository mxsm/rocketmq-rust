import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import ConfirmDialog from './ConfirmDialog';
import EmptyState from './EmptyState';
import ErrorState from './ErrorState';
import LoadingState from './LoadingState';
import MetricCard from './MetricCard';
import PageHeader from './PageHeader';
import StatusBadge from './StatusBadge';

describe('shared operational states', () => {
  it('renders page context and actions with one page heading', () => {
    render(<PageHeader title="Topics" description="Manage topic routes" actions={<button type="button">Create topic</button>} />);

    expect(screen.getByRole('heading', { level: 1, name: 'Topics' })).toBeInTheDocument();
    expect(screen.getByText('Manage topic routes')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Create topic' })).toBeInTheDocument();
  });

  it('announces loading and error states and supports retry', async () => {
    const user = userEvent.setup();
    const retry = vi.fn();
    const { rerender } = render(<LoadingState label="Loading brokers" />);

    expect(screen.getByRole('status', { name: 'Loading brokers' })).toBeInTheDocument();
    rerender(<ErrorState message="Broker request failed" onRetry={retry} />);
    expect(screen.getByRole('alert')).toHaveTextContent('Broker request failed');
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(retry).toHaveBeenCalledOnce();
  });

  it('renders empty, metric and status information with semantic names', () => {
    render(
      <>
        <EmptyState title="No topics" detail="Adjust the filters" />
        <MetricCard label="Brokers" value={3} detail="2 master, 1 replica" />
        <StatusBadge status="Healthy" tone="success" />
      </>
    );

    expect(screen.getByText('No topics')).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Brokers: 3' })).toHaveTextContent('2 master, 1 replica');
    expect(screen.getByRole('status', { name: 'Healthy' })).toHaveAttribute('data-tone', 'success');
  });

  it('runs a dangerous action only after explicit confirmation', async () => {
    const user = userEvent.setup();
    const confirm = vi.fn();
    render(
      <ConfirmDialog title="Delete orders" description="This cannot be undone." confirmLabel="Delete" onConfirm={confirm}>
        <button type="button">Delete topic</button>
      </ConfirmDialog>
    );

    await user.click(screen.getByRole('button', { name: 'Delete topic' }));
    expect(confirm).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Delete' }));
    expect(confirm).toHaveBeenCalledOnce();
  });
});
