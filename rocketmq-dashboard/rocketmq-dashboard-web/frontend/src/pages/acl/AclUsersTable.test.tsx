import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { renderAtRoute } from '../../test/render';
import AclUsersTable from './AclUsersTable';

const rows = Array.from({ length: 11 }, (_, index) => ({
  brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: `user-${index + 1}`, password: `secret-${index + 1}`
}));

describe('AclUsersTable', () => {
  it('renders a shared loading state instead of stale rows', () => {
    renderAtRoute(<AclUsersTable rows={rows} total={rows.length} page={1} pageSize={10} loading showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByRole('status', { name: 'Loading acl users' })).toBeInTheDocument();
    expect(screen.queryByText('user-1')).not.toBeInTheDocument();
  });

  it('renders one page at a time and exposes pagination controls', () => {
    renderAtRoute(<AclUsersTable rows={rows.slice(0, 10)} total={rows.length} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByText('user-10')).toBeInTheDocument();
    expect(screen.queryByText('user-11')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeInTheDocument();
  });

  it('preserves an unknown API user type instead of rewriting it as Normal', () => {
    renderAtRoute(<AclUsersTable rows={[{ ...rows[0], userType: 'CustomOperator' }]} total={1} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByRole('status', { name: 'CustomOperator' })).toBeInTheDocument();
  });

  it('uses a fixed password mask that does not reveal secret length', () => {
    renderAtRoute(<AclUsersTable rows={[
      { ...rows[0], username: 'short-secret', password: 'x' },
      { ...rows[1], username: 'long-secret', password: 'a-very-long-secret-value' }
    ]} total={2} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');

    const shortRow = screen.getByText('short-secret').closest('tr');
    const longRow = screen.getByText('long-secret').closest('tr');
    expect(within(shortRow!).getByText('************')).toBeInTheDocument();
    expect(within(longRow!).getByText('************')).toBeInTheDocument();
  });

  it('keeps omitted backend passwords unavailable even when reveal state is requested', () => {
    const { rerender } = renderAtRoute(<AclUsersTable rows={[{ ...rows[0], password: undefined }]} total={1} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByText('Unavailable')).toBeInTheDocument();

    rerender(<AclUsersTable rows={[{ ...rows[0], password: undefined }]} total={1} page={1} pageSize={10} loading={false} showPasswords disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />);
    expect(screen.getByText('Unavailable')).toBeInTheDocument();
    expect(screen.queryByText('************')).not.toBeInTheDocument();
  });

  it('gives row actions target-specific accessible names', () => {
    renderAtRoute(<AclUsersTable rows={[rows[0]]} total={1} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByRole('button', { name: 'Modify ACL user user-1' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Delete ACL user user-1' })).toBeInTheDocument();
  });

  it('names the user in destructive confirmation and honors cancellation', async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn();
    renderAtRoute(<AclUsersTable rows={[rows[0]]} total={1} page={1} pageSize={10} loading={false} showPasswords={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={onDelete} />, '/acl');

    await user.click(screen.getByRole('button', { name: 'Delete ACL user user-1' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('user-1');
    await user.click(within(confirmation).getByRole('button', { name: 'Cancel' }));
    expect(onDelete).not.toHaveBeenCalled();
  });
});
