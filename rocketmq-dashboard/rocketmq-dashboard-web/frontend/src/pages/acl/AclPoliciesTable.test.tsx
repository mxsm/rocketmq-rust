import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { renderAtRoute } from '../../test/render';
import AclPoliciesTable from './AclPoliciesTable';

const rows = Array.from({ length: 11 }, (_, index) => ({
  key: `policy-${index + 1}`, entryIndex: 0, subjectEntries: [{ resource: `Topic:${index + 1}`, actions: ['Pub'], sourceIps: [], decision: 'Allow' }], brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: `User:${index + 1}`,
  policyType: 'Custom', resource: `Topic:${index + 1}`, actions: ['Pub'], sourceIps: [], decision: 'Allow'
}));

describe('AclPoliciesTable', () => {
  it('uses the shared loading state rather than retaining policy rows', () => {
    renderAtRoute(<AclPoliciesTable rows={rows} total={rows.length} page={1} pageSize={10} loading disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByRole('status', { name: 'Loading acl policies' })).toBeInTheDocument();
    expect(screen.queryByText('User:1')).not.toBeInTheDocument();
  });

  it('receives page-sized policy rows and renders shared pagination', () => {
    renderAtRoute(<AclPoliciesTable rows={rows.slice(0, 10)} total={rows.length} page={1} pageSize={10} loading={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByText('User:10')).toBeInTheDocument();
    expect(screen.queryByText('User:11')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeInTheDocument();
  });

  it('gives row actions subject and resource-specific accessible names', () => {
    renderAtRoute(<AclPoliciesTable rows={[rows[0]]} total={1} page={1} pageSize={10} loading={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={vi.fn()} />, '/acl');
    expect(screen.getByRole('button', { name: 'Modify ACL policy User:1 on Topic:1' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Delete ACL policy User:1 on Topic:1' })).toBeInTheDocument();
  });

  it('allows deletion only for Custom rows when policy targets are otherwise identical', async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn();
    const defaultRow = { ...rows[0], key: 'default-policy', policyType: 'Default' };
    const customRow = { ...rows[0], key: 'custom-policy' };
    renderAtRoute(<AclPoliciesTable rows={[defaultRow, customRow]} total={2} page={1} pageSize={10} loading={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={onDelete} />, '/acl');

    const defaultDelete = screen.getByRole('button', { name: 'Delete ACL policy User:1 on Topic:1 unavailable: only Custom policies can be deleted' });
    expect(defaultDelete).toBeDisabled();
    expect(defaultDelete).toHaveAttribute('title', 'Only Custom ACL policies can be deleted.');
    await user.click(defaultDelete);
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(onDelete).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Delete ACL policy User:1 on Topic:1' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('User:1');
    expect(confirmation).toHaveTextContent('Topic:1');
    await user.click(within(confirmation).getByRole('button', { name: 'Delete' }));
    expect(onDelete).toHaveBeenCalledOnce();
    expect(onDelete).toHaveBeenCalledWith(customRow);
  });

  it('names the policy subject in destructive confirmation before deleting', async () => {
    const user = userEvent.setup();
    const onDelete = vi.fn();
    renderAtRoute(<AclPoliciesTable rows={[rows[0]]} total={1} page={1} pageSize={10} loading={false} disabled={false} onPageChange={vi.fn()} onEdit={vi.fn()} onDelete={onDelete} />, '/acl');

    await user.click(screen.getByRole('button', { name: 'Delete ACL policy User:1 on Topic:1' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('User:1');
    await user.click(within(confirmation).getByRole('button', { name: 'Delete' }));
    expect(onDelete).toHaveBeenCalledWith(rows[0]);
  });
});
