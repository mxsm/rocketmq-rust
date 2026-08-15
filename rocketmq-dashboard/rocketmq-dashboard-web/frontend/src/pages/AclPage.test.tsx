import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { StrictMode } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';
import { aclApi } from '../api/acl_api';
import { brokerApi } from '../api/broker_api';
import { renderAtRoute } from '../test/render';
import AclPage from './AclPage';

vi.mock('../api/acl_api', () => ({
  aclApi: {
    listUsers: vi.fn(),
    listPolicies: vi.fn(),
    createUser: vi.fn(),
    updateUser: vi.fn(),
    deleteUser: vi.fn(),
    createPolicy: vi.fn(),
    updatePolicy: vi.fn(),
    deletePolicy: vi.fn()
  }
}));

vi.mock('../api/broker_api', () => ({
  brokerApi: { list: vi.fn() }
}));

describe('AclPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{
        clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0,
        address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0
      }],
      total: 1
    });
    vi.mocked(aclApi.listUsers).mockResolvedValue([]);
    vi.mocked(aclApi.listPolicies).mockResolvedValue([]);
  });

  it('does not load ACL records until the operator confirms a valid broker scope', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AclPage />, '/acl');

    const confirm = await screen.findByRole('button', { name: 'Confirm' });
    expect(aclApi.listUsers).not.toHaveBeenCalled();
    expect(aclApi.listPolicies).not.toHaveBeenCalled();

    await user.click(confirm);
    await waitFor(() => expect(aclApi.listUsers).toHaveBeenCalledWith({
      clusterName: 'Cluster-A', brokerName: 'broker-a'
    }));
    expect(aclApi.listPolicies).toHaveBeenCalledWith({ clusterName: 'Cluster-A', brokerName: 'broker-a' });
  });

  it('uses operator-facing descriptions for users and policy records', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AclPage />, '/acl');

    expect(await screen.findByText('Manage ACL users and permissions for the selected cluster and broker.')).toBeInTheDocument();
    expect(await screen.findByText('Manage credentials, account status, and access changes for the selected broker.')).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    expect(screen.getByText('Review and manage subject permissions for resources in the selected broker.')).toBeInTheDocument();
  });

  it('finishes broker discovery when React Strict Mode probes the mount lifecycle', async () => {
    render(
      <StrictMode>
        <MemoryRouter initialEntries={['/acl']} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <AclPage />
        </MemoryRouter>
      </StrictMode>
    );

    expect(await screen.findByRole('button', { name: 'Confirm' })).toBeEnabled();
    expect(screen.queryByRole('status', { name: 'Loading ACL scope' })).not.toBeInTheDocument();
  });

  it('renders a scoped load error beside the table and retries the confirmed scope', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers)
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce([]);
    renderAtRoute(<AclPage />, '/acl');

    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    expect(await screen.findByText('Unable to load ACL users and policies for the confirmed scope.')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));

    await waitFor(() => expect(aclApi.listUsers).toHaveBeenCalledTimes(2));
    expect(screen.queryByText('Unable to load ACL users and policies for the confirmed scope.')).not.toBeInTheDocument();
  });

  it('clears stale scope A records before a deferred scope B load can make them actionable', async () => {
    const user = userEvent.setup();
    let resolveBUsers: (value: never[]) => void = () => undefined;
    let resolveBPolicies: (value: never[]) => void = () => undefined;
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [
        { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
        { clusterName: 'Cluster-B', brokerName: 'broker-b', brokerId: 0, address: '10.0.0.2:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 }
      ], total: 2
    });
    vi.mocked(aclApi.listUsers).mockImplementation((query) => query?.brokerName === 'broker-a'
      ? Promise.resolve([{ brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'scope-a-user', password: 'scope-a-secret' }])
      : new Promise((resolve) => { resolveBUsers = resolve; }));
    vi.mocked(aclApi.listPolicies).mockImplementation((query) => query?.brokerName === 'broker-a'
      ? Promise.resolve([])
      : new Promise((resolve) => { resolveBPolicies = resolve; }));
    renderAtRoute(<AclPage />, '/acl');

    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    expect(await screen.findByText('scope-a-user')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Select ACL cluster' }));
    await user.click(screen.getByRole('option', { name: 'Cluster-B' }));
    await user.click(screen.getByRole('button', { name: 'Select ACL broker' }));
    await user.click(screen.getByRole('option', { name: 'broker-b' }));
    await user.click(screen.getByRole('button', { name: 'Confirm' }));

    expect(screen.queryByText('scope-a-user')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Modify ACL/ })).not.toBeInTheDocument();
    expect(aclApi.deleteUser).not.toHaveBeenCalled();
    await act(async () => { resolveBUsers([]); resolveBPolicies([]); });
  });

  it('invalidates confirmed rows as soon as the operator changes the draft scope', async () => {
    const user = userEvent.setup();
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [
        { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
        { clusterName: 'Cluster-B', brokerName: 'broker-b', brokerId: 0, address: '10.0.0.2:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 }
      ],
      total: 2
    });
    vi.mocked(aclApi.listUsers).mockResolvedValue([
      { brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'scope-a-user', password: 'secret' }
    ]);
    renderAtRoute(<AclPage />, '/acl');

    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    expect(await screen.findByText('scope-a-user')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Select ACL cluster' }));
    await user.click(screen.getByRole('option', { name: 'Cluster-B' }));

    expect(screen.queryByText('scope-a-user')).not.toBeInTheDocument();
    expect(screen.getByText('No confirmed scope')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Add User' })).toBeDisabled();
  });

  it('locks duplicate delete submissions until the in-flight mutation settles', async () => {
    const user = userEvent.setup();
    let resolveDelete: () => void = () => undefined;
    vi.mocked(aclApi.listUsers).mockResolvedValue([{ brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'locked-user', password: 'secret' }]);
    vi.mocked(aclApi.deleteUser).mockReturnValue(new Promise((resolve) => { resolveDelete = () => resolve({ message: 'deleted', targetCount: 1 }); }));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('locked-user');
    await user.click(screen.getByRole('button', { name: 'Delete ACL user locked-user' }));
    const confirmation = screen.getByRole('alertdialog');
    const confirmDelete = within(confirmation).getByRole('button', { name: 'Delete' });
    act(() => { fireEvent.click(confirmDelete); fireEvent.click(confirmDelete); });
    await waitFor(() => expect(aclApi.deleteUser).toHaveBeenCalledTimes(1));
    expect(screen.getByRole('button', { name: 'Modify ACL user locked-user' })).toBeDisabled();
    await act(async () => { resolveDelete(); });
  });

  it('clears stale delete feedback when changing tabs or opening a new dialog', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'error-user', password: 'secret'
    }]);
    vi.mocked(aclApi.deleteUser).mockRejectedValue(new Error('failed'));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('error-user');

    await user.click(screen.getByRole('button', { name: 'Delete ACL user error-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to delete the ACL user.');
    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    await user.click(screen.getByRole('tab', { name: 'ACL Users' }));
    expect(screen.queryByText('Unable to delete the ACL user.')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Delete ACL user error-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to delete the ACL user.');
    await user.click(screen.getByRole('button', { name: 'Add User' }));
    expect(screen.queryByText('Unable to delete the ACL user.')).not.toBeInTheDocument();
  });

  it('clears delete feedback when the draft scope changes', async () => {
    const user = userEvent.setup();
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [
        { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
        { clusterName: 'Cluster-B', brokerName: 'broker-b', brokerId: 0, address: '10.0.0.2:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 }
      ], total: 2
    });
    vi.mocked(aclApi.listUsers).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'scope-error-user', password: 'secret'
    }]);
    vi.mocked(aclApi.deleteUser).mockRejectedValue(new Error('failed'));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('scope-error-user');
    await user.click(screen.getByRole('button', { name: 'Delete ACL user scope-error-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to delete the ACL user.');

    await user.click(screen.getByRole('button', { name: 'Select ACL cluster' }));
    await user.click(screen.getByRole('option', { name: 'Cluster-B' }));
    expect(screen.queryByText('Unable to delete the ACL user.')).not.toBeInTheDocument();
  });

  it('clears failed delete feedback when a subsequent delete succeeds', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'retry-delete-user', password: 'secret'
    }]);
    vi.mocked(aclApi.deleteUser)
      .mockRejectedValueOnce(new Error('failed'))
      .mockResolvedValueOnce({ message: 'deleted', targetCount: 1 });
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('retry-delete-user');
    await user.click(screen.getByRole('button', { name: 'Delete ACL user retry-delete-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to delete the ACL user.');

    await user.click(screen.getByRole('button', { name: 'Delete ACL user retry-delete-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByText('ACL user deleted.')).toBeInTheDocument();
    expect(screen.queryByText('Unable to delete the ACL user.')).not.toBeInTheDocument();
  });

  it('clears stale policy delete feedback when opening a policy dialog', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listPolicies).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:policy-error', policyType: 'Custom',
      entries: [{ resource: 'Topic:Orders', actions: ['Pub'], sourceIps: [], decision: 'Allow' }]
    }]);
    vi.mocked(aclApi.deletePolicy).mockRejectedValue(new Error('failed'));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    await user.click(await screen.findByRole('button', { name: 'Delete ACL policy User:policy-error on Topic:Orders' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to delete the ACL policy.');

    await user.click(screen.getByRole('button', { name: 'Add ACL Policy' }));
    expect(screen.queryByText('Unable to delete the ACL policy.')).not.toBeInTheDocument();
  });

  it('masks passwords by default, resets reveal state after tab changes, and does not expose passwords in failures', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([{ brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'secret-user', password: 'do-not-leak' }]);
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('secret-user');
    expect(screen.queryByText('do-not-leak')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Reveal passwords' }));
    expect(screen.getByText('do-not-leak')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Hide passwords' }));
    expect(screen.queryByText('do-not-leak')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Reveal passwords' }));
    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    await user.click(screen.getByRole('tab', { name: 'ACL Users' }));
    expect(screen.queryByText('do-not-leak')).not.toBeInTheDocument();
  });

  it('does not advertise password reveal when list responses omit password values', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([
      { brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'production-user' }
    ]);
    renderAtRoute(<AclPage />, '/acl');

    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    const row = (await screen.findByText('production-user')).closest('tr');
    expect(within(row!).getByText('Unavailable')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Reveal passwords' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Hide passwords' })).not.toBeInTheDocument();
  });

  it('keeps create input and the safe save error inside the user dialog so the operator can retry', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.createUser)
      .mockRejectedValueOnce(new Error('super-secret-password'))
      .mockResolvedValueOnce({ message: 'created', targetCount: 1 });
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await user.click(screen.getByRole('button', { name: 'Add User' }));
    const dialog = screen.getByRole('dialog', { name: 'Add User' });
    await user.type(within(dialog).getByLabelText('* Username'), 'new-user');
    await user.type(within(dialog).getByLabelText('* Password'), 'super-secret-password');
    await user.click(within(dialog).getByRole('button', { name: 'Confirm' }));
    const error = await within(dialog).findByRole('alert');
    expect(error).toHaveTextContent('Unable to save the ACL user. Verify the request and try again.');
    expect(error).not.toHaveTextContent('super-secret-password');
    expect(within(dialog).getByLabelText('* Username')).toHaveValue('new-user');
    expect(within(dialog).getByLabelText('* Password')).toHaveValue('super-secret-password');

    await user.click(within(dialog).getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(aclApi.createUser).toHaveBeenCalledTimes(2));
    expect(await screen.findByText('ACL user created.')).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Add User' })).not.toBeInTheDocument();
  });

  it('keeps a failed policy edit open with its values and retries the same update', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listPolicies).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:payments', policyType: 'Custom',
      entries: [{ resource: 'Topic:Orders', actions: ['Pub'], sourceIps: ['10.0.0.1'], decision: 'Allow' }]
    }]);
    vi.mocked(aclApi.updatePolicy)
      .mockRejectedValueOnce(new Error('failed'))
      .mockResolvedValueOnce({ message: 'updated', targetCount: 1 });
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await user.click(await screen.findByRole('tab', { name: 'ACL Policies' }));
    await user.click(await screen.findByRole('button', { name: 'Modify ACL policy User:payments on Topic:Orders' }));

    const dialog = screen.getByRole('dialog', { name: 'Edit ACL Permission' });
    expect(within(dialog).getByPlaceholderText('Topic:TopicTest1111, Group:please_rename_unique_group_name')).toHaveValue('Topic:Orders');
    await user.click(within(dialog).getByRole('button', { name: 'Confirm' }));
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('Unable to save the ACL policy. Verify the request and try again.');
    expect(within(dialog).getByPlaceholderText('Topic:TopicTest1111, Group:please_rename_unique_group_name')).toHaveValue('Topic:Orders');

    await user.click(within(dialog).getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(aclApi.updatePolicy).toHaveBeenCalledTimes(2));
    expect(await screen.findByText('ACL policy updated.')).toBeInTheDocument();
  });

  it('replaces only the selected policy entry when editing a subject with multiple entries', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listPolicies).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:payments', policyType: 'Custom',
      entries: [
        { resource: 'Topic:Orders', actions: ['Pub'], sourceIps: ['10.0.0.1'], decision: 'Allow' },
        { resource: 'Group:billing', actions: ['Sub'], sourceIps: ['10.0.0.2'], decision: 'Deny' }
      ]
    }]);
    vi.mocked(aclApi.updatePolicy).mockResolvedValue({ message: 'updated', targetCount: 1 });
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await user.click(await screen.findByRole('tab', { name: 'ACL Policies' }));
    const ordersRow = (await screen.findByText('Topic:Orders')).closest('tr');
    expect(ordersRow).not.toBeNull();
    await user.click(within(ordersRow!).getByRole('button', { name: 'Modify ACL policy User:payments on Topic:Orders' }));
    const dialog = screen.getByRole('dialog', { name: 'Edit ACL Permission' });
    await user.click(within(dialog).getByRole('button', { name: 'Sub' }));
    await user.click(within(dialog).getByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(aclApi.updatePolicy).toHaveBeenCalledWith('User:payments', {
      brokerName: 'broker-a',
      clusterName: 'Cluster-A',
      subject: 'User:payments',
      policies: [{
        policyType: 'Custom',
        entries: [
          { resource: ['Topic:Orders'], actions: ['Pub', 'Sub'], sourceIps: ['10.0.0.1'], decision: 'Allow' },
          { resource: ['Group:billing'], actions: ['Sub'], sourceIps: ['10.0.0.2'], decision: 'Deny' }
        ]
      }]
    }));
  });

  it('ignores a create completion after its dialog is closed', async () => {
    const user = userEvent.setup();
    let resolveCreate: () => void = () => undefined;
    vi.mocked(aclApi.createUser).mockReturnValue(new Promise((resolve) => {
      resolveCreate = () => resolve({ message: 'created', targetCount: 1 });
    }));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await waitFor(() => expect(aclApi.listUsers).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('button', { name: 'Add User' }));
    const dialog = screen.getByRole('dialog', { name: 'Add User' });
    await user.type(within(dialog).getByLabelText('* Username'), 'late-user');
    await user.type(within(dialog).getByLabelText('* Password'), 'late-secret');
    await user.click(within(dialog).getByRole('button', { name: 'Confirm' }));
    await user.click(within(dialog).getByTitle('Close'));

    await act(async () => { resolveCreate(); });
    expect(screen.queryByText('ACL user created.')).not.toBeInTheDocument();
    expect(aclApi.listUsers).toHaveBeenCalledTimes(1);
  });

  it('isolates the policies tab from a save that completes after the tab transition', async () => {
    const user = userEvent.setup();
    let resolveCreate: () => void = () => undefined;
    vi.mocked(aclApi.createUser).mockReturnValue(new Promise((resolve) => {
      resolveCreate = () => resolve({ message: 'created', targetCount: 1 });
    }));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await waitFor(() => expect(aclApi.listUsers).toHaveBeenCalledTimes(1));
    const policiesTab = screen.getByRole('tab', { name: 'ACL Policies' });
    await user.click(screen.getByRole('button', { name: 'Add User' }));
    const dialog = screen.getByRole('dialog', { name: 'Add User' });
    await user.type(within(dialog).getByLabelText('* Username'), 'late-tab-user');
    await user.type(within(dialog).getByLabelText('* Password'), 'late-secret');
    await user.click(within(dialog).getByRole('button', { name: 'Confirm' }));
    fireEvent.mouseDown(policiesTab, { button: 0, ctrlKey: false });
    expect(policiesTab).toHaveAttribute('aria-selected', 'true');
    expect(screen.queryByRole('dialog', { name: 'Add User' })).not.toBeInTheDocument();

    await act(async () => { resolveCreate(); });
    expect(screen.queryByText('ACL user created.')).not.toBeInTheDocument();
    expect(policiesTab).toHaveAttribute('aria-selected', 'true');
    expect(screen.queryByRole('dialog', { name: 'Add User' })).not.toBeInTheDocument();
    expect(aclApi.listUsers).toHaveBeenCalledTimes(1);
    expect(aclApi.listPolicies).toHaveBeenCalledTimes(1);
  });

  it('invalidates an in-flight delete completion when the draft scope changes', async () => {
    const user = userEvent.setup();
    let resolveDelete: () => void = () => undefined;
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [
        { clusterName: 'Cluster-A', brokerName: 'broker-a', brokerId: 0, address: '10.0.0.1:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 },
        { clusterName: 'Cluster-B', brokerName: 'broker-b', brokerId: 0, address: '10.0.0.2:10911', role: 'MASTER', version: '5.3.0', produceTps: 0, consumeTps: 0 }
      ], total: 2
    });
    vi.mocked(aclApi.listUsers).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'deferred-delete-user', password: 'secret'
    }]);
    vi.mocked(aclApi.deleteUser).mockReturnValue(new Promise((resolve) => {
      resolveDelete = () => resolve({ message: 'deleted', targetCount: 1 });
    }));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('deferred-delete-user');
    await user.click(screen.getByRole('button', { name: 'Delete ACL user deferred-delete-user' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(aclApi.deleteUser).toHaveBeenCalledTimes(1));
    const clusterSelect = screen.getByRole('button', { name: 'Select ACL cluster' });
    expect(clusterSelect).toBeEnabled();
    await user.click(clusterSelect);
    await user.click(screen.getByRole('option', { name: 'Cluster-B' }));

    await act(async () => { resolveDelete(); });
    expect(screen.queryByText('ACL user deleted.')).not.toBeInTheDocument();
    expect(screen.getByText('No confirmed scope')).toBeInTheDocument();
    expect(aclApi.listUsers).toHaveBeenCalledTimes(1);
  });

  it('guards duplicate create and edit submissions while each mutation is in flight', async () => {
    const user = userEvent.setup();
    let resolveCreate: () => void = () => undefined;
    let resolveEdit: () => void = () => undefined;
    vi.mocked(aclApi.listPolicies).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:duplicate-edit', policyType: 'Custom',
      entries: [{ resource: 'Topic:Orders', actions: ['Pub'], sourceIps: [], decision: 'Allow' }]
    }]);
    vi.mocked(aclApi.createUser).mockReturnValue(new Promise((resolve) => {
      resolveCreate = () => resolve({ message: 'created', targetCount: 1 });
    }));
    vi.mocked(aclApi.updatePolicy).mockReturnValue(new Promise((resolve) => {
      resolveEdit = () => resolve({ message: 'updated', targetCount: 1 });
    }));
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await user.click(screen.getByRole('button', { name: 'Add User' }));
    const userDialog = screen.getByRole('dialog', { name: 'Add User' });
    await user.type(within(userDialog).getByLabelText('* Username'), 'one-create');
    await user.type(within(userDialog).getByLabelText('* Password'), 'secret');
    const createButton = within(userDialog).getByRole('button', { name: 'Confirm' });
    act(() => { fireEvent.click(createButton); fireEvent.click(createButton); });
    await waitFor(() => expect(aclApi.createUser).toHaveBeenCalledTimes(1));
    await act(async () => { resolveCreate(); });

    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    const policyRow = (await screen.findByText('Topic:Orders')).closest('tr');
    await user.click(within(policyRow!).getByRole('button', { name: 'Modify ACL policy User:duplicate-edit on Topic:Orders' }));
    const policyDialog = screen.getByRole('dialog', { name: 'Edit ACL Permission' });
    const editButton = within(policyDialog).getByRole('button', { name: 'Confirm' });
    act(() => { fireEvent.click(editButton); fireEvent.click(editButton); });
    await waitFor(() => expect(aclApi.updatePolicy).toHaveBeenCalledTimes(1));
    await act(async () => { resolveEdit(); });
  });

  it('keeps independent search queries and filters for the users and policies tabs', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue(Array.from({ length: 11 }, (_, index) => ({
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: `user-${index + 1}`, password: 'secret'
    })));
    vi.mocked(aclApi.listPolicies).mockResolvedValue([
      {
        brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:invoice-reader', policyType: 'Custom',
        entries: [{ resource: 'Topic:Invoices', actions: ['Sub'], sourceIps: [], decision: 'Allow' }]
      },
      {
        brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:orders-reader', policyType: 'Custom',
        entries: [{ resource: 'Topic:Orders', actions: ['Sub'], sourceIps: [], decision: 'Allow' }]
      }
    ]);
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    expect(await screen.findByText('user-10')).toBeInTheDocument();
    expect(screen.queryByText('user-11')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Next page' }));
    expect(await screen.findByText('user-11')).toBeInTheDocument();
    await user.type(screen.getByPlaceholderText('Search users'), 'user-2');
    expect(await screen.findByText('user-2')).toBeInTheDocument();
    expect(screen.getByLabelText('Page 1 of 1')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    const policySearch = screen.getByRole('textbox', { name: 'Search ACL policies' });
    expect(policySearch).toHaveValue('');
    expect(await screen.findByText('User:invoice-reader')).toBeInTheDocument();
    expect(screen.getByText('User:orders-reader')).toBeInTheDocument();
    await user.type(policySearch, 'Invoices');
    expect(screen.getByText('User:invoice-reader')).toBeInTheDocument();
    expect(screen.queryByText('User:orders-reader')).not.toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'ACL Users' }));
    expect(screen.getByRole('textbox', { name: 'Search ACL users' })).toHaveValue('user-2');
    expect(screen.getByText('user-2')).toBeInTheDocument();
    expect(screen.queryByText('user-1')).not.toBeInTheDocument();
  });

  it('exposes persistent search labels and pressed state for policy action toggles', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    expect(screen.getByRole('textbox', { name: 'Search ACL users' })).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'ACL Policies' }));
    expect(screen.getByRole('textbox', { name: 'Search ACL policies' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Add ACL Policy' }));
    const dialog = screen.getByRole('dialog', { name: 'Add ACL Permission' });
    expect(within(dialog).getByRole('button', { name: 'Pub' })).toHaveAttribute('aria-pressed', 'true');
    expect(within(dialog).getByRole('button', { name: 'Create' })).toHaveAttribute('aria-pressed', 'false');
    await user.click(within(dialog).getByRole('button', { name: 'Create' }));
    expect(within(dialog).getByRole('button', { name: 'Create' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('summarizes only records returned for the confirmed scope', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([
      { brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'enabled-user', userStatus: 'enable' },
      { brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'disabled-user', userStatus: 'disable' }
    ]);
    vi.mocked(aclApi.listPolicies).mockResolvedValue([{
      brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', subject: 'User:summary', policyType: 'Custom',
      entries: [
        { resource: 'Topic:A', actions: ['Pub'], sourceIps: [], decision: 'Allow' },
        { resource: 'Topic:B', actions: ['Sub'], sourceIps: [], decision: 'Allow' }
      ]
    }]);
    renderAtRoute(<AclPage />, '/acl');

    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    const summary = await screen.findByRole('region', { name: 'Confirmed ACL scope summary' });
    expect(within(summary).getByText('1', { selector: 'strong' })).toBeInTheDocument();
    expect(within(summary).getByText('Users')).toBeInTheDocument();
    expect(within(summary).getByText('Enabled')).toBeInTheDocument();
    expect(within(summary).getByText('Policy rules')).toBeInTheDocument();
    expect(within(summary).getAllByText('2', { selector: 'strong' })).toHaveLength(2);
  });

  it('returns password values to a masked state after the ACL page remounts', async () => {
    const user = userEvent.setup();
    vi.mocked(aclApi.listUsers).mockResolvedValue([{ brokerName: 'broker-a', brokerAddr: '10.0.0.1:10911', username: 'remount-user', password: 'remount-secret' }]);
    const rendered = renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('remount-user');
    await user.click(screen.getByRole('button', { name: 'Reveal passwords' }));
    expect(screen.getByText('remount-secret')).toBeInTheDocument();
    rendered.unmount();
    renderAtRoute(<AclPage />, '/acl');
    await user.click(await screen.findByRole('button', { name: 'Confirm' }));
    await screen.findByText('remount-user');
    expect(screen.queryByText('remount-secret')).not.toBeInTheDocument();
  });
});
