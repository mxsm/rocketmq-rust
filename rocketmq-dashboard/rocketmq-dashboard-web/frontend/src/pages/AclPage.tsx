import * as Dialog from '@radix-ui/react-dialog';
import {
  CheckCircle2,
  Edit3,
  Eye,
  EyeOff,
  FileKey2,
  KeyRound,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Trash2,
  UserPlus,
  X
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { aclApi } from '../api/acl_api';
import { brokerApi } from '../api/broker_api';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SelectMenu, { type SelectMenuOption } from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { AclMutationResult, AclPolicyRequest, AclPolicyView, AclQueryParams, AclUserUpsertRequest, AclUserView } from '../types/acl';
import type { BrokerInfo } from '../types/broker';

type AclTab = 'users' | 'permissions';
type NoticeTone = 'success' | 'warning' | 'danger';
type UserDialogState = { mode: 'create' } | { mode: 'edit'; user: AclUserView };
type PolicyDialogState = { mode: 'create' } | { mode: 'edit'; policy: AclPolicyTableRow };

interface AclPolicyTableRow {
  key: string;
  brokerName: string;
  brokerAddr: string;
  subject: string;
  policyType: string;
  resource: string;
  actions: string[];
  sourceIps: string[];
  decision: string;
}

const pageSize = 10;
const actionOptions = ['All', 'Pub', 'Sub', 'Create', 'Update', 'Delete', 'Get', 'List'];

export default function AclPage() {
  const [brokers, setBrokers] = useState<BrokerInfo[]>([]);
  const [selectedCluster, setSelectedCluster] = useState('');
  const [selectedBroker, setSelectedBroker] = useState('');
  const [confirmedCluster, setConfirmedCluster] = useState('');
  const [confirmedBroker, setConfirmedBroker] = useState('');
  const [activeTab, setActiveTab] = useState<AclTab>('users');
  const [search, setSearch] = useState('');
  const [users, setUsers] = useState<AclUserView[]>([]);
  const [policies, setPolicies] = useState<AclPolicyView[]>([]);
  const [loading, setLoading] = useState(true);
  const [mutating, setMutating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);
  const [userDialog, setUserDialog] = useState<UserDialogState | null>(null);
  const [policyDialog, setPolicyDialog] = useState<PolicyDialogState | null>(null);
  const [usersPage, setUsersPage] = useState(1);
  const [policiesPage, setPoliciesPage] = useState(1);
  const [showPasswords, setShowPasswords] = useState(false);

  const clusters = useMemo(() => Array.from(new Set(brokers.map((broker) => broker.clusterName))).filter(Boolean).sort(), [brokers]);
  const clusterOptions = useMemo<SelectMenuOption[]>(
    () => clusters.map((cluster) => ({ value: cluster, label: cluster })),
    [clusters]
  );
  const brokerOptions = useMemo<SelectMenuOption[]>(() => {
    const scoped = selectedCluster ? brokers.filter((broker) => broker.clusterName === selectedCluster) : brokers;
    return scoped
      .map((broker) => ({ value: broker.brokerName, label: broker.brokerName }))
      .filter((option, index, list) => list.findIndex((item) => item.value === option.value) === index)
      .sort((left, right) => left.label.localeCompare(right.label));
  }, [brokers, selectedCluster]);

  const selectedScope = useMemo(() => {
    const broker = brokers.find((item) => item.brokerName === confirmedBroker);
    return {
      cluster: confirmedCluster || 'No cluster selected',
      broker: confirmedBroker || 'No broker selected',
      address: broker?.address ?? '-'
    };
  }, [brokers, confirmedBroker, confirmedCluster]);

  const policyRows = useMemo(() => flattenPolicies(policies), [policies]);
  const normalizedSearch = search.trim().toLowerCase();
  const visibleUsers = useMemo(
    () => filterRows(users, normalizedSearch),
    [normalizedSearch, users]
  );
  const visiblePolicies = useMemo(
    () => filterRows(policyRows, normalizedSearch),
    [normalizedSearch, policyRows]
  );
  const visibleUserPage = pageRows(visibleUsers, usersPage);
  const visiblePolicyPage = pageRows(visiblePolicies, policiesPage);
  const userPageCount = pageCount(visibleUsers.length);
  const policyPageCount = pageCount(visiblePolicies.length);

  const loadAclData = (scope: { clusterName?: string; brokerName?: string } = {}) => {
    const clusterName = scope.clusterName ?? confirmedCluster;
    const brokerName = scope.brokerName ?? confirmedBroker;
    const query: AclQueryParams = {
      clusterName,
      brokerName,
      filter: search,
      searchParam: search
    };

    setLoading(true);
    setError(null);
    Promise.all([aclApi.listUsers(query), aclApi.listPolicies(query)])
      .then(([nextUsers, nextPolicies]) => {
        setUsers(nextUsers);
        setPolicies(nextPolicies);
        setUsersPage(1);
        setPoliciesPage(1);
        setNotice({
          tone: 'success',
          message: `ACL query completed. ${nextUsers.length} user(s), ${flattenPolicies(nextPolicies).length} permission row(s).`
        });
      })
      .catch((requestError: Error) => {
        setUsers([]);
        setPolicies([]);
        setError(requestError.message);
      })
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    let disposed = false;
    setLoading(true);
    brokerApi
      .list()
      .then((data) => {
        if (disposed) return;
        const nextBrokers = data.items;
        const firstBroker = nextBrokers[0];
        setBrokers(nextBrokers);
        if (!firstBroker) {
          setNotice({ tone: 'warning', message: 'No broker is available for ACL management.' });
          setLoading(false);
          return;
        }
        setSelectedCluster(firstBroker.clusterName);
        setSelectedBroker(firstBroker.brokerName);
        setConfirmedCluster(firstBroker.clusterName);
        setConfirmedBroker(firstBroker.brokerName);
        loadAclData({ clusterName: firstBroker.clusterName, brokerName: firstBroker.brokerName });
      })
      .catch((requestError: Error) => {
        if (!disposed) {
          setError(requestError.message);
          setLoading(false);
        }
      });

    return () => {
      disposed = true;
    };
  }, []);

  useEffect(() => {
    setUsersPage(1);
    setPoliciesPage(1);
  }, [search, activeTab]);

  const confirmScope = () => {
    setConfirmedCluster(selectedCluster);
    setConfirmedBroker(selectedBroker);
    loadAclData({ clusterName: selectedCluster, brokerName: selectedBroker });
  };

  const saveUser = (payload: AclUserUpsertRequest, mode: UserDialogState['mode'], username?: string) => {
    setMutating(true);
    setError(null);
    const request = mode === 'edit' && username ? aclApi.updateUser(username, payload) : aclApi.createUser(payload);
    request
      .then((result) => {
        setUserDialog(null);
        setNotice({ tone: 'success', message: mutationMessage(result) });
        loadAclData();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setMutating(false));
  };

  const deleteUser = (username: string) => {
    setMutating(true);
    setError(null);
    aclApi
      .deleteUser(username, scopeQuery(confirmedCluster, confirmedBroker))
      .then((result) => {
        setNotice({ tone: 'success', message: mutationMessage(result) });
        loadAclData();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setMutating(false));
  };

  const savePolicy = (payload: AclPolicyRequest, mode: PolicyDialogState['mode'], subject?: string) => {
    setMutating(true);
    setError(null);
    const request = mode === 'edit' && subject ? aclApi.updatePolicy(subject, payload) : aclApi.createPolicy(payload);
    request
      .then((result) => {
        setPolicyDialog(null);
        setNotice({ tone: 'success', message: mutationMessage(result) });
        loadAclData();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setMutating(false));
  };

  const deletePolicy = (row: AclPolicyTableRow) => {
    setMutating(true);
    setError(null);
    aclApi
      .deletePolicy(row.subject, { ...scopeQuery(confirmedCluster, confirmedBroker), resource: row.resource })
      .then((result) => {
        setNotice({ tone: 'success', message: mutationMessage(result) });
        loadAclData();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setMutating(false));
  };

  if (loading && brokers.length === 0) {
    return <LoadingState label="Loading ACL scope" />;
  }

  if (error && brokers.length === 0) {
    return <ErrorState message={error} onRetry={() => loadAclData()} />;
  }

  return (
    <>
      <PageHeader
        title="ACL Management"
        description="Java-compatible ACL users and permissions scoped by cluster and broker."
        actions={
          <>
            <button type="button" className="button button-secondary" onClick={() => loadAclData()} disabled={loading || mutating}>
              <RefreshCw size={15} aria-hidden="true" /> Refresh
            </button>
          </>
        }
      />

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}
      {error ? <div className="notice notice-danger">{error}</div> : null}

      <section className="acl-selector-panel">
        <div className="acl-selector-grid">
          <label className="acl-field">
            <span>
              <strong>*</strong> Cluster
            </span>
            <SelectMenu value={selectedCluster} options={clusterOptions} onChange={setSelectedCluster} ariaLabel="Select ACL cluster" className="acl-select-menu" />
          </label>
          <label className="acl-field">
            <span>
              <strong>*</strong> Broker
            </span>
            <SelectMenu value={selectedBroker} options={brokerOptions} onChange={setSelectedBroker} ariaLabel="Select ACL broker" className="acl-select-menu" />
          </label>
          <button type="button" className="button acl-confirm-button" onClick={confirmScope} disabled={!selectedCluster || !selectedBroker || loading || mutating}>
            <CheckCircle2 size={15} aria-hidden="true" /> Confirm
          </button>
        </div>
        <div className="acl-scope-card">
          <ShieldCheck size={18} aria-hidden="true" />
          <div>
            <span>Active broker scope</span>
            <strong>{selectedScope.broker}</strong>
            <small>
              {selectedScope.cluster} / {selectedScope.address}
            </small>
          </div>
        </div>
      </section>

      <section className="acl-stat-grid">
        <AclStatCard icon={<UserPlus size={18} aria-hidden="true" />} label="ACL users" value={users.length} hint="broker-side user credentials" />
        <AclStatCard icon={<FileKey2 size={18} aria-hidden="true" />} label="Permission rows" value={policyRows.length} hint="flattened Java policy entries" />
        <AclStatCard icon={<KeyRound size={18} aria-hidden="true" />} label="Broker targets" value={confirmedBroker ? 1 : 0} hint="selected ACL write target" />
      </section>

      <section className="acl-workspace">
        <div className="acl-tabs">
          <button type="button" className={activeTab === 'users' ? 'active' : ''} onClick={() => setActiveTab('users')}>
            <UserPlus size={15} aria-hidden="true" /> ACL Users
          </button>
          <button type="button" className={activeTab === 'permissions' ? 'active' : ''} onClick={() => setActiveTab('permissions')}>
            <FileKey2 size={15} aria-hidden="true" /> ACL Permissions
          </button>
        </div>

        <div className="acl-table-panel">
          <div className="acl-table-header">
            <div>
              <h2>{activeTab === 'users' ? 'ACL Users' : 'ACL Permissions'}</h2>
              <p>
                {activeTab === 'users'
                  ? 'Username, masked password, user type, status, and Java-style update/delete operations.'
                  : 'Subject policy entries flattened to resource rows, matching the Java ACL table.'}
              </p>
            </div>
            <div className="acl-table-actions">
              <label className="acl-search-box">
                <Search size={15} aria-hidden="true" />
                <input
                  value={search}
                  placeholder={activeTab === 'users' ? 'Search users' : 'Search subject, resource, action, source IP'}
                  onChange={(event) => setSearch(event.target.value)}
                />
              </label>
              {activeTab === 'users' ? (
                <>
                  <button type="button" className="button button-secondary" onClick={() => setShowPasswords((value) => !value)}>
                    {showPasswords ? <EyeOff size={15} aria-hidden="true" /> : <Eye size={15} aria-hidden="true" />}
                    {showPasswords ? 'Hide' : 'View'}
                  </button>
                  <button type="button" className="button" onClick={() => setUserDialog({ mode: 'create' })}>
                    <Plus size={15} aria-hidden="true" /> Add User
                  </button>
                </>
              ) : (
                <button type="button" className="button" onClick={() => setPolicyDialog({ mode: 'create' })}>
                  <Plus size={15} aria-hidden="true" /> Add ACL Permission
                </button>
              )}
            </div>
          </div>

          {activeTab === 'users' ? (
            <AclUsersTable
              rows={visibleUserPage}
              total={visibleUsers.length}
              page={Math.min(usersPage, userPageCount)}
              pageCount={userPageCount}
              showPasswords={showPasswords}
              loading={loading}
              onPageChange={setUsersPage}
              onEdit={(user) => setUserDialog({ mode: 'edit', user })}
              onDelete={deleteUser}
            />
          ) : (
            <AclPoliciesTable
              rows={visiblePolicyPage}
              total={visiblePolicies.length}
              page={Math.min(policiesPage, policyPageCount)}
              pageCount={policyPageCount}
              loading={loading}
              onPageChange={setPoliciesPage}
              onEdit={(policy) => setPolicyDialog({ mode: 'edit', policy })}
              onDelete={deletePolicy}
            />
          )}
        </div>
      </section>

      <UserAclDialog
        state={userDialog}
        clusterName={confirmedCluster}
        brokerName={confirmedBroker}
        saving={mutating}
        onClose={() => setUserDialog(null)}
        onSubmit={saveUser}
      />
      <PolicyAclDialog
        state={policyDialog}
        clusterName={confirmedCluster}
        brokerName={confirmedBroker}
        saving={mutating}
        onClose={() => setPolicyDialog(null)}
        onSubmit={savePolicy}
      />
    </>
  );
}

function AclUsersTable({
  rows,
  total,
  page,
  pageCount,
  showPasswords,
  loading,
  onPageChange,
  onEdit,
  onDelete
}: {
  rows: AclUserView[];
  total: number;
  page: number;
  pageCount: number;
  showPasswords: boolean;
  loading: boolean;
  onPageChange: (page: number) => void;
  onEdit: (user: AclUserView) => void;
  onDelete: (username: string) => void;
}) {
  return (
    <>
      <div className="acl-table-scroll">
        <table className="acl-table acl-users-table">
          <thead>
            <tr>
              <th>Username</th>
              <th>Password</th>
              <th>User Type</th>
              <th>User Status</th>
              <th>Broker</th>
              <th>Operation</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.brokerAddr}:${row.username}`}>
                <td>
                  <div className="acl-primary-cell">
                    <code>{row.username}</code>
                    <span>access key</span>
                  </div>
                </td>
                <td>
                  <code>{showPasswords ? row.password || '-' : maskPassword(row.password)}</code>
                </td>
                <td>
                  <StatusBadge status={normalizeUserType(row.userType)} tone={userTypeTone(row.userType)} />
                </td>
                <td>
                  <StatusBadge status={normalizeUserStatus(row.userStatus)} tone={userStatusTone(row.userStatus)} />
                </td>
                <td>
                  <div className="acl-primary-cell">
                    <span>{row.brokerName || '-'}</span>
                    <code>{row.brokerAddr || '-'}</code>
                  </div>
                </td>
                <td>
                  <div className="acl-operation-row">
                    <button type="button" className="button button-secondary acl-action-button" onClick={() => onEdit(row)}>
                      <Edit3 size={14} aria-hidden="true" /> Modify
                    </button>
                    <ConfirmDialog
                      title="Delete ACL user"
                      description={`Delete ACL user ${row.username} from the selected broker target?`}
                      confirmLabel="Delete"
                      onConfirm={() => onDelete(row.username)}
                    >
                      <button type="button" className="button button-danger acl-action-button">
                        <Trash2 size={14} aria-hidden="true" /> Delete
                      </button>
                    </ConfirmDialog>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!loading && rows.length === 0 ? <EmptyState title="No ACL users" /> : null}
      <AclTableFooter total={total} page={page} pageCount={pageCount} onPageChange={onPageChange} />
    </>
  );
}

function AclPoliciesTable({
  rows,
  total,
  page,
  pageCount,
  loading,
  onPageChange,
  onEdit,
  onDelete
}: {
  rows: AclPolicyTableRow[];
  total: number;
  page: number;
  pageCount: number;
  loading: boolean;
  onPageChange: (page: number) => void;
  onEdit: (policy: AclPolicyTableRow) => void;
  onDelete: (policy: AclPolicyTableRow) => void;
}) {
  return (
    <>
      <div className="acl-table-scroll">
        <table className="acl-table acl-policy-table">
          <thead>
            <tr>
              <th>Username/Subject</th>
              <th>Policy Type</th>
              <th>Resource Name</th>
              <th>Operation Type</th>
              <th>Source IP</th>
              <th>Decision</th>
              <th>Operation</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.key}>
                <td>
                  <div className="acl-primary-cell">
                    <code>{row.subject}</code>
                    <span>{row.brokerName || row.brokerAddr}</span>
                  </div>
                </td>
                <td>
                  <StatusBadge status={row.policyType || 'Custom'} />
                </td>
                <td>
                  <code>{row.resource}</code>
                </td>
                <td>
                  <div className="acl-chip-row">
                    {row.actions.length > 0 ? row.actions.map((action) => <span key={action}>{action}</span>) : <span>None</span>}
                  </div>
                </td>
                <td>
                  <code>{row.sourceIps.join(', ') || '-'}</code>
                </td>
                <td>
                  <StatusBadge status={row.decision || '-'} tone={row.decision === 'Deny' ? 'danger' : 'success'} />
                </td>
                <td>
                  <div className="acl-operation-row">
                    <button type="button" className="button button-secondary acl-action-button" onClick={() => onEdit(row)}>
                      <Edit3 size={14} aria-hidden="true" /> Modify
                    </button>
                    <ConfirmDialog
                      title="Delete ACL permission"
                      description={`Delete ACL permission for ${row.subject} on resource ${row.resource}?`}
                      confirmLabel="Delete"
                      onConfirm={() => onDelete(row)}
                    >
                      <button type="button" className="button button-danger acl-action-button">
                        <Trash2 size={14} aria-hidden="true" /> Delete
                      </button>
                    </ConfirmDialog>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!loading && rows.length === 0 ? <EmptyState title="No ACL permissions" /> : null}
      <AclTableFooter total={total} page={page} pageCount={pageCount} onPageChange={onPageChange} />
    </>
  );
}

function UserAclDialog({
  state,
  clusterName,
  brokerName,
  saving,
  onClose,
  onSubmit
}: {
  state: UserDialogState | null;
  clusterName: string;
  brokerName: string;
  saving: boolean;
  onClose: () => void;
  onSubmit: (payload: AclUserUpsertRequest, mode: UserDialogState['mode'], username?: string) => void;
}) {
  const editingUser = state?.mode === 'edit' ? state.user : null;
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [userType, setUserType] = useState('Normal');
  const [userStatus, setUserStatus] = useState('enable');

  useEffect(() => {
    setUsername(editingUser?.username ?? '');
    setPassword(editingUser?.password ?? '');
    setUserType(normalizeUserType(editingUser?.userType));
    setUserStatus(normalizeUserStatusValue(editingUser?.userStatus));
  }, [editingUser, state]);

  const canSubmit = username.trim() && password.trim() && userType.trim() && userStatus.trim();
  const mode = state?.mode ?? 'create';

  return (
    <Dialog.Root open={state !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content acl-dialog">
          <div className="drawer-header">
            <div>
              <Dialog.Title>{editingUser ? 'Edit User' : 'Add User'}</Dialog.Title>
              <Dialog.Description className="dialog-description">
                Java ACL user request: username, password, userType, and userStatus on the selected broker.
              </Dialog.Description>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>
          <div className="acl-dialog-form">
            <label className="acl-field">
              <span>
                <strong>*</strong> Username
              </span>
              <input value={username} disabled={mode === 'edit'} onChange={(event) => setUsername(event.target.value)} />
            </label>
            <label className="acl-field">
              <span>
                <strong>*</strong> Password
              </span>
              <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} />
            </label>
            <div className="acl-dialog-grid">
              <label className="acl-field">
                <span>
                  <strong>*</strong> User Type
                </span>
                <SelectMenu
                  value={userType}
                  options={[
                    { value: 'Super', label: 'Super' },
                    { value: 'Normal', label: 'Normal' }
                  ]}
                  onChange={setUserType}
                  ariaLabel="Select ACL user type"
                  className="acl-select-menu"
                />
              </label>
              <label className="acl-field">
                <span>
                  <strong>*</strong> User Status
                </span>
                <SelectMenu
                  value={userStatus}
                  options={[
                    { value: 'enable', label: 'enable' },
                    { value: 'disable', label: 'disable' }
                  ]}
                  onChange={setUserStatus}
                  ariaLabel="Select ACL user status"
                  className="acl-select-menu"
                />
              </label>
            </div>
          </div>
          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
            <button
              type="button"
              className="button"
              disabled={!canSubmit || saving}
              onClick={() =>
                onSubmit(
                  {
                    brokerName,
                    clusterName,
                    username,
                    password,
                    userType,
                    userStatus
                  },
                  mode,
                  editingUser?.username
                )
              }
            >
              {saving ? 'Saving...' : 'Confirm'}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function PolicyAclDialog({
  state,
  clusterName,
  brokerName,
  saving,
  onClose,
  onSubmit
}: {
  state: PolicyDialogState | null;
  clusterName: string;
  brokerName: string;
  saving: boolean;
  onClose: () => void;
  onSubmit: (payload: AclPolicyRequest, mode: PolicyDialogState['mode'], subject?: string) => void;
}) {
  const editingPolicy = state?.mode === 'edit' ? state.policy : null;
  const [subject, setSubject] = useState('');
  const [policyType, setPolicyType] = useState('Custom');
  const [resource, setResource] = useState('');
  const [actions, setActions] = useState<string[]>(['Pub', 'Sub']);
  const [sourceIps, setSourceIps] = useState('');
  const [decision, setDecision] = useState('Allow');

  useEffect(() => {
    setSubject(editingPolicy?.subject ?? '');
    setPolicyType(editingPolicy?.policyType || 'Custom');
    setResource(editingPolicy?.resource ?? '');
    setActions(editingPolicy?.actions?.length ? editingPolicy.actions : ['Pub', 'Sub']);
    setSourceIps(editingPolicy?.sourceIps?.join(', ') ?? '');
    setDecision(editingPolicy?.decision || 'Allow');
  }, [editingPolicy, state]);

  const mode = state?.mode ?? 'create';
  const resources = splitCsv(resource);
  const ipValues = splitCsv(sourceIps);
  const canSubmit = subject.trim() && policyType.trim() && resources.length > 0 && actions.length > 0 && decision.trim();

  const toggleAction = (action: string) => {
    setActions((value) => (value.includes(action) ? value.filter((item) => item !== action) : [...value, action]));
  };

  return (
    <Dialog.Root open={state !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content acl-dialog acl-policy-dialog">
          <div className="drawer-header">
            <div>
              <Dialog.Title>{editingPolicy ? 'Edit ACL Permission' : 'Add ACL Permission'}</Dialog.Title>
              <Dialog.Description className="dialog-description">
                Java ACL policy request: subject, policyType, resource, actions, sourceIps, and decision.
              </Dialog.Description>
            </div>
            <Dialog.Close className="icon-button" title="Close">
              <X size={15} aria-hidden="true" />
            </Dialog.Close>
          </div>
          <div className="acl-dialog-form">
            <label className="acl-field">
              <span>
                <strong>*</strong> Subject
              </span>
              <input value={subject} disabled={mode === 'edit'} placeholder="User:rocketmq_app" onChange={(event) => setSubject(event.target.value)} />
            </label>
            <div className="acl-dialog-grid">
              <label className="acl-field">
                <span>
                  <strong>*</strong> Policy Type
                </span>
                <SelectMenu
                  value={policyType}
                  options={[
                    { value: 'Custom', label: 'Custom' },
                    { value: 'Default', label: 'Default' }
                  ]}
                  onChange={setPolicyType}
                  ariaLabel="Select ACL policy type"
                  className="acl-select-menu"
                />
              </label>
              <label className="acl-field">
                <span>
                  <strong>*</strong> Decision
                </span>
                <SelectMenu
                  value={decision}
                  options={[
                    { value: 'Allow', label: 'Allow' },
                    { value: 'Deny', label: 'Deny' }
                  ]}
                  onChange={setDecision}
                  ariaLabel="Select ACL decision"
                  className="acl-select-menu"
                />
              </label>
            </div>
            <label className="acl-field">
              <span>
                <strong>*</strong> Resource
              </span>
              <input
                value={resource}
                disabled={mode === 'edit'}
                placeholder="Topic:TopicTest1111, Group:please_rename_unique_group_name"
                onChange={(event) => setResource(event.target.value)}
              />
              <small>Multiple resources are comma-separated, matching Java ResourceInput behavior.</small>
            </label>
            <div className="acl-field">
              <span>Operation Type</span>
              <div className="acl-action-toggle-grid">
                {actionOptions.map((action) => (
                  <button
                    type="button"
                    className={actions.includes(action) ? 'active' : ''}
                    key={action}
                    onClick={() => toggleAction(action)}
                  >
                    {action}
                  </button>
                ))}
              </div>
            </div>
            <label className="acl-field">
              <span>Source IP</span>
              <input value={sourceIps} placeholder="127.0.0.1, 192.168.1.1" onChange={(event) => setSourceIps(event.target.value)} />
              <small>Leave empty to use broker defaults. IPv4, IPv6, and CIDR values can be typed as tags in Java.</small>
            </label>
          </div>
          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
            <button
              type="button"
              className="button"
              disabled={!canSubmit || saving}
              onClick={() =>
                onSubmit(
                  {
                    brokerName,
                    clusterName,
                    subject,
                    policies: [
                      {
                        policyType,
                        entries: [
                          {
                            resource: resources,
                            actions,
                            sourceIps: ipValues,
                            decision
                          }
                        ]
                      }
                    ]
                  },
                  mode,
                  editingPolicy?.subject
                )
              }
            >
              {saving ? 'Saving...' : 'Confirm'}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function AclStatCard({ icon, label, value, hint }: { icon: ReactNode; label: string; value: number; hint: string }) {
  return (
    <div className="acl-stat-card">
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{hint}</small>
      </div>
      <div className="acl-stat-icon">{icon}</div>
    </div>
  );
}

function AclTableFooter({
  total,
  page,
  pageCount,
  onPageChange
}: {
  total: number;
  page: number;
  pageCount: number;
  onPageChange: (page: number) => void;
}) {
  return (
    <div className="acl-table-footer">
      <span>
        {total} rows / page {page} of {pageCount}
      </span>
      <div className="pagination">
        <button type="button" className="button button-secondary" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
          Previous
        </button>
        <button type="button" className="button button-secondary" disabled={page >= pageCount} onClick={() => onPageChange(page + 1)}>
          Next
        </button>
      </div>
    </div>
  );
}

function flattenPolicies(policies: AclPolicyView[]): AclPolicyTableRow[] {
  return policies.flatMap((policy, policyIndex) =>
    policy.entries.flatMap((entry, entryIndex) => {
      const resource = entry.resource || '*';
      return {
        key: `${policy.brokerAddr}:${policy.subject ?? ''}:${policy.policyType ?? ''}:${resource}:${policyIndex}:${entryIndex}`,
        brokerName: policy.brokerName,
        brokerAddr: policy.brokerAddr,
        subject: policy.subject ?? '-',
        policyType: policy.policyType ?? 'Custom',
        resource,
        actions: entry.actions ?? [],
        sourceIps: entry.sourceIps ?? [],
        decision: entry.decision ?? 'Allow'
      };
    })
  );
}

function pageRows<T>(rows: T[], page: number) {
  return rows.slice((Math.max(1, page) - 1) * pageSize, Math.max(1, page) * pageSize);
}

function pageCount(total: number) {
  return Math.max(1, Math.ceil(total / pageSize));
}

function filterRows<T>(rows: T[], query: string) {
  if (!query) {
    return rows;
  }
  return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(query));
}

function scopeQuery(clusterName: string, brokerName: string): AclQueryParams {
  return { clusterName, brokerName };
}

function splitCsv(value: string) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function mutationMessage(result: AclMutationResult) {
  return `${result.message || 'ACL mutation completed.'} Target count: ${result.targetCount}.`;
}

function maskPassword(value?: string) {
  if (!value) {
    return '********';
  }
  return '*'.repeat(Math.max(8, Math.min(16, value.length)));
}

function normalizeUserType(value?: string) {
  const normalized = (value || 'Normal').toLowerCase();
  return normalized === 'super' ? 'Super' : normalized === 'normal' ? 'Normal' : value || 'Normal';
}

function normalizeUserStatus(value?: string) {
  const normalized = normalizeUserStatusValue(value);
  return normalized === 'enable' ? 'enable' : 'disable';
}

function normalizeUserStatusValue(value?: string) {
  const normalized = (value || 'enable').toLowerCase();
  return normalized.includes('disabled') || normalized === 'disable' ? 'disable' : 'enable';
}

function userTypeTone(value?: string) {
  return normalizeUserType(value) === 'Super' ? 'warning' : 'neutral';
}

function userStatusTone(value?: string) {
  return normalizeUserStatus(value) === 'enable' ? 'success' : 'danger';
}
