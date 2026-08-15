import { Eye, EyeOff, FileKey2, Plus, RefreshCw, Search, UserCheck, UserPlus, Users } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { aclApi } from '../api/acl_api';
import { brokerApi } from '../api/broker_api';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import { Button } from '../components/ui/Button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/Tabs';
import type { AclPolicyRequest, AclPolicyView, AclUserUpsertRequest, AclUserView } from '../types/acl';
import type { BrokerInfo } from '../types/broker';
import AclPoliciesTable from './acl/AclPoliciesTable';
import AclPolicyDialog from './acl/AclPolicyDialog';
import AclScopePicker from './acl/AclScopePicker';
import AclUserDialog from './acl/AclUserDialog';
import AclUsersTable from './acl/AclUsersTable';
import { createAclScopeQuery, filterAclPolicyRows, filterAclUsers, flattenAclPolicies, type AclPolicyRow, type AclScope } from './acl/acl-model';

type AclTab = 'users' | 'policies';
type Notice = { tone: 'success' | 'warning'; message: string };
const emptyScope: AclScope = { clusterName: '', brokerName: '' };
const pageSize = 10;

export default function AclPage() {
  const [brokers, setBrokers] = useState<BrokerInfo[]>([]);
  const [draftScope, setDraftScope] = useState<AclScope>(emptyScope);
  const [confirmedScope, setConfirmedScope] = useState<AclScope | null>(null);
  const [users, setUsers] = useState<AclUserView[]>([]);
  const [policies, setPolicies] = useState<AclPolicyView[]>([]);
  const [activeTab, setActiveTab] = useState<AclTab>('users');
  const [usersSearch, setUsersSearch] = useState('');
  const [policiesSearch, setPoliciesSearch] = useState('');
  const [loadingScope, setLoadingScope] = useState(true);
  const [loadingRecords, setLoadingRecords] = useState(false);
  const [mutating, setMutating] = useState(false);
  const [showPasswords, setShowPasswords] = useState(false);
  const [usersPage, setUsersPage] = useState(1);
  const [policiesPage, setPoliciesPage] = useState(1);
  const [scopeError, setScopeError] = useState<string | null>(null);
  const [recordsError, setRecordsError] = useState<string | null>(null);
  const [userSaveError, setUserSaveError] = useState<string | null>(null);
  const [policySaveError, setPolicySaveError] = useState<string | null>(null);
  const [userDeleteError, setUserDeleteError] = useState<string | null>(null);
  const [policyDeleteError, setPolicyDeleteError] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice | null>(null);
  const [userDialog, setUserDialog] = useState<AclUserView | null | undefined>(undefined);
  const [policyDialog, setPolicyDialog] = useState<AclPolicyRow | null | undefined>(undefined);
  const requestGeneration = useRef(0);
  const interactionGeneration = useRef(0);
  const mutationInFlight = useRef(false);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  useEffect(() => {
    void brokerApi.list()
      .then((response) => {
        if (!mounted.current) return;
        setBrokers(response.items);
        const firstBroker = response.items[0];
        if (firstBroker) setDraftScope({ clusterName: firstBroker.clusterName, brokerName: firstBroker.brokerName });
        else setNotice({ tone: 'warning', message: 'No broker is available for ACL management.' });
      })
      .catch(() => { if (mounted.current) setScopeError('Unable to load available ACL broker scopes.'); })
      .finally(() => { if (mounted.current) setLoadingScope(false); });
  }, []);

  const loadAclRecords = useCallback(async (scope: AclScope) => {
    const query = createAclScopeQuery(scope, brokers);
    if (!query) return;
    const request = ++requestGeneration.current;
    setLoadingRecords(true);
    setRecordsError(null);
    try {
      const [nextUsers, nextPolicies] = await Promise.all([aclApi.listUsers(query), aclApi.listPolicies(query)]);
      if (mounted.current && request === requestGeneration.current) {
        setUsers(nextUsers);
        setPolicies(nextPolicies);
        setUsersPage(1);
        setPoliciesPage(1);
      }
    } catch {
      if (mounted.current && request === requestGeneration.current) {
        setUsers([]);
        setPolicies([]);
        setRecordsError('Unable to load ACL users and policies for the confirmed scope.');
      }
    } finally {
      if (mounted.current && request === requestGeneration.current) setLoadingRecords(false);
    }
  }, [brokers]);

  const confirmScope = (scope: AclScope) => {
    if (!createAclScopeQuery(scope, brokers)) return;
    interactionGeneration.current += 1;
    setUsers([]);
    setPolicies([]);
    setUsersPage(1);
    setPoliciesPage(1);
    setUserDialog(undefined);
    setPolicyDialog(undefined);
    setShowPasswords(false);
    setConfirmedScope(scope);
    setNotice(null);
    setRecordsError(null);
    setUserSaveError(null);
    setPolicySaveError(null);
    setUserDeleteError(null);
    setPolicyDeleteError(null);
    void loadAclRecords(scope);
  };
  const refresh = () => { if (confirmedScope) void loadAclRecords(confirmedScope); };

  const changeDraftScope = (scope: AclScope) => {
    setDraftScope(scope);
    if (!confirmedScope || (scope.clusterName === confirmedScope.clusterName && scope.brokerName === confirmedScope.brokerName)) return;
    interactionGeneration.current += 1;
    requestGeneration.current += 1;
    setConfirmedScope(null);
    setUsers([]);
    setPolicies([]);
    setUsersPage(1);
    setPoliciesPage(1);
    setLoadingRecords(false);
    setShowPasswords(false);
    setUserDialog(undefined);
    setPolicyDialog(undefined);
    setNotice(null);
    setRecordsError(null);
    setUserSaveError(null);
    setPolicySaveError(null);
    setUserDeleteError(null);
    setPolicyDeleteError(null);
  };

  const changeTab = (tab: AclTab) => {
    if (tab === activeTab) return;
    interactionGeneration.current += 1;
    setActiveTab(tab);
    setShowPasswords(false);
    setUserDialog(undefined);
    setPolicyDialog(undefined);
    setNotice(null);
    setUserSaveError(null);
    setPolicySaveError(null);
    setUserDeleteError(null);
    setPolicyDeleteError(null);
  };

  const openUserDialog = (user: AclUserView | null) => {
    interactionGeneration.current += 1;
    setUserSaveError(null);
    setUserDeleteError(null);
    setPolicyDeleteError(null);
    setUserDialog(user);
  };
  const closeUserDialog = () => {
    interactionGeneration.current += 1;
    setUserSaveError(null);
    setUserDialog(undefined);
  };
  const openPolicyDialog = (policy: AclPolicyRow | null) => {
    interactionGeneration.current += 1;
    setPolicySaveError(null);
    setUserDeleteError(null);
    setPolicyDeleteError(null);
    setPolicyDialog(policy);
  };
  const closePolicyDialog = () => {
    interactionGeneration.current += 1;
    setPolicySaveError(null);
    setPolicyDialog(undefined);
  };

  const saveUser = async (request: AclUserUpsertRequest, username?: string) => {
    if (mutationInFlight.current) return;
    const scope = confirmedScope;
    if (!createAclScopeQuery(scope, brokers)) return;
    const interaction = interactionGeneration.current;
    mutationInFlight.current = true;
    setMutating(true);
    setUserSaveError(null);
    try {
      if (username) await aclApi.updateUser(username, request);
      else await aclApi.createUser(request);
      if (!mounted.current || interaction !== interactionGeneration.current || !scope) return;
      setUserDeleteError(null);
      setPolicyDeleteError(null);
      setUserDialog(undefined);
      setNotice({ tone: 'success', message: username ? 'ACL user updated.' : 'ACL user created.' });
      void loadAclRecords(scope);
    } catch {
      if (mounted.current && interaction === interactionGeneration.current) {
        setUserSaveError('Unable to save the ACL user. Verify the request and try again.');
      }
    } finally {
      mutationInFlight.current = false;
      if (mounted.current) setMutating(false);
    }
  };
  const deleteUser = async (username: string) => {
    if (mutationInFlight.current) return;
    const query = confirmedScope && createAclScopeQuery(confirmedScope, brokers);
    if (!query) return;
    const scope = confirmedScope;
    const interaction = interactionGeneration.current;
    mutationInFlight.current = true;
    setMutating(true);
    setUserDeleteError(null);
    try {
      await aclApi.deleteUser(username, query);
      if (!mounted.current || interaction !== interactionGeneration.current || !scope) return;
      setUserDeleteError(null);
      setPolicyDeleteError(null);
      setNotice({ tone: 'success', message: 'ACL user deleted.' });
      void loadAclRecords(scope);
    } catch {
      if (mounted.current && interaction === interactionGeneration.current) setUserDeleteError('Unable to delete the ACL user.');
    } finally {
      mutationInFlight.current = false;
      if (mounted.current) setMutating(false);
    }
  };
  const savePolicy = async (request: AclPolicyRequest, subject?: string) => {
    if (mutationInFlight.current) return;
    const scope = confirmedScope;
    if (!createAclScopeQuery(scope, brokers)) return;
    const interaction = interactionGeneration.current;
    mutationInFlight.current = true;
    setMutating(true);
    setPolicySaveError(null);
    try {
      if (subject) await aclApi.updatePolicy(subject, request);
      else await aclApi.createPolicy(request);
      if (!mounted.current || interaction !== interactionGeneration.current || !scope) return;
      setUserDeleteError(null);
      setPolicyDeleteError(null);
      setPolicyDialog(undefined);
      setNotice({ tone: 'success', message: subject ? 'ACL policy updated.' : 'ACL policy created.' });
      void loadAclRecords(scope);
    } catch {
      if (mounted.current && interaction === interactionGeneration.current) {
        setPolicySaveError('Unable to save the ACL policy. Verify the request and try again.');
      }
    } finally {
      mutationInFlight.current = false;
      if (mounted.current) setMutating(false);
    }
  };
  const deletePolicy = async (policy: AclPolicyRow) => {
    if (mutationInFlight.current) return;
    const query = confirmedScope && createAclScopeQuery(confirmedScope, brokers);
    if (!query) return;
    const scope = confirmedScope;
    const interaction = interactionGeneration.current;
    mutationInFlight.current = true;
    setMutating(true);
    setPolicyDeleteError(null);
    try {
      await aclApi.deletePolicy(policy.subject, { ...query, resource: policy.resource });
      if (!mounted.current || interaction !== interactionGeneration.current || !scope) return;
      setUserDeleteError(null);
      setPolicyDeleteError(null);
      setNotice({ tone: 'success', message: 'ACL policy deleted.' });
      void loadAclRecords(scope);
    } catch {
      if (mounted.current && interaction === interactionGeneration.current) setPolicyDeleteError('Unable to delete the ACL policy.');
    } finally {
      mutationInFlight.current = false;
      if (mounted.current) setMutating(false);
    }
  };

  const visibleUsers = useMemo(() => filterAclUsers(users, usersSearch), [users, usersSearch]);
  const policyRows = useMemo(() => flattenAclPolicies(policies), [policies]);
  const enabledUsers = useMemo(() => users.filter((user) => {
    const status = (user.userStatus || 'enable').toLowerCase();
    return status !== 'disable' && !status.includes('disabled');
  }).length, [users]);
  const visiblePolicies = useMemo(() => filterAclPolicyRows(policyRows, policiesSearch), [policyRows, policiesSearch]);
  const pagedUsers = useMemo(() => visibleUsers.slice((usersPage - 1) * pageSize, usersPage * pageSize), [usersPage, visibleUsers]);
  const pagedPolicies = useMemo(() => visiblePolicies.slice((policiesPage - 1) * pageSize, policiesPage * pageSize), [policiesPage, visiblePolicies]);
  const scopeReady = Boolean(confirmedScope && createAclScopeQuery(confirmedScope, brokers));
  const hasRevealablePasswords = users.some((user) => Boolean(user.password?.length));

  useEffect(() => {
    setUsersPage(1);
  }, [usersSearch]);
  useEffect(() => {
    setPoliciesPage(1);
  }, [policiesSearch]);
  if (loadingScope) return <LoadingState label="Loading ACL scope" />;
  if (scopeError && brokers.length === 0) return <ErrorState message={scopeError} onRetry={() => window.location.reload()} />;

  return (
    <>
      <PageHeader title="ACL Management" description="Manage ACL users and permissions for the selected cluster and broker." actions={
        <Button type="button" variant="secondary" onClick={refresh} disabled={!scopeReady || loadingRecords || mutating}><RefreshCw size={15} aria-hidden="true" /> Refresh</Button>
      } />
      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}
      <AclScopePicker brokers={brokers} draftScope={draftScope} confirmedScope={confirmedScope} disabled={loadingRecords} onDraftScopeChange={changeDraftScope} onConfirm={confirmScope} />
      {scopeReady && !loadingRecords && !recordsError ? (
        <section className="acl-stat-grid" aria-label="Confirmed ACL scope summary">
          <article className="acl-stat-card"><div><span>Users</span><strong>{users.length}</strong><small>Accounts in this broker scope</small></div><span className="acl-stat-icon"><Users size={19} aria-hidden="true" /></span></article>
          <article className="acl-stat-card"><div><span>Enabled</span><strong>{enabledUsers}</strong><small>Users currently enabled</small></div><span className="acl-stat-icon"><UserCheck size={19} aria-hidden="true" /></span></article>
          <article className="acl-stat-card"><div><span>Policy rules</span><strong>{policyRows.length}</strong><small>Flattened resource permissions</small></div><span className="acl-stat-icon"><FileKey2 size={19} aria-hidden="true" /></span></article>
        </section>
      ) : null}
      <Tabs value={activeTab} onValueChange={(value) => changeTab(value as AclTab)} className="acl-workspace">
        <TabsList className="acl-tabs" aria-label="ACL records">
          <TabsTrigger value="users"><UserPlus size={15} aria-hidden="true" /> ACL Users</TabsTrigger>
          <TabsTrigger value="policies"><FileKey2 size={15} aria-hidden="true" /> ACL Policies</TabsTrigger>
        </TabsList>
        <TabsContent value="users" className="acl-table-panel">
          <AclTableHeader title="ACL Users" description="Manage credentials, account status, and access changes for the selected broker." search={usersSearch} onSearchChange={setUsersSearch} searchLabel="Search ACL users" placeholder="Search users" actions={<>
            {hasRevealablePasswords ? <Button type="button" variant="secondary" aria-label={showPasswords ? 'Hide passwords' : 'Reveal passwords'} aria-pressed={showPasswords} onClick={() => setShowPasswords((value) => !value)} disabled={!scopeReady}>{showPasswords ? <EyeOff size={15} aria-hidden="true" /> : <Eye size={15} aria-hidden="true" />}{showPasswords ? 'Hide' : 'Reveal'}</Button> : null}
            <Button type="button" disabled={!scopeReady || mutating} onClick={() => openUserDialog(null)}><Plus size={15} aria-hidden="true" /> Add User</Button>
          </>} />
          {userDeleteError ? <div className="acl-action-error" role="alert">{userDeleteError}</div> : null}
          {scopeReady ? <AclUsersTable rows={pagedUsers} total={visibleUsers.length} page={usersPage} pageSize={pageSize} loading={loadingRecords} error={recordsError} onRetry={refresh} showPasswords={showPasswords} disabled={mutating} onPageChange={setUsersPage} onEdit={openUserDialog} onDelete={(username) => void deleteUser(username)} /> : <AclScopeEmptyState />}
        </TabsContent>
        <TabsContent value="policies" className="acl-table-panel">
          <AclTableHeader title="ACL Policies" description="Review and manage subject permissions for resources in the selected broker." search={policiesSearch} onSearchChange={setPoliciesSearch} searchLabel="Search ACL policies" placeholder="Search subject, resource, action, source IP" actions={<Button type="button" disabled={!scopeReady || mutating} onClick={() => openPolicyDialog(null)}><Plus size={15} aria-hidden="true" /> Add ACL Policy</Button>} />
          {policyDeleteError ? <div className="acl-action-error" role="alert">{policyDeleteError}</div> : null}
          {scopeReady ? <AclPoliciesTable rows={pagedPolicies} total={visiblePolicies.length} page={policiesPage} pageSize={pageSize} loading={loadingRecords} error={recordsError} onRetry={refresh} disabled={mutating} onPageChange={setPoliciesPage} onEdit={openPolicyDialog} onDelete={(policy) => void deletePolicy(policy)} /> : <AclScopeEmptyState />}
        </TabsContent>
      </Tabs>
      {confirmedScope ? <AclUserDialog open={userDialog !== undefined} user={userDialog} scope={confirmedScope} saving={mutating} error={userSaveError} onOpenChange={(open) => !open && closeUserDialog()} onSubmit={(request, username) => void saveUser(request, username)} /> : null}
      {confirmedScope ? <AclPolicyDialog open={policyDialog !== undefined} policy={policyDialog} scope={confirmedScope} saving={mutating} error={policySaveError} onOpenChange={(open) => !open && closePolicyDialog()} onSubmit={(request, subject) => void savePolicy(request, subject)} /> : null}
    </>
  );
}

function AclTableHeader({ title, description, search, onSearchChange, searchLabel, placeholder, actions }: { title: string; description: string; search: string; onSearchChange: (value: string) => void; searchLabel: string; placeholder: string; actions: ReactNode }) {
  return <div className="acl-table-header"><div><h2>{title}</h2><p>{description}</p></div><div className="acl-table-actions"><label className="acl-search-box"><span className="sr-only">{searchLabel}</span><Search size={15} aria-hidden="true" /><input value={search} placeholder={placeholder} onChange={(event) => onSearchChange(event.target.value)} /></label>{actions}</div></div>;
}

function AclScopeEmptyState() {
  return <div className="empty-state"><h2>Confirm an ACL scope</h2><p>Select and confirm a valid cluster and broker before loading ACL users or policies.</p></div>;
}
