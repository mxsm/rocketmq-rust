import { useCallback, useEffect, useRef, useState } from 'react';
import { ArrowRight, Plus } from 'lucide-react';
import { AclService } from '../../../services/acl.service';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../../components/ui/tabs';
import { PageState } from '../../../components/layout/PageState';
import { usePageRefresh } from '../../../app/layout/pageToolbar';
import { useAclActions } from '../aclActionContext';
import { aclContextMatches, type AclContext } from '../aclActions';
import { aclScopeKey, type AclScope } from '../types';
import { useAclRead } from '../hooks/useAclRead';
import { AclUsers } from './AclUsers';
import { AclPolicies, AclPolicyResources } from './AclPolicies';

export function AclWorkspace({ scope, context, scopeReady, scopePending, refreshScope }: { scope: AclScope; context: AclContext; scopeReady: boolean; scopePending: boolean; refreshScope: () => Promise<boolean> }) {
    const users = useAclRead(() => AclService.listUsers(scope), context, 'Unable to read ACL users.');
    const policies = useAclRead(() => AclService.listPolicies(scope), context, 'Unable to read ACL policies.');
    const { open, receipt } = useAclActions();
    const [tab, setTab] = useState('users');
    const [query, setQuery] = useState('');
    const [selectedName, setSelectedName] = useState('');
    const policiesTab = useRef<HTMLButtonElement>(null);
    const selected = users.data?.find(user => user.username === selectedName) ?? null;
    const refresh = useCallback(() => {
        if (!scopeReady) { void refreshScope(); return; }
        void users.refresh(); void policies.refresh();
    }, [scopeReady, refreshScope, users.refresh, policies.refresh]);
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    useEffect(() => {
        if (scopeReady && users.ready && policies.ready) setRefreshedAt(Math.min(users.observedAt!, policies.observedAt!));
    }, [scopeReady, users.ready, policies.ready, users.observedAt, policies.observedAt]);
    usePageRefresh({ refresh, pending: scopePending || users.loading || policies.loading, refreshedAt });
    const currentScopeKey = aclScopeKey(scope);
    useEffect(() => {
        if (receipt && aclContextMatches(context, receipt.context) && currentScopeKey === aclScopeKey(receipt.target.scope)) {
            void users.refreshAfterWrite(); void policies.refreshAfterWrite();
        }
    }, [receipt, currentScopeKey, context, users.refreshAfterWrite, policies.refreshAfterWrite]);
    const search = query.trim().toLowerCase();
    const shownUsers = (users.data ?? []).filter(user => user.username.toLowerCase().includes(search));
    const shownPolicies = (policies.data ?? []).filter(policy => [policy.subject, policy.policyType, ...policy.entries.map(entry => entry.resource)].some(value => value?.toLowerCase().includes(search)));
    const visibleUser = selected && shownUsers.some(user => user.username === selected.username) ? selected : null;
    const subject = visibleUser ? `User:${visibleUser.username}` : null;
    const related = (policies.data ?? []).filter(policy => policy.subject === subject);
    const directory = tab === 'users' ? users : policies;
    return <>
        <section className="ops-acl-panel"><Tabs value={tab} onValueChange={value => { setTab(value); setQuery(''); }}>
            <div className="ops-acl-toolbar"><TabsList aria-label="ACL sections"><TabsTrigger value="users">Users</TabsTrigger><TabsTrigger value="policies" ref={policiesTab}>Policies</TabsTrigger></TabsList>
                <Button icon={Plus} disabled={!scopeReady || !directory.ready} onClick={() => open(tab === 'users' ? { kind: 'user_create' } : { kind: 'policy_create' }, scope, context)}>Create {tab === 'users' ? 'user' : 'policy'}</Button></div>
            <div className="ops-acl-filter"><Input aria-label={tab === 'users' ? 'Search ACL users' : 'Search ACL policies'} placeholder={tab === 'users' ? 'Search username' : 'Search subject, type or resource'} value={query} onChange={event => setQuery(event.target.value)} />
                <p className="ops-acl-note">{directory.data?.length ?? '—'} {tab} · {directory.observedAt ? `Observed ${new Date(directory.observedAt).toLocaleTimeString()}` : 'No observation'}</p></div>
            {directory.loading && <PageState kind="loading" title={`Reading ACL ${tab}`} description="Actions become available after the current directory is loaded." />}
            {directory.error && <PageState kind="error" title={`ACL ${tab} could not be refreshed`} description={directory.error} action={<Button variant="outline" disabled={!scopeReady} onClick={() => { void directory.refresh(); }}>Retry {tab}</Button>} />}
            <TabsContent value="users"><AclUsers scope={scope} context={context} users={shownUsers} selected={visibleUser} onSelect={user => setSelectedName(user.username)} disabled={!scopeReady || !users.ready} showEmpty={users.ready} /></TabsContent>
            <TabsContent value="policies"><AclPolicies scope={scope} context={context} policies={shownPolicies} disabled={!scopeReady || !policies.ready} showEmpty={policies.ready} /></TabsContent>
        </Tabs></section>
        {tab === 'users' && <section className="ops-acl-panel"><header className="ops-section-header"><div><h2>Resource policies</h2><p>{visibleUser ? `Resource access policies for ${visibleUser.username}.` : 'Select a user to inspect related resource policies.'}</p></div>
            <Button variant="ghost" icon={ArrowRight} onClick={() => { setTab('policies'); setQuery(subject ?? ''); policiesTab.current?.focus(); }}>Manage policies</Button></header>
            {policies.loading && <PageState kind="loading" title="Reading resource policies" />}
            {policies.error && <PageState kind="error" title="Resource policies could not be refreshed" description={policies.error} action={<Button variant="outline" disabled={!scopeReady} onClick={() => { void policies.refresh(); }}>Retry policies</Button>} />}
            {visibleUser && policies.data && <AclPolicyResources scope={scope} context={context} policies={related} disabled={!scopeReady || !users.ready || !policies.ready} />}
        </section>}
    </>;
}
