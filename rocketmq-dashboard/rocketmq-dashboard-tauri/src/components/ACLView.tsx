import { useCallback, useEffect, useState, useSyncExternalStore } from 'react';
import { Info, RefreshCw } from 'lucide-react';
import { ClusterService } from '../services/cluster.service';
import { ConnectionStore } from '../services/connection.store';
import { getConnectionSettings } from '../services/connection.service';
import { dashboardErrorMessage } from '../services/invoke';
import { useNavigationState } from '../stores/app.store';
import { usePageRefresh } from '../app/layout/pageToolbar';
import type { AclContext } from '../features/acl/aclActions';
import { Button } from './ui/LegacyButton';
import { PageState } from './layout/PageState';
import { aclScopeKey, type AclScope } from '../features/acl/types';
import { useAclRead } from '../features/acl/hooks/useAclRead';
import { useAclActions } from '../features/acl/aclActionContext';
import { AclWorkspace } from '../features/acl/components/AclWorkspace';
import { AclReceiptPanel } from '../features/acl/components/AclReceiptPanel';
import '../features/acl/acl.css';

export const ACLView = () => {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const [error, setError] = useState('');
    const [attempt, setAttempt] = useState(0);
    useEffect(() => {
        let active = true;
        if (!settings) {
            setError('');
            void getConnectionSettings().catch(error => { if (active) setError(dashboardErrorMessage(error, 'Connection settings could not be read.')); });
        }
        return () => { active = false; };
    }, [settings, attempt]);
    if (!settings) return <PageState kind={error ? 'error' : 'loading'} title={error ? 'Connection settings could not be read' : 'Reading connection settings'} description={error}
        action={error ? <Button variant="outline" onClick={() => setAttempt(value => value + 1)}>Retry connection</Button> : undefined} />;
    return <AclPage key={settings.revision} context={settings} />;
};

function AclPage({ context }: { context: AclContext }) {
    const catalogue = useAclRead(async () => (await ClusterService.getClusterHomePage({ forceRefresh: true })).items.filter(broker => broker.brokerId === 0).map(broker => ({ clusterName: broker.clusterName, brokerName: broker.brokerName, brokerAddr: broker.address })), context, 'Unable to discover ACL Brokers.');
    const [cluster, setCluster] = useNavigationState('aclCluster', '');
    const [scopeKey, setScopeKey] = useNavigationState('aclBroker', '');
    const { receipt } = useAclActions();
    const scopes: AclScope[] = [...new Map((catalogue.data ?? []).map(scope => [aclScopeKey(scope), scope])).values()];
    const clusters = [...new Set(scopes.map(scope => scope.clusterName))].sort();
    const scope = scopes.find(scope => scope.clusterName === cluster && aclScopeKey(scope) === scopeKey) ?? null;
    return <div className="ops-acl">
        <section className="ops-acl-scope" aria-label="ACL Broker scope">
            <label className="ops-acl-select">Cluster<select disabled={!catalogue.ready} value={cluster} onChange={event => { setCluster(event.target.value); setScopeKey(''); }}><option value="">Choose a Cluster</option>{clusters.map(name => <option key={name}>{name}</option>)}</select></label>
            <label className="ops-acl-select">Master Broker<select disabled={!catalogue.ready || !cluster} value={scope?.clusterName === cluster ? scopeKey : ''} onChange={event => setScopeKey(event.target.value)}>
                <option value="">Choose a master Broker</option>{scopes.filter(scope => scope.clusterName === cluster).map(scope => <option key={aclScopeKey(scope)} value={aclScopeKey(scope)}>{scope.brokerName} · {scope.brokerAddr}</option>)}
            </select></label>
            {scope && <Button variant="outline" icon={RefreshCw} disabled={catalogue.loading} onClick={() => { void catalogue.refresh(); }}>Refresh Brokers</Button>}
        </section>
        <p className="ops-acl-account-note"><Info aria-hidden="true" />Broker ACL users are separate from your local Dashboard account.</p>
        {catalogue.loading && <PageState kind="loading" title="Discovering master Brokers" />}
        {catalogue.error && <PageState kind="error" title="ACL Broker discovery failed" description={catalogue.error} />}
        {catalogue.ready && !scopes.length && <PageState kind="empty" title="No master Brokers are available" description="Connect to a RocketMQ-Rust environment and refresh the Broker catalogue." />}
        {scope ? <AclWorkspace key={aclScopeKey(scope)} scope={scope} context={context} scopeReady={catalogue.ready} scopePending={catalogue.loading} refreshScope={catalogue.refresh} /> : <>
            <AclCatalogueToolbar refresh={catalogue.refresh} pending={catalogue.loading} observedAt={catalogue.observedAt} />
            {catalogue.ready && scopes.length > 0 && <PageState kind="empty" title="Choose a Broker scope" description="Select a Cluster and master Broker to load its users and policies." />}
        </>}
        {receipt && <AclReceiptPanel receipt={receipt} />}
    </div>;
}

function AclCatalogueToolbar({ refresh, pending, observedAt }: { refresh: () => Promise<boolean>; pending: boolean; observedAt: number | null }) {
    const read = useCallback(() => { void refresh(); }, [refresh]);
    usePageRefresh({ refresh: read, pending, refreshedAt: observedAt });
    return null;
}
