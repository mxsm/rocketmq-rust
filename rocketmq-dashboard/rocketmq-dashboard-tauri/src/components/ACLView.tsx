import { useEffect, useState } from 'react';
import { ClusterService } from '../services/cluster.service';
import { dashboardErrorMessage } from '../services/invoke';
import { AclUsers } from '../features/acl/components/AclUsers';
import { aclScopeKey, type AclScope } from '../features/acl/types';

export const ACLView = () => {
    const [scopes, setScopes] = useState<AclScope[]>([]);
    const [selected, setSelected] = useState('');
    const [scope, setScope] = useState<AclScope | null>(null);
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(true);
    useEffect(() => {
        let cancelled = false;
        void ClusterService.getClusterHomePage({ forceRefresh: true }).then(result => {
            if (!cancelled) setScopes(result.items.filter(broker => broker.brokerId === 0).map(broker => ({ clusterName: broker.clusterName, brokerName: broker.brokerName, brokerAddr: broker.address })));
        }).catch(error => { if (!cancelled) setError(dashboardErrorMessage(error, 'Unable to discover ACL Brokers.')); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, []);
    return <div className="space-y-6 text-gray-900 dark:text-gray-100">
        <section className="space-y-4 rounded-xl border bg-white p-6 dark:border-gray-800 dark:bg-gray-900">
            <h1 className="text-xl font-semibold">Broker access control</h1><p className="text-sm text-gray-500">Manage RocketMQ Broker ACL identities. Dashboard login accounts are managed separately in Account settings.</p>
            {error && <p role="alert" className="text-red-600">{error}</p>}
            <div className="flex flex-wrap gap-3"><select aria-label="ACL Broker scope" disabled={loading} className="min-w-64 rounded border bg-transparent p-2" value={selected}
                onChange={event => { setSelected(event.target.value); setScope(null); }}>
                <option value="">{loading ? 'Loading Brokers…' : 'Select a master Broker'}</option>
                {scopes.map(scope => <option key={aclScopeKey(scope)} value={aclScopeKey(scope)}>{scope.clusterName} / {scope.brokerName} · {scope.brokerAddr}</option>)}
            </select><button disabled={!selected} onClick={() => setScope(scopes.find(scope => aclScopeKey(scope) === selected) ?? null)} className="rounded bg-blue-600 px-4 py-2 text-white disabled:opacity-50">Confirm scope</button></div>
            {!loading && !error && scopes.length === 0 && <p>No master Brokers are available in the current environment.</p>}
        </section>
        <section className="rounded-xl border bg-white p-6 dark:border-gray-800 dark:bg-gray-900">
            {scope ? <AclUsers key={aclScopeKey(scope)} scope={scope} /> : <p>Select and confirm a Broker scope to load ACL users.</p>}
        </section>
    </div>;
};
